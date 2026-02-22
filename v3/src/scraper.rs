use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use rusqlite::Connection;
use spider_client::shapes::request::{ReturnFormat, ReturnFormatHandling};
use spider_client::{RequestParams, Spider};
use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::db::ScrapeRow;

const CONCURRENCY: usize = 10;
const MAX_RETRIES: u32 = 3;
const BASE_BACKOFF_MS: u64 = 2000;

/// Scrape stats returned after completion.
pub struct ScrapeStats {
    pub total: usize,
    pub ok: usize,
    pub errors: usize,
}

/// Scrape pages concurrently, saving each result to DB as it arrives.
pub async fn scrape_pages_streaming(
    conn: &Connection,
    pages: Vec<(i64, String, String)>,
) -> Result<ScrapeStats> {
    let api_key =
        std::env::var("SPIDER_API_KEY").expect("SPIDER_API_KEY environment variable must be set");

    let spider = Arc::new(Spider::new(Some(api_key)).expect("Failed to create Spider client"));
    let semaphore = Arc::new(Semaphore::new(CONCURRENCY));
    let total = pages.len();

    let pb = ProgressBar::new(total as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40} {pos}/{len} ({per_sec}, eta {eta})")?
            .progress_chars("=> "),
    );

    // Channel: workers send results, main loop saves to DB
    let (tx, mut rx) = tokio::sync::mpsc::channel::<ScrapeRow>(CONCURRENCY * 2);

    // Spawn all scrape tasks
    for (page_id, url, slug) in pages {
        let spider = Arc::clone(&spider);
        let sem = Arc::clone(&semaphore);
        let tx = tx.clone();

        tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            match scrape_with_retry(&spider, page_id, &url, &slug).await {
                Ok(row) => { let _ = tx.send(row).await; }
                Err(e) => {
                    warn!("Task failed for {}: {}", slug, e);
                    // Send error row so we still mark as visited
                    let _ = tx.send(ScrapeRow {
                        page_id,
                        url,
                        slug,
                        markdown: None,
                        status: None,
                        error: Some(e.to_string()),
                        latency_ms: None,
                    }).await;
                }
            }
        });
    }

    // Drop our copy of tx so rx closes when all spawned tasks finish
    drop(tx);

    // Receive and save each result immediately
    let mut ok = 0usize;
    let mut errors = 0usize;

    // Prepare statements once, reuse for each row
    let mut insert_stmt = conn.prepare(
        "INSERT INTO page_data (page_id, url, slug, markdown, status, error, latency_ms)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
    )?;
    let mut update_stmt = conn.prepare(
        "UPDATE pages SET visited = 1, visited_at = datetime('now') WHERE id = ?1",
    )?;

    while let Some(row) = rx.recv().await {
        if row.error.is_some() {
            errors += 1;
        } else {
            ok += 1;
        }

        // Save immediately
        save_one(&mut insert_stmt, &mut update_stmt, &row)?;
        pb.inc(1);
    }

    pb.finish_and_clear();
    info!("Scraped {} pages ({} ok, {} errors)", total, ok, errors);

    Ok(ScrapeStats { total, ok, errors })
}

/// Save a single scrape result to DB using pre-prepared statements.
fn save_one(
    insert: &mut rusqlite::Statement,
    update: &mut rusqlite::Statement,
    row: &ScrapeRow,
) -> Result<()> {
    insert.execute(rusqlite::params![
        row.page_id, row.url, row.slug, row.markdown, row.status, row.error, row.latency_ms,
    ])?;
    update.execute(rusqlite::params![row.page_id])?;
    Ok(())
}

async fn scrape_with_retry(
    spider: &Spider,
    page_id: i64,
    url: &str,
    slug: &str,
) -> Result<ScrapeRow> {
    for attempt in 0..=MAX_RETRIES {
        let row = scrape_one(spider, page_id, url, slug).await?;

        let should_retry = match &row.error {
            Some(e) if e.contains("429") || e.contains("rate") => true,
            Some(e) if e.contains("500") || e.contains("502") || e.contains("503") => true,
            _ => false,
        };

        if !should_retry || attempt == MAX_RETRIES {
            return Ok(row);
        }

        let backoff = Duration::from_millis(BASE_BACKOFF_MS * 2u64.pow(attempt));
        warn!(
            "Rate limited on {} (attempt {}/{}), backing off {:.1}s",
            slug,
            attempt + 1,
            MAX_RETRIES,
            backoff.as_secs_f64()
        );
        tokio::time::sleep(backoff).await;
    }

    scrape_one(spider, page_id, url, slug).await
}

async fn scrape_one(spider: &Spider, page_id: i64, url: &str, slug: &str) -> Result<ScrapeRow> {
    let params = RequestParams {
        return_format: Some(ReturnFormatHandling::Single(ReturnFormat::Markdown)),
        ..Default::default()
    };

    let start = Instant::now();
    let response = spider
        .scrape_url(url, Some(params), "application/json")
        .await;
    let elapsed = start.elapsed().as_millis() as i64;

    match response {
        Ok(value) => {
            let parsed: serde_json::Value = match value.as_str() {
                Some(s) => serde_json::from_str(s).unwrap_or(value.clone()),
                None => value,
            };

            let first = parsed.as_array().and_then(|arr| arr.first());

            let content = first
                .and_then(|obj| obj.get("content"))
                .and_then(|c| c.as_str())
                .map(strip_images);

            let status = first
                .and_then(|obj| obj.get("status"))
                .and_then(|s| s.as_i64())
                .map(|s| s as i32);

            Ok(ScrapeRow {
                page_id,
                url: url.to_string(),
                slug: slug.to_string(),
                markdown: content,
                status,
                error: None,
                latency_ms: Some(elapsed),
            })
        }
        Err(e) => Ok(ScrapeRow {
            page_id,
            url: url.to_string(),
            slug: slug.to_string(),
            markdown: None,
            status: None,
            error: Some(e.to_string()),
            latency_ms: Some(elapsed),
        }),
    }
}

/// Scrape a single URL and return its markdown content.
pub async fn scrape_single_page(url: &str) -> Result<String> {
    let api_key = std::env::var("SPIDER_API_KEY")
        .map_err(|_| anyhow::anyhow!("SPIDER_API_KEY environment variable must be set"))?;
    let spider = Spider::new(Some(api_key))
        .map_err(|e| anyhow::anyhow!("Failed to create Spider client: {}", e))?;

    let params = RequestParams {
        return_format: Some(ReturnFormatHandling::Single(ReturnFormat::Markdown)),
        ..Default::default()
    };

    let response = spider
        .scrape_url(url, Some(params), "application/json")
        .await
        .map_err(|e| anyhow::anyhow!("Spider scrape failed: {}", e))?;

    let parsed: serde_json::Value = match response.as_str() {
        Some(s) => serde_json::from_str(s).unwrap_or(response.clone()),
        None => response,
    };

    let content = parsed
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|obj| obj.get("content"))
        .and_then(|c| c.as_str())
        .map(strip_images)
        .ok_or_else(|| anyhow::anyhow!("No content in spider response"))?;

    Ok(content)
}

/// Remove markdown image syntax: ![alt](url) and [![alt](url)](link)
fn strip_images(md: &str) -> String {
    let re = Regex::new(r"!\[[^\]]*\]\([^)]*\)").unwrap();
    let cleaned = re.replace_all(md, "");
    let blanks = Regex::new(r"\n{3,}").unwrap();
    blanks.replace_all(&cleaned, "\n\n").to_string()
}
