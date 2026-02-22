mod db;
mod metrics;
mod passes;
mod text;
mod utils;

use anyhow::Result;
use config::Config;
use db::PageInput;
use metrics::{new_run_id, PassTracker};
use text::WorkItem;
use tracing::info;

#[cfg(feature = "rayon")]
use rayon::prelude::*;

#[cfg(feature = "rayon")]
fn to_work_items(pages: Vec<PageInput>) -> Vec<WorkItem> {
    pages
        .into_par_iter()
        .map(|p| WorkItem {
            url: p.url,
            slug: None,
            name: None,
            text: text::WorkingText::from_raw(&p.text),
            external_links: p.external_links,
        })
        .collect()
}

#[cfg(not(feature = "rayon"))]
fn to_work_items(pages: Vec<PageInput>) -> Vec<WorkItem> {
    pages
        .into_iter()
        .map(|p| WorkItem {
            url: p.url,
            slug: None,
            name: None,
            text: text::WorkingText::from_raw(&p.text),
            external_links: p.external_links,
        })
        .collect()
}

fn persist_residuals(conn: &rusqlite::Connection, run_id: &str, items: &[WorkItem]) -> Result<()> {
    for item in items {
        if let Some(ref slug) = item.slug {
            let sample = item.text.sample(320);
            db::insert_residual(conn, run_id, slug, "final", item.text.char_len(), &sample)?;
        }
    }
    Ok(())
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

fn main() -> Result<()> {
    init_tracing();
    let settings = Config::builder()
        .add_source(config::Environment::with_prefix("YC"))
        .build()
        .unwrap_or_default();

    info!(settings_loaded = ?settings, msg = "Starting company processor");

    println!("YC Company Metadata Extraction");
    println!("==============================\n");

    let conn = db::connect()?;
    println!("Database: {:?}\n", db::path());
    db::create_tables(&conn)?;

    let pages = db::fetch_pages(&conn)?;
    println!("Loaded {} company pages\n", pages.len());
    if pages.is_empty() {
        return Ok(());
    }

    let mut work = to_work_items(pages);
    let run_id = new_run_id();
    let mut tracker = PassTracker::new(run_id.clone());

    conn.execute("BEGIN TRANSACTION", [])?;
    passes::pass_slug_and_name(&conn, &mut work, &mut tracker)?;
    passes::pass_batch_status_location(&conn, &mut work, &mut tracker)?;
    passes::pass_tagline_and_sidebar(&conn, &mut work, &mut tracker)?;
    passes::pass_tags(&conn, &mut work, &mut tracker)?;
    passes::pass_founders(&conn, &mut work, &mut tracker)?;
    passes::pass_news(&conn, &mut work, &mut tracker)?;
    passes::pass_links(&conn, &mut work, &mut tracker)?;
    conn.execute("COMMIT", [])?;

    tracker.persist(&conn)?;
    persist_residuals(&conn, tracker.run_id(), &work)?;

    println!("\nDone.");
    Ok(())
}
