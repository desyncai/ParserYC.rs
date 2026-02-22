use crate::db::JobPage;
use crate::utils::parse_job_url;
use anyhow::Result;
use rusqlite::Connection;
use std::collections::VecDeque;

pub fn run(conn: &Connection, jobs: &[JobPage]) -> Result<usize> {
    let mut stmt = conn.prepare(
        r#"
        INSERT OR REPLACE INTO job_text_shortened (
            job_id, url, company_slug, job_slug, text_shortened,
            raw_len, shortened_len, is_blank, is_404,
            nav_removed, similar_removed, footer_removed, founder_removed, scraped_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )?;

    let mut written = 0;
    for job in jobs {
        let result = shorten_text(&job.text_content);
        let (company_slug, job_slug) = match parse_job_url(&job.url) {
            Some((c, j)) => (Some(c), Some(j)),
            None => (None, None),
        };

        stmt.execute(rusqlite::params![
            job.job_id,
            job.url,
            company_slug,
            job_slug,
            result.text_shortened,
            result.raw_len as i64,
            result.shortened_len as i64,
            bool_to_int(result.is_blank),
            bool_to_int(result.is_404),
            bool_to_int(result.nav_removed),
            bool_to_int(result.similar_removed),
            bool_to_int(result.footer_removed),
            bool_to_int(result.founder_removed),
            job.scraped_at
        ])?;
        written += 1;
    }

    Ok(written)
}

fn bool_to_int(b: bool) -> i64 {
    if b {
        1
    } else {
        0
    }
}

#[derive(Debug)]
struct ShortenResult {
    text_shortened: String,
    raw_len: usize,
    shortened_len: usize,
    is_blank: bool,
    is_404: bool,
    nav_removed: bool,
    similar_removed: bool,
    footer_removed: bool,
    founder_removed: bool,
}

fn shorten_text(raw: &str) -> ShortenResult {
    let normalized = raw.replace("\r\n", "\n");
    let raw_len = normalized.len();
    let is_404 = detect_404(&normalized);

    let mut lines: VecDeque<String> = normalized
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();

    let mut nav_removed = false;
    while let Some(front) = lines.front() {
        if is_nav_line(front) {
            nav_removed = true;
            lines.pop_front();
        } else {
            break;
        }
    }

    let mut body_lines = Vec::new();
    let mut similar_removed = false;
    for line in lines {
        if line.to_lowercase().contains("similar jobs") {
            similar_removed = true;
            break;
        }
        body_lines.push(line);
    }

    let mut footer_removed = false;
    while let Some(last) = body_lines.last() {
        if is_footer_line(last) {
            footer_removed = true;
            body_lines.pop();
        } else {
            break;
        }
    }

    let mut founder_removed = false;
    body_lines.retain(|l| {
        let lower = l.to_lowercase();
        let remove = lower.contains("founder");
        if remove {
            founder_removed = true;
        }
        !remove
    });

    let text_shortened = body_lines.join("\n");
    let shortened_len = text_shortened.len();
    let is_blank = text_shortened.trim().is_empty();

    ShortenResult {
        text_shortened,
        raw_len,
        shortened_len,
        is_blank,
        is_404,
        nav_removed,
        similar_removed,
        footer_removed,
        founder_removed,
    }
}

fn detect_404(text: &str) -> bool {
    let lower = text.to_lowercase();
    lower.contains("404") || lower.contains("file not found")
}

fn is_nav_line(line: &str) -> bool {
    let lower = line.to_lowercase();
    lower.contains("startup jobs")
        || lower.contains("open main menu")
        || lower.contains("aboutcompanies")
        || lower.contains("about companies")
        || lower.contains("company list")
        || lower.contains("yc jobs")
        || lower.starts_with("apply now")
        || lower.starts_with("apply")
}

fn is_footer_line(line: &str) -> bool {
    let lower = line.to_lowercase();
    lower.contains("privacy")
        || lower.contains("terms")
        || lower.contains("copyright")
        || lower.contains("y combinator")
        || lower.contains("back to top")
        || lower.starts_with("©")
}
