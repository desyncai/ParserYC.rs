//! Pass 5: Extract remaining company details.
//!
//! - tagline (second line after name)
//! - founded_year (from "Founded:\n")
//! - team_size (from "Team Size:\n")
//! - primary_partner (from "Primary Partner:\n")
//! - job_count (number after "Jobs\n")
//! - is_hiring (job_count > 0)
//!
//! Updates companies created in pass1.

use anyhow::Result;
use rusqlite::Connection;

use crate::{db, utils};

pub fn run(conn: &Connection, pages: &[(String, Option<String>, Option<String>)]) -> Result<usize> {
    let mut count = 0;

    for (url, text, _) in pages {
        let slug = match utils::slug_from_url(url) {
            Some(s) => s,
            None => continue,
        };

        let text = match text {
            Some(t) => t,
            None => continue,
        };

        let tagline = extract_tagline(text);
        let founded_year = extract_after_label(text, "Founded").and_then(|s| s.parse::<i32>().ok());
        let team_size = extract_after_label(text, "Team Size").and_then(|s| s.parse::<i32>().ok());
        let primary_partner = extract_after_label(text, "Primary Partner");
        let job_count = extract_job_count(text).unwrap_or(0);
        let is_hiring: i32 = if job_count > 0 { 1 } else { 0 };

        db::update(
            conn,
            "companies",
            &[
                ("tagline", &tagline as &dyn rusqlite::ToSql),
                ("founded_year", &founded_year),
                ("team_size", &team_size),
                ("primary_partner", &primary_partner),
                ("job_count", &job_count),
                ("is_hiring", &is_hiring),
            ],
            "slug",
            &slug,
        )?;

        count += 1;
    }

    Ok(count)
}


fn extract_tagline(text: &str) -> Option<String> {
    let marker = "Companies\n›\n";
    let start = text.find(marker)? + marker.len();
    let rest = &text[start..];

    // Skip first line (name), get second line (tagline)
    let mut lines = rest.lines();
    lines.next()?; // skip name
    let tagline = lines.next()?.trim();

    if tagline.is_empty()
        || tagline
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_whitespace())
    {
        // Tagline shouldn't be all caps (that's probably batch info)
        None
    } else {
        Some(tagline.to_string())
    }
}

fn extract_after_label(text: &str, label: &str) -> Option<String> {
    let pattern = format!("{}:\n", label);
    let start = text.find(&pattern)? + pattern.len();
    let rest = &text[start..];
    let end = rest.find('\n').unwrap_or(rest.len());
    let value = rest[..end].trim();

    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

fn extract_job_count(text: &str) -> Option<i32> {
    // Look for "Jobs\n" followed by a number
    let marker = "Jobs\n";
    let pos = text.find(marker)?;
    let start = pos + marker.len();
    let rest = &text[start..];
    let end = rest.find('\n').unwrap_or(rest.len());
    rest[..end].trim().parse().ok()
}
