//! Pass 3: Extract status and location.
//!
//! Status: ACTIVE | INACTIVE | PUBLIC | ACQUIRED
//! Location: from sidebar "Location:\n[value]"
//!
//! Updates companies created in pass1.

use anyhow::Result;
use rusqlite::Connection;

use crate::{db, utils};

const STATUSES: &[&str] = &["INACTIVE", "ACTIVE", "PUBLIC", "ACQUIRED"];

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

        let status = extract_status(text);
        let location = extract_location(text);

        if status.is_none() && location.is_none() {
            continue;
        }

        db::update(
            conn,
            "companies",
            &[
                ("status", &status as &dyn rusqlite::ToSql),
                ("location", &location),
            ],
            "slug",
            &slug,
        )?;

        count += 1;
    }

    Ok(count)
}

fn extract_status(text: &str) -> Option<String> {
    STATUSES
        .iter()
        .find(|s| text.contains(*s))
        .map(|s| s.to_string())
}

fn extract_location(text: &str) -> Option<String> {
    let marker = "Location:\n";
    let start = text.find(marker)? + marker.len();
    let rest = &text[start..];
    let end = rest.find('\n').unwrap_or(rest.len());
    let loc = rest[..end].trim();
    if loc.is_empty() {
        None
    } else {
        Some(loc.to_string())
    }
}

/// Get all locations from DB (for use in later passes to filter tags).
pub fn get_all_locations(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt =
        conn.prepare("SELECT DISTINCT location FROM companies WHERE location IS NOT NULL")?;
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}
