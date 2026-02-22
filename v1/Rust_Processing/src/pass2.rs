//! Pass 2: Extract batch (season + year).
//!
//! Pattern: "WINTER 2009", "SUMMER 2024", etc.
//! Updates companies created in pass1.

use anyhow::Result;
use regex::Regex;
use rusqlite::Connection;
use std::sync::OnceLock;

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

        let (season, year) = match extract_batch(text) {
            Some(b) => b,
            None => continue,
        };

        db::update(
            conn,
            "companies",
            &[
                ("batch_season", &season as &dyn rusqlite::ToSql),
                ("batch_year", &year),
            ],
            "slug",
            &slug,
        )?;

        count += 1;
    }

    Ok(count)
}


fn extract_batch(text: &str) -> Option<(String, i32)> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"(WINTER|SUMMER|FALL|SPRING)\s+(\d{4})").unwrap());

    let caps = re.captures(text)?;
    let season = caps.get(1)?.as_str().to_string();
    let year = caps.get(2)?.as_str().parse().ok()?;
    Some((season, year))
}
