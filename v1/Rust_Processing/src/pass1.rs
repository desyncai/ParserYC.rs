//! Pass 1: Extract slug, name, url from each page.
//!
//! slug = last segment of URL (e.g., "airbnb" from /companies/airbnb)
//! name = first line after "Companies\n›\n"
//! source_url = the full URL

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

        let name = match extract_name(text) {
            Some(n) => n,
            None => continue,
        };

        db::insert(
            conn,
            "companies",
            &[
                ("slug", &slug as &dyn rusqlite::ToSql),
                ("name", &name),
                ("source_url", &url),
            ],
        )?;

        count += 1;
    }

    Ok(count)
}

fn extract_name(text: &str) -> Option<String> {
    let marker = "Companies\n›\n";
    let start = text.find(marker)? + marker.len();
    let rest = &text[start..];
    let end = rest.find('\n')?;
    let name = rest[..end].trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}
