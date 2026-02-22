//! Pass 6: Extract founders.
//!
//! Founders section starts with "Founders\n", "Active Founders\n", or "Former Founders\n"
//! Pattern: [Name]\n \n[Title]
//!
//! Inserts into founders table.

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

        let section = match extract_founders_section(text) {
            Some(s) => s,
            None => continue,
        };

        let founders = parse_founders(&section);

        for (name, title) in founders {
            db::insert(
                conn,
                "founders",
                &[
                    ("company_slug", &slug as &dyn rusqlite::ToSql),
                    ("name", &name),
                    ("title", &title),
                ],
            )?;
            count += 1;
        }
    }

    Ok(count)
}


fn extract_founders_section(text: &str) -> Option<String> {
    let start_markers = ["Active Founders\n", "Former Founders\n", "Founders\n"];
    let stop_markers = ["Latest News", "Footer", "Jobs at", "\nFounded:"];

    let mut start_pos = None;
    let mut start_len = 0;

    for marker in &start_markers {
        if let Some(pos) = text.find(marker) {
            if start_pos.is_none() || pos < start_pos.unwrap() {
                start_pos = Some(pos);
                start_len = marker.len();
            }
        }
    }

    let start_pos = start_pos? + start_len;
    let rest = &text[start_pos..];

    let mut end_pos = rest.len();
    for marker in &stop_markers {
        if let Some(pos) = rest.find(marker) {
            if pos < end_pos {
                end_pos = pos;
            }
        }
    }

    Some(rest[..end_pos].to_string())
}

fn parse_founders(section: &str) -> Vec<(String, Option<String>)> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        // Match: Name\n followed by whitespace (including non-breaking space \u{00A0}) then \n then Title
        Regex::new(r"([A-Z][a-zA-Z'\-]+(?: [A-Z][a-zA-Z'\-]+)+)\n[\s\u{00A0}]+\n([^\n]+)").unwrap()
    });

    re.captures_iter(section)
        .filter_map(|cap| {
            let name = cap.get(1)?.as_str().trim().to_string();
            let title = cap.get(2).map(|m| m.as_str().trim().to_string());

            if name.len() < 3 || name.contains("http") {
                return None;
            }

            Some((name, title))
        })
        .collect()
}
