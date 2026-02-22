//! Pass 7: Extract news.
//!
//! News section starts with "Latest News\n"
//! Pattern: [Title] - [Source (optional)]\n[Date]
//! Date format: "May 09, 2023"
//!
//! Inserts into news table.

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

        let section = match extract_news_section(text) {
            Some(s) => s,
            None => continue,
        };

        let news_items = parse_news(&section);

        for (title, source, date) in news_items {
            db::insert(
                conn,
                "news",
                &[
                    ("company_slug", &slug as &dyn rusqlite::ToSql),
                    ("title", &title),
                    ("source", &source),
                    ("published_date", &date),
                ],
            )?;
            count += 1;
        }
    }

    Ok(count)
}


fn extract_news_section(text: &str) -> Option<String> {
    let start_marker = "Latest News\n";
    let stop_markers = [
        "Jobs at",
        "Founders",
        "Active Founders",
        "Former Founders",
        "Footer",
        "\nFounded:",
        "YC Photos",
    ];

    let start_pos = text.find(start_marker)? + start_marker.len();
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

fn parse_news(section: &str) -> Vec<(String, Option<String>, Option<String>)> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| {
        Regex::new(r"([^\n]+?)(?:\s*[-–|]\s*([^\n]+?))?\n([A-Z][a-z]{2} \d{1,2}, \d{4})").unwrap()
    });

    re.captures_iter(section)
        .filter_map(|cap| {
            let title = cap.get(1)?.as_str().trim().to_string();
            let source = cap.get(2).map(|m| m.as_str().trim().to_string());
            let date = cap.get(3).map(|m| m.as_str().to_string());

            if title.len() < 5 {
                return None;
            }

            Some((title, source, date))
        })
        .collect()
}
