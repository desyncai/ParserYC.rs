//! Pass 4: Extract tags (ALL_CAPS words).
//!
//! Tags are ALL_CAPS words from the header section, excluding:
//!   - Status words (ACTIVE, INACTIVE, PUBLIC, ACQUIRED)
//!   - Season words (WINTER, SUMMER, FALL, SPRING)
//!   - Location words (from pass3)
//!   - Noise words (COMPANY, JOBS, NEWS, etc.)
//!
//! Inserts into tags table.

use anyhow::Result;
use regex::Regex;
use rusqlite::Connection;
use std::collections::HashSet;
use std::sync::OnceLock;

use crate::{db, utils};
use crate::pass3;

const STATUSES: &[&str] = &["ACTIVE", "INACTIVE", "PUBLIC", "ACQUIRED"];
const SEASONS: &[&str] = &["WINTER", "SUMMER", "FALL", "SPRING"];
const NOISE: &[&str] = &[
    "COMPANY",
    "JOBS",
    "NEWS",
    "HOME",
    "YC",
    "FOOTER",
    "COMPANIES",
    "APPLY",
    "ABOUT",
    "LIBRARY",
    "SAFE",
    "RESOURCES",
];

pub fn run(conn: &Connection, pages: &[(String, Option<String>, Option<String>)]) -> Result<usize> {
    let mut count = 0;

    // Build exclusion set from locations
    let locations = pass3::get_all_locations(conn)?;
    let mut excluded: HashSet<String> = HashSet::new();

    // Add status, season, noise words
    for w in STATUSES.iter().chain(SEASONS.iter()).chain(NOISE.iter()) {
        excluded.insert(w.to_string());
    }

    // Add location words (uppercase, also split by comma for city names)
    for loc in &locations {
        excluded.insert(loc.to_uppercase());
        // Also add city part (before comma)
        if let Some(city) = loc.split(',').next() {
            excluded.insert(city.trim().to_uppercase());
        }
        // Add individual words from location
        for word in loc.split_whitespace() {
            let w = word
                .trim_matches(|c: char| !c.is_alphanumeric())
                .to_uppercase();
            if w.len() >= 2 {
                excluded.insert(w);
            }
        }
    }

    for (url, text, _) in pages {
        let slug = match utils::slug_from_url(url) {
            Some(s) => s,
            None => continue,
        };

        let text = match text {
            Some(t) => t,
            None => continue,
        };

        // Extract header section (between "Companies\n›\n" and "Company\nJobs")
        let header = match extract_header(text) {
            Some(h) => h,
            None => continue,
        };

        // Extract ALL_CAPS words from header
        let caps_words = extract_all_caps(&header);

        // Filter and insert
        for word in caps_words {
            // Skip excluded words
            if excluded.contains(&word) {
                continue;
            }
            // Skip pure numbers
            if word.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }
            // Skip single char or very short
            if word.len() < 2 {
                continue;
            }
            // Skip years (4 digits)
            if word.len() == 4 && word.chars().all(|c| c.is_ascii_digit()) {
                continue;
            }

            db::insert(
                conn,
                "tags",
                &[
                    ("company_slug", &slug as &dyn rusqlite::ToSql),
                    ("tag", &word),
                ],
            )?;
            count += 1;
        }
    }

    Ok(count)
}


fn extract_header(text: &str) -> Option<String> {
    let start_marker = "Companies\n›\n";
    let stop_marker = "Company\nJobs";

    let start = text.find(start_marker)? + start_marker.len();
    let rest = &text[start..];
    let end = rest.find(stop_marker).unwrap_or(rest.len());
    Some(rest[..end].to_string())
}

fn extract_all_caps(text: &str) -> Vec<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"\b([A-Z][A-Z0-9\-&/]{1,})\b").unwrap());

    let mut seen = HashSet::new();
    re.captures_iter(text)
        .filter_map(|c| c.get(1))
        .map(|m| m.as_str().to_string())
        .filter(|s| seen.insert(s.clone()))
        .collect()
}
