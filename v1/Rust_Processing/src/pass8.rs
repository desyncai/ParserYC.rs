//! Pass 8: Extract and classify external links.
//!
//! Links come from external_links JSON field.
//! - Skip internal YC links
//! - Classify by URL pattern (linkedin.com/in, twitter.com, etc.)
//! - Match founder links to founders by name in URL
//!
//! Inserts into links table.

use anyhow::Result;
use rusqlite::Connection;

use crate::{db, utils};

const INTERNAL: &[&str] = &["ycombinator.com", "startupschool.org"];

#[derive(Debug)]
struct LinkClassification {
    pattern: Option<String>,
    is_personal: bool,
}

// Generic, non-company-specific links we want to drop (normalized without scheme/www).
const GENERIC_LINKS: &[&str] = &[
    "twitter.com/ycombinator",
    "x.com/ycombinator",
    "instagram.com/ycombinator",
    "facebook.com/ycombinator",
    "facebook.com/ycombinator/",
    "youtube.com/c/ycombinator",
    "youtube.com/channel/uccefczrl2oaa_ubneo5uowg",
    "linkedin.com/company/y-combinator",
];

pub fn run(conn: &Connection, pages: &[(String, Option<String>, Option<String>)]) -> Result<usize> {
    let mut count = 0;

    // Purge YC-owned social links from previous runs so they don’t linger.
    prune_yc_links(conn)?;

    for (url, _, links_json) in pages {
        let slug = match utils::slug_from_url(url) {
            Some(s) => s,
            None => continue,
        };

        let links_json = match links_json {
            Some(j) => j,
            None => continue,
        };

        let urls: Vec<String> = match serde_json::from_str(links_json) {
            Ok(u) => u,
            Err(_) => continue,
        };

        // Get founders for this company
        let founders = get_founders(conn, slug).unwrap_or_default();

        for link_url in urls {
            // Skip internal
            if INTERNAL.iter().any(|d| link_url.contains(d)) {
                continue;
            }

            // Skip generic YC links that are the same across companies.
            if is_generic_link(&link_url) {
                continue;
            }

            let classification = classify_link(&link_url);
            let founder_id = if classification.is_personal {
                match_founder(&link_url, &founders)
            } else {
                None
            };

            db::insert(
                conn,
                "links",
                &[
                    ("company_slug", &slug as &dyn rusqlite::ToSql),
                    ("founder_id", &founder_id),
                    ("url", &link_url),
                    ("pattern", &classification.pattern),
                ],
            )?;
            count += 1;
        }
    }

    Ok(count)
}

fn classify_link(url: &str) -> LinkClassification {
    let url_lower = url.to_lowercase();

    let (raw_domain, segments) = match parse_url(url) {
        Some(p) => p,
        None => {
            return LinkClassification {
                pattern: None,
                is_personal: false,
            }
        }
    };

    let domain = normalize_domain(&raw_domain);

    // Helper to build result
    let lc = |pattern: &str, is_personal: bool| LinkClassification {
        pattern: Some(pattern.to_string()),
        is_personal,
    };

    match domain.as_str() {
        "linkedin.com" => {
            if let Some(first) = segments.get(0) {
                match first.as_str() {
                    "in" | "pub" => lc("linkedin_person", true),
                    "company" => lc("linkedin_company", false),
                    "school" => lc("linkedin_school", false),
                    _ => lc("linkedin_other", false),
                }
            } else {
                lc("linkedin", false)
            }
        }
        "twitter.com" => {
            if let Some(handle) = segments.get(0) {
                // Skip intent/home utility paths
                let util = ["home", "intent", "hashtag", "explore", "share", "search"];
                if util.contains(&handle.as_str()) {
                    lc("twitter_org", false)
                } else {
                    lc("twitter_person", true)
                }
            } else {
                lc("twitter", false)
            }
        }
        "instagram.com" => {
            if let Some(_handle) = segments.get(0) {
                lc("instagram_person", true)
            } else {
                lc("instagram", false)
            }
        }
        "facebook.com" => lc("facebook_page", false),
        "crunchbase.com" => {
            if segments
                .get(0)
                .map(|s| s == "organization")
                .unwrap_or(false)
            {
                lc("crunchbase_org", false)
            } else {
                lc("crunchbase_other", false)
            }
        }
        "youtube.com" => {
            if url_lower.contains("youtube.com/watch") && url_lower.contains("v=") {
                return lc("youtube_video", false);
            }
            if let Some(first) = segments.get(0) {
                match first.as_str() {
                    "c" => lc("youtube_channel", false),
                    "channel" => lc("youtube_channel_id", false),
                    s if s.starts_with('@') => lc("youtube_handle", false),
                    _ => lc("youtube", false),
                }
            } else {
                lc("youtube", false)
            }
        }
        "youtu.be" => lc("youtube_video", false),
        "github.com" => {
            if segments.len() >= 2 {
                lc("github_repo", false)
            } else if segments.len() == 1 {
                lc("github_profile", true)
            } else {
                lc("github", false)
            }
        }
        "calendly.com" => lc("calendly_person", true),
        "cal.com" => lc("calcom_person", true),
        "medium.com" => {
            if let Some(first) = segments.get(0) {
                if first.starts_with('@') {
                    lc("medium_person", true)
                } else {
                    lc("medium_publication", false)
                }
            } else {
                lc("medium", false)
            }
        }
        d if d.ends_with(".substack.com") => lc("substack", true),
        "discord.gg" => lc("discord_invite", false),
        "loom.com" => lc("loom_video", false),
        d if is_press_domain(&d) => lc(&format!("press:{}", d), false),
        _ => LinkClassification {
            pattern: Some(domain),
            is_personal: false,
        },
    }
}

fn get_founders(conn: &Connection, company_slug: &str) -> Result<Vec<(i64, String)>> {
    let mut stmt = conn.prepare("SELECT id, name FROM founders WHERE company_slug = ?")?;
    let rows = stmt
        .query_map([company_slug], |row| Ok((row.get(0)?, row.get(1)?)))?
        .filter_map(|r| r.ok())
        .collect();
    Ok(rows)
}

fn match_founder(url: &str, founders: &[(i64, String)]) -> Option<i64> {
    let url_lower = url.to_lowercase();

    for (id, name) in founders {
        let name_lower = name.to_lowercase();
        let name_parts: Vec<&str> = name_lower.split_whitespace().collect();

        let variants = [
            name_parts.join("-"),
            name_parts.join(""),
            name_parts.join("_"),
        ];

        // Try last name only
        if let Some(last) = name_parts.last() {
            if last.len() >= 4 && url_lower.contains(last) {
                return Some(*id);
            }
        }

        for variant in &variants {
            if !variant.is_empty() && variant.len() >= 4 && url_lower.contains(variant) {
                return Some(*id);
            }
        }
    }

    None
}

fn parse_url(url: &str) -> Option<(String, Vec<String>)> {
    let mut s = url.trim();
    s = s
        .strip_prefix("https://")
        .or_else(|| s.strip_prefix("http://"))
        .unwrap_or(s);
    if let Some((left, _)) = s.split_once('?') {
        s = left;
    }
    let s = s.strip_prefix("www.").unwrap_or(s);
    let mut iter = s.split('/');
    let domain = iter.next()?.to_lowercase();
    if domain.is_empty() {
        return None;
    }
    let segments: Vec<String> = iter
        .take_while(|p| !p.is_empty())
        .map(|p| p.to_lowercase())
        .collect();
    Some((domain, segments))
}

fn normalize_domain(domain: &str) -> String {
    if domain == "x.com" {
        return "twitter.com".to_string();
    }
    if domain.ends_with("linkedin.com") {
        return "linkedin.com".to_string();
    }
    domain.to_string()
}

fn is_generic_link(url: &str) -> bool {
    let key = normalize_url_key(url);
    if GENERIC_LINKS.contains(&key.as_str()) {
        return true;
    }

    // Drop any YC-owned social profiles even if extra path/query noise is present.
    if let Some((domain, segments)) = parse_url(url) {
        let first = segments.get(0).map(|s| s.as_str());
        let yc_social = match domain.as_str() {
            "twitter.com" | "x.com" => matches!(first, Some("ycombinator")),
            "instagram.com" => matches!(first, Some("ycombinator")),
            "facebook.com" => matches!(first, Some("ycombinator")),
            "youtube.com" => {
                // common channel patterns
                url.contains("youtube.com/c/ycombinator")
                    || url.contains("youtube.com/channel/")
                        && url.to_lowercase().contains("ycombinator")
            }
            "linkedin.com" => {
                matches!(first, Some("company"))
                    && segments
                        .get(1)
                        .map(|s| s.contains("y-combinator") || s.contains("ycombinator"))
                        .unwrap_or(false)
            }
            _ => false,
        };
        if yc_social {
            return true;
        }
    }

    false
}

fn normalize_url_key(url: &str) -> String {
    let mut s = url.to_lowercase();
    if let Some(stripped) = s.strip_prefix("https://") {
        s = stripped.to_string();
    } else if let Some(stripped) = s.strip_prefix("http://") {
        s = stripped.to_string();
    }
    if let Some(stripped) = s.strip_prefix("www.") {
        s = stripped.to_string();
    }
    if let Some((left, _)) = s.split_once('?') {
        s = left.to_string();
    }
    while s.ends_with('/') {
        s.pop();
    }
    s
}

fn is_press_domain(domain: &str) -> bool {
    matches!(
        domain,
        "techcrunch.com"
            | "forbes.com"
            | "businessinsider.com"
            | "axios.com"
            | "bloomberg.com"
            | "yourstory.com"
            | "inc42.com"
            | "techinasia.com"
            | "venturebeat.com"
            | "theinformation.com"
            | "wsj.com"
            | "ft.com"
            | "reuters.com"
    )
}

fn prune_yc_links(conn: &Connection) -> Result<()> {
    conn.execute(
        "
        DELETE FROM links
        WHERE lower(url) LIKE '%facebook.com/ycombinator%'
           OR lower(url) LIKE '%instagram.com/ycombinator%'
           OR lower(url) LIKE '%twitter.com/ycombinator%'
           OR lower(url) LIKE '%youtube.com/%ycombinator%'
           OR lower(url) LIKE '%linkedin.com/company/%y-combinator%'
        ",
        [],
    )?;
    Ok(())
}

pub fn print_stats(conn: &Connection) -> Result<()> {
    let mut stmt = conn.prepare(
        "SELECT pattern, COUNT(*) as cnt FROM links
         WHERE pattern IS NOT NULL
         GROUP BY pattern ORDER BY cnt DESC LIMIT 15",
    )?;

    println!("\nTop 15 link patterns:");
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?))
    })?;

    for row in rows.flatten() {
        let (pattern, cnt) = row;
        println!("  {:40} {}", pattern.unwrap_or_default(), cnt);
    }

    let founder_links: i64 = conn.query_row(
        "SELECT COUNT(*) FROM links WHERE founder_id IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    let company_links: i64 = conn.query_row(
        "SELECT COUNT(*) FROM links WHERE founder_id IS NULL",
        [],
        |r| r.get(0),
    )?;

    println!("\nLink breakdown:");
    println!("  Company links: {}", company_links);
    println!("  Founder links: {}", founder_links);

    Ok(())
}
