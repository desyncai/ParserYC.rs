use anyhow::Result;
use regex::Regex;
use rusqlite::{types::Value as SqlValue, Connection};
use std::collections::HashSet;
use std::sync::OnceLock;

use crate::db;
use crate::metrics::PassTracker;
use crate::text::{total_chars, WorkItem};
use crate::utils::{classify_link, looks_like_year, slug_from_url};

pub fn pass_slug_and_name(
    conn: &Connection,
    items: &mut Vec<WorkItem>,
    tracker: &mut PassTracker,
) -> Result<()> {
    let before = total_chars(items);
    let mut next = Vec::new();
    for mut item in items.drain(..) {
        let slug = match slug_from_url(&item.url) {
            Some(s) => s,
            None => continue,
        };
        let name = extract_name(&mut item).unwrap_or_else(|| slug.clone());
        db::insert_company(conn, &slug, &name, &item.url)?;
        item.slug = Some(slug.clone());
        item.name = Some(name);
        next.push(item);
    }
    *items = next;
    tracker.record("pass1_slug_name", items, before);
    Ok(())
}

pub fn pass_batch_status_location(
    conn: &Connection,
    items: &mut Vec<WorkItem>,
    tracker: &mut PassTracker,
) -> Result<()> {
    let before = total_chars(items);
    for item in items.iter_mut() {
        let Some(slug) = item.slug.clone() else {
            continue;
        };

        let mut season: Option<String> = None;
        let mut year: Option<i32> = None;
        if let Some(batch) = extract_batch(item) {
            season = Some(batch.0);
            year = Some(batch.1);
        }

        let status = extract_status(item);
        let location = extract_location(item);

        let mut fields: Vec<(&str, SqlValue)> = Vec::new();
        if let Some(s) = season {
            fields.push(("batch_season", SqlValue::from(s)));
        }
        if let Some(y) = year {
            fields.push(("batch_year", SqlValue::from(y)));
        }
        if let Some(s) = status {
            fields.push(("status", SqlValue::from(s)));
        }
        if let Some(l) = location {
            fields.push(("location", SqlValue::from(l)));
        }
        db::update_company(conn, &slug, fields)?;
    }
    tracker.record("pass2_batch_status_location", items, before);
    Ok(())
}

pub fn pass_tagline_and_sidebar(
    conn: &Connection,
    items: &mut Vec<WorkItem>,
    tracker: &mut PassTracker,
) -> Result<()> {
    let before = total_chars(items);
    static TEAM_RE: OnceLock<Regex> = OnceLock::new();
    let team_re =
        TEAM_RE.get_or_init(|| Regex::new(r"(?i)(\d{1,4})\s+(people|employees|team)").unwrap());

    static JOBS_RE: OnceLock<Regex> = OnceLock::new();
    let jobs_re = JOBS_RE.get_or_init(|| Regex::new(r"(?i)(\d+)\s+jobs?").unwrap());

    for item in items.iter_mut() {
        let Some(slug) = item.slug.clone() else {
            continue;
        };

        let mut tagline: Option<String> = None;
        let mut team_size: Option<i64> = None;
        let mut founded_year: Option<i64> = None;
        let mut primary_partner: Option<String> = None;
        let mut job_count: Option<i64> = None;
        let mut is_hiring: Option<i64> = None;

        let tagline_lines = item.text.take_first_n(2);
        if let Some(line) = tagline_lines
            .iter()
            .find(|l| !l.is_empty() && !l.to_lowercase().contains("company") && !l.contains("Jobs"))
        {
            tagline = Some(line.to_string());
        }

        let team_line = item
            .text
            .remove_where(|l| l.to_lowercase().contains("team size") || team_re.is_match(l))
            .into_iter()
            .next();
        if let Some(line) = team_line {
            if let Some(caps) = team_re.captures(&line) {
                if let Ok(val) = caps.get(1).unwrap().as_str().parse::<i64>() {
                    team_size = Some(val);
                }
            }
        }

        let founded_line = item
            .text
            .remove_where(|l| l.to_lowercase().starts_with("founded"))
            .into_iter()
            .next();
        if let Some(line) = founded_line {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(year_str) = parts.into_iter().find(|p| looks_like_year(p)) {
                if let Ok(val) = year_str.trim().parse::<i64>() {
                    founded_year = Some(val);
                }
            }
        }

        let partner_line = item
            .text
            .remove_where(|l| l.to_lowercase().contains("Primary Partner"))
            .into_iter()
            .next();
        if let Some(line) = partner_line {
            let partner = line
                .split(':')
                .skip(1)
                .next()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty());
            if let Some(p) = partner {
                primary_partner = Some(p);
            }
        }

        let job_line = item
            .text
            .remove_where(|l| l.to_lowercase().contains("jobs"))
            .into_iter()
            .next();
        if let Some(line) = job_line {
            if let Some(caps) = jobs_re.captures(&line) {
                if let Ok(val) = caps.get(1).unwrap().as_str().parse::<i64>() {
                    job_count = Some(val);
                    if val > 0 {
                        is_hiring = Some(1);
                    }
                }
            } else if line.to_lowercase().contains("hiring") {
                is_hiring = Some(1);
            }
        }

        let mut fields: Vec<(&str, SqlValue)> = Vec::new();
        if let Some(t) = tagline {
            fields.push(("tagline", SqlValue::from(t)));
        }
        if let Some(val) = team_size {
            fields.push(("team_size", SqlValue::from(val)));
        }
        if let Some(val) = founded_year {
            fields.push(("founded_year", SqlValue::from(val)));
        }
        if let Some(p) = primary_partner {
            fields.push(("primary_partner", SqlValue::from(p)));
        }
        if let Some(c) = job_count {
            fields.push(("job_count", SqlValue::from(c)));
        }
        if let Some(h) = is_hiring {
            fields.push(("is_hiring", SqlValue::from(h)));
        }

        db::update_company(conn, &slug, fields)?;
    }
    tracker.record("pass3_tagline_sidebar", items, before);
    Ok(())
}

pub fn pass_tags(
    conn: &Connection,
    items: &mut Vec<WorkItem>,
    tracker: &mut PassTracker,
) -> Result<()> {
    let before = total_chars(items);
    static CAPS_RE: OnceLock<Regex> = OnceLock::new();
    let caps_re = CAPS_RE.get_or_init(|| Regex::new(r"\b([A-Z][A-Z0-9\-/&]{1,})\b").unwrap());

    let noise: HashSet<&'static str> = [
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
        "STATUS",
        "LOCATION",
    ]
    .into_iter()
    .collect();

    for item in items.iter_mut() {
        let Some(slug) = item.slug.clone() else {
            continue;
        };
        let mut seen = HashSet::new();
        for line in item.text.lines() {
            for cap in caps_re.captures_iter(line) {
                let raw = cap.get(1).unwrap().as_str();
                if noise.contains(raw) {
                    continue;
                }
                if raw.len() == 4 && raw.chars().all(|c| c.is_ascii_digit()) {
                    continue;
                }
                if seen.insert(raw.to_string()) {
                    db::insert_tag(conn, &slug, raw)?;
                }
            }
        }
        // Drop lines that are mostly uppercase tags to shrink text for later passes
        // fix this
        item.text.remove_where(|l| {
            l.chars()
                .all(|c| c.is_ascii_uppercase() || !c.is_alphabetic())
        });
    }
    tracker.record("pass4_tags", items, before);
    Ok(())
}

pub fn pass_founders(
    conn: &Connection,
    items: &mut Vec<WorkItem>,
    tracker: &mut PassTracker,
) -> Result<()> {
    let before = total_chars(items);
    for item in items.iter_mut() {
        let Some(slug) = item.slug.clone() else {
            continue;
        };
        let founder_lines = item.text.remove_where(|l| is_founder_line(l));
        for line in founder_lines {
            let (name, title) = split_name_title(&line);
            if name.len() >= 2 {
                db::insert_founder(conn, &slug, &name, title.as_deref())?;
            }
        }
    }
    tracker.record("pass5_founders", items, before);
    Ok(())
}

pub fn pass_news(
    conn: &Connection,
    items: &mut Vec<WorkItem>,
    tracker: &mut PassTracker,
) -> Result<()> {
    let before = total_chars(items);
    for item in items.iter_mut() {
        let Some(slug) = item.slug.clone() else {
            continue;
        };
        let news_lines = item.text.remove_where(|l| {
            l.to_lowercase().contains("news")
                || l.contains("TechCrunch")
                || l.contains("Forbes")
                || l.contains("TC ")
        });
        for line in news_lines {
            let source = infer_source(&line);
            db::insert_news(conn, &slug, line.trim(), source.as_deref())?;
        }
    }
    tracker.record("pass6_news", items, before);
    Ok(())
}

pub fn pass_links(
    conn: &Connection,
    items: &mut Vec<WorkItem>,
    tracker: &mut PassTracker,
) -> Result<()> {
    let before = total_chars(items);
    for item in items.iter_mut() {
        let Some(slug) = item.slug.clone() else {
            continue;
        };
        for link in &item.external_links {
            let pattern = classify_link(link);
            db::insert_link(conn, &slug, None, link, pattern)?;
        }
        let http_lines = item
            .text
            .remove_where(|l| l.contains("http://") || l.contains("https://"));
        for line in http_lines {
            let parts: Vec<&str> = line.split_whitespace().collect();
            for part in parts {
                if part.starts_with("http://") || part.starts_with("https://") {
                    let pattern = classify_link(part);
                    db::insert_link(conn, &slug, None, part, pattern)?;
                }
            }
        }
    }
    tracker.record("pass7_links", items, before);
    Ok(())
}

fn extract_name(item: &mut WorkItem) -> Option<String> {
    let mut header = item.text.take_prefix_until_blank();
    if header.is_empty() {
        header = item.text.take_first_n(3);
    }
    header
        .into_iter()
        .map(|l| {
            l.replace("Company", "")
                .replace("Companies", "")
                .replace("›", "")
                .trim()
                .to_string()
        })
        .filter(|l| !l.is_empty())
        .next()
}

fn extract_batch(item: &mut WorkItem) -> Option<(String, i32)> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"(?i)(WINTER|SUMMER|FALL|SPRING)\s+(\d{4})").unwrap());

    if let Some(line) = item.text.take_first_matching(|l| re.is_match(l)) {
        if let Some(caps) = re.captures(&line) {
            let season = caps.get(1)?.as_str().to_uppercase();
            let year: i32 = caps.get(2)?.as_str().parse().ok()?;
            return Some((season, year));
        }
    }
    None
}

fn extract_status(item: &mut WorkItem) -> Option<String> {
    let statuses = ["INACTIVE", "ACTIVE", "PUBLIC", "ACQUIRED"];
    let line = item
        .text
        .take_first_matching(|l| statuses.iter().any(|s| l.to_uppercase().contains(s)))?;
    statuses
        .iter()
        .find(|s| line.to_uppercase().contains(*s))
        .map(|s| s.to_string())
}

fn extract_location(item: &mut WorkItem) -> Option<String> {
    let line = item.text.take_first_matching(|l| {
        let lower = l.to_lowercase();
        lower.starts_with("location") || lower.contains(',') || lower.contains("remote")
    })?;
    let cleaned = line
        .split(':')
        .skip(1)
        .next()
        .unwrap_or(&line)
        .trim()
        .to_string();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

fn is_founder_line(line: &str) -> bool {
    let lower = line.to_lowercase();
    lower.contains("founder")
        || lower.contains("co-founder")
        || lower.contains("ceo")
        || lower.contains("cto")
}

fn split_name_title(line: &str) -> (String, Option<String>) {
    let parts: Vec<&str> = line
        .split(|c| c == '-' || c == '–' || c == '|' || c == ':')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if parts.is_empty() {
        return (line.trim().to_string(), None);
    }
    if parts.len() == 1 {
        return (parts[0].to_string(), None);
    }
    (parts[0].to_string(), Some(parts[1].to_string()))
}

fn infer_source(line: &str) -> Option<String> {
    let lower = line.to_lowercase();
    let domains = ["techcrunch", "forbes", "bloomberg", "reuters", "yahoo"];
    for d in domains {
        if lower.contains(d) {
            return Some(d.to_string());
        }
    }
    None
}
