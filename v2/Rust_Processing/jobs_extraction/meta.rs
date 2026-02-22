use crate::db::{self, ShortenedRow};
use crate::utils::has_emoji;
use anyhow::Result;
use regex::Regex;
use rusqlite::Connection;
use std::sync::OnceLock;

pub fn run(conn: &Connection) -> Result<usize> {
    let rows = db::fetch_shortened(conn)?;
    let mut stmt = conn.prepare(
        r#"
        INSERT OR REPLACE INTO job_meta (
            job_id, url, company_slug, job_slug,
            job_title, role_raw, role_bucket,
            job_type, position_type, location_raw, pay_raw,
            experience_raw, visa_raw, has_emoji, header_ok, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        "#,
    )?;

    let mut written = 0;
    for row in rows {
        let meta = extract_meta(&row);
        stmt.execute(rusqlite::params![
            row.job_id,
            row.url,
            row.company_slug,
            row.job_slug,
            meta.job_title,
            meta.role_raw,
            meta.role_bucket,
            meta.job_type,
            meta.position_type,
            meta.location_raw,
            meta.pay_raw,
            meta.experience_raw,
            meta.visa_raw,
            bool_to_int(meta.has_emoji),
            bool_to_int(meta.header_ok),
        ])?;
        written += 1;
    }
    Ok(written)
}

#[derive(Debug)]
struct JobMeta {
    job_title: Option<String>,
    role_raw: Option<String>,
    role_bucket: String,
    job_type: Option<String>,
    position_type: Option<String>,
    location_raw: Option<String>,
    pay_raw: Option<String>,
    experience_raw: Option<String>,
    visa_raw: Option<String>,
    has_emoji: bool,
    header_ok: bool,
}

fn extract_meta(row: &ShortenedRow) -> JobMeta {
    let lines: Vec<String> = row
        .text_shortened
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .collect();

    let job_title = guess_job_title(&lines, row.company_slug.as_deref());
    let role_raw = find_role(&lines).or_else(|| job_title.clone());
    let job_type = find_job_type(&lines);
    let position_type = job_type.clone();
    let location_raw = find_location(&lines);
    let pay_raw = find_pay(&lines);
    let experience_raw = find_experience(&lines);
    let visa_raw = find_visa(&lines);
    let role_bucket = derive_role_bucket(role_raw.as_deref(), job_title.as_deref());
    let has_emoji = has_emoji(&row.text_shortened);
    let header_ok = !row.is_blank && !row.is_404;

    JobMeta {
        job_title,
        role_raw,
        role_bucket,
        job_type,
        position_type,
        location_raw,
        pay_raw,
        experience_raw,
        visa_raw,
        has_emoji,
        header_ok,
    }
}

fn bool_to_int(b: bool) -> i64 {
    if b {
        1
    } else {
        0
    }
}

fn find_job_type(lines: &[String]) -> Option<String> {
    let type_keywords = [
        ("full-time", "Full-time"),
        ("full time", "Full-time"),
        ("part-time", "Part-time"),
        ("contract", "Contract"),
        ("intern", "Internship"),
        ("co-founder", "Co-founder"),
        ("cofounder", "Co-founder"),
        ("founder", "Co-founder"),
    ];
    for line in lines.iter().take(6) {
        let lower = line.to_lowercase();
        for (needle, label) in &type_keywords {
            if lower.contains(needle) {
                return Some((*label).to_string());
            }
        }
        if lower.starts_with("job type") {
            return line.split(':').nth(1).map(|v| v.trim().to_string());
        }
    }
    None
}

fn find_role(lines: &[String]) -> Option<String> {
    for line in lines.iter().take(6) {
        let lower = line.to_lowercase();
        if lower.starts_with("role") {
            return line.split(':').nth(1).map(|v| v.trim().to_string());
        }
    }
    None
}

fn find_location(lines: &[String]) -> Option<String> {
    for line in lines.iter().take(8) {
        let lower = line.to_lowercase();
        if lower.starts_with("location") {
            return line
                .split(':')
                .nth(1)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty());
        }
        if lower.contains("remote")
            || lower.contains(',')
            || lower.contains("hybrid")
            || lower.contains("onsite")
        {
            return Some(line.clone());
        }
    }
    None
}

fn find_pay(lines: &[String]) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"(?i)(\$|£|€)\s?\d").unwrap());
    for line in lines.iter().take(12) {
        if re.is_match(line) || line.to_lowercase().contains("equity") {
            return Some(line.clone());
        }
    }
    None
}

fn find_experience(lines: &[String]) -> Option<String> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"(?i)(\d+\+?\s+years?)").unwrap());
    for line in lines.iter().take(12) {
        if let Some(m) = re.find(&line.to_lowercase()) {
            return Some(m.as_str().to_string());
        }
    }
    None
}

fn find_visa(lines: &[String]) -> Option<String> {
    for line in lines.iter().take(12) {
        let lower = line.to_lowercase();
        if lower.contains("visa") || lower.contains("sponsorship") {
            return Some(line.clone());
        }
    }
    None
}

fn guess_job_title(lines: &[String], company_slug: Option<&str>) -> Option<String> {
    let company = company_slug.map(|s| s.replace('-', " ").to_lowercase());
    for line in lines.iter().take(6) {
        let lower = line.to_lowercase();
        if lower.starts_with("apply") || lower.starts_with("location") || lower.starts_with("job type") {
            continue;
        }
        if let Some(ref c) = company {
            if lower == *c {
                continue;
            }
        }
        return Some(line.clone());
    }
    None
}

fn derive_role_bucket(role_raw: Option<&str>, job_title: Option<&str>) -> String {
    let source = role_raw.or(job_title).unwrap_or("Other");
    let lower = source.to_lowercase();

    if starts_with_any(
        &lower,
        &[
            "engineering",
            "software",
            "developer",
            "devops",
            "data eng",
            "ml engineer",
            "machine learning",
            "ai engineer",
        ],
    ) {
        "Engineering"
    } else if starts_with_any(
        &lower,
        &["sales", "account executive", "ae", "business development"],
    ) || lower.contains("sales")
    {
        "Sales"
    } else if starts_with_any(&lower, &["marketing", "growth"]) {
        "Marketing"
    } else if starts_with_any(&lower, &["operations", "ops"]) {
        "Operations"
    } else if starts_with_any(&lower, &["product"]) {
        "Product"
    } else if starts_with_any(&lower, &["design", "designer", "ux", "ui"]) {
        "Design"
    } else if starts_with_any(&lower, &["support", "customer"]) {
        "Support"
    } else if starts_with_any(&lower, &["finance"]) {
        "Finance"
    } else if starts_with_any(&lower, &["recruit", "talent", "people", "hr"]) {
        "Recruiting & HR"
    } else if starts_with_any(&lower, &["science", "research", "data scientist"]) {
        "Science"
    } else {
        "Other"
    }
    .to_string()
}

fn starts_with_any(target: &str, prefixes: &[&str]) -> bool {
    prefixes.iter().any(|p| target.starts_with(p))
}
