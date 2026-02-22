use regex::Regex;
use std::sync::OnceLock;

/// Extract company slug from YC company URL.
pub fn slug_from_url(url: &str) -> Option<String> {
    let rest = url.strip_prefix("https://www.ycombinator.com/companies/")?;
    if rest.is_empty() {
        return None;
    }
    let slug = rest.split('/').next().unwrap_or_default();
    if slug.is_empty() {
        None
    } else {
        Some(slug.to_string())
    }
}

pub fn looks_like_year(val: &str) -> bool {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^(20\d{2}|19\d{2})$").unwrap());
    re.is_match(val.trim())
}

#[allow(dead_code)]
pub fn normalize_token(token: &str) -> String {
    token
        .trim()
        .trim_matches(|c: char| !c.is_alphanumeric() && c != '-')
        .to_string()
}

pub fn classify_link(url: &str) -> Option<&'static str> {
    let lower = url.to_lowercase();
    if lower.contains("linkedin.com/in/") || lower.contains("linkedin.com/company/") {
        Some("linkedin")
    } else if lower.contains("twitter.com/") || lower.contains("x.com/") {
        Some("twitter")
    } else if lower.contains("crunchbase.com/") {
        Some("crunchbase")
    } else if lower.contains("github.com/") {
        Some("github")
    } else if lower.contains("angel.co/") {
        Some("angel")
    } else {
        None
    }
}
