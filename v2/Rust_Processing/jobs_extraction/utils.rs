use regex::Regex;

/// Parse a YC job URL into (company_slug, job_slug).
/// Expected format: https://www.ycombinator.com/companies/<company>/jobs/<job>
pub fn parse_job_url(url: &str) -> Option<(String, String)> {
    let prefix = "https://www.ycombinator.com/companies/";
    let rest = url.strip_prefix(prefix)?;
    let mut parts = rest.trim_end_matches('/').split('/');
    let company = parts.next()?.to_string();
    if company.is_empty() {
        return None;
    }
    let jobs_literal = parts.next()?;
    if jobs_literal != "jobs" {
        return None;
    }
    let job_slug = parts.next()?.to_string();
    if job_slug.is_empty() {
        return None;
    }
    Some((company, job_slug))
}

pub fn has_emoji(text: &str) -> bool {
    text.chars().any(is_emoji_char)
}

fn is_emoji_char(c: char) -> bool {
    let cp = c as u32;
    matches!(
        cp,
        0x1F300..=0x1F5FF
            | 0x1F600..=0x1F64F
            | 0x1F680..=0x1F6FF
            | 0x1F700..=0x1F77F
            | 0x1F780..=0x1F7FF
            | 0x1F800..=0x1F8FF
            | 0x1F900..=0x1F9FF
            | 0x1FA70..=0x1FAFF
            | 0x2600..=0x26FF
            | 0x2700..=0x27BF
    )
}

/// Case-insensitive whole-word containment (best-effort; falls back to substring).
pub fn contains_word(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let pattern = format!(r"(?i)\b{}\b", regex::escape(needle));
    Regex::new(&pattern)
        .ok()
        .map(|re| re.is_match(haystack))
        .unwrap_or(false)
}
