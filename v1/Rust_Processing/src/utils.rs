//! Common utilities shared across passes.

/// Extract company slug from YC company URL.
/// Returns None if URL is not a valid company page or contains sub-paths.
pub fn slug_from_url(url: &str) -> Option<&str> {
    let rest = url.strip_prefix("https://www.ycombinator.com/companies/")?;
    if rest.is_empty() || rest.contains('/') {
        None
    } else {
        Some(rest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_slug_from_url() {
        assert_eq!(
            slug_from_url("https://www.ycombinator.com/companies/airbnb"),
            Some("airbnb")
        );
        assert_eq!(
            slug_from_url("https://www.ycombinator.com/companies/"),
            None
        );
        assert_eq!(
            slug_from_url("https://www.ycombinator.com/companies/airbnb/jobs"),
            None
        );
        assert_eq!(slug_from_url("https://example.com"), None);
    }
}
