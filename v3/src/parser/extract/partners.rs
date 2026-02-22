use regex::Regex;
use std::sync::LazyLock;

use crate::db::PartnerRow;

static CLOSE_URL_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"\]\(https?://(?:www\.)?ycombinator\.com/people/([a-z0-9-]+)\)(\[?)$").unwrap()
});

static PEOPLE_URL_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"/people/([a-z][a-z0-9-]+)").unwrap());

const TITLE_KEYWORDS: &[&str] = &[
    "Partner", "President", "CEO", "Managing", "General", "Emeritus",
    "Visiting", "Head of", "Founder",
];

/// Decode common HTML entities in spider.cloud markdown output.
fn decode_entities(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
}

/// Build a PartnerRow from accumulated content lines and a slug.
fn build_partner(content: &[&str], slug: String) -> Option<PartnerRow> {
    let name = content.first().map(|s| decode_entities(s))?;
    if name.is_empty() {
        return None;
    }

    let title = content.get(1).and_then(|t| {
        let decoded = decode_entities(t);
        if TITLE_KEYWORDS.iter().any(|kw| decoded.contains(kw)) {
            Some(decoded)
        } else {
            None
        }
    });

    let bio_start = if title.is_some() { 2 } else { 1 };
    let bio = if content.len() > bio_start {
        Some(decode_entities(&content[bio_start..].join(" ")))
    } else {
        None
    };

    Some(PartnerRow {
        url: format!("/people/{}", slug),
        slug,
        name,
        title,
        bio,
    })
}

/// Parse the /people page markdown into PartnerRow entries.
///
/// Spider.cloud format: each partner is a multi-line markdown link block.
/// Blocks are either standalone (`[`...`](url)`) or chained (`](url)[`...`](url)`).
pub fn parse_partners_page(markdown: &str) -> Vec<PartnerRow> {
    let mut partners = Vec::new();
    let mut in_block = false;
    let mut content: Vec<&str> = Vec::new();

    for line in markdown.lines() {
        let trimmed = line.trim();

        // Check for closing ](url) or ](url)[
        if let Some(caps) = CLOSE_URL_RE.captures(trimmed) {
            if in_block {
                let slug = caps[1].to_string();
                if let Some(partner) = build_partner(&content, slug) {
                    if !partners.iter().any(|p: &PartnerRow| p.slug == partner.slug) {
                        partners.push(partner);
                    }
                }
                content.clear();
            }

            // If line ends with [, next block starts immediately
            let chains = caps.get(2).is_some_and(|m| !m.as_str().is_empty());
            in_block = chains;
            continue;
        }

        // Detect start of a block: standalone "[" line
        if trimmed == "[" {
            in_block = true;
            content.clear();
            continue;
        }

        if !in_block {
            continue;
        }

        // Skip bullet markers
        if trimmed == "*" || trimmed == "* " {
            continue;
        }

        if !trimmed.is_empty() {
            content.push(trimmed);
        }
    }

    partners
}

/// Search a company's raw markdown for /people/{slug} references.
/// Returns the deduplicated slugs of all partners found.
pub fn find_partner_urls_in_markdown(markdown: &str) -> Vec<String> {
    PEOPLE_URL_RE
        .captures_iter(markdown)
        .map(|c| c[1].to_string())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_spider_format_single() {
        let md = "[\n* \nGarry Tan\nPresident &amp; CEO\nSome bio text here.\n](https://www.ycombinator.com/people/garry-tan)";
        let partners = parse_partners_page(md);
        assert_eq!(partners.len(), 1);
        assert_eq!(partners[0].slug, "garry-tan");
        assert_eq!(partners[0].name, "Garry Tan");
        assert_eq!(partners[0].title.as_deref(), Some("President & CEO"));
        assert_eq!(partners[0].bio.as_deref(), Some("Some bio text here."));
    }

    #[test]
    fn parse_spider_format_chained() {
        let md = "[\n* \nGarry Tan\nPresident &amp; CEO\nBio 1.\n](https://www.ycombinator.com/people/garry-tan)[\n* \nJared Friedman\nManaging Partner\nBio 2.\n](https://www.ycombinator.com/people/jared-friedman)";
        let partners = parse_partners_page(md);
        assert_eq!(partners.len(), 2);
        assert_eq!(partners[0].slug, "garry-tan");
        assert_eq!(partners[1].slug, "jared-friedman");
        assert_eq!(partners[1].title.as_deref(), Some("Managing Partner"));
    }

    #[test]
    fn parse_three_way_chain() {
        let md = "[\n* \nA\nGeneral Partner\nBio A.\n](https://www.ycombinator.com/people/aaa)[\n* \nB\nManaging Partner\nBio B.\n](https://www.ycombinator.com/people/bbb)[\n* \nC\nGeneral Partner\nBio C.\n](https://www.ycombinator.com/people/ccc)";
        let partners = parse_partners_page(md);
        assert_eq!(partners.len(), 3);
        assert_eq!(partners[2].slug, "ccc");
    }

    #[test]
    fn decode_html_entities() {
        let md = "[\n* \nGarry Tan\nPresident &amp; CEO\nHe &amp; his team are &lt;great&gt;.\n](https://www.ycombinator.com/people/garry-tan)";
        let partners = parse_partners_page(md);
        assert_eq!(partners[0].title.as_deref(), Some("President & CEO"));
        assert_eq!(partners[0].bio.as_deref(), Some("He & his team are <great>."));
    }

    #[test]
    fn dedup_by_slug() {
        let md = "[\n* \nGarry Tan\nPresident &amp; CEO\n](https://www.ycombinator.com/people/garry-tan)\n[\n* \nGarry Tan\nPresident &amp; CEO\n](https://www.ycombinator.com/people/garry-tan)";
        let partners = parse_partners_page(md);
        assert_eq!(partners.len(), 1);
    }

    #[test]
    fn parse_real_fixture() {
        let md = std::fs::read_to_string("data/people_raw.md");
        if let Ok(md) = md {
            let partners = parse_partners_page(&md);
            assert!(partners.len() >= 10, "Expected 10+ partners, got {}", partners.len());
            assert_eq!(partners[0].slug, "garry-tan");
            assert!(partners[0].title.is_some());
            assert!(partners[0].bio.is_some());
        }
    }

    #[test]
    fn find_partner_urls() {
        let md = "Some text about /people/jared-friedman and also /people/garry-tan mentioned.";
        let slugs = find_partner_urls_in_markdown(md);
        assert!(slugs.contains(&"jared-friedman".to_string()));
        assert!(slugs.contains(&"garry-tan".to_string()));
    }

    #[test]
    fn no_false_positives() {
        let md = "Check out /companies/stripe and /batch/s09";
        let slugs = find_partner_urls_in_markdown(md);
        assert!(slugs.is_empty());
    }
}
