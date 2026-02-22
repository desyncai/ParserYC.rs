use std::collections::HashSet;
use std::sync::LazyLock;

use regex::Regex;

static HEADING_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^(#{1,6})\s+(.+)$").unwrap());
static SINGLE_LINK_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\[([^\]]*)\]\(([^)]+)\)$").unwrap());
static INLINE_LINKS_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\[([^\]]*)\]\(([^)]+)\)").unwrap());
static CLOSE_LINK_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\]\(([^)]+)\)(.*)$").unwrap());
static META_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^([A-Z][A-Za-z ]{1,22}):(.*)$").unwrap());
static TAG_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"/companies/(industry|location)/").unwrap());
static URL_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\((https?://[^)]+)\)").unwrap());
static DOMAIN_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"https?://(?:www\.)?([^/]+)").unwrap());

#[derive(Debug, Clone)]
pub enum Block {
    Heading { level: u8, text: String },
    Link { text: String, url: String },
    TagLink { tag: String, url: String },
    MetaField { key: String, value: String },
    StatusLine(String),
    Person {
        name: String,
        title: Option<String>,
        bio: Option<String>,
        links: Vec<(String, String)>, // (domain, url)
    },
    Text(String),
    Empty,
}

const STATUS_KEYWORDS: &[&str] = &["Active", "Public", "Acquired", "Inactive"];
const TITLE_KEYWORDS: &[&str] = &["Founder", "CEO", "CTO", "COO", "Co-", "President", "Partner"];

pub fn classify_lines(markdown: &str) -> Vec<Block> {
    if markdown.trim().is_empty() {
        return vec![Block::Empty];
    }

    let lines: Vec<&str> = markdown.lines().collect();
    let mut blocks = Vec::with_capacity(lines.len());
    let mut seen_names: HashSet<String> = HashSet::new();
    let mut i = 0;

    while i < lines.len() {
        let line = lines[i].trim();

        if line.is_empty() {
            blocks.push(Block::Empty);
            i += 1;
            continue;
        }

        // ── Multi-line link: line is "[" or starts a sequence [\ntext\n](url) ──
        if line == "[" {
            i = consume_multiline_link(&lines, i, &mut blocks);
            continue;
        }

        // ── Continuation link: ](url) possibly with trailing [ ──
        if line.starts_with("](") {
            if let Some(caps) = CLOSE_LINK_RE.captures(line) {
                // This is a stray ](url) — emit as bare link
                emit_link("", &caps[1], &mut blocks);
                let rest = caps[2].trim();
                if rest == "[" {
                    i = consume_multiline_link(&lines, i + 1, &mut blocks);
                    continue;
                }
            }
            i += 1;
            continue;
        }

        // ── Heading: ### text ──
        if let Some(caps) = HEADING_RE.captures(line) {
            blocks.push(Block::Heading {
                level: caps[1].len() as u8,
                text: caps[2].to_string(),
            });
            i += 1;
            continue;
        }

        // ── Single link on the line: [text](url) ──
        if SINGLE_LINK_RE.is_match(line) {
            let caps = SINGLE_LINK_RE.captures(line).unwrap();
            emit_link(&caps[1], &caps[2], &mut blocks);
            i += 1;
            continue;
        }

        // ── Line with multiple inline links: [](url1)[](url2) or ending with [ ──
        if line.contains("](") && line.contains('[') {
            // Extract all links on this line
            for caps in INLINE_LINKS_RE.captures_iter(line) {
                emit_link(&caps[1], &caps[2], &mut blocks);
            }
            // If line ends with [, next link is multi-line
            if line.ends_with('[') {
                i = consume_multiline_link(&lines, i + 1, &mut blocks);
                continue;
            }
            i += 1;
            continue;
        }

        // ── Status line ──
        if STATUS_KEYWORDS.contains(&line) {
            blocks.push(Block::StatusLine(line.to_string()));
            i += 1;
            continue;
        }

        // ── Meta field: Key:Value or Key: (empty value) ──
        if let Some(caps) = META_RE.captures(line) {
            blocks.push(Block::MetaField {
                key: caps[1].trim().to_string(),
                value: caps[2].trim().to_string(),
            });
            i += 1;
            continue;
        }

        // ── Person detection ──
        if line.len() < 60
            && !line.contains("](")
            && !line.contains(':')
            && !line.contains('›')
            && !line.starts_with("[>")
            && !is_date_like(line)
            && !is_noise_line(line)
            && line.split_whitespace().count() <= 6
        {
            if let Some((person, consumed)) =
                try_parse_person(&lines, i, &mut seen_names)
            {
                blocks.push(person);
                i += consumed;
                continue;
            }
        }

        // ── Plain text ──
        blocks.push(Block::Text(line.to_string()));
        i += 1;
    }

    blocks
}

/// Consume a multi-line link starting at `start` (which should be a "[" line or
/// the line after a trailing "["). Reads text lines until ](url).
/// Returns the next line index to process.
fn consume_multiline_link(
    lines: &[&str],
    start: usize,
    blocks: &mut Vec<Block>,
) -> usize {
    let mut text_parts = Vec::new();
    let mut j = start;

    // If current line is "[", skip it
    if j < lines.len() && lines[j].trim() == "[" {
        j += 1;
    }

    // Collect text until ](url)
    while j < lines.len() {
        let l = lines[j].trim();
        if let Some(url_part) = l.strip_prefix("](") {
            let (url, has_trailing_open) = if let Some(end) = url_part.find(')') {
                let u = &url_part[..end];
                let rest = url_part[end + 1..].trim();
                (u, rest == "[" || rest.ends_with('['))
            } else {
                (url_part.trim_end_matches(')'), false)
            };

            let text = text_parts.join(" ");
            emit_link(&text, url, blocks);

            if has_trailing_open {
                return consume_multiline_link(lines, j + 1, blocks);
            }
            return j + 1;
        }
        text_parts.push(l);
        j += 1;
    }

    // Never found closing — push as text
    for part in text_parts {
        blocks.push(Block::Text(part.to_string()));
    }
    j
}

fn emit_link(text: &str, url: &str, blocks: &mut Vec<Block>) {
    if TAG_RE.is_match(url) {
        let tag = url.rsplit('/').next().unwrap_or("").replace("%20", " ");
        blocks.push(Block::TagLink {
            tag,
            url: url.to_string(),
        });
    } else {
        blocks.push(Block::Link {
            text: text.to_string(),
            url: url.to_string(),
        });
    }
}

fn try_parse_person(
    lines: &[&str],
    start: usize,
    seen: &mut HashSet<String>,
) -> Option<(Block, usize)> {
    let name = lines[start].trim().to_string();

    if seen.contains(&name) {
        let consumed = skip_person_block(lines, start);
        return Some((Block::Empty, consumed));
    }

    let mut j = start + 1;
    let mut person_links = Vec::new();

    // Collect social links: only BARE links [](url) or ](url), not [Title](url)
    while j < lines.len() {
        let l = lines[j].trim();
        if l.is_empty() {
            j += 1;
            continue;
        }
        // Bare link: [](url) or starts with ]( (continuation)
        // Also handle angle-bracket URLs: [](<url>)
        let is_bare = (l.starts_with("[](") || l.starts_with("]("))
            || (l.starts_with('[') && !l.contains(|c: char| c.is_alphabetic()));
        if is_bare {
            // Strip angle brackets from URLs like <https://...>
            let cleaned = l.replace(['<', '>'], "");
            for cap in URL_RE.captures_iter(&cleaned) {
                let url = cap[1].to_string();
                let domain = DOMAIN_RE
                    .captures(&url)
                    .map(|c| c[1].to_string())
                    .unwrap_or_default();
                person_links.push((domain, url));
            }
            j += 1;
            continue;
        }
        break;
    }

    // Accept person if they have social links OR a recognized title on the next line
    if person_links.is_empty() {
        let next_is_title = j < lines.len()
            && TITLE_KEYWORDS.iter().any(|kw| lines[j].trim().contains(kw));
        if !next_is_title {
            return None;
        }
    }

    let title = if j < lines.len() {
        let t = lines[j].trim();
        if TITLE_KEYWORDS.iter().any(|kw| t.contains(kw)) {
            j += 1;
            Some(t.to_string())
        } else {
            None
        }
    } else {
        None
    };

    let mut bio_parts = Vec::new();
    while j < lines.len() {
        let l = lines[j].trim();
        if l.is_empty() || l.starts_with('[') || l.starts_with('#') {
            break;
        }
        if l.len() < 60 && !l.contains("](") && seen.contains(l) {
            break;
        }
        bio_parts.push(l.to_string());
        j += 1;
    }
    let bio = if bio_parts.is_empty() {
        None
    } else {
        Some(bio_parts.join(" "))
    };

    seen.insert(name.clone());

    Some((
        Block::Person {
            name,
            title,
            bio,
            links: person_links,
        },
        j - start,
    ))
}

fn is_date_like(s: &str) -> bool {
    // "May 07, 2023", "Nov 20, 2022", "Dec 01, 2025"
    const MONTHS: &[&str] = &[
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let trimmed = s.trim();
    MONTHS.iter().any(|m| trimmed.starts_with(m))
        && trimmed.as_bytes().last().is_some_and(|c| c.is_ascii_digit())
}

fn is_noise_line(s: &str) -> bool {
    let lower = s.to_lowercase();
    // Section headers, metrics, navigation fragments, media placeholders
    lower == "latest news"
        || lower.starts_with("jobs at ")
        || lower.contains("view all")
        || lower.ends_with("+ years")
        || lower.ends_with("+ employees")
        || lower.starts_with("company launches")
        || lower.starts_with("active founders")
        || lower.starts_with("former founders")
        || lower == "founders"
        || lower == "inactive founders"
        || lower.starts_with("yc ")  // "YC Photos", "YC Summer 2018 Demo Day Video"
        || lower.contains("demo day")
        || s.chars().all(|c| c.is_ascii_digit() || c == ',' || c == ' ')
}

fn skip_person_block(lines: &[&str], start: usize) -> usize {
    let mut j = start + 1;
    while j < lines.len() {
        let l = lines[j].trim();
        if l.is_empty() {
            j += 1;
            continue;
        }
        if l.starts_with('[') || l.starts_with("](") || l.contains("](") {
            j += 1;
            continue;
        }
        break;
    }
    if j < lines.len() && TITLE_KEYWORDS.iter().any(|kw| lines[j].trim().contains(kw)) {
        j += 1;
    }
    while j < lines.len() && !lines[j].trim().is_empty() {
        if lines[j].trim().starts_with('[') || lines[j].trim().starts_with('#') {
            break;
        }
        j += 1;
    }
    j - start
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heading() {
        let blocks = classify_lines("### Some heading text");
        assert!(matches!(&blocks[0], Block::Heading { level: 3, text } if text == "Some heading text"));
    }

    #[test]
    fn link() {
        let blocks = classify_lines("[Stripe](https://stripe.com)");
        assert!(matches!(&blocks[0], Block::Link { text, url } if text == "Stripe" && url == "https://stripe.com"));
    }

    #[test]
    fn tag_link() {
        let blocks = classify_lines("[Fintech](https://www.ycombinator.com/companies/industry/Fintech)");
        assert!(matches!(&blocks[0], Block::TagLink { tag, .. } if tag == "Fintech"));
    }

    #[test]
    fn meta_field() {
        let blocks = classify_lines("Founded:2009");
        assert!(matches!(&blocks[0], Block::MetaField { key, value } if key == "Founded" && value == "2009"));
    }

    #[test]
    fn meta_field_empty_value() {
        let blocks = classify_lines("Status:");
        assert!(matches!(&blocks[0], Block::MetaField { key, value } if key == "Status" && value.is_empty()));
    }

    #[test]
    fn status_line() {
        for kw in STATUS_KEYWORDS {
            let blocks = classify_lines(kw);
            assert!(matches!(&blocks[0], Block::StatusLine(s) if s == kw));
        }
    }

    #[test]
    fn empty_string() {
        let blocks = classify_lines("");
        assert_eq!(blocks.len(), 1);
        assert!(matches!(&blocks[0], Block::Empty));
    }

    #[test]
    fn empty_line() {
        let blocks = classify_lines("text\n\nmore");
        assert!(matches!(&blocks[1], Block::Empty));
    }

    #[test]
    fn multiline_link() {
        let md = "[\nSummer 2009\n](https://example.com?batch=Summer%202009)";
        let blocks = classify_lines(md);
        let links: Vec<_> = blocks.iter().filter(|b| matches!(b, Block::Link { .. })).collect();
        assert_eq!(links.len(), 1);
        if let Block::Link { text, .. } = &links[0] {
            assert_eq!(text, "Summer 2009");
        }
    }

    #[test]
    fn person_detection() {
        let md = "Patrick Collison\n[](https://twitter.com/patrickc)\n[](https://www.linkedin.com/in/patrickcollison/)\nFounder/CEO";
        let blocks = classify_lines(md);
        let persons: Vec<_> = blocks.iter().filter(|b| matches!(b, Block::Person { .. })).collect();
        assert_eq!(persons.len(), 1);
        if let Block::Person { name, title, .. } = &persons[0] {
            assert_eq!(name, "Patrick Collison");
            assert_eq!(title.as_deref(), Some("Founder/CEO"));
        }
    }

    #[test]
    fn person_dedup() {
        let md = "Patrick Collison\n[](https://twitter.com/patrickc)\nFounder/CEO\n\nPatrick Collison\n[](https://twitter.com/patrickc)\nFounder/CEO";
        let blocks = classify_lines(md);
        let persons: Vec<_> = blocks.iter().filter(|b| matches!(b, Block::Person { .. })).collect();
        assert_eq!(persons.len(), 1);
    }

    #[test]
    fn stripe_fixture() {
        let md = std::fs::read_to_string("tests/fixtures/stripe.md").unwrap();
        let blocks = classify_lines(&md);
        assert!(blocks.iter().any(|b| matches!(b, Block::Heading { .. })));
        assert!(blocks.iter().any(|b| matches!(b, Block::MetaField { .. })));
        assert!(blocks.iter().any(|b| matches!(b, Block::Person { .. })));
        let persons: Vec<_> = blocks.iter().filter(|b| matches!(b, Block::Person { .. })).collect();
        assert_eq!(persons.len(), 2, "Expected Patrick + John Collison, got: {:?}", persons);
    }

    #[test]
    fn groupahead_fixture() {
        let md = std::fs::read_to_string("tests/fixtures/groupahead.md").unwrap();
        let blocks = classify_lines(&md);
        let persons: Vec<_> = blocks.iter().filter(|b| matches!(b, Block::Person { .. })).collect();
        assert_eq!(persons.len(), 2, "Expected Brian Glick + Julian Frumar, got: {:?}", persons);
        for b in &blocks {
            if let Block::Person { name, .. } = b {
                assert!(!name.contains("Batch"), "Batch leaked into person: {}", name);
            }
        }
    }

    #[test]
    fn doordash_fixture() {
        let md = std::fs::read_to_string("tests/fixtures/doordash.md").unwrap();
        let blocks = classify_lines(&md);
        let persons: Vec<_> = blocks.iter().filter(|b| matches!(b, Block::Person { .. })).collect();
        assert_eq!(persons.len(), 3, "Expected Tony Xu + Andy Fang + Stanley Tang, got: {:?}", persons);
    }
}
