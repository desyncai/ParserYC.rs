use std::sync::LazyLock;

use regex::Regex;

use super::blocks::Block;

static DATE_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[A-Z][a-z]{2} \d{2}, \d{4}$").unwrap());

#[derive(Debug, Clone)]
pub struct Section {
    pub kind: String,
    pub blocks: Vec<Block>,
}

/// Cluster a flat Vec<Block> into named sections by structural transitions.
pub fn cluster_sections(blocks: &[Block]) -> Vec<Section> {
    let mut sections: Vec<Section> = Vec::new();
    let mut current_blocks: Vec<Block> = Vec::new();
    let mut current_kind = "header".to_string();
    for (i, block) in blocks.iter().enumerate() {
        if let Some(new_kind) = detect_transition(block, blocks, i, &current_kind) {
            if !current_blocks.is_empty() {
                sections.push(Section {
                    kind: current_kind,
                    blocks: std::mem::take(&mut current_blocks),
                });
            }
            current_kind = new_kind;
        }
        current_blocks.push(block.clone());
    }

    if !current_blocks.is_empty() {
        sections.push(Section {
            kind: current_kind,
            blocks: current_blocks,
        });
    }

    sections
}

fn detect_transition(
    block: &Block,
    all: &[Block],
    idx: usize,
    current_kind: &str,
) -> Option<String> {
    match block {
        // ### heading → description
        Block::Heading { level: 3, .. } => Some("description".to_string()),

        // Cluster of MetaField blocks (3+ consecutive, allowing gaps of Empty/StatusLine/bare Link)
        Block::MetaField { .. } if current_kind != "footer_meta" => {
            let meta_count = count_meta_cluster(all, idx);
            if meta_count >= 3 {
                Some("footer_meta".to_string())
            } else {
                None
            }
        }

        // First Person block starts "founders" section
        Block::Person { .. } if current_kind != "founders" => Some("founders".to_string()),

        // "Founders" / "Active Founders" / "Former Founders" text labels
        Block::Text(t)
            if (t == "Founders"
                || t == "Active Founders"
                || t == "Former Founders"
                || t == "Inactive Founders")
                && current_kind != "founders" =>
        {
            Some("founders".to_string())
        }

        // External news link followed by a date → first one starts "news"
        Block::Link { url, text, .. }
            if !text.is_empty()
                && !url.contains("ycombinator.com")
                && current_kind != "news"
                && current_kind != "jobs" =>
        {
            let has_date = all[idx + 1..]
                .iter()
                .find(|b| !matches!(b, Block::Empty))
                .map(|b| matches!(b, Block::Text(t) if DATE_RE.is_match(t.trim())))
                .unwrap_or(false);
            if has_date {
                Some("news".to_string())
            } else {
                None
            }
        }

        // Job link → starts "jobs"
        Block::Link { url, text, .. }
            if url.contains("/jobs/") && !text.is_empty() && current_kind != "jobs" =>
        {
            Some("jobs".to_string())
        }

        // "Latest News" text marker
        Block::Text(t) if t.contains("Latest News") && current_kind != "news" => {
            Some("news".to_string())
        }

        // "Jobs at" text marker
        Block::Text(t) if t.starts_with("Jobs at ") && current_kind != "jobs" => {
            Some("jobs".to_string())
        }

        // "View all jobs" link
        Block::Link { text, .. } if text.contains("View all jobs") && current_kind != "jobs" => {
            Some("jobs".to_string())
        }

        // "Company Launches" text marker
        Block::Text(t) if t.contains("Company Launches") => Some("launches".to_string()),

        _ => None,
    }
}

/// Count how many MetaField-like blocks appear consecutively from `start`,
/// allowing Empty, StatusLine, and bare Link gaps.
fn count_meta_cluster(blocks: &[Block], start: usize) -> usize {
    let mut meta_count = 0;
    for b in &blocks[start..] {
        match b {
            Block::MetaField { .. } => meta_count += 1,
            Block::StatusLine(_) | Block::Empty => {}
            Block::Link { text, .. } if text.is_empty() => {} // bare social links in footer
            _ => break,
        }
    }
    meta_count
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::blocks::classify_lines;

    fn section_kinds(md: &str) -> Vec<String> {
        let blocks = classify_lines(md);
        let sections = cluster_sections(&blocks);
        sections.iter().map(|s| s.kind.clone()).collect()
    }

    #[test]
    fn stripe_sections() {
        let md = std::fs::read_to_string("tests/fixtures/stripe.md").unwrap();
        let kinds = section_kinds(&md);
        assert!(kinds.contains(&"description".to_string()));
        assert!(kinds.contains(&"footer_meta".to_string()));
        assert!(kinds.contains(&"founders".to_string()));
        assert!(kinds.contains(&"news".to_string()));
        assert!(kinds.contains(&"jobs".to_string()));
    }

    #[test]
    fn groupahead_founders_before_footer() {
        let md = std::fs::read_to_string("tests/fixtures/groupahead.md").unwrap();
        let kinds = section_kinds(&md);
        // Both must exist regardless of order
        assert!(kinds.contains(&"founders".to_string()));
        assert!(kinds.contains(&"footer_meta".to_string()));
    }

    #[test]
    fn doordash_has_jobs() {
        let md = std::fs::read_to_string("tests/fixtures/doordash.md").unwrap();
        let blocks = classify_lines(&md);
        let sections = cluster_sections(&blocks);
        let jobs = sections.iter().find(|s| s.kind == "jobs");
        assert!(jobs.is_some());
        // Should have multiple job Link blocks
        let job_links: Vec<_> = jobs
            .unwrap()
            .blocks
            .iter()
            .filter(|b| matches!(b, Block::Link { url, .. } if url.contains("/jobs/")))
            .collect();
        assert!(job_links.len() >= 4);
    }

    #[test]
    fn unknown_sections_not_lost() {
        let blocks = classify_lines("Random paragraph\nthat matches nothing");
        let sections = cluster_sections(&blocks);
        assert!(!sections.is_empty());
    }
}
