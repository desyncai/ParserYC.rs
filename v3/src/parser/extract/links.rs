use std::collections::HashSet;

use crate::db::LinkRow;
use crate::parser::blocks::Block;
use crate::parser::sections::Section;

pub fn extract(slug: &str, sections: &[Section]) -> Vec<LinkRow> {
    let mut seen = HashSet::new();
    let mut links = Vec::new();

    for section in sections {
        for block in &section.blocks {
            if let Block::Link { url, .. } = block {
                if url.contains("ycombinator.com") || seen.contains(url) {
                    continue;
                }
                seen.insert(url.clone());
                let domain = extract_domain(url);
                let link_type = classify_domain(&domain);
                links.push(LinkRow {
                    company_slug: slug.to_string(),
                    url: url.clone(),
                    domain,
                    link_type,
                });
            }
            // Also extract links from Person blocks
            if let Block::Person { links: plinks, .. } = block {
                for (_, url) in plinks {
                    if url.contains("ycombinator.com") || seen.contains(url) {
                        continue;
                    }
                    seen.insert(url.clone());
                    let domain = extract_domain(url);
                    let link_type = classify_domain(&domain);
                    links.push(LinkRow {
                        company_slug: slug.to_string(),
                        url: url.clone(),
                        domain,
                        link_type,
                    });
                }
            }
        }
    }

    links
}

fn extract_domain(url: &str) -> String {
    url.split("//")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or("")
        .trim_start_matches("www.")
        .to_string()
}

fn classify_domain(domain: &str) -> Option<String> {
    match domain {
        d if d.contains("linkedin.com") => Some("linkedin".into()),
        d if d.contains("twitter.com") || d.contains("x.com") => Some("twitter".into()),
        d if d.contains("facebook.com") => Some("facebook".into()),
        d if d.contains("crunchbase.com") => Some("crunchbase".into()),
        d if d.contains("github.com") => Some("github".into()),
        d if d.contains("glassdoor.com") => Some("glassdoor".into()),
        d if d.contains("youtube.com") => Some("youtube".into()),
        d if d.contains("instagram.com") => Some("instagram".into()),
        _ => None,
    }
}
