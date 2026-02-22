use regex::Regex;

use crate::db::NewsRow;
use crate::parser::blocks::Block;
use crate::parser::sections::Section;

pub fn extract(slug: &str, sections: &[Section]) -> Vec<NewsRow> {
    let date_re = Regex::new(r"^[A-Z][a-z]{2} \d{2}, \d{4}$").unwrap();
    let mut items = Vec::new();

    for section in sections.iter().filter(|s| s.kind == "news") {
        let blocks = &section.blocks;
        let mut i = 0;
        while i < blocks.len() {
            if let Block::Link { text, url, .. } = &blocks[i] {
                if !text.is_empty() && !url.contains("ycombinator.com") {
                    // Look ahead for date
                    let published = blocks[i + 1..]
                        .iter()
                        .find(|b| !matches!(b, Block::Empty))
                        .and_then(|b| match b {
                            Block::Text(t) if date_re.is_match(t.trim()) => {
                                Some(t.trim().to_string())
                            }
                            _ => None,
                        });
                    items.push(NewsRow {
                        company_slug: slug.to_string(),
                        title: text.clone(),
                        url: url.clone(),
                        published,
                    });
                }
            }
            i += 1;
        }
    }

    items
}
