use crate::db::FounderRow;
use crate::parser::blocks::Block;
use crate::parser::sections::Section;

pub fn extract(slug: &str, sections: &[Section]) -> Vec<FounderRow> {
    let mut founders = Vec::new();
    let mut is_active = true;

    for section in sections.iter().filter(|s| s.kind == "founders") {
        for block in &section.blocks {
            match block {
                Block::Text(t) if t.contains("Former") || t.contains("Inactive") => {
                    is_active = false;
                }
                Block::Text(t) if t.contains("Active Founders") || t == "Founders" => {
                    is_active = true;
                }
                Block::Person {
                    name,
                    title,
                    bio,
                    links,
                } => {
                    founders.push(FounderRow {
                        company_slug: slug.to_string(),
                        name: name.clone(),
                        title: title.clone(),
                        bio: bio.clone(),
                        is_active,
                        linkedin: find_link(links, "linkedin.com"),
                        twitter: find_link(links, "twitter.com")
                            .or_else(|| find_link(links, "x.com")),
                    });
                }
                _ => {}
            }
        }
    }

    founders
}

fn find_link(links: &[(String, String)], domain_pattern: &str) -> Option<String> {
    links
        .iter()
        .find(|(domain, _)| domain.contains(domain_pattern))
        .map(|(_, url)| url.clone())
}
