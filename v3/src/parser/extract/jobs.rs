use regex::Regex;

use crate::db::JobRow;
use crate::parser::blocks::Block;
use crate::parser::sections::Section;

pub fn extract(slug: &str, sections: &[Section]) -> Vec<JobRow> {
    let salary_re = Regex::new(r"^\$[\d,]+K?\s*-\s*\$[\d,]+K?").unwrap();
    let exp_re = Regex::new(r"^\d+\+?\s*years?$").unwrap();
    let apply_re = Regex::new(r"\[Apply Now[^\]]*\]\(([^)]+)\)").unwrap();
    let mut items = Vec::new();

    for section in sections.iter().filter(|s| s.kind == "jobs") {
        let blocks = &section.blocks;
        let mut i = 0;

        while i < blocks.len() {
            if let Block::Link { text, url, .. } = &blocks[i] {
                if url.contains("/jobs/")
                    && !text.is_empty()
                    && !text.to_lowercase().contains("view all")
                {
                    let mut location = None;
                    let mut salary = None;
                    let mut experience = None;
                    let mut apply_url = None;

                    // Scan ahead for metadata (up to 6 blocks)
                    let mut j = i + 1;
                    while j < blocks.len() && j < i + 7 {
                        match &blocks[j] {
                            Block::Empty => {}
                            Block::Text(t) => {
                                let t = t.trim();
                                if let Some(caps) = apply_re.captures(t) {
                                    apply_url = Some(caps[1].to_string());
                                    j += 1;
                                    break;
                                } else if salary_re.is_match(t) {
                                    salary = Some(t.to_string());
                                } else if exp_re.is_match(t) {
                                    experience = Some(t.to_string());
                                } else {
                                    location = Some(t.to_string());
                                }
                            }
                            Block::Link { url: u, text: t, .. }
                                if t.contains("Apply Now") || u.contains("workatastartup") =>
                            {
                                apply_url = Some(u.clone());
                                j += 1;
                                break;
                            }
                            Block::Link { url: u, .. } if u.contains("/jobs/") => break,
                            _ => break,
                        }
                        j += 1;
                    }

                    items.push(JobRow {
                        company_slug: slug.to_string(),
                        title: text.clone(),
                        url: url.clone(),
                        location,
                        salary,
                        experience,
                        apply_url,
                    });

                    i = j;
                    continue;
                }
            }
            i += 1;
        }
    }

    items
}
