pub mod company;
pub mod founders;
pub mod jobs;
pub mod links;
pub mod meetings;
pub mod news;

use super::sections::Section;
use crate::db::*;

pub struct ExtractedData {
    pub sections: SectionRow,
    pub company: CompanyRow,
    pub founders: Vec<FounderRow>,
    pub news: Vec<NewsRow>,
    pub jobs: Vec<JobRow>,
    pub links: Vec<LinkRow>,
    pub meeting_links: Vec<MeetingLinkRow>,
}

pub fn extract_all(
    slug: &str,
    url: &str,
    page_data_id: i64,
    sections: &[Section],
) -> ExtractedData {
    let company = company::extract(slug, url, sections);
    let founder_rows = founders::extract(slug, sections);
    let news_rows = news::extract(slug, sections);
    let job_rows = jobs::extract(slug, sections);
    let link_rows = links::extract(slug, sections);
    let meeting_rows = meetings::extract(slug, sections);
    let section_row = build_section_row(slug, url, page_data_id, sections);

    ExtractedData {
        sections: section_row,
        company,
        founders: founder_rows,
        news: news_rows,
        jobs: job_rows,
        links: link_rows,
        meeting_links: meeting_rows,
    }
}

fn build_section_row(slug: &str, url: &str, page_data_id: i64, sections: &[Section]) -> SectionRow {
    let get_raw = |kind: &str| -> Option<String> {
        sections
            .iter()
            .find(|s| s.kind == kind)
            .map(section_to_text)
            .filter(|t| !t.is_empty())
    };

    // Collect unknown sections as JSON extras
    let unknowns: Vec<_> = sections
        .iter()
        .filter(|s| {
            !matches!(
                s.kind.as_str(),
                "header"
                    | "description"
                    | "news"
                    | "jobs"
                    | "launches"
                    | "footer_meta"
                    | "founders"
            )
        })
        .map(|s| serde_json::json!({ "kind": s.kind, "text": section_to_text(s) }))
        .collect();
    let extras = if unknowns.is_empty() {
        None
    } else {
        Some(serde_json::to_string(&unknowns).unwrap_or_default())
    };

    SectionRow {
        page_data_id,
        slug: slug.to_string(),
        url: url.to_string(),
        navbar: get_raw("header"), // header section contains navbar data
        header: get_raw("header"),
        description: get_raw("description"),
        news: get_raw("news"),
        jobs: get_raw("jobs"),
        footer: get_raw("footer_meta"),
        founders_raw: get_raw("founders"),
        launches: get_raw("launches"),
        extras,
    }
}

fn section_to_text(section: &Section) -> String {
    use super::blocks::Block;
    section
        .blocks
        .iter()
        .map(|b| match b {
            Block::Empty => String::new(),
            Block::Text(t) => t.clone(),
            Block::Heading { text, level } => format!("{} {}", "#".repeat(*level as usize), text),
            Block::Link { text, url } => {
                if text.is_empty() {
                    format!("[]({})", url)
                } else {
                    format!("[{}]({})", text, url)
                }
            }
            Block::TagLink { tag, url } => format!("[{}]({})", tag, url),
            Block::MetaField { key, value } => format!("{}:{}", key, value),
            Block::StatusLine(s) => s.clone(),
            Block::Person { name, title, .. } => {
                let t = title.as_deref().unwrap_or("");
                format!("{} — {}", name, t)
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string()
}

// ── Tests ──

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::blocks::classify_lines;
    use crate::parser::sections::cluster_sections;

    fn parse(fixture: &str) -> Vec<Section> {
        let md = std::fs::read_to_string(format!("tests/fixtures/{}.md", fixture)).unwrap();
        let blocks = classify_lines(&md);
        cluster_sections(&blocks)
    }

    #[test]
    fn stripe_company() {
        let sections = parse("stripe");
        let c = company::extract("stripe", "https://www.ycombinator.com/companies/stripe", &sections);
        assert_eq!(c.name.as_deref(), Some("Stripe"));
        assert_eq!(c.status.as_deref(), Some("Active"));
        assert_eq!(c.team_size, Some(7000));
        assert_eq!(c.founded_year, Some(2009));
        assert!(c.linkedin.is_some());
        assert!(c.github.is_some());
    }

    #[test]
    fn stripe_founders() {
        let sections = parse("stripe");
        let f = founders::extract("stripe", &sections);
        assert_eq!(f.len(), 2);
        let names: Vec<&str> = f.iter().map(|x| x.name.as_str()).collect();
        assert!(names.contains(&"Patrick Collison"));
        assert!(names.contains(&"John Collison"));
    }

    #[test]
    fn doordash_news() {
        let sections = parse("doordash");
        let n = news::extract("doordash", &sections);
        assert!(n.len() >= 3);
        assert!(n.iter().all(|x| !x.url.contains("ycombinator.com")));
    }

    #[test]
    fn doordash_jobs() {
        let sections = parse("doordash");
        let j = jobs::extract("doordash", &sections);
        assert!(j.len() >= 4);
        assert!(j.iter().any(|x| x.salary.is_some()));
    }

    #[test]
    fn groupahead_no_news_or_jobs() {
        let sections = parse("groupahead");
        assert!(news::extract("groupahead", &sections).is_empty());
        assert!(jobs::extract("groupahead", &sections).is_empty());
    }

    #[test]
    fn groupahead_founders_clean() {
        let sections = parse("groupahead");
        let f = founders::extract("groupahead", &sections);
        // No "Batch:Winter 2015" contamination
        assert!(f.iter().all(|x| !x.name.contains("Batch")));
        let names: Vec<&str> = f.iter().map(|x| x.name.as_str()).collect();
        assert!(names.contains(&"Brian Glick"));
        assert!(names.contains(&"Julian Frumar"));
    }
}
