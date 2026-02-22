use regex::Regex;

use crate::db::CompanyRow;
use crate::parser::blocks::Block;
use crate::parser::sections::Section;

pub fn extract(slug: &str, url: &str, sections: &[Section]) -> CompanyRow {
    let header = find_section(sections, "header");
    let footer = find_section(sections, "footer_meta");
    let jobs = find_section(sections, "jobs");

    // Name + tagline: skip page title ("… | Y Combinator") and breadcrumbs ("…›…")
    let header_texts: Vec<&String> = header
        .iter()
        .flat_map(|s| &s.blocks)
        .filter_map(|b| match b {
            Block::Text(t)
                if !t.is_empty()
                    && !t.contains("| Y Combinator")
                    && !t.contains('›') =>
            {
                Some(t)
            }
            _ => None,
        })
        .collect();
    let name = header_texts.first().map(|t| t.to_string());
    let tagline = header_texts.get(1).map(|t| t.to_string());

    // Tags from TagLink blocks (anywhere)
    let all_tags: Vec<String> = sections
        .iter()
        .flat_map(|s| &s.blocks)
        .filter_map(|b| match b {
            Block::TagLink { tag, .. } => Some(tag.clone()),
            _ => None,
        })
        .collect();
    let tags = if all_tags.is_empty() {
        None
    } else {
        Some(all_tags.join(", "))
    };

    // Batch from Link containing ?batch=
    let batch_re = Regex::new(r"\?batch=([^)]+)").unwrap();
    let batch_raw = header
        .iter()
        .flat_map(|s| &s.blocks)
        .find_map(|b| match b {
            Block::Link { url, .. } => batch_re.captures(url).map(|c| c[1].replace("%20", " ")),
            _ => None,
        });
    let (batch_season, batch_year) = batch_raw
        .as_ref()
        .map(|b| parse_batch(b))
        .unwrap_or((None, None));

    // Status from StatusLine (anywhere in header or footer)
    let status = sections
        .iter()
        .flat_map(|s| &s.blocks)
        .find_map(|b| match b {
            Block::StatusLine(s) => Some(s.clone()),
            _ => None,
        });

    // Homepage: first external Link in header
    let homepage = header
        .iter()
        .flat_map(|s| &s.blocks)
        .find_map(|b| match b {
            Block::Link { url, .. }
                if url.starts_with("http") && !url.contains("ycombinator.com") =>
            {
                Some(url.clone())
            }
            _ => None,
        });

    // Footer MetaField values
    let founded_year = get_meta(footer, "Founded").and_then(|s| s.parse::<i32>().ok());
    let team_size =
        get_meta(footer, "Team Size").and_then(|s| s.replace(",", "").parse::<i32>().ok());
    let location = get_meta(footer, "Location");
    let batch_footer = get_meta(footer, "Batch");

    // Primary Partner
    let primary_partner = get_meta(footer, "Primary Partner");

    // Social links from footer bare Link blocks
    let social_links: Vec<&String> = footer
        .iter()
        .flat_map(|s| &s.blocks)
        .filter_map(|b| match b {
            Block::Link { url, text } if text.is_empty() && url.starts_with("http") => Some(url),
            _ => None,
        })
        .collect();

    let linkedin = social_links
        .iter()
        .find(|u| u.contains("linkedin.com"))
        .map(|u| u.to_string());
    let twitter = social_links
        .iter()
        .find(|u| u.contains("twitter.com") || u.contains("x.com"))
        .map(|u| u.to_string());
    let facebook = social_links
        .iter()
        .find(|u| u.contains("facebook.com"))
        .map(|u| u.to_string());
    let crunchbase = social_links
        .iter()
        .find(|u| u.contains("crunchbase.com"))
        .map(|u| u.to_string());
    let github = social_links
        .iter()
        .find(|u| u.contains("github.com"))
        .map(|u| u.to_string());

    // Job count from jobs section
    let job_count = jobs
        .map(|s| {
            s.blocks
                .iter()
                .filter(|b| {
                    matches!(b, Block::Link { url, text, .. } if url.contains("/jobs/") && !text.to_lowercase().contains("view all"))
                })
                .count() as i32
        })
        .unwrap_or(0);

    CompanyRow {
        slug: slug.to_string(),
        url: url.to_string(),
        name,
        tagline,
        batch: batch_raw.or(batch_footer),
        batch_season,
        batch_year,
        status,
        homepage,
        founded_year,
        team_size,
        location,
        primary_partner,
        tags,
        job_count,
        linkedin,
        twitter,
        facebook,
        crunchbase,
        github,
    }
}

fn find_section<'a>(sections: &'a [Section], kind: &str) -> Option<&'a Section> {
    sections.iter().find(|s| s.kind == kind)
}

fn get_meta(section: Option<&Section>, key: &str) -> Option<String> {
    section.and_then(|s| {
        s.blocks.iter().find_map(|b| match b {
            Block::MetaField { key: k, value } if k == key => Some(value.clone()),
            _ => None,
        })
    })
}

fn parse_batch(batch: &str) -> (Option<String>, Option<i32>) {
    let parts: Vec<&str> = batch.split_whitespace().collect();
    let season = parts.first().map(|s| s.to_string());
    let year = parts.last().and_then(|y| y.parse::<i32>().ok());
    (season, year)
}
