use std::collections::HashSet;

use crate::db::MeetingLinkRow;
use crate::parser::blocks::Block;
use crate::parser::sections::Section;

const MEETING_DOMAINS: &[(&str, &str)] = &[
    ("calendly.com", "calendly"),
    ("cal.com", "cal.com"),
    ("usemotion.com", "motion"),
    ("meetings.hubspot.com", "hubspot"),
    ("outlook.office365.com/owa/calendar", "outlook"),
    ("outlook.office.com/bookings", "outlook"),
    ("book.vimcal.com", "vimcal"),
    ("savvycal.com", "savvycal"),
    ("tidycal.com", "tidycal"),
    ("koalendar.com", "koalendar"),
    ("zcal.co", "zcal"),
    ("doodle.com", "doodle"),
    ("youcanbook.me", "youcanbook"),
    ("acuityscheduling.com", "acuity"),
    ("appointlet.com", "appointlet"),
    ("chili-piper.com", "chili-piper"),
    ("reclaim.ai", "reclaim"),
    ("cronify.com", "cronify"),
];

pub fn extract(slug: &str, sections: &[Section]) -> Vec<MeetingLinkRow> {
    let mut seen = HashSet::new();
    let mut rows = Vec::new();

    for section in sections {
        for block in &section.blocks {
            let urls: Vec<&str> = match block {
                Block::Link { url, .. } => vec![url.as_str()],
                Block::Person { links, .. } => links.iter().map(|(_, u)| u.as_str()).collect(),
                _ => continue,
            };

            for url in urls {
                if seen.contains(url) {
                    continue;
                }
                if let Some(link_type) = classify_meeting_url(url) {
                    seen.insert(url.to_string());
                    let domain = extract_domain(url);
                    rows.push(MeetingLinkRow {
                        company_slug: slug.to_string(),
                        url: url.to_string(),
                        domain,
                        link_type: link_type.to_string(),
                    });
                }
            }
        }
    }

    rows
}

fn classify_meeting_url(url: &str) -> Option<&'static str> {
    MEETING_DOMAINS
        .iter()
        .find(|(domain, _)| url.contains(domain))
        .map(|(_, kind)| *kind)
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
