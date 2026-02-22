use anyhow::{Context, Result};
use regex::Regex;
use tracing::info;

const COMPANIES_SITEMAP_URL: &str = "https://www.ycombinator.com/companies/sitemap";
const COMPANY_PATTERN: &str =
    r"^https://www\.ycombinator\.com/companies/([a-zA-Z0-9][a-zA-Z0-9_-]*)$";

/// Fetch the YC companies sitemap and return filtered (url, slug) pairs.
pub async fn fetch_company_urls() -> Result<Vec<(String, String)>> {
    let client = reqwest::Client::new();
    let re = Regex::new(COMPANY_PATTERN)?;

    info!("Fetching companies sitemap: {}", COMPANIES_SITEMAP_URL);
    let xml = client
        .get(COMPANIES_SITEMAP_URL)
        .send()
        .await?
        .text()
        .await
        .context("Failed to fetch companies sitemap")?;

    let all_urls = parse_urlset(&xml)?;
    info!("Total URLs in sitemap: {}", all_urls.len());

    // Filter to company pages only (exclude /industry/, /location/, /batch/, etc.)
    let filtered: Vec<(String, String)> = all_urls
        .into_iter()
        .filter_map(|url| {
            let slug = re.captures(&url)?.get(1)?.as_str().to_string();
            Some((url, slug))
        })
        .collect();

    info!("Company pages after filtering: {}", filtered.len());
    Ok(filtered)
}

/// Parse a urlset XML and return all <loc> URLs.
fn parse_urlset(xml: &str) -> Result<Vec<String>> {
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut urls = Vec::new();
    let mut in_url = false;
    let mut in_loc = false;
    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(quick_xml::events::Event::Start(e)) => match e.name().as_ref() {
                b"url" => in_url = true,
                b"loc" if in_url => in_loc = true,
                _ => {}
            },
            Ok(quick_xml::events::Event::Text(e)) if in_loc => {
                urls.push(e.unescape()?.to_string());
            }
            Ok(quick_xml::events::Event::End(e)) => match e.name().as_ref() {
                b"loc" => in_loc = false,
                b"url" => in_url = false,
                _ => {}
            },
            Ok(quick_xml::events::Event::Eof) => break,
            Err(e) => return Err(e.into()),
            _ => {}
        }
        buf.clear();
    }
    Ok(urls)
}
