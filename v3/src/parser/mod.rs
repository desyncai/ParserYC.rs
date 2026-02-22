pub mod blocks;
pub mod extract;
pub mod sections;

use crate::db::ScrapedPage;
use extract::ExtractedData;

/// Three-pass pipeline: markdown → blocks → sections → extracted data.
pub fn process_page(page: &ScrapedPage) -> ExtractedData {
    let blocks = blocks::classify_lines(&page.markdown);
    let sections = sections::cluster_sections(&blocks);
    extract::extract_all(&page.slug, &page.url, page.page_data_id, &sections)
}
