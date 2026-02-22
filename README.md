# Parsing YC

Scraping and structured data extraction pipeline for every Y Combinator company page. Three iterations — each version rebuilt from scratch based on what the previous one got wrong.

```
5,723 companies. 11,286 founders. 42,779 links. One SQLite file.
```

## The Evolution

### v1 — Proof of Concept

**Stack:** Python (Desync API) + Rust (8 sequential regex passes)

The first attempt. Python scrapes raw HTML via the Desync API, dumps it into SQLite, then a single Rust binary runs 8 sequential passes over the text (`pass1.rs` through `pass8.rs`). Each pass extracts one thing — slug, batch, status, tags, founders, news, links — using regex on raw scraped text.

**What worked:** Proved the end-to-end pipeline. Rust regex over raw text is fast enough.

**What didn't:** 8 passes on raw HTML is brittle. Order matters. Each pass operates on the full text, so later passes re-scan content that earlier passes already consumed. No parallelism. Python + Rust glue code is annoying to maintain.

```
Sitemaps → Python Desync scraper → raw text → 8 Rust regex passes → SQLite
```

### v2 — Better Architecture, Same Foundation

**Stack:** Python (Desync API, Typer, Pydantic) + Rust (WorkItem abstraction, optional Rayon)

Addressed v1's structural problems without changing the core approach. Introduced `WorkItem` to carry state through passes and `WorkingText` for progressive text consumption — each pass removes what it matched, so later passes work on a shrinking buffer instead of re-scanning everything. Split into two Rust binaries (companies + jobs). Added per-pass metrics, structured logging (`tracing`), and a proper Python CLI layer with Typer.

**What worked:** `WorkingText` eliminated redundant scanning. Metrics showed exactly where time went. Modular Python layer was much cleaner.

**What didn't:** Still scraping raw HTML. Still using Desync. Still two languages bolted together. The fundamental problem — regex on unstructured HTML — remained.

```
Sitemaps → Python Desync scraper → raw text → WorkItem pipeline → SQLite
```

### v3 — Clean Slate

**Stack:** Pure Rust. Single binary. No Python.

Threw out the Desync scraper and the HTML-first approach entirely. v3 uses [spider.cloud](https://spider.cloud) to fetch pages as **markdown** instead of HTML — which turns out to be the key insight. Markdown has predictable structure: headings, links, and text blocks that map directly to the data we want.

The parser is a 3-pass structural pipeline instead of 8 sequential regex passes:

1. **Lexer** — classifies every line into typed blocks (Heading, Link, Person, MetaField, etc.)
2. **Clusterer** — groups blocks into named sections by detecting structural transitions
3. **Extractors** — dedicated extractor per section type pulls structured rows

Scraping is async (tokio + spider-client, 10 concurrent, streaming results to DB via mpsc channel). Parsing is parallel (Rayon across 18 cores). Regex patterns compile once via `LazyLock` and share across threads.

**Result:** 5,723 pages scraped in 11 minutes, parsed in 8 seconds. Zero errors. 68 MB SQLite database with 9 normalized tables.

```
YC Sitemap → URL queue → spider.cloud (async, streaming) → markdown → Lexer → Clusterer → Extractors → SQLite
```

## Why Rust (and Why Not Python)

The scraping is the easy part. The hard part is parsing 5,700+ semi-structured pages into clean, normalized data — and that's where Python breaks down:

**SQLite concurrency.** Python's `sqlite3` module holds the GIL during writes. You can't read and write from multiple threads without serializing everything through a single connection. In practice this means your "concurrent" Python pipeline is sequential the moment it touches the database. Rust's `rusqlite` doesn't have this problem — Rayon threads parse in parallel, and SQLite's WAL mode handles the rest.

**Regex at scale.** Python's `re` module compiles patterns to bytecode interpreted by the regex engine. Rust's `regex` crate compiles to native DFA/NFA automata. On 5,700 pages with 8+ patterns each, that's the difference between "takes a few minutes" and "takes 8 seconds." Add `LazyLock` (compile once, share across threads) and there's zero redundant work.

**Memory control.** Python strings are heap-allocated objects with refcount overhead. When you're splitting, slicing, and matching across thousands of multi-KB markdown buffers, Python's garbage collector starts thrashing. Rust gives you `&str` slices — zero-copy views into the original buffer, no allocation, no GC pauses.

**No runtime surprises.** Python's duck typing means a `None` sneaking through at page 4,892 crashes the whole pipeline. Rust's type system catches that at compile time. When you're processing 5,700 pages, "it works on my test file" isn't enough — you need the compiler to guarantee every code path handles every case.

**Single binary, no venv.** v1 and v2 required Python 3.12+, a venv, pip dependencies, and a separate Rust build. v3 is `cargo build` and done. No dependency conflicts, no "works on my machine" — the Cargo.toml is the complete specification.

## Version Comparison

| | v1 | v2 | v3 |
|--|----|----|-----|
| Languages | Python + Rust | Python + Rust | Rust |
| Scraper | Desync API | Desync API | spider.cloud |
| Input format | Raw HTML | Raw HTML | Markdown |
| Parser | 8 regex passes | 7 passes + WorkingText | 3-pass AST (Lexer → Clusterer → Extractors) |
| Parallelism | None | Optional Rayon | Rayon (parse) + Tokio (scrape) |
| Parse speed | — | — | 681 pages/sec on 18 cores |
| DB writes | Batch after all passes | Batch after all passes | Streaming per-page via mpsc |

The biggest lever wasn't even Rust or parallelism — it was switching from HTML to markdown. Markdown gives you structure for free. You don't need to fight `<div>` soup to find a founder's name when it's just a line of text followed by social links.

## Quick Start (v3)

```bash
cd v3
export SPIDER_API_KEY="your-key"

cargo run -- init        # Fetch sitemap → 5,723 URLs
cargo run -- scrape      # Scrape all (streams to DB)
cargo run -- process     # Parse → structured tables
cargo run -- run         # Scrape + process in one shot
cargo run -- overview    # Company table (--status, --batch, -n)
cargo run -- stats       # Pipeline progress
```

## Project Layout

```
v1/                     Python + Rust, Desync API, 8-pass regex
v2/                     Python + Rust, Desync API, WorkItem pipeline
v3/                     Pure Rust, spider.cloud, 3-pass structural parser
```
