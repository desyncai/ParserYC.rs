# Parsing YC

```
5,723 companies. 11,286 founders. 42,779 links. One SQLite file.
```

## Why This Exists

This started as a class project in November 2025 — a [group assignment](https://github.com/MDaly27/Desync-Data-Processing-Extension) to scrape and process data from the Y Combinator company directory. The original goal was straightforward: pull every company page, extract structured fields, store them in SQLite.

The class version worked. But it left me wondering how far the pipeline could actually go — how fast, how clean, how reliable. So I kept iterating. Three versions later, I'd rewritten the entire system twice, dropped Python entirely, switched from HTML to markdown, and built a structural parser that processes 5,700+ pages in 8 seconds with zero errors.

What started as curiosity about the data became an engineering challenge about the pipeline itself.

## The Evolution

### [v1](v1/) — The Class Project

**Stack:** Python + Rust | **When:** November 2025

The [original version](https://github.com/MDaly27/Desync-Data-Processing-Extension). Python scrapes raw HTML via the Desync API, dumps it into SQLite, then a single Rust binary runs 8 sequential regex passes over the text — `pass1.rs` through `pass8.rs`. Each pass extracts one thing: slug, batch, status, tags, founders, news, links.

**What worked:** Proved the end-to-end pipeline. Rust regex over raw text is fast enough. The basic structure — scrape, store, parse — was right.

**What didn't:** 8 passes on raw HTML is brittle. Order matters. Each pass operates on the full text, so later passes re-scan content that earlier passes already consumed. No parallelism. Python + Rust glue code is annoying to maintain.

```
Sitemaps → Python Desync scraper → raw text → 8 Rust regex passes → SQLite
```

### [v2](v2/) — The Refactor

**Stack:** Python + Rust | **Insight:** stop re-scanning text you've already matched

Kept the same foundation but fixed v1's biggest architectural problem. Introduced `WorkingText` — a buffer that *shrinks* with every pass. Each pass removes what it matched, so later passes only see what's left. No more redundant scanning.

Split the Rust side into two binaries (companies + jobs), added per-pass metrics with `PassTracker` so I could see exactly where time went, and built a proper Python CLI layer with Typer and structured logging via `tracing`.

**What worked:** `WorkingText` eliminated redundant scanning. Metrics showed exactly where time went. The modular Python layer was much cleaner.

**What didn't:** Still scraping raw HTML. Still using Desync. Still two languages bolted together. The progressive consumption fixed the scanning problem, but not the real one — regex on unstructured HTML is fundamentally fragile.

```
Sitemaps → Python Desync scraper → raw text → WorkItem pipeline (shrinking buffer) → SQLite
```

### [v3](v3/) — Clean Slate

**Stack:** Pure Rust | **Insight:** don't parse HTML — parse markdown

Threw out the Desync scraper, the HTML-first approach, and Python entirely. v3 uses [spider.cloud](https://spider.cloud) to fetch pages as **markdown** instead of HTML — which turns out to be the key insight. Markdown has predictable structure: headings, links, and text blocks that map directly to the data we want.

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

## At a Glance

| | [v1](v1/) | [v2](v2/) | [v3](v3/) |
|--|----|----|-----|
| Languages | Python + Rust | Python + Rust | Rust |
| Scraper | Desync API | Desync API | spider.cloud |
| Input format | Raw HTML | Raw HTML | Markdown |
| Parser | 8 regex passes | 7 passes + WorkingText | 3-pass AST (Lexer → Clusterer → Extractors) |
| Parallelism | None | Optional Rayon | Rayon (parse) + Tokio (scrape) |
| Parse speed | — | — | 681 pages/sec on 18 cores |
| DB writes | Batch after all passes | Batch after all passes | Streaming per-page via mpsc |

The biggest lever wasn't even Rust or parallelism — it was switching from HTML to markdown. Markdown gives you structure for free. You don't need to fight `<div>` soup to find a founder's name when it's just a line of text followed by social links.

## The Data

The final v3 pipeline produces a 68 MB SQLite database with 9 normalized tables. Some highlights:

| | |
|---|---|
| **Companies** | 5,723 (69% active, 741 acquired, 23 public) |
| **Founders** | 11,286 — 93% have LinkedIn, 40% have Twitter/X |
| **External links** | 42,779 (LinkedIn alone accounts for 15,280) |
| **Top location** | San Francisco (37% of all YC companies) |
| **Peak batch** | Winter 2022 (399 companies) |
| **AI prevalence** | ~2,373 AI-related tag mentions — nearly half of all companies |

Full dataset breakdown in [`v3/stats.md`](v3/stats.md).
