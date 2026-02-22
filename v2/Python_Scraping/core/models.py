from __future__ import annotations

from typing import Any, List, Optional

from pydantic import BaseModel, ConfigDict


class UrlRecordModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: int
    url: str
    lastmod: Optional[str] = None
    sitemap_source: Optional[str] = None
    visited: int = 0
    visited_at: Optional[str] = None


class PageDataModel(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    id: Optional[int] = None
    url: str
    domain: Optional[str] = None
    timestamp: Optional[int] = None
    bulk_search_id: Optional[str] = None
    search_type: Optional[str] = None
    text_content: Optional[str] = None
    html_content: Optional[str] = None
    internal_links: Optional[List[str]] = None
    external_links: Optional[List[str]] = None
    latency_ms: Optional[int] = None
    complete: bool = True
    created_at: Optional[Any] = None
    scraped_at: Optional[str] = None

    @classmethod
    def from_obj(cls, obj: Any) -> "PageDataModel":
        data = {
            "id": getattr(obj, "id", None),
            "url": getattr(obj, "url", None),
            "domain": getattr(obj, "domain", None),
            "timestamp": getattr(obj, "timestamp", None),
            "bulk_search_id": getattr(obj, "bulk_search_id", None),
            "search_type": getattr(obj, "search_type", None),
            "text_content": getattr(obj, "text_content", None),
            "html_content": getattr(obj, "html_content", None),
            "internal_links": getattr(obj, "internal_links", None),
            "external_links": getattr(obj, "external_links", None),
            "latency_ms": getattr(obj, "latency_ms", None),
            "complete": bool(getattr(obj, "complete", True)),
            "created_at": getattr(obj, "created_at", None),
            "scraped_at": getattr(obj, "scraped_at", None),
        }
        return cls(**data)


class SitemapEntryModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    url: str
    lastmod: Optional[str] = None
    source: str


class JobPageModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    job_id: Optional[int] = None
    url: str
    sitemap_id: Optional[int] = None
    desync_id: Optional[int] = None
    domain: Optional[str] = None
    timestamp: Optional[int] = None
    bulk_search_id: Optional[str] = None
    search_type: Optional[str] = None
    text_content: Optional[str] = None
    html_content: Optional[str] = None
    internal_links: Optional[List[str]] = None
    external_links: Optional[List[str]] = None
    latency_ms: Optional[int] = None
    complete: bool = False
    created_at: Optional[Any] = None
    scraped_at: Optional[str] = None

    @classmethod
    def from_page(cls, obj: Any, job_id: Optional[int]) -> "JobPageModel":
        return cls(
            job_id=job_id,
            url=getattr(obj, "url", None),
            sitemap_id=getattr(obj, "sitemap_id", None),
            desync_id=getattr(obj, "id", None),
            domain=getattr(obj, "domain", None),
            timestamp=getattr(obj, "timestamp", None),
            bulk_search_id=getattr(obj, "bulk_search_id", None),
            search_type=getattr(obj, "search_type", None),
            text_content=getattr(obj, "text_content", None),
            html_content=getattr(obj, "html_content", None),
            internal_links=getattr(obj, "internal_links", None),
            external_links=getattr(obj, "external_links", None),
            latency_ms=getattr(obj, "latency_ms", None),
            complete=bool(getattr(obj, "complete", False)),
            created_at=getattr(obj, "created_at", None),
            scraped_at=getattr(obj, "scraped_at", None),
        )
