from __future__ import annotations

from typing import List, Sequence, Tuple

Clause = Tuple[str, str]

REAL_COMPANY_FILTERS: Sequence[Clause] = (
    ("url LIKE ?", "https://www.ycombinator.com/companies/%"),
    ("url NOT LIKE ?", "%/industry/%"),
    ("url NOT LIKE ?", "%/location/%"),
    ("url NOT LIKE ?", "%/batch/%"),
    ("url NOT LIKE ?", "%/tags/%"),
    ("url NOT LIKE ?", "%/jobs%"),
    ("url NOT LIKE ?", "%/launches%"),
)

JOB_URL_FILTER: Sequence[Clause] = (
    ("url LIKE ?", "https://www.ycombinator.com/companies/%/jobs/%"),
)


def where_clause(
    clauses: Sequence[Clause], *, visited: int | None = None
) -> tuple[str, List[str]]:
    parts: List[str] = []
    params: List[str] = []
    if visited is not None:
        parts.append("visited = ?")
        params.append(str(visited))
    for col, val in clauses:
        parts.append(col)
        params.append(val)
    joined = " AND ".join(parts) if parts else "1=1"
    return joined, params
