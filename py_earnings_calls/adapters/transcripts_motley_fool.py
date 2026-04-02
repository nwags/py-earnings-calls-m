from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re

from bs4 import BeautifulSoup
import pandas as pd

from py_earnings_calls.adapters.base import TranscriptFetchAdapter
from py_earnings_calls.adapters.transcript_bulk_utils import (
    call_id_from_identity,
    infer_symbol_from_title,
    parse_call_datetime,
    stable_identity,
)
from py_earnings_calls.http import HttpClient, HttpRequestError, TRANSIENT_STATUS_CODES
from py_earnings_calls.models import TranscriptDocument


FAILURE_HTTP_ERROR = "HTTP_ERROR"
FAILURE_RETRY_EXHAUSTED = "RETRY_EXHAUSTED"
FAILURE_MISSING_TRANSCRIPT_BODY = "MISSING_TRANSCRIPT_BODY"
FAILURE_EMPTY_TRANSCRIPT_TEXT = "EMPTY_TRANSCRIPT_TEXT"
FAILURE_NON_TRANSCRIPT_PAGE = "NON_TRANSCRIPT_PAGE"
FAILURE_PARSE_ERROR = "PARSE_ERROR"
DATETIME_SOURCE_TRANSCRIPT_VISIBLE = "transcript_visible"
DATETIME_SOURCE_TRANSCRIPT_STRUCTURED = "transcript_structured"
DATETIME_SOURCE_ARTICLE_PUBLISHED = "article_published"
DATETIME_SOURCE_NONE = "none"

_ERROR_PAGE_PATTERNS = (
    "page not found",
    "404",
    "captcha",
    "access denied",
    "forbidden",
    "just a moment",
    "cloudflare",
)
_TRANSCRIPT_HINT_PATTERNS = (
    "operator",
    "question-and-answer",
    "prepared remarks",
    "earnings call transcript",
)
_ARTICLE_CLASS_RE = re.compile(r"(article|content|transcript)", flags=re.IGNORECASE)
_TIME_OF_DAY_HINT_RE = re.compile(r"\b\d{1,2}:\d{2}\b|\b(?:a\.m\.|p\.m\.|am|pm)\b", flags=re.IGNORECASE)
_VISIBLE_CALL_DATETIME_RE = re.compile(
    r"(?P<value>"
    r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s+"
    r"(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    r"\s+at\s+\d{1,2}(?::\d{2})?\s*(?:a\.m\.|p\.m\.|am|pm)\.?"
    r"(?:\s*(?:ET|EST|EDT|Eastern(?:\s+Time)?))?"
    r")",
    flags=re.IGNORECASE,
)


@dataclass(frozen=True)
class TranscriptFetchFailure:
    reason: str
    message: str
    http_status: int | None = None


@dataclass(frozen=True)
class TranscriptFetchOutcome:
    url: str
    symbol: str | None
    document: TranscriptDocument | None = None
    failure: TranscriptFetchFailure | None = None


class MotleyFoolTranscriptAdapter(TranscriptFetchAdapter):
    provider = "motley_fool"

    def __init__(self, http_client: HttpClient) -> None:
        self._http = http_client

    def fetch_document(self, url: str, symbol: str | None = None) -> TranscriptDocument:
        outcome = self.fetch_document_outcome(url, symbol=symbol)
        if outcome.document is None or outcome.failure is not None:
            failure = outcome.failure or TranscriptFetchFailure(reason=FAILURE_PARSE_ERROR, message="Unknown fetch failure.")
            raise ValueError(f"{failure.reason}: {failure.message}")
        return outcome.document

    def fetch_document_outcome(self, url: str, symbol: str | None = None) -> TranscriptFetchOutcome:
        try:
            html = self._http.request_text(url, max_attempts=3)
        except HttpRequestError as exc:
            failure_reason = self._http_failure_reason(exc)
            status = exc.failure.status_code
            return TranscriptFetchOutcome(
                url=url,
                symbol=symbol,
                failure=TranscriptFetchFailure(
                    reason=failure_reason,
                    message=exc.failure.reason,
                    http_status=status,
                ),
            )

        try:
            soup = BeautifulSoup(html, "lxml")
            title = soup.title.get_text(" ", strip=True) if soup.title else None

            if self._looks_like_non_transcript_page(title=title, html=html):
                return TranscriptFetchOutcome(
                    url=url,
                    symbol=symbol,
                    failure=TranscriptFetchFailure(
                        reason=FAILURE_NON_TRANSCRIPT_PAGE,
                        message="Page matched non-transcript/error heuristics.",
                    ),
                )

            article = self._find_article_block(soup)
            if article is None:
                return TranscriptFetchOutcome(
                    url=url,
                    symbol=symbol,
                    failure=TranscriptFetchFailure(
                        reason=FAILURE_MISSING_TRANSCRIPT_BODY,
                        message="Could not locate transcript content block.",
                    ),
                )

            transcript_text = article.get_text("\n", strip=True)
            if not transcript_text or len(transcript_text) < 120:
                return TranscriptFetchOutcome(
                    url=url,
                    symbol=symbol,
                    failure=TranscriptFetchFailure(
                        reason=FAILURE_EMPTY_TRANSCRIPT_TEXT,
                        message="Transcript content was empty or too short.",
                    ),
                )

            inferred_symbol = (symbol or self._infer_symbol(title or "", transcript_text)).upper()
            published, published_source = self._infer_datetime(soup, article=article, title=title)
            provider_call_id, identity = stable_identity(
                provider=self.provider,
                provider_call_id=url,
                symbol=inferred_symbol,
                call_datetime=published,
                title=title,
            )

            return TranscriptFetchOutcome(
                url=url,
                symbol=inferred_symbol,
                document=TranscriptDocument(
                    call_id=call_id_from_identity(identity),
                    provider=self.provider,
                    provider_call_id=provider_call_id,
                    symbol=inferred_symbol,
                    company_name=None,
                    call_datetime=published,
                    fiscal_year=None,
                    fiscal_period=None,
                    title=title,
                    source_url=url,
                    transcript_text=transcript_text,
                    call_datetime_source=published_source,
                    raw_html=html,
                    speaker_count=None,
                ),
            )
        except Exception as exc:  # pragma: no cover - defensive catch for unexpected parser issues
            return TranscriptFetchOutcome(
                url=url,
                symbol=symbol,
                failure=TranscriptFetchFailure(
                    reason=FAILURE_PARSE_ERROR,
                    message=str(exc),
                ),
            )

    def _http_failure_reason(self, error: HttpRequestError) -> str:
        failure = error.failure
        is_retry_exhausted = (
            error.attempts >= error.max_attempts
            and (
                failure.reason == "request_exception"
                or (failure.status_code in TRANSIENT_STATUS_CODES if failure.status_code is not None else False)
            )
        )
        if is_retry_exhausted:
            return FAILURE_RETRY_EXHAUSTED
        return FAILURE_HTTP_ERROR

    def _find_article_block(self, soup: BeautifulSoup):
        candidates = list(soup.find_all("article"))
        candidates.extend(
            soup.find_all(
                ["div", "section", "main"],
                attrs={"class": lambda value: isinstance(value, str) and bool(_ARTICLE_CLASS_RE.search(value))},
            )
        )
        if not candidates:
            return None
        return max(candidates, key=lambda item: len(item.get_text(" ", strip=True)))

    def _looks_like_non_transcript_page(self, *, title: str | None, html: str) -> bool:
        lower_blob = f"{title or ''}\n{html[:3000]}".lower()
        has_error_tokens = any(token in lower_blob for token in _ERROR_PAGE_PATTERNS)
        has_transcript_hint = any(token in lower_blob for token in _TRANSCRIPT_HINT_PATTERNS)
        return has_error_tokens and not has_transcript_hint

    def _infer_symbol(self, title: str, transcript_text: str) -> str:
        inferred = infer_symbol_from_title(title)
        if inferred:
            return inferred
        for source in [title, transcript_text[:800]]:
            match = pd.Series([source]).str.extract(r"\(([A-Z]{1,6})\)").iloc[0, 0]
            if isinstance(match, str) and match.strip():
                return match.strip()
        return "UNKNOWN"

    def _infer_datetime(self, soup: BeautifulSoup, *, article, title: str | None = None) -> tuple[datetime | None, str]:
        # 1) Transcript-specific visible call datetime in page content.
        visible = self._extract_visible_transcript_datetime(article)
        if visible is not None:
            return visible, DATETIME_SOURCE_TRANSCRIPT_VISIBLE

        # 2) Transcript-specific structured metadata (prefer in-article <time> tags).
        structured = self._extract_structured_transcript_datetime(soup, article)
        if structured is not None:
            return structured, DATETIME_SOURCE_TRANSCRIPT_STRUCTURED

        # 3) Provider/article publication metadata fallback only.
        published = self._extract_article_publish_datetime(soup, title=title)
        if published is not None:
            return published, DATETIME_SOURCE_ARTICLE_PUBLISHED

        return None, DATETIME_SOURCE_NONE

    def _extract_visible_transcript_datetime(self, article) -> datetime | None:
        if article is None:
            return None
        blob = article.get_text("\n", strip=True)
        if not blob:
            return None
        match = _VISIBLE_CALL_DATETIME_RE.search(blob[:4000])
        if match:
            parsed = parse_call_datetime(match.group("value"))
            if parsed is not None:
                return parsed
        # Secondary visible signal: explicit "at ... ET" snippet near transcript headings.
        lines = [line.strip() for line in blob.splitlines() if line.strip()]
        for line in lines[:40]:
            if "earnings call transcript" not in line.lower() and "prepared remarks" not in line.lower():
                continue
            parsed = parse_call_datetime(line)
            if parsed is not None:
                return parsed
        return None

    def _extract_structured_transcript_datetime(self, soup: BeautifulSoup, article) -> datetime | None:
        if article is None:
            return None
        for time_tag in article.find_all("time"):
            if time_tag.get("datetime"):
                try:
                    parsed = pd.to_datetime(time_tag["datetime"], errors="coerce")
                    if not pd.isna(parsed):
                        return parsed.to_pydatetime()
                except Exception:
                    pass
            text = time_tag.get_text(" ", strip=True)
            if text:
                parsed = parse_call_datetime(text)
            if parsed is not None:
                return parsed
        # Tertiary structured signal: page-level <time> tags with explicit time-of-day
        # on transcript pages (avoids treating date-only publish metadata as transcript call time).
        article_blob = article.get_text(" ", strip=True).lower()
        transcriptish = "earnings call transcript" in article_blob or "prepared remarks" in article_blob
        if transcriptish:
            for time_tag in soup.find_all("time"):
                if time_tag in article.find_all("time"):
                    continue
                time_text = time_tag.get_text(" ", strip=True)
                datetime_attr = (time_tag.get("datetime") or "").strip()
                has_time_of_day_hint = bool(
                    _TIME_OF_DAY_HINT_RE.search(time_text)
                    or _TIME_OF_DAY_HINT_RE.search(datetime_attr)
                )
                if not has_time_of_day_hint:
                    continue
                parsed = None
                if datetime_attr:
                    try:
                        as_dt = pd.to_datetime(datetime_attr, errors="coerce")
                        if not pd.isna(as_dt):
                            parsed = as_dt.to_pydatetime()
                    except Exception:
                        parsed = None
                if parsed is None and time_text:
                    parsed = parse_call_datetime(time_text)
                if parsed is not None:
                    return parsed
        # Secondary structured signal: global <time> tags positioned close to transcript article.
        for sibling in article.find_all_previous(limit=4):
            if getattr(sibling, "name", None) != "time":
                continue
            text = sibling.get_text(" ", strip=True)
            parsed = None
            if sibling.get("datetime"):
                try:
                    as_dt = pd.to_datetime(sibling["datetime"], errors="coerce")
                    if not pd.isna(as_dt):
                        parsed = as_dt.to_pydatetime()
                except Exception:
                    parsed = None
            if parsed is None and text:
                parsed = parse_call_datetime(text)
            if parsed is not None:
                return parsed
        return None

    def _extract_article_publish_datetime(self, soup: BeautifulSoup, *, title: str | None) -> datetime | None:
        time_tag = soup.find("time")
        if time_tag and time_tag.get("datetime"):
            try:
                parsed = pd.to_datetime(time_tag["datetime"], errors="coerce")
                if not pd.isna(parsed):
                    return parsed.to_pydatetime()
            except Exception:
                pass
        if time_tag and time_tag.get_text(strip=True):
            parsed = parse_call_datetime(time_tag.get_text(strip=True))
            if parsed is not None:
                return parsed
        if title:
            parsed = parse_call_datetime(title)
            if parsed is not None:
                return parsed
        return None
