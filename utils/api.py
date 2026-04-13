"""
TMDB API data fetcher.

Responsibilities
----------------
- Fetch movie detail + credits for a list of IDs.
- Retry on transient HTTP errors with exponential back-off.
- Return a raw, un-mutated list of dicts — no side effects.

Design note: all public functions are pure transformations or I/O wrappers
that surface errors explicitly rather than swallowing them.
"""

import time
from typing import Any, Iterator, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from config.settings import get_settings
from utils.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# HTTP client factory
# ---------------------------------------------------------------------------

def _get_client() -> httpx.Client:
    """Return an httpx Client pre-configured with auth and timeouts."""
    settings = get_settings().tmdb
    return httpx.Client(
        base_url=settings.baseUrl,
        params={"api_key": settings.api_key},
        timeout=settings.request_timeout,
    )

# ---------------------------------------------------------------------------
# Retry-enabled fetchers (I/O wrappers)
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=10),
    reraise=False,
)
def fetch_movie_detail(client: httpx.Client, movie_id: int) -> Optional[dict[str, Any]]:
    """
    Fetch the /movie/{id} endpoint.
    Returns the parsed JSON dict on success, or None if the request fails.
    """
    try:
        response = client.get(f"/movie/{movie_id}")
        response.raise_for_status()
        return response.json()
    except httpx.HTTPStatusError as exc:
        logger.warning(f"HTTP {exc.response.status_code} for movie_id={movie_id} — skipping.")
        return None
    except httpx.RequestError as exc:
        logger.error(f"Network error for movie_id={movie_id}: {exc}")
        return None

@retry(
    retry=retry_if_exception_type(httpx.HTTPError),
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=10),
    reraise=False,
)
def fetch_movie_credits(client: httpx.Client, movie_id: int) -> Optional[dict[str, Any]]:
    """
    Fetch the /movie/{id}/credits endpoint.
    Returns the parsed JSON dict on success, or None on failure.
    """
    try:
        response = client.get(f"/movie/{movie_id}/credits")
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as exc:
        logger.warning(f"Could not fetch credits for movie_id={movie_id}: {exc}")
        return None

# ---------------------------------------------------------------------------
# Merge helper (Pure Function)
# ---------------------------------------------------------------------------

def _merge_credits_into_detail(
    detail: dict[str, Any],
    credits: Optional[dict[str, Any]],
) -> dict[str, Any]:
    """
    Attach cast/crew lists from credits into the detail dict.
    Pure function — does not mutate inputs.
    """
    cast = (credits or {}).get("cast", [])
    crew = (credits or {}).get("crew", [])

    return {
        **detail,
        "cast_raw": cast,
        "crew_raw": crew,
    }

# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def fetch_all_movies(movie_ids: list[int]) -> list[dict[str, Any]]:
    """
    Fetch detail + credits for every movie ID and return a list of merged dicts.
    """
    settings = get_settings().tmdb
    results: list[dict[str, Any]] = []

    with _get_client() as client:
        for movie_id in movie_ids:
            logger.info(f"Fetching movie_id={movie_id} …")

            detail = fetch_movie_detail(client, movie_id)
            if not detail:
                continue

            credits = fetch_movie_credits(client, movie_id)
            merged = _merge_credits_into_detail(detail, credits)
            results.append(merged)

            # Polite delay based on settings
            time.sleep(settings.rate_limit_period / settings.rate_limit)

    logger.info(f"Fetched {len(results)} / {len(movie_ids)} movies successfully.")
    return results
