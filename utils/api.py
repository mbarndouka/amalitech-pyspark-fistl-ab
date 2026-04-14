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

import asyncio
from typing import Any, Optional

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

def _get_async_client() -> httpx.AsyncClient:
    """Return an httpx AsyncClient pre-configured with auth and timeouts."""
    settings = get_settings().tmdb
    return httpx.AsyncClient(
        base_url=settings.baseUrl,
        params={"api_key": settings.api_key},
        timeout=settings.request_timeout,
        http2=True,
    )

# ---------------------------------------------------------------------------
# Retry-enabled async fetcher
# ---------------------------------------------------------------------------

@retry(
    retry=retry_if_exception_type(httpx.RequestError),
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=10),
    reraise=False,
)
async def fetch_movie_with_credits_async(
    client: httpx.AsyncClient, movie_id: int
) -> Optional[dict[str, Any]]:
    """
    Fetch the /movie/{id} endpoint with embedded credits.
    Returns the parsed and merged JSON dict on success, or None if the request fails.
    """
    try:
        response = await client.get(
            f"/movie/{movie_id}",
            params={"append_to_response": "credits", "language": "en-US"}
        )

        if response.status_code == 404:
            logger.warning(f"Movie {movie_id} not found (404). Skipping.")
            return None

        response.raise_for_status()
        data = response.json()

        credits = data.pop("credits", {})
        data["cast_raw"] = credits.get("cast", [])
        data["crew_raw"] = credits.get("crew", [])

        logger.info(f"Successfully fetched movie_id={movie_id} + credits.")
        return data

    except httpx.HTTPStatusError as exc:
        if exc.response.status_code >= 500:
            logger.error(f"Server error {exc.response.status_code} for movie_id={movie_id}. Retrying...")
            raise httpx.RequestError(f"Server error {exc.response.status_code}", request=exc.request) from exc

        logger.warning(f"HTTP {exc.response.status_code} for movie_id={movie_id} — skipping.")
        return None
    except httpx.RequestError as exc:
        logger.error(f"Network error for movie_id={movie_id}: {exc}")
        raise

# ---------------------------------------------------------------------------
# Concurrency / Orchestration
# ---------------------------------------------------------------------------

async def _fetch_all_movies_async(movie_ids: list[int], max_concurrent: int = 15) -> list[dict[str, Any]]:
    """
    Asynchronously fetch all movies bounded by a concurrency semaphore.
    """
    semaphore = asyncio.Semaphore(max_concurrent)

    async with _get_async_client() as client:

        async def fetch_with_semaphore(movie_id: int) -> Optional[dict[str, Any]]:
            async with semaphore:
                return await fetch_movie_with_credits_async(client, movie_id)

        tasks = [fetch_with_semaphore(movie_id) for movie_id in movie_ids]

        logger.info(f"Starting batch fetch for {len(movie_ids)} movies...")
        results = await asyncio.gather(*tasks, return_exceptions=True)

        valid_results: list[dict[str, Any]] = []
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"Batch fetch encountered an error: {r}")
            elif r is not None:
                valid_results.append(r)

        logger.info(f"Batch fetch completed: {len(valid_results)} / {len(movie_ids)} movies downloaded successfully.")
        return valid_results

# ---------------------------------------------------------------------------
# Public entry point (Synchronous compatibility layer)
# ---------------------------------------------------------------------------

def fetch_all_movies(movie_ids: list[int]) -> list[dict[str, Any]]:
    """
    Fetch detail + credits for every movie ID using async concurrency under the hood.
    Synchronous wrapper to maintain API compatibility with current application.
    """
    return asyncio.run(_fetch_all_movies_async(movie_ids, max_concurrent=15))
