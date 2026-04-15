import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.mark.asyncio
async def test_fetch_skips_404():
    """A 404 response should return None, not raise."""
    from utils.api import fetch_movie_with_credits_async

    mock_response = MagicMock()
    mock_response.status_code = 404

    mock_client = AsyncMock()
    mock_client.get.return_value = mock_response

    result = await fetch_movie_with_credits_async(mock_client, 999)
    assert result is None


@pytest.mark.asyncio
async def test_fetch_movie_retries_on_429():
    """A 429 response should raise RequestError so tenacity can retry it."""
    from utils.api import fetch_movie_with_credits_async

    mock_request = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 429

    mock_client = AsyncMock()
    mock_client.get.side_effect = httpx.HTTPStatusError(
        "429", request=mock_request, response=mock_response
    )

    with pytest.raises(httpx.RequestError):
        # Call the unwrapped function directly to bypass tenacity retry loop
        await fetch_movie_with_credits_async.__wrapped__(mock_client, 1)


def test_fetch_all_movies_returns_list():
    """fetch_all_movies should return a list."""
    from utils.api import fetch_all_movies

    with patch("utils.api.asyncio.run", return_value=[{"id": 1, "title": "Test"}]):
        result = fetch_all_movies([1])

    assert isinstance(result, list)
    assert result[0]["id"] == 1
