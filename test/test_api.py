import pytest
from unittest.mock import MagicMock
from utils.api import fetch_movie_detail, _merge_credits_into_detail
import structlog
from structlog.testing import capture_logs

def test_fetch_movie_detail_success():
    # Mock httpx Client
    mock_client = MagicMock()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": 550, "title": "Fight Club"}
    mock_client.get.return_value = mock_response

    result = fetch_movie_detail(mock_client, 550)

    assert result["title"] == "Fight Club"
    mock_client.get.assert_called_once_with("/movie/550")

def test_merge_credits_into_detail():
    detail = {"id": 1, "title": "Movie A"}
    credits = {"cast": [{"name": "Actor 1"}], "crew": [{"name": "Director 1"}]}

    merged = _merge_credits_into_detail(detail, credits)

    assert "cast_raw" in merged
    assert len(merged["cast_raw"]) == 1
    assert merged["title"] == "Movie A"

def test_fetch_movie_detail_not_found():
    with capture_logs() as captured:
        mock_client = MagicMock()
        # Simulate a 404 error
        from httpx import HTTPStatusError, Request, Response
        request = Request("GET", "https://api.themoviedb.org/3/movie/999")
        response = Response(404, request=request)
        mock_client.get.side_effect = HTTPStatusError("Not Found", request=request, response=response)

        result = fetch_movie_detail(mock_client, 999)

        assert result is None
        assert any("HTTP 404" in log["event"] for log in captured)
