import pytest
import asyncio
from aioresponses import aioresponses
import pytest_asyncio
from harv.clients.asyncapi import AsyncAPIClient, APIClientError


@pytest.mark.asyncio
async def test_fetch_json_success():
    client = AsyncAPIClient(base_url="https://api.example.com")

    with aioresponses() as m:
        m.get("https://api.example.com/test", payload={"foo": "bar"})
        async with client:
            result = await client.fetch("/test")
            assert result == {"foo": "bar"}

@pytest.mark.asyncio
async def test_fetch_text_success():
    client = AsyncAPIClient(base_url="https://api.example.com")

    with aioresponses() as m:
        m.get("https://api.example.com/raw", body="some raw content")
        async with client:
            result = await client.fetch("/raw", json_response=False)
            assert result == "some raw content"

@pytest.mark.asyncio
async def test_fetch_raises_on_http_error():
    client = AsyncAPIClient(base_url="https://api.example.com")

    with aioresponses() as m:
        m.get("https://api.example.com/fail", status=500)
        async with client:
            with pytest.raises(APIClientError) as exc_info:
                await client.fetch("/fail")
            assert "Error fetching data" in str(exc_info.value)

@pytest.mark.asyncio
async def test_fetch_all():
    client = AsyncAPIClient(base_url="https://api.example.com")

    with aioresponses() as m:
        m.get("https://api.example.com/one", payload={"x": 1})
        m.get("https://api.example.com/two", payload={"x": 2})

        async with client:
            results = await client.fetch_all(["/one", "/two"])
            assert results == [{"x": 1}, {"x": 2}]
