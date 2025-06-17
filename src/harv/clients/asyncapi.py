import asyncio
from typing import Any, Dict, List, Optional

import aiohttp
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


class APIClientError(Exception):
    """Custom exception for API client errors."""

    pass


class AsyncAPIClient:
    """
    An asynchronous API client for making HTTP requests.

    This client supports fetching data from endpoints with optional retries,
    parsing JSON and CSV responses, and managing an aiohttp session.
    """

    def __init__(self, base_url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 30):
        """
        Initialize the AsyncAPIClient.

        Args:
            base_url (str): The base URL for the API.
            headers (Optional[Dict[str, str]]): Optional HTTP headers to include in requests.
            timeout (int): Timeout for HTTP requests in seconds. Defaults to 30.

        """
        self.base_url = base_url
        self.headers = headers or {}
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.session = aiohttp.ClientSession(headers=self.headers, timeout=self.timeout)

    async def authenticate(self, token: str, token_type: str = "Bearer") -> None:  # noqa: S107
        """
        Add or update the Authorization header for authentication.

        Args:
            self: The instance of the AsyncAPIClient.
            token (str): The authentication token (e.g., personal access token or API key).
            token_type (str): The type of token (e.g., 'Bearer', 'Token'). Defaults to 'Bearer'.

        """
        await self.session.close()  # Close any existing session before updating headers
        self.headers["Authorization"] = f"{token_type} {token}"  # type: ignore
        self.session = aiohttp.ClientSession(headers=self.headers, timeout=self.timeout)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError)),
    )
    async def fetch(self, endpoint: str, params: Optional[Dict[str, Any]] = None, json_response: bool = True) -> Any:  # noqa: ANN401, FBT001, FBT002
        """
        Fetch data from a specific API endpoint.

        Args:
            endpoint (str): The API endpoint to fetch data from.
            params (Optional[Dict[str, Any]]): Query parameters to include in the request. Defaults to None.
            json_response (bool): Whether to parse the response as JSON. Defaults to True.

        Returns:
            Any: Response data as JSON or text

        Raises:
            ValueError: If the return type is unsupported.
            APIClientError: If there is an error during the request or response parsing.

        """
        try:
            async with self.session.get(f"{self.base_url}{endpoint}", params=params) as response:
                response.raise_for_status()
                if json_response:
                    return await response.json()
                return await response.text()
        except Exception as e:
            error_message = f"Error fetching data from {endpoint}: {str(e)}"
            raise APIClientError(error_message) from e

    def _raise_value_error(self, message: str) -> None:
        """
        Raise a ValueError with the given message.

        Args:
            message (str): The error message to raise.

        Raises:
            ValueError: The error message to raise.

        """
        raise ValueError(message)

    async def fetch_all(self, endpoints: List[str], json_response: bool = True) -> List[Any]:  # noqa: FBT001, FBT002
        """
        Fetch data from multiple API endpoints concurrently.

        Args:
            endpoints (List[str]): A list of API endpoints to fetch data from.
            json_response (bool): Whether the respons is JSON. Defaults to True.

        Returns:
            List[Any]: A list of response data in the specified format for each endpoint.

        Raises:
            APIClientError: If there is an error during any of the requests or response parsing.

        """
        tasks = [self.fetch(endpoint, json_response=json_response) for endpoint in endpoints]
        return await asyncio.gather(*tasks, return_exceptions=False)

    async def close(self) -> None:
        """
        Close the aiohttp session.

        This method ensures that the aiohttp session is properly closed to release resources.
        """
        await self.session.close()

    async def __aenter__(self) -> "AsyncAPIClient":
        """
        Enter the runtime context for the asynchronous API client.

        Returns:
            AsyncAPIClient: The instance of the asynchronous API client.

        """
        return self

    async def __aexit__(self, exc_type: Optional[type], exc: Optional[BaseException], tb: Optional[Any]) -> None:  # noqa: ANN401
        """
        Exit the runtime context for the asynchronous API client.

        This method ensures that the aiohttp session is properly closed when exiting the context.
        """
        await self.close()
