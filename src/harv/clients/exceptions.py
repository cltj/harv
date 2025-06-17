# These classes are created in order to have an extensible way to handle different types of exceptions,
# and improve readability for the different exceptions that might occur.
# Together with unit tests, this will make it easier to identify and fix issues in the code.

# "Exception" is python base class for all exceptions.
# It is not recommended to catch this exception as it will catch all exceptions and you
# will not be able to identify the root cause of the problem.
# It is a good practice to catch specific exceptions and handle them accordingly.

# The following classes are custom exceptions that inherit from the base Exception class.


import json

import urllib3


class StatusCodeException(Exception):
    """
    Base class for all status code exceptions.

    Args:
        Exception (_type_): the type of the exception

    """

    def __init__(self, endpoint: str, message: str) -> None:
        """
        Initializer for the StatusCodeException class.

        Args:
            endpoint (str): the last part of the url that was called
            message (str): the message to be displayed when the exception is raised

        """
        super().__init__(f"Endpoint: {endpoint} - {message}")

    @classmethod
    def from_status_code(cls, status_code: int, endpoint: str):  # noqa: C901, ANN206
        """
        Create a status code exception based on the status code.

        Args:
            status_code (int): the status code of the response
            endpoint (str): the last part of the url that was called

        Returns:
            Class: exception that was raised with the message

        """
        if status_code == 400:
            return BadRequest(endpoint)
        if status_code == 401:
            return Unauthorized(endpoint)
        if status_code == 403:
            return Forbidden(endpoint)
        if status_code == 404:
            return NotFound(endpoint)
        if status_code == 429:
            return TooManyRequests(endpoint)
        if status_code == 500:
            return InternalServerError(endpoint)
        if status_code == 503:
            return ServiceUnavailable(endpoint)
        if status_code == 504:
            return GatewayTimeout(endpoint)
        return cls(endpoint, f"Unexpected status code: {status_code}")


# Status code exceptions
class BadRequest(StatusCodeException):
    """BadRequest class for 400 status code exception."""

    def __init__(self, endpoint: str) -> None:
        """
        Initializer for the BadRequest class.

        Args:
            endpoint (str): the last part of the url that was called that caused the exception

        """
        super().__init__(
            endpoint, "400 Bad Request: The request could not be understood or was missing required parameters.",
        )


class Unauthorized(StatusCodeException):
    """Unauthorized class for 401 status code exception."""

    def __init__(self, endpoint: str) -> None:
        """
        Initializer for the BadRequest class.

        Args:
            endpoint (str): the last part of the url that was called that caused the exception.

        """
        super().__init__(
            endpoint,
            "401 Unauthorized: Authentication failed or has not yet been provided.",
            )


class Forbidden(StatusCodeException):
    """Forbidden class for 403 status code exception."""

    def __init__(self, endpoint: str) -> None:
        """
        Initializer for the Forbidden class.

        Args:
            endpoint (str): the last part of the url that was called that caused the exception.

        """
        super().__init__(
            endpoint,
            """403 Forbidden: Authentication succeeded but the
             authenticated user does not have access to the requested resource.""",
        )


class NotFound(StatusCodeException):
    """NotFound class for 404 status code exception."""

    def __init__(self, endpoint:str) -> None:
        """
        Initializer for the NotFound class.

        Args:
            endpoint (str): the last part of the url that was called that caused the exception.

        """
        super().__init__(
            endpoint,
            "404 Not Found: No content found.",
            )


class TooManyRequests(StatusCodeException):
    """TooManyRequests class for 429 status code exception."""

    def __init__(self, endpoint: str) -> None:
        """
        Initializer for the TooManyRequests class.

        Args:
            endpoint (str): the last part of the url that was called that caused the exception.

        """
        super().__init__(
            endpoint,
            "429 Too Many Requests: The user has sent too many requests in a given amount of time.",
        )


class InternalServerError(StatusCodeException):
    """InternalServerError class for 500 status code exception."""

    def __init__(self, endpoint: str) -> None:
        """
        Initializer for the InternalServerError class.

        Args:
            endpoint (str): the last part of the url that was called that caused the exception.

        """
        super().__init__(
            endpoint,
            "500 Internal Server Error: Server error. The request could not be completed.",
            )


class ServiceUnavailable(StatusCodeException):
    """ServiceUnavailable class for 503 status code exception."""

    def __init__(self, endpoint: str) -> None:
        """
        Initializer for the ServerUnavailable class.

        Args:
            endpoint (str): the last part of the url that was called that caused the exception.

        """
        super().__init__(
            endpoint,
            "503 Service Unavailable: The server is currently unavailable.",
            )


class GatewayTimeout(StatusCodeException):
    """GatewayTimeout class for 504 status code exception."""

    def __init__(self, endpoint: str) -> None:
        """
        Initializer for the GatewayTimeout class.

        Args:
            endpoint (str): the last part of the url that was called that caused the exception.

        """
        super().__init__(
            endpoint,
            "504 Gateway Timeout: The request could not be completed.",
        )


class ApiException(Exception):
    """Base class for all API exceptions."""

    @classmethod
    def from_exception(cls, endpoint: str, exception: Exception) -> str:
        """
        Create a urllib3 or json exception based on the status code.

        Args:
            endpoint (str): the last part of the url that was called
            exception (Exception): the type exception that was raised

        Returns:
            ApiException: exception that was raised with the message

        """
        if isinstance(exception, urllib3.exceptions.NewConnectionError):
            return NewConnectionError(endpoint)
        if isinstance(exception, urllib3.exceptions.MaxRetryError):
            return MaxRetryError(endpoint, exception)
        if isinstance(exception, json.JSONDecodeError):
            return JSONDecodeError(endpoint)
        if isinstance(exception, urllib3.exceptions.HTTPError):
            return HTTPError(endpoint, exception)
        return cls(f"Endpoint: {endpoint} - Unexpected error occurred {exception}")



class NewConnectionError(ApiException):
    """NewConnectionError class for urllib3 NewConnectionError exception."""

    def __init__(self, endpoint: str) -> str:
        """
        Initializer for the NewConnectionError class.

        Args:
            endpoint (str): the last part of the url that was called with text.


        """
        super().__init__(f"Endpoint: {endpoint} - Failed to establish a new connection.")


class MaxRetryError(ApiException):
    """MaxRetryError class for urllib3 MaxRetryError exception."""

    def __init__(self, endpoint: str, exception: Exception) -> str:
        """
        Initializer for the NewConnectionError class.

        Args:
            endpoint (str): the last part of the url that was called with text.
            exception (str): the exception type that was raised.

        Returns:
            Class: endpoint that was raised with the message and exception.

        """
        super().__init__(f"Endpoint: {endpoint} - Max retries exceeded. {exception}")


class JSONDecodeError(ApiException):
    """JSONDecodeError class for json JSONDecodeError exception."""

    def __init__(self, endpoint: str) -> str:
        """
        Initializer for the NewConnectionError class.

        Args:
            endpoint (str): the last part of the url that was called with text.

        Returns:
            Class: endpoint that was raised with the message and exception.

        """
        super().__init__(f"Endpoint: {endpoint} - Response is not valid JSON.")


class HTTPError(ApiException):
    """HTTPError class for urllib3 HTTPError exception."""

    def __init__(self, endpoint: str, original_exception: Exception) -> str:
        """
        Initializer for the HTTPError class.

        Args:
            endpoint (str): the last part of the url that was called with text.
            original_exception (Exception): the exception type that was raised.

        Returns:
            Class: endpoint that was raised with the message and exception.

        """
        super().__init__(f"Endpoint: {endpoint} - HTTP error occurred: {original_exception}")
