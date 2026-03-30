"""
Retry decorator with exponential backoff.
Use on any function that makes external API calls.

Usage:
    @retry(max_attempts=5, base_delay=1.0)
    def call_api():
        ...
"""
import re
import time
import functools
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _parse_retry_after(error: Exception) -> float | None:
    """Extract suggested retry delay from a 429 error response.

    Gemini API 429 responses include a JSON body like:
        "Please retry in 45.6s"  or  "retryDelay": "29s"

    Args:
        error: The exception raised by the API call.

    Returns:
        Seconds to wait, or None if not parseable.
    """
    text = str(error)
    # Match "retryDelay": "29s" or "retry in 45.6s"
    match = re.search(r"retry[^0-9]*(\d+\.?\d*)\s*s", text, re.IGNORECASE)
    if match:
        return float(match.group(1)) + 2.0  # add a small buffer
    return None


def retry(max_attempts: int = 5, base_delay: float = 1.0, max_delay: float = 120.0):
    """Decorator that retries a function with exponential backoff.

    For 429 rate-limit errors the suggested retryDelay from the response body
    is honoured instead of the exponential backoff schedule.

    Args:
        max_attempts: Maximum number of retry attempts.
        base_delay: Initial delay in seconds (doubles each retry).
        max_delay: Maximum delay cap in seconds.

    Returns:
        A decorator that wraps the target function with retry logic.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt == max_attempts:
                        logger.error(
                            "max retries exceeded",
                            function=func.__name__,
                            attempts=max_attempts,
                            error=str(e),
                        )
                        raise

                    # Honour retryDelay from 429 responses; fall back to backoff
                    is_rate_limit = "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e)
                    if is_rate_limit:
                        suggested = _parse_retry_after(e)
                        delay = suggested if suggested else min(base_delay * (2 ** (attempt - 1)), max_delay)
                    else:
                        delay = min(base_delay * (2 ** (attempt - 1)), max_delay)

                    logger.warning(
                        "retrying after error",
                        function=func.__name__,
                        attempt=attempt,
                        max_attempts=max_attempts,
                        delay_seconds=delay,
                        error=str(e),
                    )
                    time.sleep(delay)
            raise last_exception  # Should never reach here

        return wrapper

    return decorator
