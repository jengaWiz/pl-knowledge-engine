"""
Retry decorator with exponential backoff.
Use on any function that makes external API calls.

Usage:
    @retry(max_attempts=5, base_delay=1.0)
    def call_api():
        ...
"""
import time
import functools
from src.utils.logger import get_logger

logger = get_logger(__name__)


def retry(max_attempts: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
    """Decorator that retries a function with exponential backoff.

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
