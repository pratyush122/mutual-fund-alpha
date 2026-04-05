import time
import functools
from typing import Callable, TypeVar, Any
import logging

F = TypeVar("F", bound=Callable[..., Any])


def retry(n: int = 3, backoff: int = 2) -> Callable[[F], F]:
    """
    Retry decorator with exponential backoff.

    Args:
        n: Number of retry attempts
        backoff: Backoff multiplier for delay between retries
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(__name__)

            for attempt in range(n):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == n - 1:  # Last attempt
                        logger.error(
                            f"Function {func.__name__} failed after {n} attempts: {e}"
                        )
                        raise e

                    delay = backoff**attempt
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)

            # This should never be reached
            raise RuntimeError("Retry logic failed unexpectedly")

        return wrapper

    return decorator
