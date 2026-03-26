"""
Structured logging configuration.
Usage in any module:
    from src.utils.logger import get_logger
    logger = get_logger(__name__)
    logger.info("processing match", match_id=123, team="Liverpool")
"""
import structlog
import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    """Configure structlog with console output. Call once at startup.

    Args:
        level: Logging level string, e.g., "INFO", "DEBUG", "WARNING".
    """
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper()),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a named logger instance.

    Args:
        name: Logger name, typically ``__name__`` of the calling module.

    Returns:
        A structlog BoundLogger instance for structured logging.
    """
    return structlog.get_logger(name)
