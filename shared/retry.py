import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)


def retry(fn=None, max_retries=5, delay=2, backoff=2, exceptions=(Exception,)):
    if fn is None:
        return lambda f: retry(f, max_retries=max_retries, delay=delay,
                               backoff=backoff, exceptions=exceptions)

    @wraps(fn)
    def wrapper(*args, **kwargs):
        last_exc = None
        for attempt in range(max_retries + 1):
            try:
                return fn(*args, **kwargs)
            except exceptions as e:
                last_exc = e
                if attempt < max_retries:
                    wait = delay * (backoff ** attempt)
                    logger.warning(
                        "Retry %d/%d after %.1fs: %s",
                        attempt + 1, max_retries, wait, e,
                    )
                    time.sleep(wait)
        raise last_exc

    return wrapper
