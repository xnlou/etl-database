import os
import pwd
import logging

def get_username():
    """Retrieve the current username using multiple methods."""
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    try:
        user = os.environ.get("USER") or os.environ.get("USERNAME")
        if user:
            logger.info("Username retrieved from environment variable.")
            return user
        user = pwd.getpwuid(os.getuid())[0]
        logger.info("Username retrieved from pwd.getpwuid.")
        return user
    except Exception:
        user = os.getlogin()
        logger.info("Username retrieved from os.getlogin.")
        return user