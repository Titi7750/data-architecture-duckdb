import os
import logging

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, "access.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def log_access(username: str, endpoint: str):

    logging.info(f"User '{username}' accessed '{endpoint}'")

def log_failed_login(username: str):

    logging.warning(f"⚠ Failed login attempt for username: '{username}'")