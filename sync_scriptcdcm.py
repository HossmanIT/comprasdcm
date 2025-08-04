import requests
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    response = requests.post(
        "http://localhost:8001/sync-recent-purchasescdcm",
        headers={"Content-Type": "application/json"}
    )
    logger.info(f"Sync executed: {response.json()}")
except Exception as e:
    logger.error(f"Sync failed: {str(e)}")