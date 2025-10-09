import os
import logging
from mangum import Mangum
from .main import app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Mangum handler for AWS Lambda
handler = Mangum(app, lifespan="off")

# Log startup
logger.info(f"FastAPI Alert Processing Lambda initialized in {os.environ.get('ENVIRONMENT', 'development')} environment")

