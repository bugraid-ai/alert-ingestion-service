from fastapi import Security, HTTPException, Depends
from fastapi.security import APIKeyHeader
from starlette.status import HTTP_403_FORBIDDEN
import os
from typing import Dict, Optional
import logging

# Set up logging
logger = logging.getLogger(__name__)

# API key header
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)

# Environment-specific API keys
# Default to the key used in the Step Functions state machine for testing
DEFAULT_API_KEY = "GSBBcEiPCN44Rk78FAjLpamP3cH9TD3v95AT6p2E"
API_KEYS: Dict[str, str] = {
    "development": os.environ.get("API_KEY_DEV", DEFAULT_API_KEY),
    "production": os.environ.get("API_KEY_PROD", DEFAULT_API_KEY),
}

async def get_api_key(
    api_key: str = Security(api_key_header),
    environment: Optional[str] = None
) -> str:
    """
    Validate API key based on environment
    
    Args:
        api_key: The API key from the request header
        environment: The environment to validate against
        
    Returns:
        The validated API key
        
    Raises:
        HTTPException: If the API key is missing or invalid
    """
    if not api_key:
        logger.warning("Missing API key in request")
        raise HTTPException(
            status_code=HTTP_403_FORBIDDEN, detail="Missing API key"
        )
    
    # If environment is specified, check against that environment's key
    if environment and environment in API_KEYS:
        if api_key == API_KEYS[environment]:
            return api_key
    # Otherwise check against all environments
    else:
        for env, env_key in API_KEYS.items():
            if api_key == env_key and env_key:
                return api_key
    
    logger.warning(f"Invalid API key provided for environment: {environment}")
    raise HTTPException(
        status_code=HTTP_403_FORBIDDEN, detail="Invalid API key"
    )