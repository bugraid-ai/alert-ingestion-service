from typing import List, Dict, Any, Optional
import logging
import json
from datetime import datetime
import uuid
import httpx
import os

# Set up logging
logger = logging.getLogger(__name__)

# Alert ingestion service URL
ALERT_INGESTION_URL = os.getenv("ALERT_INGESTION_URL", "http://alert-ingestion.bugraid-dev-ai-ml.local:8000")

async def process_alerts(alerts: List[Dict[str, Any]], environment: str) -> Dict[str, Any]:
    """
    Process alerts by forwarding them to the alert-ingestion service
    
    Args:
        alerts: List of alert dictionaries
        environment: The environment (development or production)
        
    Returns:
        Dictionary with processing results
    """
    logger.info(f"Forwarding {len(alerts)} alerts to alert-ingestion service in {environment} environment")
    
    # Prepare payload for alert-ingestion service
    payload = {
        "alerts": alerts,
        "source": "fastapi-service",
        "environment": environment,
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }
    
    try:
        # Forward alerts to alert-ingestion service
        async with httpx.AsyncClient(timeout=30.0) as client:
            logger.info(f"Sending alerts to {ALERT_INGESTION_URL}/process")
            response = await client.post(
                f"{ALERT_INGESTION_URL}/process",
                json=payload,
                headers={
                    "Content-Type": "application/json"
                }
            )
            
            if response.status_code == 200:
                response_data = response.json()
                logger.info(f"Successfully forwarded {len(alerts)} alerts to alert-ingestion service")
                return {
                    "status": "forwarded",
                    "alert_count": len(alerts),
                    "ingestion_response": response_data,
                    "environment": environment,
                    "timestamp": datetime.utcnow().isoformat() + 'Z'
                }
            else:
                logger.error(f"Failed to forward alerts to alert-ingestion: {response.status_code} - {response.text}")
                return {
                    "status": "error",
                    "alert_count": len(alerts),
                    "error": f"HTTP {response.status_code}: {response.text}",
                    "environment": environment,
                    "timestamp": datetime.utcnow().isoformat() + 'Z'
                }
                
    except Exception as e:
        logger.error(f"Exception while forwarding alerts to alert-ingestion: {str(e)}")
        return {
            "status": "error",
            "alert_count": len(alerts),
            "error": str(e),
        "environment": environment,
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }

def enrich_alert(alert: Dict[str, Any]) -> Dict[str, Any]:
    """
    Enrich alert with additional information
    
    Args:
        alert: The alert to enrich
        
    Returns:
        Enriched alert
    """
    # Add your enrichment logic here
    # This could include adding context, related information, etc.
    return alert
