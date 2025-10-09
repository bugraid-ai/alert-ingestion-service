"""
Firehose Data Forwarder Service

This is a completely independent service that:
1. Monitors the /context endpoint logs
2. Extracts Firehose data 
3. Forwards to firehose-ingest-dev-service

"""

import logging
import httpx
import asyncio
import os
from typing import Dict, Any

logger = logging.getLogger(__name__)

class IndependentFirehoseForwarder:
    """Independent service that forwards Firehose data to downstream services"""
    
    def __init__(self):
        # Get environment configuration
        environment = os.environ.get("ENVIRONMENT", "development").lower()
        
        # Target service configuration based on environment
        if environment == "production":
            # Production configuration
            self.firehose_service_url = "http://firehose-ingest-prd-service.bugraid-prd-ai-ml.local:8080/context"
            self.firehose_display_url = "https://fastapi-service-prd-alb.ap-southeast-1.elb.amazonaws.com/context"
            self.rca_service_url = "http://rca-agent-service.bugraid-prd-ai-ml.local:8005/analyze-incident"
        else:
            # Development configuration
            self.firehose_service_url = "http://firehose-ingest-dev-service.bugraid-dev-ai-ml.local:8080/context"
            self.firehose_display_url = "https://fastapi-service-alb-468168040.ap-southeast-1.elb.amazonaws.com/context"
            self.rca_service_url = "http://rca-agent-service.bugraid-dev-ai-ml.local:8005/analyze-incident"
        
        self.timeout = 30.0
        self.is_running = False
        self.environment = environment
        
        logger.info(f"Forwarder initialized for {environment} environment")
        logger.info(f"Firehose URL: {self.firehose_service_url}")
        logger.info(f"RCA URL: {self.rca_service_url}")
        
        # This would monitor logs or use a different mechanism
        # For now, it's a standalone service ready to be integrated
    
    async def forward_to_firehose_service(self, data: bytes, headers: Dict[str, str]) -> Dict[str, Any]:
        """
        Forward Firehose data to firehose-ingest-dev-service
        
        Args:
            data: Raw Firehose data
            headers: Request headers to forward
            
        Returns:
            Dict with forwarding result
        """
        try:
            # Log the full forwarding details
            data_preview = data[:500].decode('utf-8', errors='ignore') if len(data) > 500 else data.decode('utf-8', errors='ignore')
            logger.info(f"🚀 FORWARDING TO: {self.firehose_display_url}")
            logger.info(f"📦 DATA SIZE: {len(data)} bytes")
            logger.info(f"📋 DATA PREVIEW: {data_preview}")
            logger.info(f"🔗 HEADERS: {dict(headers)}")
            
            forward_headers = {
                "Content-Type": headers.get("content-type", "application/json"),
                "User-Agent": f"firehose-proxy/{headers.get('user-agent', 'unknown')}"
            }
            logger.info(f"📤 FORWARD HEADERS: {forward_headers}")
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                forward_response = await client.post(
                    self.firehose_service_url,  # Use actual private IP for forwarding
                    content=data,
                    headers=forward_headers
                )
                
                logger.info(f"✅ FORWARDED TO: {self.firehose_display_url} -> STATUS: {forward_response.status_code}")
                
                if forward_response.status_code == 200:
                    logger.info(f"🎉 SUCCESS: Data successfully forwarded to {self.firehose_display_url}")
                    return {
                        "success": True,
                        "status_code": forward_response.status_code,
                        "message": f"Successfully forwarded to {self.firehose_display_url}",
                        "data_size": len(data),
                        "target_url": self.firehose_display_url
                    }
                else:
                    logger.warning(f"⚠️ WARNING: {self.firehose_display_url} returned {forward_response.status_code}: {forward_response.text}")
                    return {
                        "success": False,
                        "status_code": forward_response.status_code,
                        "message": f"{self.firehose_display_url} returned {forward_response.status_code}",
                        "error": forward_response.text,
                        "target_url": self.firehose_display_url
                    }
                    
        except httpx.RequestError as e:
            logger.error(f"❌ NETWORK ERROR: Failed to forward to {self.firehose_display_url}: {str(e)}")
            return {
                "success": False,
                "status_code": None,
                "message": f"Network error forwarding to {self.firehose_display_url}",
                "error": str(e),
                "target_url": self.firehose_display_url
            }
        except Exception as e:
            logger.error(f"💥 UNEXPECTED ERROR: Failed to forward to {self.firehose_display_url}: {str(e)}")
            return {
                "success": False,
                "status_code": None,
                "message": f"Unexpected error forwarding to {self.firehose_display_url}",
                "error": str(e),
                "target_url": self.firehose_display_url
            }
    
    async def forward_to_rca_service(self, data: bytes, headers: Dict[str, str]) -> Dict[str, Any]:
        """
        Forward RCA data to rca-agent-service
        
        Args:
            data: Raw RCA data
            headers: Request headers to forward
            
        Returns:
            Dict with forwarding result
        """
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                forward_response = await client.post(
                    self.rca_service_url,
                    content=data,
                    headers={
                        "Content-Type": headers.get("content-type", "application/json"),
                        "User-Agent": f"rca-proxy/{headers.get('user-agent', 'unknown')}"
                    }
                )
                
                logger.info(f"Forwarded to rca-agent-service: {forward_response.status_code}")
                
                if forward_response.status_code == 200:
                    return {
                        "success": True,
                        "status_code": forward_response.status_code,
                        "message": "Successfully forwarded to rca-agent-service"
                    }
                else:
                    logger.warning(f"RCA service returned {forward_response.status_code}: {forward_response.text}")
                    return {
                        "success": False,
                        "status_code": forward_response.status_code,
                        "message": f"RCA service returned {forward_response.status_code}",
                        "error": forward_response.text
                    }
                    
        except httpx.RequestError as e:
            logger.error(f"Failed to forward to RCA service: {str(e)}")
            return {
                "success": False,
                "status_code": None,
                "message": "Network error forwarding to RCA service",
                "error": str(e)
            }
        except Exception as e:
            logger.error(f"Unexpected error forwarding RCA data: {str(e)}")
            return {
                "success": False,
                "status_code": None,
                "message": "Unexpected error during RCA forwarding",
                "error": str(e)
            }
    
    async def run_as_service(self):
        """
        Run this as an independent service
        This could monitor logs, poll endpoints, or use other mechanisms
        """
        logger.info("Independent Firehose Forwarder Service started")
        logger.info(f"Target: {self.firehose_service_url}")
        
        # This is where you could add:
        # - Log monitoring
        # - File watching
        # - Database polling
        # - Message queue consumption
        # etc.
        
        while True:
            await asyncio.sleep(10)  # Keep service alive
            logger.debug("Forwarder service running...")

# Utility functions for manual forwarding
async def forward_data_to_firehose(data: bytes, headers: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Utility function to forward data to firehose service
    Can be called from anywhere when needed
    """
    forwarder = IndependentFirehoseForwarder()
    return await forwarder.forward_to_firehose_service(data, headers or {})

async def forward_data_to_rca_service(data: bytes, headers: Dict[str, str] = None) -> Dict[str, Any]:
    """
    Utility function to forward data to RCA service
    Can be called from anywhere when needed
    """
    forwarder = IndependentFirehoseForwarder()
    return await forwarder.forward_to_rca_service(data, headers or {})

# For running as standalone service
if __name__ == "__main__":
    async def main():
        forwarder = IndependentFirehoseForwarder()
        await forwarder.run_as_service()
    
    asyncio.run(main())
