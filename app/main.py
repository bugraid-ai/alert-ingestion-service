from fastapi import FastAPI, Depends, HTTPException, Header, Request
from typing import Optional, Dict, Any
import time
import logging
import os
import asyncio
from contextlib import asynccontextmanager
from .models import AlertsPayload, ProcessingResponse
from .security import get_api_key
from .processor import process_alerts
from .forwarder import forward_data_to_firehose, forward_data_to_rca_service

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Get environment
ENVIRONMENT = os.environ.get("ENVIRONMENT", "development").lower()
if ENVIRONMENT not in ["development", "production"]:
    ENVIRONMENT = "development"

app = FastAPI(
    title="Alerts Processing API",
    description="API for processing alert data",
    version="1.0.0"
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Middleware to log requests and responses"""
    request_id = str(time.time())
    logger.info(f"Request {request_id} started: {request.method} {request.url.path}")
    start_time = time.time()
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    logger.info(f"Request {request_id} completed in {process_time:.2f}s with status {response.status_code}")
    
    return response

@app.post("/process", response_model=ProcessingResponse)
async def process_alerts_endpoint(
    payload: AlertsPayload,
    api_key: str = Depends(get_api_key),
    x_environment: Optional[str] = Header(None)
):
    """
    Process alerts - replacement for SageMaker endpoint
    
    Args:
        payload: The alerts payload
        api_key: The API key (injected by dependency)
        x_environment: Optional environment header
        
    Returns:
        ProcessingResponse: The processing response
    """
    start_time = time.time()
    
    # Validate environment in payload against header if provided
    if x_environment and payload.environment != x_environment:
        logger.warning(f"Environment mismatch: payload={payload.environment}, header={x_environment}")
        raise HTTPException(
            status_code=400, 
            detail=f"Environment mismatch: payload={payload.environment}, header={x_environment}"
        )
    
    # Validate environment is one of the supported environments
    if payload.environment not in ["development", "production"]:
        logger.warning(f"Invalid environment: {payload.environment}")
        raise HTTPException(
            status_code=400,
            detail=f"Invalid environment: {payload.environment}. Must be 'development' or 'production'."
        )
    
    try:
        logger.info(f"Processing {len(payload.alerts)} alerts from {payload.source} in {payload.environment} environment")
        
        # Process alerts
        result = await process_alerts(payload.alerts, payload.environment)
        
        end_time = time.time()
        logger.info(f"Processing completed in {end_time - start_time:.2f} seconds")
        
        return ProcessingResponse(
            message="Successfully processed alerts",
            alert_count=len(payload.alerts),
            details=result
        )
    except Exception as e:
        logger.error(f"Error processing alerts: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing alerts: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "environment": ENVIRONMENT,
        "timestamp": time.time()
    }

@app.post("/context")
async def context_endpoint(request: Request):
    """Context endpoint for Firehose data - no API key required for Firehose"""
    try:
        # Get the raw body
        body = await request.body()
        
        # Get request details
        user_agent = request.headers.get("user-agent", "")
        content_type = request.headers.get("content-type", "")
        client_ip = request.client.host if request.client else "unknown"
        
        # Log detailed received data information
        logger.info(f"🔥 FIREHOSE DATA RECEIVED")
        logger.info(f"📊 DATA SIZE: {len(body)} bytes")
        logger.info(f"🤖 USER AGENT: {user_agent}")
        logger.info(f"📋 CONTENT TYPE: {content_type}")
        logger.info(f"🌐 CLIENT IP: {client_ip}")
        
        # Show data preview (first 300 chars for visibility)
        data_preview = body[:300].decode('utf-8', errors='ignore') if len(body) > 300 else body.decode('utf-8', errors='ignore')
        logger.info(f"📄 DATA PREVIEW: {data_preview}")
        
        # Forward to firehose-ingest-dev-service
        logger.info(f"🔄 INITIATING FORWARD TO FIREHOSE INGEST SERVICE...")
        forward_result = await forward_data_to_firehose(body, dict(request.headers))
        
        if forward_result.get("success", False):
            logger.info(f"✅ CONTEXT PROCESSING COMPLETE: Data forwarded successfully")
        else:
            logger.warning(f"⚠️ CONTEXT PROCESSING WARNING: Forward failed - {forward_result.get('message', 'Unknown error')}")
        
        return {
            "status": "success", 
            "message": "Context data received and processed",
            "timestamp": time.time(),
            "data_size": len(body),
            "forward_result": forward_result
        }
        
    except Exception as e:
        logger.error(f"💥 ERROR PROCESSING CONTEXT DATA: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing context data: {str(e)}"
        )

@app.post("/rca")
async def rca_endpoint(request: Request):
    """RCA endpoint - forwards to rca-agent-service"""
    try:
        # Get the raw body
        body = await request.body()
        
        # Log the received data
        logger.info(f"Received RCA data: {len(body)} bytes")
        
        # Get the User-Agent to identify the request source
        user_agent = request.headers.get("user-agent", "")
        logger.info(f"RCA request from User-Agent: {user_agent}")
        
        # Forward to RCA agent service
        await forward_data_to_rca_service(body, dict(request.headers))
        
        return {
            "status": "success", 
            "message": "RCA data received and forwarded",
            "timestamp": time.time(),
            "data_size": len(body)
        }
        
    except Exception as e:
        logger.error(f"Error processing RCA data: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing RCA data: {str(e)}"
        )

