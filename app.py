import json
import boto3
import asyncio
import aioboto3
import httpx
import time
import random
from fastapi import FastAPI, HTTPException, Request, Depends, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import os
import hashlib
import re
import logging
import uuid
from contextlib import asynccontextmanager

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
HTTP_TIMEOUT = 60.0  # seconds - increased for better reliability with large payloads
MAX_RETRIES = 5  # Maximum number of retries for API calls
BASE_RETRY_DELAY = 1.0  # Base delay for exponential backoff

# Define Pydantic models for request/response validation
class AlertData(BaseModel):
    id: Optional[str] = None
    title: Optional[str] = None
    service: Optional[Any] = None
    severity: Optional[str] = None
    timestamp: Optional[str] = None
    source: Optional[str] = None
    company_id: Optional[str] = None
    business_unit: Optional[str] = None
    environment: Optional[str] = None
    
    class Config:
        extra = "allow"  # Allow additional fields

class AlertsPayload(BaseModel):
    alerts: List[AlertData]
    source: str
    environment: str
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')

class ProcessingResponse(BaseModel):
    message: str
    alert_count: int
    details: Optional[Dict[str, Any]] = None
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')

# Lifespan context manager for startup/shutdown tasks
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Initialize shared resources
    app.state.s3_client = None
    app.state.http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            connect=10.0,  # Connection timeout
            read=HTTP_TIMEOUT,  # Read timeout
            write=30.0,  # Write timeout
            pool=10.0  # Pool timeout
        ),
        limits=httpx.Limits(
            max_keepalive_connections=20,
            max_connections=100,
            keepalive_expiry=30.0
        )
    )
    
    try:
        # Yield control to FastAPI
        yield
    finally:
        # Shutdown: Clean up resources
        if app.state.s3_client:
            await app.state.s3_client.__aexit__(None, None, None)
        await app.state.http_client.aclose()

# Create FastAPI app
app = FastAPI(
    title="Alert Ingestion API",
    description="API for ingesting and processing alerts",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Environment validation middleware
@app.middleware("http")
async def validate_environment(request: Request, call_next):
    # Get environment from request headers or query params
    env = request.headers.get("X-Environment") or request.query_params.get("environment")
    
    if env:
        env = env.lower()
        if env not in ["development", "production"]:
            return JSONResponse(
                status_code=400,
                content={"detail": f"Invalid environment: {env}. Must be 'development' or 'production'."}
            )
    
    # Continue with request processing
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        # Handle protocol errors and other exceptions
        logger.error(f"Error processing request: {e}")
        return JSONResponse(
            status_code=500,
            content={"detail": f"Internal server error: {str(e)}"}
        )

# Helper function to get S3 client
async def get_s3_client(request: Request):
    if not request.app.state.s3_client:
        session = aioboto3.Session()
        request.app.state.s3_client = await session.client('s3').__aenter__()
    return request.app.state.s3_client

class AlertIngestionService:
    """Service for alert ingestion and processing"""
    
    def __init__(self, environment: str):
        """
        Initialize the alert ingestion service
        
        Args:
            environment: The environment (development or production)
        """
        # Validate environment
        if environment not in ["development", "production"]:
            raise ValueError(f"Invalid environment: {environment}. Must be 'development' or 'production'.")
        
        self.environment = environment
        self.processed_alert_ids = set()
    
    def parse_time(self, tstr: str) -> Optional[datetime]:
        """Parse timestamp from various formats"""
        if not tstr or not isinstance(tstr, str) or tstr.strip() == "":
            return None
        
        # Try multiple formats in sequence
        formats = [
            # ISO 8601 format
            lambda t: datetime.fromisoformat(t[:-1] if t.endswith('Z') else t),
            # Year only - convert to current date with that year
            lambda t: datetime(int(t), datetime.now().month, datetime.now().day) if t.isdigit() and len(t) == 4 else None,
            # Older format
            lambda t: datetime.strptime(t, '%a %b %d %Y'),
            # Unix timestamp (seconds)
            lambda t: datetime.fromtimestamp(float(t)) if t.replace('.', '', 1).isdigit() else None
        ]
        
        for format_parser in formats:
            try:
                result = format_parser(tstr)
                if result:
                    return result
            except Exception:
                continue
                
        # If all parsing attempts failed
        logger.debug(f"Could not parse timestamp: {tstr}")
        return None
    
    def generate_alert_id(self, alert_key: str, alert_data: Optional[Dict] = None) -> str:
        """Generate a compact unique ID for an alert if one is missing"""
        try:
            # If payload has stable id-like fields, hash them
            core = ""
            if isinstance(alert_data, dict):
                title = str(alert_data.get('title') or alert_data.get('trigger') or '').strip().lower()
                service = str(alert_data.get('service') or '').strip().lower()
                company_id = str(alert_data.get('company_id') or '').strip()
                ts = str(alert_data.get('timestamp') or alert_data.get('created_at') or '').strip()
                core = f"{company_id}|{service}|{title}|{ts}"
            if not core:
                # Fallback to S3 key
                core = alert_key
            # Short numeric-like id from hash
            digest = hashlib.sha256(core.encode()).hexdigest()
            # Take first 12 hex chars -> convert to int -> base10 string
            numeric = str(int(digest[:12], 16))
            return numeric
        except Exception:
            # Fallback to timestamp-based numeric
            return str(int(datetime.utcnow().timestamp() * 1000))
    
    def _make_processed_key(self, alert_data: Dict, fallback_id: str, bucket_name: Optional[str] = None, object_key: Optional[str] = None) -> str:
        """Build a stable unique deduplication key"""
        try:
            s3_key = (object_key or alert_data.get('s3_key') or alert_data.get('key'))
            bucket = (bucket_name or alert_data.get('s3_bucket') or alert_data.get('bucket_name'))
            if bucket and s3_key:
                return f"s3:{bucket}:{s3_key}"
            # Fallback to id only
            return f"id:{str(fallback_id)}"
        except Exception:
            return f"id:{str(fallback_id)}"
    
    def extract_service_info_from_path(self, alert_key: str) -> Dict[str, str]:
        """Extract service information from S3 path"""
        path_parts = alert_key.split('/')
        service_info = {}
        
        # New path structure:
        # s3://bucket/year=YYYY/month=MM/day=DD/hour=HH/data-type=alerts/severity=XX/business-unit=YY/service=ZZ/source=SS/
        
        # Extract information from path parts
        for part in path_parts:
            if part.startswith("business-unit="):
                service_info['business_unit'] = part.split('=', 1)[1]
            elif part.startswith("service="):
                service_info['service'] = part.split('=', 1)[1]
            elif part.startswith("source="):
                service_info['source'] = part.split('=', 1)[1]
            elif part.startswith("severity="):
                service_info['severity'] = part.split('=', 1)[1]
        
        return service_info
    
    def filter_valid_buckets(self, all_buckets: List[str]) -> List[str]:
        """Filter buckets by active environment and naming rules"""
        valid_buckets = []
        dev_pattern = "dev-bugraid-"
        prod_pattern = "bugraid-"
        
        for bucket in all_buckets:
            # Enforce strict environment bucket selection
            if self.environment == "development" and not bucket.startswith(dev_pattern):
                continue
            if self.environment == "production" and not (bucket.startswith(prod_pattern) and not bucket.startswith(dev_pattern)):
                # Ensure prod matches non-dev prod buckets only
                continue

            if bucket.startswith(dev_pattern) or bucket.startswith(prod_pattern):
                # Check if the bucket ends with a 12-digit number
                bucket_parts = bucket.split('-')
                if len(bucket_parts) >= 3 and bucket_parts[-1].isdigit() and len(bucket_parts[-1]) == 12:
                    logger.info(f"Found valid alert bucket: {bucket}")
                    valid_buckets.append(bucket)
        
        return valid_buckets
    
    async def process_s3_alert(self, s3_client, bucket_name: str, object_key: str, cutoff_time: datetime) -> Optional[Dict]:
        """Process a single alert from S3"""
        try:
            # Download and parse alert content
            response = await s3_client.get_object(Bucket=bucket_name, Key=object_key)
            content = await response['Body'].read()
            alert_data = json.loads(content.decode('utf-8'))
            
            # Get last modified time
            obj_info = await s3_client.head_object(Bucket=bucket_name, Key=object_key)
            last_modified = obj_info['LastModified'].replace(tzinfo=None)
            
            # Add required fields if missing
            alert_data['bucket_name'] = bucket_name
            alert_data['s3_bucket'] = bucket_name
            alert_data['s3_key'] = object_key
            
            # Resolve environment strictly from bucket
            inferred_env = "development" if bucket_name.startswith("dev-bugraid-") else "production"
            alert_data['environment'] = inferred_env
            
            if inferred_env != self.environment:
                logger.info(f"Skipping alert from env '{inferred_env}' while running in '{self.environment}' (key={object_key})")
                return None
            
            # Extract service information from path
            service_info = self.extract_service_info_from_path(object_key)
            for field, value in service_info.items():
                if field not in alert_data or not alert_data[field]:
                    alert_data[field] = value
            
            # Generate ID if missing
            if 'id' not in alert_data or not alert_data['id']:
                normalized_id = self.generate_alert_id(object_key, alert_data)
                alert_data['id'] = normalized_id
            
            # Parse timestamp
            raw_ts = alert_data.get('timestamp') or alert_data.get('created_at') or alert_data.get('ts') or ''
            ts = self.parse_time(raw_ts)
            
            if ts is None:
                # Try to parse from the filename
                try:
                    filename = object_key.split('/')[-1]
                    if filename.startswith('20') and 'T' in filename:
                        timestamp_part = filename.split('-')[0]
                        ts = self.parse_time(timestamp_part)
                except Exception:
                    pass
            
            if ts is None:
                # Use the last modified time
                ts = last_modified
            
            # Check if within cutoff time
            if ts < cutoff_time:
                return None
            
            # Add parsed time for processing
            alert_data['parsed_time'] = ts
            
            return alert_data
        
        except Exception as e:
            logger.error(f"Error processing S3 object {bucket_name}/{object_key}: {e}")
            return None
    
    async def get_alerts_from_lambda_api(self, http_client, minutes: int = 60) -> List[Dict]:
        """
        Get alerts from the Lambda FastAPI endpoint
        
        Args:
            http_client: HTTPX client for making requests
            minutes: Time window in minutes
            
        Returns:
            List of alert objects
        """
        # Determine API endpoint based on environment
        if self.environment == "development":
            api_endpoint = os.environ.get("LAMBDA_API_ENDPOINT_DEV")
            api_key = os.environ.get("LAMBDA_API_KEY_DEV")
        else:
            api_endpoint = os.environ.get("LAMBDA_API_ENDPOINT_PROD")
            api_key = os.environ.get("LAMBDA_API_KEY_PROD")
        
        if not api_endpoint:
            logger.warning(f"No Lambda API endpoint configured for {self.environment} environment")
            return []
        
        try:
            # Prepare headers
            headers = {
                "Content-Type": "application/json",
                "X-Environment": self.environment
            }
            
            if api_key:
                headers["X-API-Key"] = api_key
            
            # Prepare request payload
            payload = {
                "minutes": minutes,
                "environment": self.environment
            }
            
            logger.info(f"Requesting alerts from Lambda API at {api_endpoint}/alerts")
            
            # Make request to Lambda API with retry logic
            max_retries = 5  # Increased from 3 to 5
            retry_delay = 1.0  # seconds
            
            for attempt in range(max_retries):
                try:
                    # Make request with timeout
                    response = await http_client.post(
                        f"{api_endpoint}/alerts",
                        json=payload,
                        headers=headers,
                        timeout=30.0
                    )
                    
                    # Check for successful response
                    if response.status_code == 200:
                        data = response.json()
                        alerts = data.get("alerts", [])
                        logger.info(f"Retrieved {len(alerts)} alerts from Lambda API")
                        return alerts
                    elif response.status_code == 429:  # Rate limiting
                        logger.warning(f"Lambda API rate limited (attempt {attempt+1}/{max_retries})")
                        if attempt < max_retries - 1:
                            # Add jitter to avoid thundering herd
                            jitter = 0.1 * (attempt + 1) * (0.5 + 0.5 * (hash(str(time.time())) % 100) / 100.0)
                            wait_time = retry_delay * (2 ** attempt) + jitter
                            logger.info(f"Backing off for {wait_time:.2f}s before retry")
                            await asyncio.sleep(wait_time)  # Exponential backoff with jitter
                            continue
                        else:
                            logger.error(f"Failed to get alerts from Lambda API after {max_retries} attempts: {response.status_code} {response.text}")
                            return []
                    else:
                        logger.error(f"Failed to get alerts from Lambda API: {response.status_code} {response.text}")
                        return []
                        
                except httpx.ReadTimeout:
                    logger.warning(f"Lambda API request timed out (attempt {attempt+1}/{max_retries})")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(retry_delay * (2 ** attempt))  # Exponential backoff
                    else:
                        logger.error("Lambda API request timed out after all retry attempts")
                        return []
                except Exception as e:
                    logger.error(f"Error requesting alerts from Lambda API: {e}")
                    return []
            
            return []  # If we get here, all retries failed
        
        except Exception as e:
            logger.error(f"Error getting alerts from Lambda API: {e}")
            return []
    
    async def get_alerts_from_s3(self, s3_client, minutes: int = 60, target_bucket: Optional[str] = None) -> List[Dict]:
        """
        Get alerts directly from S3 buckets (fallback method)
        
        Args:
            s3_client: S3 client for accessing buckets
            minutes: Time window in minutes
            target_bucket: Specific bucket to target (optional)
            
        Returns:
            List of alert objects
        """
        logger.info(f"Getting alerts from S3 with {minutes} minute window")
        
        # Calculate cutoff time
        cutoff_time = datetime.utcnow() - timedelta(minutes=minutes)
        alerts = []
        
        try:
            if target_bucket:
                # Process specific bucket
                logger.info(f"Targeting specific bucket: {target_bucket}")
                
                # List objects in the bucket with the new path structure
                # Format: s3://bucket/year=YYYY/month=MM/day=DD/hour=HH/data-type=alerts/...
                
                # Get current date components for efficient filtering
                now = datetime.utcnow()
                year = now.strftime("%Y")
                month = now.strftime("%m")
                day = now.strftime("%d")
                
                # List objects for today and yesterday to cover the time window
                for day_offset in [0, 1]:
                    target_date = now - timedelta(days=day_offset)
                    year_str = target_date.strftime("%Y")
                    month_str = target_date.strftime("%m")
                    day_str = target_date.strftime("%d")
                    
                    # Create prefix for the date
                    prefix = f"year={year_str}/month={month_str}/day={day_str}/"
                    
                    # List objects with the prefix
                    paginator = s3_client.get_paginator('list_objects_v2')
                    async for page in paginator.paginate(Bucket=target_bucket, Prefix=prefix):
                        if 'Contents' in page:
                            # Process each object
                            for obj in page['Contents']:
                                key = obj['Key']
                                
                                # Check if this is an alert file (should contain data-type=alerts in path)
                                if 'data-type=alerts' in key and key.endswith('.json'):
                                    alert = await self.process_s3_alert(s3_client, target_bucket, key, cutoff_time)
                                    if alert:
                                        alerts.append(alert)
            else:
                # Get all S3 buckets
                response = await s3_client.list_buckets()
                all_buckets = [bucket['Name'] for bucket in response['Buckets']]
                
                # Filter valid buckets
                valid_buckets = self.filter_valid_buckets(all_buckets)
                logger.info(f"Found {len(valid_buckets)} valid alert buckets to process")
                
                # Process each bucket
                for bucket_name in valid_buckets:
                    # Get current date components
                    now = datetime.utcnow()
                    
                    # List objects for today and yesterday
                    for day_offset in [0, 1]:
                        target_date = now - timedelta(days=day_offset)
                        year_str = target_date.strftime("%Y")
                        month_str = target_date.strftime("%m")
                        day_str = target_date.strftime("%d")
                        
                        # Create prefix for the date
                        prefix = f"year={year_str}/month={month_str}/day={day_str}/"
                        
                        # List objects with the prefix
                        try:
                            paginator = s3_client.get_paginator('list_objects_v2')
                            async for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
                                if 'Contents' in page:
                                    # Process each object
                                    for obj in page['Contents']:
                                        key = obj['Key']
                                        
                                        # Check if this is an alert file
                                        if 'data-type=alerts' in key and key.endswith('.json'):
                                            alert = await self.process_s3_alert(s3_client, bucket_name, key, cutoff_time)
                                            if alert:
                                                alerts.append(alert)
                        except Exception as e:
                            logger.error(f"Error listing objects in bucket {bucket_name}: {e}")
        
        except Exception as e:
            logger.error(f"Error getting alerts from S3: {e}")
        
        logger.info(f"Retrieved {len(alerts)} alerts from S3")
        return alerts
    
    def serialize_datetime(self, obj):
        """Helper function to serialize datetime objects to ISO format"""
        if isinstance(obj, datetime):
            return obj.isoformat() + 'Z'
        return obj
    
    def prepare_alert_for_json(self, alert: Dict) -> Dict:
        """Prepare alert data for JSON serialization"""
        serialized_alert = {}
        for key, value in alert.items():
            if isinstance(value, datetime):
                serialized_alert[key] = self.serialize_datetime(value)
            else:
                serialized_alert[key] = value
        return serialized_alert
    
    async def process_alerts(self, alerts: List[Dict]) -> Dict:
        """
        Process alerts (without triggering client API)
        
        Args:
            alerts: List of alert objects
            
        Returns:
            Dict with processing details
        """
        if not alerts:
            logger.info("No alerts to process")
            return {"message": "No alerts to process"}
        
        try:
            # Send alerts to noise reduction agent
            logger.info(f"Sending {len(alerts)} alerts to noise reduction agent")
            
            noise_reduction_url = os.getenv("NOISE_REDUCTION_API_URL", "http://localhost:8003")
            
            async with httpx.AsyncClient(timeout=60.0) as client:
                # Preserve original sources from alerts
                original_sources = list(set(alert.get('source', 'unknown') for alert in alerts))
                source_info = original_sources[0] if len(original_sources) == 1 else f"mixed:{','.join(original_sources[:3])}"
                
                payload = {
                    "alerts": alerts,
                    "environment": self.environment,
                    "source": source_info,
                    "timestamp": datetime.utcnow().isoformat() + 'Z'
                }
                
                response = await client.post(
                    f"{noise_reduction_url}/reduce-noise",
                    json=payload
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"Successfully processed {len(alerts)} alerts through noise reduction pipeline")
                    return {
                        "message": f"Processed {len(alerts)} alerts through noise reduction",
                        "alert_count": len(alerts),
                        "filtered_count": result.get("filtered_count", 0),
                        "correlation_response": result.get("correlation_response"),
                        "environment": self.environment,
                        "timestamp": datetime.utcnow().isoformat() + 'Z'
                    }
                else:
                    logger.error(f"Failed to send alerts to noise reduction: {response.status_code} - {response.text}")
                    return {
                        "error": f"Noise reduction failed: {response.status_code}",
                        "alert_count": len(alerts),
                        "environment": self.environment,
                        "timestamp": datetime.utcnow().isoformat() + 'Z'
                    }
            
        except Exception as e:
            logger.error(f"Error processing alerts: {e}")
            return {"error": str(e)}

# API Routes
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat() + 'Z',
        "version": "1.0.0"
    }

@app.post("/ingest")
async def ingest_alerts(
    request: Request,
    background_tasks: BackgroundTasks,
    minutes: int = 60,
    target_bucket: Optional[str] = None,
    environment: Optional[str] = None
):
    """
    Ingest alerts from Lambda API with S3 fallback
    
    Args:
        minutes: Time window in minutes
        target_bucket: Specific bucket to target (optional)
        environment: Environment to use (development or production)
    """
    # Get environment from request or use default
    env = environment or request.headers.get("X-Environment") or os.environ.get("ENVIRONMENT")
    
    if not env:
        raise HTTPException(status_code=400, detail="Environment must be specified")
    
    env = env.lower()
    if env not in ["development", "production"]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid environment: {env}. Must be 'development' or 'production'."
        )
    
    # Create service instance
    service = AlertIngestionService(environment=env)
    
    # Get alerts from Lambda API
    lambda_alerts = await service.get_alerts_from_lambda_api(
        http_client=request.app.state.http_client,
        minutes=minutes
    )
    
    # If Lambda API fails or returns no alerts, fall back to S3
    if not lambda_alerts:
        logger.info("No alerts from Lambda API, falling back to S3")
        s3_client = await get_s3_client(request)
        s3_alerts = await service.get_alerts_from_s3(
            s3_client=s3_client,
            minutes=minutes,
            target_bucket=target_bucket
        )
        alerts = s3_alerts
    else:
        alerts = lambda_alerts
    
    # Process alerts in the background
    if alerts:
        background_tasks.add_task(
            service.process_alerts,
            alerts=alerts
        )
    
    # Return response
    return {
        "message": "Alert ingestion initiated",
        "alert_count": len(alerts),
        "source": "lambda_api" if lambda_alerts else "s3_fallback",
        "timestamp": datetime.utcnow().isoformat() + 'Z'
    }

@app.post("/process", response_model=ProcessingResponse)
async def process_alerts_endpoint(
    request: Request,
    payload: AlertsPayload,
    background_tasks: BackgroundTasks
):
    """
    Process alerts and forward to client API
    
    Args:
        payload: The alerts payload
    """
    # Validate environment
    if payload.environment not in ["development", "production"]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid environment: {payload.environment}. Must be 'development' or 'production'."
        )
    
    # Create service instance
    service = AlertIngestionService(environment=payload.environment)
    
    # Convert alerts to dict format
    alert_dicts = [alert.dict(exclude_unset=True) for alert in payload.alerts]
    
    # Process alerts in the background
    background_tasks.add_task(
        service.process_alerts,
        alerts=alert_dicts
    )
    
    # Return response
    return ProcessingResponse(
        message="Alerts processing initiated",
        alert_count=len(payload.alerts),
        details={
            "source": payload.source,
            "environment": payload.environment
        }
    )

# Direct execution for development server
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    
    # Get environment from environment variable
    env = os.environ.get("ENVIRONMENT", "development").lower()
    if env not in ["development", "production"]:
        logger.warning(f"Invalid environment: {env}. Using development as default.")
        env = "development"
    
    logger.info(f"Starting Alert Ingestion API in {env} environment on port {port}")
    
    uvicorn.run("fastapi_alert_ingestion:app", host="0.0.0.0", port=port, reload=True)
