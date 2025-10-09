from typing import List, Dict, Any, Optional
from pydantic import BaseModel, Field
from datetime import datetime

class Alert(BaseModel):
    """Alert data model"""
    id: str
    title: Optional[str] = None
    service: Optional[Any] = None
    severity: Optional[str] = None
    timestamp: Optional[str] = None
    source: Optional[str] = None
    company_id: Optional[str] = None
    business_unit: Optional[str] = None
    incident_id: Optional[str] = None
    environment: Optional[str] = None
    
    class Config:
        extra = "allow"  # Allow additional fields

class AlertsPayload(BaseModel):
    """Payload for processing alerts"""
    alerts: List[Dict[str, Any]]
    source: str
    environment: str
    timestamp: str

class ProcessingResponse(BaseModel):
    """Response from alert processing"""
    message: str
    alert_count: int
    details: Optional[Dict[str, Any]] = None
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + 'Z')

