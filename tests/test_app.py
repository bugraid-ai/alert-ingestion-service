# Repo/alert-ingestion-service/tests/test_app.py

import pytest
from datetime import datetime

# Import service from app.py
from app import AlertIngestionService


def test_parse_time_iso_format():
    service = AlertIngestionService("development")
    ts = "2024-01-01T12:00:00Z"
    result = service.parse_time(ts)
    assert isinstance(result, datetime)
    assert result.year == 2024
    assert result.month == 1
    assert result.day == 1


def test_generate_alert_id_is_consistent():
    service = AlertIngestionService("development")
    id1 = service.generate_alert_id("key1", {"title": "Test Alert"})
    id2 = service.generate_alert_id("key1", {"title": "Test Alert"})
    assert id1 == id2  # same input → same ID
    assert isinstance(id1, str)
    assert id1.isdigit()  # numeric-like string


def test_filter_valid_buckets():
    service = AlertIngestionService("development")
    all_buckets = [
        "dev-bugraid-123456789012",   # valid dev
        "bugraid-123456789012",       # valid prod
        "random-bucket"               # invalid
    ]
    result = service.filter_valid_buckets(all_buckets)
    assert "dev-bugraid-123456789012" in result
    assert "bugraid-123456789012" not in result  # filtered out since env=dev
    assert "random-bucket" not in result


def test_prepare_alert_for_json_serializes_datetime():
    service = AlertIngestionService("development")
    alert = {"id": "1", "timestamp": datetime(2024, 1, 1, 12, 0, 0)}
    result = service.prepare_alert_for_json(alert)
    assert "timestamp" in result
    assert isinstance(result["timestamp"], str)  # should be ISO string
    assert result["timestamp"].endswith("Z")
