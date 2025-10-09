# Alert Ingestion Service - Production

This is the production version of the Alert Ingestion service for BugRaid AI platform.

## Features

- Receives alerts from Lambda/S3 events
- Forwards processed alerts to noise reduction service
- Production environment configuration
- Health check endpoints

## Production Deployment

This service is deployed to AWS ECS Fargate in the production environment with the following configuration:

- **Cluster**: bugraid-prd-ai-ml
- **VPC**: vpc-0536ae4785eeab939
- **Service Discovery**: alert-ingestion.bugraid-prd-ai-ml.local
- **CPU**: 512
- **Memory**: 1024 MB

## Environment Variables

- `ENVIRONMENT`: production
- `AWS_DEFAULT_REGION`: ap-southeast-1
- `NOISE_REDUCTION_API_URL`: URL for noise reduction service
- `LAMBDA_API_ENDPOINT_PROD`: Production Lambda API endpoint
- `LAMBDA_API_KEY_PROD`: Production API key

## Health Check

The service provides a health check endpoint at `/health` for load balancer health monitoring.

## Production Branch

This code is maintained in the `prod` branch for production deployments.
