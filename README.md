# Alert Ingestion Service

FastAPI service that receives alerts from Lambda/S3 and forwards them to the Noise Reduction service.

## Files Structure
```
alert-ingestion-service/
├── README.md
├── requirements.txt (from ecs-deployment/requirements/requirements-alert-ingestion.txt)
├── Dockerfile (from ecs-deployment/docker/Dockerfile.alert-ingestion)
├── fastapi_alert_ingestion.py (from ecs-deployment/services/fastapi_alert_ingestion.py)
├── task-definition.json (from ecs-deployment/infrastructure/task-definitions/alert-ingestion-task.json)
├── environment-config.json (from ecs-deployment/update-alert-ingestion-env.json)
└── deploy.sh (deployment script)
```

## Deployment
```bash
./deploy.sh
```

## Environment Variables
- `ENVIRONMENT`: development/production
- `NOISE_REDUCTION_API_URL`: URL of noise reduction service
- `AWS_DEFAULT_REGION`: AWS region

## Port
- Service runs on port **8000**
- Health check: `GET /health`
- Main endpoint: `POST /process`
