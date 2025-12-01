# Configuration API Documentation

The bytefreezer-packer includes an API endpoint for monitoring and retrieving the current system configuration.

## Configuration Endpoint

### Get System Configuration

**Endpoint:** `GET /api/v1/config`

Retrieves the current system configuration with sensitive values masked for security.

### Response Structure

```json
{
  "app": {
    "name": "bytefreezer-packer",
    "version": "v1.0.0"
  },
  "server": {
    "api_port": 8082
  },
  "bytefreezer": {
    "controller": "https://controller.example.com",
    "cache_path": "/tmp/bytefreezer-cache"
  },
  "soc": {
    "enabled": true,
    "endpoint": "https://soc.example.com",
    "timeout": 30
  },
  "s3_source": {
    "bucket_name": "source-data-bucket",
    "region": "us-east-1",
    "access_key": "AKIA***2345",
    "secret_key": "exam***defg",
    "secret_name": "source-credentials",
    "endpoint": "s3.us-east-1.amazonaws.com",
    "ssl": true
  },
  "secrets_manager": {
    "region": "us-east-1",
    "access_key": "AKIA***9876",
    "secret_key": "anot***cret"
  },
  "postgres_lock": {
    "database": "bytefreezer-locks",
    "region": "us-east-1",
    "endpoint": "postgres.example.com:5432",
    "ssl": true,
    "access_key": "AKIA***1111",
    "secret_key": "dyna***lock"
  },
  "upload_pool": {
    "worker_count": 10,
    "cleanup_source_files": true
  },
  "tenant_failures": {
    "database": "tenant-failures",
    "threshold": 5
  },
  "dataset_failures": {
    "database": "dataset-failures", 
    "threshold": 3
  },
  "processed_files": {
    "database": "processed-files",
    "region": "us-east-1",
    "endpoint": "postgres.example.com:5432",
    "ssl": true,
    "access_key": "AKIA***4444",
    "secret_key": "proc***files",
    "ttl_days": 30,
    "dlq": {
      "enabled": true,
      "bucket_name": "dlq-bucket",
      "max_age_hours": 48,
      "check_interval_minutes": 60
    }
  },
  "otel": {
    "enabled": true,
    "endpoint": "http://localhost:4317",
    "scrape_interval_seconds": 60
  },
  "housekeeping": {
    "enabled": true,
    "interval_seconds": 300,
    "cleanup": {
      "enabled": true,
      "processing_dir_max_age_hours": 24,
      "orphaned_files_max_age_hours": 48
    }
  },
  "dev": false,
  "component_status": {
    "tenant_database": true,
    "soc_alert_client": true,
    "s3_source_client": true,
    "secrets_manager": true,
    "postgres_lock": true,
    "tenant_failure_tracker": true,
    "dataset_failure_tracker": true,
    "processed_file_tracker": true
  }
}
```

### Configuration Sections

#### Application (`app`)
- **`name`**: Application name
- **`version`**: Application version

#### Server (`server`)  
- **`api_port`**: API server port

#### ByteFreezer (`bytefreezer`)
- **`controller`**: Controller service URL
- **`cache_path`**: Local cache directory path

#### Security Operations Center (`soc`)
- **`enabled`**: SOC alerting enabled/disabled
- **`endpoint`**: SOC alert webhook URL
- **`timeout`**: Request timeout in seconds

#### S3 Source (`s3_source`)
- **`bucket_name`**: Source S3 bucket name
- **`region`**: AWS region
- **`access_key`**: AWS access key (masked)
- **`secret_key`**: AWS secret key (masked)
- **`secret_name`**: AWS Secrets Manager secret name
- **`endpoint`**: S3 endpoint URL
- **`ssl`**: SSL/TLS enabled

#### Secrets Manager (`secrets_manager`)
- **`region`**: AWS region
- **`access_key`**: AWS access key (masked)
- **`secret_key`**: AWS secret key (masked)

#### PostgreSQL Lock (`postgres_lock`)
- **`table_name`**: PostgreSQL table for distributed locks
- **`region`**: AWS region
- **`endpoint`**: PostgreSQL endpoint URL
- **`ssl`**: SSL/TLS enabled
- **`access_key`**: AWS access key (masked)
- **`secret_key`**: AWS secret key (masked)

#### Upload Pool (`upload_pool`)
- **`worker_count`**: Number of upload worker threads
- **`cleanup_source_files`**: Auto-delete source files after processing

#### Tenant Failures (`tenant_failures`)
- **`table_name`**: PostgreSQL table for tenant failure tracking
- **`threshold`**: Failure count before tenant is disabled

#### Dataset Failures (`dataset_failures`)
- **`table_name`**: PostgreSQL table for dataset failure tracking
- **`threshold`**: Failure count before dataset is disabled

#### Processed Files (`processed_files`)
- **`table_name`**: PostgreSQL table for processed file tracking
- **`region`**: AWS region
- **`endpoint`**: PostgreSQL endpoint URL
- **`ssl`**: SSL/TLS enabled
- **`access_key`**: AWS access key (masked)
- **`secret_key`**: AWS secret key (masked)
- **`ttl_days`**: Record time-to-live in days
- **`dlq`**: Dead letter queue configuration

#### OpenTelemetry (`otel`)
- **`enabled`**: OTEL metrics enabled/disabled
- **`endpoint`**: OTEL collector endpoint
- **`scrape_interval_seconds`**: Metrics collection interval

#### Housekeeping (`housekeeping`)
- **`enabled`**: Housekeeping tasks enabled/disabled
- **`interval_seconds`**: Housekeeping run interval
- **`cleanup`**: File cleanup configuration

#### Component Status (`component_status`)
Real-time status of all system components:
- **`tenant_database`**: Tenant database health
- **`soc_alert_client`**: SOC alert client status
- **`s3_source_client`**: S3 source connectivity
- **`secrets_manager`**: AWS Secrets Manager client
- **`postgres_lock`**: PostgreSQL lock client
- **`tenant_failure_tracker`**: Tenant failure tracking
- **`dataset_failure_tracker`**: Dataset failure tracking
- **`processed_file_tracker`**: Processed file tracking

### Security Features

- **Sensitive Data Masking**: API keys and secrets are automatically masked
- **Component Health Monitoring**: Real-time status of all system components  
- **Configuration Validation**: Displays current active configuration values

### Usage Examples

**Check system configuration:**
```bash
curl -X GET http://localhost:8082/api/v1/config | jq .
```

**Verify component health:**
```bash
curl -X GET http://localhost:8082/api/v1/config | jq '.component_status'
```

**Check S3 source configuration:**
```bash
curl -X GET http://localhost:8082/api/v1/config | jq '.s3_source'
```

**Monitor failure tracking settings:**
```bash
curl -X GET http://localhost:8082/api/v1/config | jq '{tenant_failures, dataset_failures}'
```

### Integration with OpenAPI

This endpoint is fully documented in the OpenAPI schema and available in the Swagger UI at `/v2/docs`.