# ByteFreezer Packer

A high-performance data processing service that transforms compressed NDJSON files into optimized Parquet format with data lake partitioning, tenant health monitoring, and comprehensive failure handling.

## Overview

ByteFreezer Packer reads compressed NDJSON files from a source S3 bucket, processes them per tenant/dataset with configurable transformations, converts them to Parquet format with ZSTD compression, and uploads to dataset-specific S3 destinations using secure data lake best practices with tenant isolation.

### Key Features

- **Tenant Health Monitoring** - Automatic failure tracking with circuit breaker pattern
- **Multi-Tenant Security** - Tenant/dataset isolation with cross-contamination prevention
- **Data Lake Optimization** - Hive-style partitioning for DuckDB/Spark/Athena compatibility
- **Advanced Compression** - ZSTD Parquet files optimized for analytics (256 MiB target)
- **JSON Processing** - Optional flattening with configurable delimiters
- **Data Lineage** - Optional raw NDJSON storage (gzip compressed) for auditing and debugging
- **Async Processing** - Worker pool architecture with retry mechanisms
- **SOC Integration** - Comprehensive alerting for all failure scenarios
- **Distributed Locking** - PostgreSQL-based coordination for multi-instance deployments

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│  Source S3      │    │  ByteFreezer     │    │  Tenant S3          │
│  (.ndjson.gz)   │───▶│  Packer          │───▶│  (Parquet + Raw)    │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │  PostgreSQL      │
                       │  (Locks)         │
                       └──────────────────┘
```

### Processing Pipeline

1. **Housekeeping** - Periodic tenant discovery and health checks
2. **Distributed Locking** - Acquire tenant-specific locks via PostgreSQL
3. **File Aggregation** - Download and decompress .ndjson.gz files from `source-bucket/tenant-id/dataset-id/` for each dataset
4. **Transformation** - Optional JSON flattening and schema normalization
5. **Parquet Conversion** - Convert to columnar format with ZSTD compression
6. **Data Lake Upload** - Upload with Hive-style partitioning to dataset-specific S3 destinations with tenant/dataset isolation
7. **Failure Tracking** - Monitor and auto-disable problematic tenants

## Incoming Data Structure

The service expects compressed NDJSON files (.ndjson.gz) to be organized in a hierarchical structure within the source S3 bucket:

### Required Directory Structure
```
source-bucket/
├── tenant-001/                    # Tenant ID directory
│   ├── dataset-web-analytics/     # Dataset ID directory  
│   │   ├── events-2024-01-15.zip     # ZIP file containing multiple NDJSON files (recommended)
│   │   ├── events-2024-01-16.zip
│   │   └── sessions-2024-01-15.ndjson  # Raw NDJSON also supported
│   └── dataset-user-events/       # Another dataset for same tenant
│       ├── clicks-pageviews-2024-01-15.zip  # ZIP with multiple NDJSON files inside
│       └── events-2024-01-15.ndjson.gz      # Gzipped NDJSON
├── tenant-002/                    # Different tenant
│   └── dataset-sales-data/
│       ├── daily-sales-2024-01-15.zip       # ZIP containing transactions + orders
│       └── hourly-sales-2024-01-15.jsonl    # JSONL format also supported
```

### Key Requirements

1. **Tenant Directory**: Each tenant must have a directory named with their tenant ID
2. **Dataset Directory**: Each dataset within a tenant must have its own subdirectory  
3. **Compressed Files**: ZIP files containing NDJSON data (recommended) or raw NDJSON/JSONL files
4. **File Formats**: Supports `.zip`, `.ndjson`, `.jsonl`, and gzipped variants (`.ndjson.gz`, `.jsonl.gz`)
5. **File Naming**: No specific naming requirements - all supported files in dataset directories are processed  
6. **Nested Structure**: Files must be in `tenant-id/dataset-id/` structure for proper isolation
7. **ZIP Contents**: ZIP files can contain multiple NDJSON files for batch processing

### Processing Flow

The service processes each dataset independently:

1. **Discovery**: Scan `source-bucket/tenant-id/dataset-id/` for supported files (ZIP, NDJSON, JSONL)
2. **Download & Extract**: Download files; extract ZIP archives and process all NDJSON files inside
3. **Decompress**: Handle gzip-compressed files automatically  
4. **Merge**: Combine multiple files into a single dataset
5. **Transform**: Apply dataset-specific processing (flattening, validation)  
6. **Convert**: Transform to optimized Parquet format with ZSTD compression
7. **Upload**: Store in dataset-specific destination with secure isolation

### Example Data Upload

**Create and upload ZIP compressed files (recommended):**
```bash
# Create ZIP with multiple NDJSON files for web analytics
zip web-analytics-$(date +%Y%m%d).zip events.ndjson sessions.ndjson pageviews.ndjson
aws s3 cp web-analytics-$(date +%Y%m%d).zip s3://piper-bucket/tenant-001/dataset-web-analytics/

# Create ZIP with user events data
zip user-events-$(date +%Y%m%d).zip clicks.ndjson conversions.ndjson
aws s3 cp user-events-$(date +%Y%m%d).zip s3://piper-bucket/tenant-001/dataset-user-events/

# Upload gzipped NDJSON (alternative to ZIP)
gzip sales-data.ndjson
aws s3 cp sales-data.ndjson.gz s3://piper-bucket/tenant-002/dataset-sales-data/

# Upload raw NDJSON (less efficient, but supported)
aws s3 cp transactions.ndjson s3://piper-bucket/tenant-002/dataset-sales-data/
```

**Sample NDJSON Content:**
```json
{"user_id": "user123", "event": "page_view", "timestamp": "2024-01-15T14:30:22Z", "properties": {"page": "/home", "referrer": "google.com"}}
{"user_id": "user456", "event": "click", "timestamp": "2024-01-15T14:31:15Z", "properties": {"element": "signup_button", "page": "/pricing"}}
{"user_id": "user123", "event": "conversion", "timestamp": "2024-01-15T14:32:08Z", "properties": {"plan": "pro", "value": 29.99}}
```

### ZIP Compression Benefits

- **Reduced Storage Costs**: ZIP compression significantly reduces S3 storage costs
- **Faster Transfers**: Smaller files upload and download faster, improving processing speed
- **Batch Processing**: Multiple related NDJSON files can be bundled into single ZIP files
- **Better Organization**: Logical grouping of related data files (e.g., hourly batches)
- **Bandwidth Efficiency**: Lower network usage for large datasets

### Multi-Tenant Benefits

- **Isolated Processing**: Each tenant/dataset combination is processed independently
- **Parallel Processing**: Multiple datasets can be processed simultaneously  
- **Secure Separation**: Cross-tenant contamination is prevented at the directory level
- **Flexible Configuration**: Each dataset can have different processing configurations
- **Scalable Architecture**: Supports thousands of tenants with multiple datasets each

## Quick Start

### 1. Build the Service

```bash
go build -o bytefreezer-packer .
```

### 2. Configuration

Create `config.yaml` with your settings:

```yaml
# Minimal configuration example
bytefreezer:
  controller: "http://controller:8080/api/tenants"
  cachepath: "/tmp/bytefreezer"

s3source:
  bucket_name: "piper"
  region: "us-east-1"
  access_key: "your-access-key"
  secret_key: "your-secret-key"

uploadpool:
  worker_count: 5
  cleanup_source_files: true

soc:
  enabled: true
  endpoint: "https://soc.company.com/webhook"

dev: false  # Set to true for fake tenant data
```

### 3. Run the Service

```bash
./bytefreezer-packer --config config.yaml
```

## Complete Configuration

### Configuration Structure

<details>
<summary>Complete config.yaml example (click to expand)</summary>

```yaml
# Application metadata
app:
  name: "bytefreezer-packer"
  version: "1.0.0"

# Logging configuration
logging:
  level: "info"               # Log level: debug, info, warn, error
  encoding: "json"            # Log encoding: json, console

# API server configuration  
server:
  apiport: 8080              # HTTP API port for health checks and metrics

# Core bytefreezer configuration
bytefreezer:
  controller: "http://controller:8080/api/tenants"  # Controller API endpoint for tenant list
  cachepath: "/tmp/bytefreezer"                     # Local cache directory for temp files

# S3 source configuration (where .ndjson.gz files are read from)
s3source:
  bucket_name: "piper"                # Source bucket containing .ndjson.gz files
  region: "us-east-1"                  # AWS region
  access_key: ""                       # AWS access key (optional if using IAM)
  secret_key: ""                       # AWS secret key (optional if using IAM)
  secret_name: ""                      # AWS Secrets Manager secret name (optional)
  endpoint: "localhost:4566"           # Custom endpoint (MinIO/LocalStack)
  ssl: false                           # Use SSL/TLS for S3 connections

# OpenTelemetry metrics configuration
otel:
  enabled: true                    # Enable metrics collection
  endpoint: "http://otel:4317"     # OTEL collector endpoint  
  scrapeintervalseconds: 30        # Metrics scrape interval

# AWS Secrets Manager configuration (optional)
secretsmanager:
  region: "us-west-2"              # AWS region for Secrets Manager
  access_key: ""                   # AWS access key (optional if using IAM)
  secret_key: ""                   # AWS secret key (optional if using IAM) 
  endpoint: ""                     # Custom endpoint (optional, for LocalStack)
  ssl: true                        # Use SSL for connections

# PostgreSQL distributed locking configuration (preferred)
postgres:
  host: "localhost"                # PostgreSQL host
  port: 5432                       # PostgreSQL port
  database: "bytefreezer"          # Database name
  username: "bytefreezer"          # Database username
  password: "bytefreezer123"       # Database password
  ssl_mode: "disable"              # SSL mode (disable, require, verify-ca, verify-full)
  schema: "public"                 # Database schema
  max_connections: 10              # Maximum connection pool size
  connection_timeout: 30           # Connection timeout in seconds

# Housekeeping process configuration
housekeeping:
  enabled: true                # Enable periodic tenant processing
  intervalseconds: 300         # Run every 5 minutes
  cleanup:                     # Cache directory cleanup settings
    enabled: true              # Enable cache cleanup during housekeeping
    processing_dir_max_age_hours: 2    # Remove tenant directories older than 2 hours
    orphaned_files_max_age_hours: 24   # Remove orphaned cache files older than 24 hours

# Tenant failure tracking configuration
tenantfailures:
  table_name: "tenant-failures"       # Feature disabled - was DynamoDB table for failure tracking
  threshold: 3                        # Max consecutive failures before auto-disable

# Dataset failure tracking configuration  
datasetfailures:
  table_name: "dataset-failures"      # Feature disabled - was DynamoDB table for dataset failure tracking
  threshold: 3                        # Max failures in 24h before dataset auto-disable

# Upload worker pool configuration
uploadpool:
  worker_count: 5                     # Number of concurrent upload workers
  cleanup_source_files: true         # Cleanup source files after processing

# SOC alerting configuration
soc:
  enabled: true                       # Enable SOC alerting
  endpoint: "https://soc.company.com/webhook"  # SOC webhook endpoint

# Development mode
dev: false                           # Set to true for fake tenant data
```

### Cache Directory Cleanup

The service includes comprehensive cache cleanup to prevent disk space accumulation with both immediate and periodic cleanup strategies.

**Configuration:**
```yaml
housekeeping:
  cleanup:
    enabled: true
    processing_dir_max_age_hours: 2
    orphaned_files_max_age_hours: 24
```

## S3 Source Bucket Configuration

```yaml
s3source:
  bucket_name: "piper"                # Source bucket containing .ndjson.gz files
  region: "us-east-1"                  # AWS region
  endpoint: "localhost:4566"           # Custom endpoint (MinIO/LocalStack)
  ssl: false                           # Use SSL/TLS for S3 connections
  
  # Option 1: Static credentials (not recommended for production)
  access_key: "your-access-key"
  secret_key: "your-secret-key"
  
  # Option 2: Use Secrets Manager (recommended for production)
  secret_name: "s3-source-credentials"
```

## AWS Secrets Manager Configuration

```yaml
secretsmanager:
  region: "us-east-1"                  # AWS region
  endpoint: "localhost:4566"           # Custom endpoint (LocalStack/testing)
  ssl: false                           # Use SSL/TLS
  access_key: ""                       # Optional static credentials (use IAM roles in production)
  secret_key: ""                       # Optional static credentials
```

## PostgreSQL Distributed Locking Configuration

```yaml
postgres:
  host: "localhost"                    # PostgreSQL host
  port: 5432                           # PostgreSQL port
  database: "bytefreezer"              # Database name
  username: "bytefreezer"              # Database username
  password: "bytefreezer123"           # Database password
  ssl_mode: "disable"                  # SSL mode (disable, require, verify-ca, verify-full)
  schema: "public"                     # Database schema
  max_connections: 10                  # Maximum connection pool size
  connection_timeout: 30               # Connection timeout in seconds

# Lock heartbeat system - prevents stale locks from blocking processing
housekeeping:
  cleanup:
    lock_cleanup:
      enabled: true                    # Enable automatic stale lock cleanup
      stale_threshold_minutes: 5       # Remove locks older than 5 minutes without heartbeat
```

### Lock Heartbeat System

The service implements an advanced lock heartbeat mechanism to prevent stale locks from blocking processing:

- **Active Heartbeats**: Processing instances update lock heartbeat every 2 minutes
- **Stale Detection**: Housekeeping removes locks with heartbeat older than 5 minutes
- **TTL Safety Net**: Locks still expire after 30 minutes as final protection
- **Graceful Cleanup**: Heartbeat stops immediately when processing completes

This ensures crashed or network-partitioned instances don't block datasets for the full 30-minute TTL period.

## Upload Worker Pool Configuration

```yaml
uploadpool:
  worker_count: 5                      # Number of concurrent upload workers
  cleanup_source_files: true          # Auto-delete source NDJSON files after successful processing
```

## Tenant Failure Tracking Configuration

```yaml
tenantfailures:
  table_name: "tenant-failures"       # Feature disabled - was DynamoDB table for failure tracking
  threshold: 3                        # Number of failures before tenant is disabled
```

## SOC Alerting Configuration

```yaml
soc:
  enabled: true                           # Enable SOC alerts
  endpoint: "https://soc.company.com/api" # SOC webhook endpoint
  timeout: 30                             # Request timeout in seconds
```

## Development Mode

```yaml
dev: false                               # Enable development mode with fake tenant data
```

</details>

### Environment Variables

All configuration options support environment variable overrides using the `BYTEFREEZER_` prefix:

```bash
# Core configuration
export BYTEFREEZER_DEV=true
export BYTEFREEZER_LOGGING_LEVEL=debug
export BYTEFREEZER_SERVER_APIPORT=8080

# S3 source configuration  
export BYTEFREEZER_S3SOURCE_BUCKET_NAME=piper
export BYTEFREEZER_S3SOURCE_REGION=us-east-1
export BYTEFREEZER_S3SOURCE_ENDPOINT=localhost:4566

# Upload pool configuration
export BYTEFREEZER_UPLOADPOOL_WORKER_COUNT=10
export BYTEFREEZER_UPLOADPOOL_CLEANUP_SOURCE_FILES=true

# SOC alerting
export BYTEFREEZER_SOC_ENABLED=true
export BYTEFREEZER_SOC_ENDPOINT=https://soc.company.com/webhook
```

## Tenant Configuration

Tenants retrieved from the controller API support advanced processing configurations:

```json
{
  "id": "tenant-123",
  "name": "Example Tenant",
  "datasets": [
    {
      "id": "dataset-web-analytics",
      "name": "Web Analytics Data",
      "tenant_id": "tenant-123",
      "s3_destination": {
        "bucket_name": "tenant-123-web-analytics",
        "region": "us-west-2", 
        "access_key": "AKIXXXXX",
        "secret_key": "xxxxx",
        "endpoint": "",  // Optional for custom S3 endpoints
        "ssl": true
      },
      "processing_config": {
        "flatten_json": true,              // Enable JSON flattening
        "flatten_delimiter": "_",          // Custom delimiter (default: ".")  
        "enable_raw_storage": true,        // Store original NDJSON for lineage
        "partitioning_scheme": "date"      // Partitioning: "date", "date_hour", "none"
      }
    },
    {
      "id": "dataset-user-events", 
      "name": "User Events Data",
      "tenant_id": "tenant-123",
      "s3_destination": {
        "bucket_name": "tenant-123-user-events",
        "region": "us-west-2",
        "access_key": "AKIXXXXX", 
        "secret_key": "xxxxx",
        "endpoint": "",
        "ssl": true
      },
      "processing_config": {
        "flatten_json": false,             // Different config per dataset
        "enable_raw_storage": false,       // No raw storage for events
        "partitioning_scheme": "date_hour" // Hourly partitioning for events
      }
    }
  ]
}
```

### Processing Options

- **flatten_json**: Flattens nested JSON objects for better Parquet compatibility
- **flatten_delimiter**: Custom delimiter for flattened keys (default: `.`)
- **enable_raw_storage**: Stores original merged NDJSON files (gzip compressed) alongside Parquet
- **partitioning_scheme**:
  - `"hive"`: Hive-style key=value partitioning (tenant=customer-1/dataset=ebpf-data/year=2025/month=01/day=15/) - **Best for Trino/Presto**
  - `"date"`: Simple date partitioning (customer-1/ebpf-data/2025/01/15/) - **Best for DuckDB**
  - `"date_hour"`: Hourly partitioning (customer-1/ebpf-data/2025/01/15/14/) - **Best for high-volume streaming**
  - `"columnar"`: Optimized for analytical engines (customer-1/ebpf-data/dt=20250115/) - **Best for ClickHouse**
  - `"iceberg"`: Modern table format style (customer-1/ebpf-data/data/) - **Best for Iceberg/Delta Lake**
  - `"none"`: Flat directory structure (customer-1/ebpf-data/)
- **partition_layout**: Template for custom partition paths using variables like {{.TenantID}}, {{.DatasetID}}, {{.Year}}, {{.Month}}, {{.Day}}, {{.Hour}}

For detailed partitioning strategies and analytics engine optimization, see [docs/PARTITIONING.md](docs/PARTITIONING.md).

## Secure Data Lake Structure

The service creates a secure, multi-tenant data lake structure with tenant/dataset isolation:

### Architecture Overview
- **Tenants** contain multiple **Datasets**
- Each dataset has its own S3 destination and processing configuration
- Strong security isolation prevents cross-tenant data contamination

### Directory Structure
```
dataset-bucket/                                    # Dedicated bucket per dataset
├── tenant-acme-corp-a1b2c3d4/                   # Secure tenant directory (hashed)
│   ├── dataset-web-analytics-e5f6g7h8/           # Secure dataset directory (hashed)
│   │   └── data/
│   │       ├── parquet/                          # Optimized columnar data (ZSTD compressed)
│   │       │   └── year=2024/month=01/day=15/
│   │       │       └── acme-corp_web-analytics_20240115_143022_abc123def456.parquet
│   │       └── raw/                              # Original NDJSON for lineage (gzipped)
│   │           └── year=2024/month=01/day=15/
│   │               └── acme-corp_web-analytics_20240115_143022_abc123def456.ndjson.gz
│   └── dataset-user-events-i9j0k1l2/             # Another dataset for same tenant
│       └── data/
│           ├── parquet/...
│           └── raw/...
└── tenant-beta-corp-x7y8z9a0/                   # Different tenant (completely isolated)
    └── dataset-sales-data-m3n4o5p6/
        └── data/...
```

### Security Features

✅ **Cross-tenant contamination prevention** (bucket ownership registry)  
✅ **Directory-level isolation** (tenant + dataset subdirectories)  
✅ **Path traversal protection** (sanitization + validation)  
✅ **Filesystem-safe naming** (character sanitization)  
✅ **Unique filename generation** (content hashing)  
✅ **Length limits** (prevent filesystem issues)  
✅ **Case-insensitive bucket validation**  
✅ **Content hash uniqueness** (prevents overwrites)

### Multi-Tenant Benefits

- **Bucket Isolation**: Each tenant can use separate buckets for maximum security
- **Dataset Separation**: Multiple data streams per tenant with independent configurations  
- **Secure Directory Names**: SHA256-based hashing prevents directory guessing
- **Audit Trail**: Complete bucket ownership registry for compliance
- **Scalable**: Supports thousands of tenants and datasets

### Query Examples

**DuckDB with tenant/dataset isolation:**
```sql
-- Query specific tenant's dataset with partition pruning
SELECT * FROM 's3://web-analytics-bucket/tenant-acme-corp-*/dataset-web-analytics-*/data/parquet/year=2024/month=01/**/*.parquet' 
WHERE year = 2024 AND month = 1;

-- Cross-dataset analytics for single tenant
SELECT dataset_name, COUNT(*) 
FROM 's3://*/tenant-acme-corp-*/dataset-*/data/parquet/**/*.parquet'
GROUP BY dataset_name;

-- Time-series analysis with proper isolation
SELECT DATE(timestamp), COUNT(*) 
FROM 's3://events-bucket/tenant-acme-corp-*/dataset-user-events-*/data/parquet/**/*.parquet'
WHERE year = 2024 AND month >= 6;
```

**Raw NDJSON access (with security context):**
```bash
# Download and decompress raw files for debugging (with proper tenant/dataset isolation)
aws s3 cp s3://web-analytics-bucket/tenant-acme-corp-a1b2c3d4/dataset-web-analytics-e5f6g7h8/data/raw/year=2024/month=01/day=15/acme-corp_web-analytics_20240115_143022_abc123def456.ndjson.gz .
gunzip acme-corp_web-analytics_20240115_143022_abc123def456.ndjson.gz

# Direct streaming decompression with secure paths
aws s3 cp s3://events-bucket/tenant-acme-corp-a1b2c3d4/dataset-user-events-i9j0k1l2/data/raw/year=2024/month=01/day=15/acme-corp_user-events_20240115_150422_def789abc123.ndjson.gz - | gunzip | head -10

# List dataset contents with security context
aws s3 ls --recursive s3://web-analytics-bucket/tenant-acme-corp-a1b2c3d4/dataset-web-analytics-e5f6g7h8/data/parquet/year=2024/
```

## Tenant Health Monitoring

Comprehensive health monitoring with circuit breaker functionality at both tenant and dataset levels:

### Tenant-Level Monitoring
- **Failure counter** tracks consecutive upload failures per tenant
- **Automatic disabling** after threshold breaches (default: 3 failures)
- **Controller integration** calls `/tenant/{id}/disable` API endpoint
- **TTL cleanup** removes failure records after 30 days
- **SOC alerting** for all failure scenarios

### Dataset-Level Monitoring
- **Dataset isolation** - Failed datasets are skipped without affecting other datasets for the same tenant
- **24-hour failure window** - Failure count resets automatically after 24 hours
- **Automatic disabling** of problematic datasets after threshold breaches (default: 3 failures)
- **Global coordination** - PostgreSQL-based tracking works across multiple service instances
- **Critical alerting** - SOC alerts when datasets are disabled due to failures

### Configuration
```yaml
# Tenant failure tracking
tenantfailures:
  table_name: "tenant-failures"  # Feature disabled - was DynamoDB table for failure tracking
  threshold: 3                   # Failures before auto-disable

# Dataset failure tracking  
datasetfailures:
  table_name: "dataset-failures" # Feature disabled - was DynamoDB table for dataset failure tracking
  threshold: 3                   # Failures before dataset auto-disable
```

### Failure Flows

#### Tenant-Level Failures
```
Upload Failure #1 → SOC Alert (MEDIUM) → Retry
Upload Failure #2 → SOC Alert (MEDIUM) → Retry  
Upload Failure #3 → SOC Alert (CRITICAL) → Auto-disable tenant → Controller API call
```

#### Dataset-Level Failures
```
Dataset Failure #1 → SOC Alert → Continue processing other datasets
Dataset Failure #2 → SOC Alert → Continue processing other datasets
Dataset Failure #3 → SOC Alert (CRITICAL) → Auto-disable dataset → Skip in future runs
After 24 hours  → Automatic re-enable → Resume processing
```

## SOC Alerting

Comprehensive alerting integration for operational monitoring:

### Alert Types

1. **🔴 CRITICAL: Tenant Upload Permanently Failed**
   - Triggered when upload fails after max retry attempts  
   - Contains: Tenant ID, error details, file path, attempt count

2. **🟠 HIGH: Tenant Processing Failed**
   - Triggered when entire tenant processing pipeline fails
   - Contains: Tenant ID, error details, file count, data size

3. **🟡 MEDIUM: Tenant Upload Failed (Retrying)**
   - Triggered on each upload failure before max attempts
   - Contains: Tenant ID, error details, current attempt, max attempts

4. **🔵 INFO: Tenant Automatically Disabled**
   - Triggered when tenant reaches failure threshold and is disabled
   - Contains: Tenant ID, failure count, disable reason

### Configuration
```yaml
soc:
  enabled: true                             # Enable SOC alerting
  endpoint: "https://your-soc-endpoint/alerts"  # SOC webhook endpoint  
  timeout: 30                              # Request timeout in seconds
```

## Multi-Instance Deployment

bytefreezer-packer is designed for multi-instance deployment with distributed coordination via PostgreSQL locking.

### Parallel Processing Benefits

- **Increased Throughput** - Multiple instances process different tenants simultaneously
- **High Availability** - Service remains available if individual instances fail
- **Load Distribution** - Work is automatically distributed across available instances
- **Fault Tolerance** - Failed instances don't block other tenants from processing

### Multi-Instance Configuration

Each instance requires identical configuration with these considerations:

```yaml
# Shared configuration across all instances
bytefreezer:
  controller: "http://controller:8080/api/tenants"
  cachepath: "/tmp/bytefreezer-${HOSTNAME}"  # Instance-specific cache path

# PostgreSQL coordination (shared across instances)
postgres:
  host: "postgres.example.com"
  port: 5432
  database: "bytefreezer"
  username: "bytefreezer"
  password: "secure_password"
  ssl_mode: "require"

# Failure tracking disabled in current version

# Instance-specific settings
housekeeping:
  enabled: true
  intervalseconds: 300  # Stagger across instances: 300, 330, 360

uploadpool:
  worker_count: 5  # Adjust per instance based on resources

# Unique instance identification via environment
app:
  name: "bytefreezer-packer-${HOSTNAME}"

# Instance-specific cache directories to prevent cleanup conflicts
bytefreezer:
  cachepath: "/tmp/bytefreezer-${HOSTNAME}"  # Unique cache per instance
```

### Deployment Strategies

#### 1. Kubernetes Deployment

**deployment.yaml:**
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bytefreezer-packer
spec:
  replicas: 3  # Run 3 parallel instances
  selector:
    matchLabels:
      app: bytefreezer-packer
  template:
    metadata:
      labels:
        app: bytefreezer-packer
    spec:
      containers:
      - name: bytefreezer-packer
        image: bytefreezer-packer:latest
        env:
        - name: BYTEFREEZER_BYTEFREEZER_CACHEPATH
          value: "/tmp/bytefreezer-$(HOSTNAME)"
        - name: BYTEFREEZER_APP_NAME
          value: "bytefreezer-packer-$(HOSTNAME)"
        # Stagger housekeeping intervals
        - name: BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS
          value: "300"  # Different values: 300, 330, 360
        # Cache cleanup configuration
        - name: BYTEFREEZER_HOUSEKEEPING_CLEANUP_ENABLED
          value: "true"
        - name: BYTEFREEZER_HOUSEKEEPING_CLEANUP_PROCESSING_DIR_MAX_AGE_HOURS
          value: "2"
        resources:
          requests:
            memory: "512Mi"
            cpu: "0.5"
          limits:
            memory: "2Gi" 
            cpu: "2"
```

#### 2. Docker Compose Deployment

**docker-compose.yml:**
```yaml
version: '3.8'
services:
  bytefreezer-1:
    image: bytefreezer-packer:latest
    environment:
      - BYTEFREEZER_BYTEFREEZER_CACHEPATH=/tmp/bytefreezer-1
      - BYTEFREEZER_APP_NAME=bytefreezer-packer-1
      - BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS=300
      - BYTEFREEZER_UPLOADPOOL_WORKER_COUNT=5
    volumes:
      - ./config.yaml:/app/config.yaml
      - /tmp/cache1:/tmp/bytefreezer-1
    
  bytefreezer-2:
    image: bytefreezer-packer:latest
    environment:
      - BYTEFREEZER_BYTEFREEZER_CACHEPATH=/tmp/bytefreezer-2
      - BYTEFREEZER_APP_NAME=bytefreezer-packer-2
      - BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS=330
      - BYTEFREEZER_UPLOADPOOL_WORKER_COUNT=5
    volumes:
      - ./config.yaml:/app/config.yaml
      - /tmp/cache2:/tmp/bytefreezer-2
    
  bytefreezer-3:
    image: bytefreezer-packer:latest
    environment:
      - BYTEFREEZER_BYTEFREEZER_CACHEPATH=/tmp/bytefreezer-3
      - BYTEFREEZER_APP_NAME=bytefreezer-packer-3
      - BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS=360
      - BYTEFREEZER_UPLOADPOOL_WORKER_COUNT=5
    volumes:
      - ./config.yaml:/app/config.yaml
      - /tmp/cache3:/tmp/bytefreezer-3
```

#### 3. Systemd Service Deployment

**Create multiple service files:**

**/etc/systemd/system/bytefreezer-packer-1.service:**
```ini
[Unit]
Description=ByteFreezer Packer Instance 1
After=network.target

[Service]
Type=simple
User=bytefreezer
WorkingDirectory=/opt/bytefreezer-packer
ExecStart=/opt/bytefreezer-packer/bytefreezer-packer --config config.yaml
Environment=BYTEFREEZER_BYTEFREEZER_CACHEPATH=/var/cache/bytefreezer-1
Environment=BYTEFREEZER_APP_NAME=bytefreezer-packer-1
Environment=BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS=300
Restart=always

[Install]
WantedBy=multi-user.target
```

### Load Balancing Considerations

**Housekeeping Interval Staggering:**
```bash
# Instance 1: Every 5 minutes starting at :00
BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS=300

# Instance 2: Every 5 minutes starting at :00 + 30s  
BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS=300
# Add 30s delay via init script

# Instance 3: Every 5 minutes starting at :00 + 60s
BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS=300  
# Add 60s delay via init script
```

**Worker Pool Sizing:**
- **Small instances**: 3-5 workers each
- **Medium instances**: 5-10 workers each
- **Large instances**: 10-20 workers each
- **Total system capacity**: Sum of all instance workers

### Monitoring Multi-Instance Setup

**Health Check All Instances:**
```bash
# Check all instances via load balancer
curl http://load-balancer:8080/health

# Check individual instances
curl http://instance-1:8080/health
curl http://instance-2:8080/health
curl http://instance-3:8080/health
```

**Monitor Distributed Locking:**
```bash
# Check active locks across all instances
# Feature disabled - was: # Feature disabled - was: aws dynamodb scan --table-name tenant-locks --projection-expression "tenant_id,locked_by,locked_at"

# Monitor lock contention
# Feature disabled - was: # Feature disabled - was: aws dynamodb scan --table-name tenant-locks | jq '.Items | length'
```

**Log Aggregation Setup:**
```yaml
# Fluent Bit configuration for multi-instance logs
[OUTPUT]
    Name cloudwatch_logs
    Match bytefreezer-*
    region us-east-1
    log_group_name /aws/bytefreezer-packer
    log_stream_prefix instance-
    auto_create_group On
```

## Development Setup

### Complete LocalStack Development Environment

Full development environment with multiple instances for testing distributed behavior:

#### 1. LocalStack Infrastructure Setup

**Start LocalStack with all services:**
```bash
# docs/docker-compose.localstack.yml
version: '3.8'
services:
  localstack:
    image: localstack/localstack:latest
    ports:
      - "4566:4566"  # All AWS services
      - "4571:4571"  # Alternate port
    environment:
      - SERVICES=s3,secretsmanager  # PostgreSQL used instead of DynamoDB
      - DEBUG=1
      - DATA_DIR=/tmp/localstack/data
    volumes:
      - "/tmp/localstack:/tmp/localstack"
      - "/var/run/docker.sock:/var/run/docker.sock"
```

```bash
docker-compose -f docs/docker-compose.localstack.yml up -d
```

#### 2. Initialize LocalStack Resources

**Complete setup script (`scripts/setup-localstack.sh`):**
```bash
#!/bin/bash
set -e

ENDPOINT="http://localhost:4566"
AWS_CMD="aws --endpoint-url=$ENDPOINT --region us-east-1"

echo "🚀 Setting up LocalStack for bytefreezer-packer development..."

# DynamoDB table creation disabled - now using PostgreSQL for locking
echo "📊 DynamoDB table creation disabled - now using PostgreSQL for locking"
# Disabled - PostgreSQL used instead: $AWS_CMD dynamodb create-table \
    --table-name tenant-locks \
    --attribute-definitions AttributeName=tenant_id,AttributeType=S \
    --key-schema AttributeName=tenant_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --table-class STANDARD

# Disabled - PostgreSQL used instead: $AWS_CMD dynamodb create-table \
    --table-name tenant-failures \
    --attribute-definitions AttributeName=tenant_id,AttributeType=S \
    --key-schema AttributeName=tenant_id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --table-class STANDARD

# Disabled - PostgreSQL used instead: $AWS_CMD dynamodb create-table \
    --table-name dataset-failures \
    --attribute-definitions AttributeName=dataset_key,AttributeType=S \
    --key-schema AttributeName=dataset_key,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --table-class STANDARD

# Enable TTL on failure tracking tables
# Disabled - PostgreSQL used instead: $AWS_CMD dynamodb update-time-to-live \
    --table-name tenant-failures \
    --time-to-live-specification "Enabled=true,AttributeName=ttl"

# Disabled - PostgreSQL used instead: $AWS_CMD dynamodb update-time-to-live \
    --table-name dataset-failures \
    --time-to-live-specification "Enabled=true,AttributeName=ttl"

# Create S3 buckets (updated for dataset-specific buckets)
echo "🪣 Creating S3 buckets..."
$AWS_CMD s3 mb s3://piper
$AWS_CMD s3 mb s3://dev-alpha-web-bucket      # tenant-001 web analytics dataset
$AWS_CMD s3 mb s3://dev-alpha-events-bucket   # tenant-001 user events dataset  
$AWS_CMD s3 mb s3://dev-beta-sales-bucket     # tenant-002 sales dataset
$AWS_CMD s3 mb s3://dev-gamma-logs-bucket     # tenant-003 logs dataset

# Create sample compressed NDJSON files for testing with proper tenant/dataset structure
echo "📄 Creating sample test data..."
mkdir -p /tmp/test-data

# Sample web analytics data for tenant-001/dataset-001-web
cat > /tmp/test-data/web-analytics.ndjson << 'EOF'
{"user_id": "user1", "event": "page_view", "timestamp": "2024-01-15T14:30:22Z", "properties": {"page": "/home", "referrer": "google.com", "device": "mobile", "location": {"country": "US", "city": "NYC"}}}
{"user_id": "user2", "event": "click", "timestamp": "2024-01-15T14:31:15Z", "properties": {"element": "signup_button", "page": "/pricing", "device": "desktop"}}
{"user_id": "user3", "event": "conversion", "timestamp": "2024-01-15T14:32:01Z", "properties": {"plan": "pro", "value": 29.99, "currency": "USD"}}
EOF

# Sample user events data for tenant-001/dataset-001-events  
cat > /tmp/test-data/user-events.ndjson << 'EOF'
{"user_id": "user1", "event": "login", "timestamp": "2024-01-15T14:30:22Z", "properties": {"ip": "192.168.1.1", "method": "email", "device": "mobile"}}
{"user_id": "user2", "event": "logout", "timestamp": "2024-01-15T14:45:30Z", "properties": {"session_duration": 900, "pages_viewed": 8}}
{"user_id": "user3", "event": "purchase", "timestamp": "2024-01-15T14:50:15Z", "properties": {"amount": 99.99, "items": [{"id": "item1", "price": 49.99}]}}
EOF

# Sample sales data for tenant-002/dataset-002-sales
cat > /tmp/test-data/sales-data.ndjson << 'EOF'  
{"order_id": "ord1", "customer_id": "cust1", "timestamp": "2024-01-15T14:30:22Z", "amount": 199.99, "items": [{"sku": "ABC123", "qty": 2, "price": 99.99}]}
{"order_id": "ord2", "customer_id": "cust2", "timestamp": "2024-01-15T14:35:10Z", "amount": 49.99, "items": [{"sku": "XYZ789", "qty": 1, "price": 49.99}]}
EOF

# Compress NDJSON files to .ndjson.gz format
echo "🗜️ Compressing NDJSON files to .ndjson.gz format..."
cd /tmp/test-data
gzip -k web-analytics.ndjson  # Creates web-analytics.ndjson.gz
gzip -k user-events.ndjson    # Creates user-events.ndjson.gz  
gzip -k sales-data.ndjson     # Creates sales-data.ndjson.gz

# Upload .ndjson.gz files with proper tenant/dataset directory structure
echo "📤 Uploading compressed NDJSON test data with tenant/dataset structure..."

# Upload compressed NDJSON files for tenant-001/dataset-001-web
$AWS_CMD s3 cp /tmp/test-data/web-analytics.ndjson.gz s3://piper/tenant-001/dataset-001-web/web-analytics-$(date +%Y%m%d).ndjson.gz

# Upload compressed NDJSON files for tenant-001/dataset-001-events
$AWS_CMD s3 cp /tmp/test-data/user-events.ndjson.gz s3://piper/tenant-001/dataset-001-events/user-events-$(date +%Y%m%d).ndjson.gz

# Upload compressed NDJSON files for tenant-002/dataset-002-sales  
$AWS_CMD s3 cp /tmp/test-data/sales-data.ndjson.gz s3://piper/tenant-002/dataset-002-sales/sales-data-$(date +%Y%m%d).ndjson.gz

# Create Secrets Manager secret for S3 credentials
echo "🔐 Creating Secrets Manager secrets..."
$AWS_CMD secretsmanager create-secret \
    --name "s3-source-credentials" \
    --description "S3 source bucket credentials for development" \
    --secret-string '{"access_key_id": "test", "secret_access_key": "test"}'

# Verify setup
echo "✅ Verifying LocalStack setup..."
echo "DynamoDB setup disabled - using PostgreSQL:"
# Disabled - PostgreSQL used instead: $AWS_CMD dynamodb list-tables | jq '.TableNames'

echo "S3 buckets:"
$AWS_CMD s3 ls

echo "Incoming data structure (tenant/dataset hierarchy):"
$AWS_CMD s3 ls --recursive s3://piper/

echo "Expected structure should show:"
echo "  tenant-001/dataset-001-web/web-analytics-YYYYMMDD.zip"  
echo "  tenant-001/dataset-001-web/raw-web-analytics-YYYYMMDD.ndjson"
echo "  tenant-001/dataset-001-events/user-events-YYYYMMDD.zip"
echo "  tenant-001/dataset-001-events/user-events-compressed.ndjson.gz"
echo "  tenant-002/dataset-002-sales/sales-data-YYYYMMDD.zip"

echo "Secrets:"
$AWS_CMD secretsmanager list-secrets | jq '.SecretList[].Name'

echo "🎉 LocalStack setup complete!"
echo ""
echo "Next steps:"
echo "1. Run: ./bytefreezer-packer --config config/config.localstack.yaml"
echo "2. Or start multiple instances with different configs"
echo "3. Monitor logs for processing activity"
```

```bash
chmod +x scripts/setup-localstack.sh
./scripts/setup-localstack.sh
```

#### 3. LocalStack Configuration Files

**config/config.localstack.yaml:**
```yaml
# Development configuration for LocalStack
app:
  name: "bytefreezer-packer-dev"
  version: "dev"

logging:
  level: "debug"
  encoding: "console"

server:
  apiport: 8080

bytefreezer:
  controller: "http://localhost:3000/api/tenants"  # Mock controller
  cachepath: "/tmp/bytefreezer-dev"

otel:
  enabled: false  # Disable for local dev

housekeeping:
  enabled: true
  intervalseconds: 60  # Faster for development
  cleanup:
    enabled: true
    processing_dir_max_age_hours: 1  # More aggressive cleanup for dev
    orphaned_files_max_age_hours: 4

s3source:
  bucket_name: "piper"
  region: "us-east-1"
  endpoint: "localhost:4566"
  ssl: false
  access_key: "test"
  secret_key: "test"

secretsmanager:
  region: "us-east-1"
  endpoint: "localhost:4566"
  ssl: false
  access_key: "test"
  secret_key: "test"

dynamodblock:
  table_name: "tenant-locks"
  region: "us-east-1"
  endpoint: "localhost:4566"
  ssl: false
  access_key: "test"
  secret_key: "test"

uploadpool:
  worker_count: 3
  cleanup_source_files: false  # Keep files for debugging

tenantfailures:
  table_name: "tenant-failures"
  threshold: 2  # Lower threshold for testing

datasetfailures:
  table_name: "dataset-failures"
  threshold: 2  # Lower threshold for testing

soc:
  enabled: false  # Disable for local dev

dev: true  # Use fake tenant data
```

#### 4. Multi-Instance Development Testing

**Start multiple instances for parallel testing:**

```bash
# Terminal 1 - Instance 1
BYTEFREEZER_APP_NAME=dev-instance-1 \
BYTEFREEZER_BYTEFREEZER_CACHEPATH=/tmp/bytefreezer-1 \
BYTEFREEZER_SERVER_APIPORT=8081 \
./bytefreezer-packer --config config/config.localstack.yaml

# Terminal 2 - Instance 2  
BYTEFREEZER_APP_NAME=dev-instance-2 \
BYTEFREEZER_BYTEFREEZER_CACHEPATH=/tmp/bytefreezer-2 \
BYTEFREEZER_SERVER_APIPORT=8082 \
BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS=90 \
./bytefreezer-packer --config config/config.localstack.yaml

# Terminal 3 - Instance 3
BYTEFREEZER_APP_NAME=dev-instance-3 \
BYTEFREEZER_BYTEFREEZER_CACHEPATH=/tmp/bytefreezer-3 \
BYTEFREEZER_SERVER_APIPORT=8083 \
BYTEFREEZER_HOUSEKEEPING_INTERVALSECONDS=120 \
./bytefreezer-packer --config config/config.localstack.yaml
```

#### 5. Development Testing Tools

**Monitor distributed locking:**
```bash
# Watch active locks in real-time
watch -n 2 'aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name tenant-locks --projection-expression "tenant_id,locked_by,locked_at" | jq ".Items"'

# Check failure tracking
aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name tenant-failures | jq ".Items"
aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name dataset-failures | jq ".Items"
```

**Health check all instances:**
```bash
#!/bin/bash
# scripts/health-check-all.sh
for port in 8081 8082 8083; do
    echo "Instance on port $port:"
    curl -s http://localhost:$port/health | jq '.' || echo "Instance down"
    echo ""
done
```

**Upload test data during development:**
```bash
#!/bin/bash
# scripts/upload-test-data.sh

# Create fresh test data
cat > /tmp/test-live.ndjson << 'EOF'
{"tenant": "tenant-001", "event": "test", "timestamp": "$(date -Iseconds)", "data": {"test": true}}
{"tenant": "tenant-001", "event": "test2", "timestamp": "$(date -Iseconds)", "data": {"nested": {"value": 42}}}
EOF

# Upload to trigger processing
gzip -c /tmp/test-live.ndjson | aws --endpoint-url=http://localhost:4566 s3 cp - s3://piper/tenant-001/$(date +%Y/%m/%d)/test-$(date +%s).ndjson.gz

echo "Test data uploaded - check logs for processing activity"
```

#### 6. Debug and Troubleshooting

**Common LocalStack development commands:**
```bash
# Reset all data
docker-compose -f docs/docker-compose.localstack.yml down
docker-compose -f docs/docker-compose.localstack.yml up -d
./scripts/setup-localstack.sh

# Check S3 content
aws --endpoint-url=http://localhost:4566 s3 ls s3://piper --recursive

# Check DynamoDB content  
aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name tenant-locks
aws --endpoint-url=http://localhost:4566 dynamodb scan --table-name tenant-failures

# Monitor logs with filtering
tail -f /var/log/bytefreezer/*.log | grep -E "(ERROR|WARN|tenant-001)"
```

### Development Mode Features

**Built-in fake tenant/dataset data for testing:**
```yaml
dev: true  # Enables fake tenants with multiple datasets per tenant:
           # - tenant-001: 
           #   - dataset-001-web: JSON flattening + raw storage + date partitioning
           #   - dataset-001-events: No flattening + raw storage + hourly partitioning
           # - tenant-002: 
           #   - dataset-002-sales: No flattening + no raw storage + hourly partitioning  
           # - tenant-003: Various other test configurations
```

**Development-specific settings:**
- Faster housekeeping intervals (60s instead of 300s)
- Reduced failure thresholds (2 instead of 3)
- Console logging instead of JSON
- Disabled external integrations (OTEL, SOC)
- File preservation for debugging

## Monitoring & Operations

### Health Checks

The service exposes health check endpoints:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/metrics  # If OTEL is enabled
```

### Log Analysis

Key log messages to monitor:

```bash
# Successful processing
grep "Successfully completed processing pipeline" logs/

# Failure tracking
grep "Incremented failure count" logs/
grep "Successfully disabled tenant" logs/

# Performance metrics
grep "successfully uploaded.*bytes.*in" logs/
```

### Common Operations

**Check tenant failure status:**
```bash
# Feature disabled - was: aws dynamodb get-item \
  --table-name tenant-failures \
  --key '{"tenant_id":{"S":"tenant-001"}}'
```

**Manual tenant reset:**
```bash
# Feature disabled - was: aws dynamodb delete-item \
  --table-name tenant-failures \
  --key '{"tenant_id":{"S":"tenant-001"}}'
```


## Performance Tuning

### Upload Pool Configuration

Adjust worker count based on your infrastructure:

```yaml
uploadpool:
  worker_count: 10  # Increase for high-throughput scenarios
```

### Parquet Optimization

Files are automatically optimized:
- **ZSTD compression** for better compression ratios
- **Dictionary encoding** enabled by default
- **Target file size**: 256 MiB (128-512 MiB range)
- **Large batch processing** (10,000 records) for compression efficiency

### Memory Usage

Monitor memory usage for large file processing:
- Cache path: Configure adequate disk space for `bytefreezer.cachepath`
- Batch size: Adjust based on available memory (currently fixed at 10K records)

## Security

### Production Recommendations

- Use **IAM roles** instead of static access keys
- Enable **SSL/TLS** for all S3 connections
- Configure **VPC endpoints** for AWS services
- Use **AWS Secrets Manager** for credential management
- Enable **CloudTrail** for audit logging

### Network Security

- Configure security groups to restrict DynamoDB/S3 access
- Use VPC endpoints to avoid internet traffic
- Enable S3 bucket policies for tenant/dataset isolation with cross-tenant contamination prevention

## Troubleshooting

### Common Issues

**Tenant not processing:**
- Check DynamoDB locks table for stuck locks
- Verify tenant is not disabled in failures table
- Check SOC alerts for processing failures

**Upload failures:**
- Verify dataset S3 destination credentials for each dataset
- Check network connectivity to dataset-specific buckets
- Ensure tenant/dataset directory permissions are correct
- Review upload worker pool configuration

**Performance issues:**
- Monitor cache path disk usage
- Increase upload worker count if needed
- Check for DynamoDB throttling

**Cache/Disk space issues:**
- Check cache directory disk usage: `du -sh /tmp/bytefreezer*`
- Verify cleanup is enabled in configuration
- Manual cleanup: `find /tmp/bytefreezer* -type f -mtime +1 -delete`

### Debug Configuration

Enable debug logging:

```yaml
logging:
  level: "debug"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Implement changes with tests
4. Update documentation
5. Submit a pull request

## License

[Add your license information here]

---

## Architecture Decisions

### Why Parquet + ZSTD?
- Optimal compression ratios for analytical workloads
- Native support in modern data tools (DuckDB, Spark, Athena)
- Column-oriented storage for analytical queries

### Why Hive-style Partitioning?
- Enables partition pruning in all major analytics engines
- Standard practice for data lake architectures
- Optimal for time-series data analysis

### Why Tenant Health Monitoring?
- Prevents resource waste on permanently failing tenants
- Provides operational visibility into tenant issues
- Enables automatic recovery workflows

### Why PostgreSQL for Coordination?
- Serverless scaling without operational overhead
- Strong consistency for distributed locking
- TTL support for automatic cleanup