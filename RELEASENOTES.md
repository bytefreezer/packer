# ByteFreezer Packer - Release Notes

## 2026-03-01 - Housekeeping Interval Config Fix

### Bug Fix

#### Housekeeping IntervalSeconds mapstructure tag mismatch
- **Issue**: `Housekeeping.IntervalSeconds` had mapstructure tag `"intervalseconds"` but YAML config uses `interval_seconds` (with underscore). Config value was silently ignored, defaulting to 10s fallback.
- **Fix**: Changed mapstructure tag from `"intervalseconds"` to `"interval_seconds"` in `config/config.go:92`
- **Additional**: Improved error message format in `main.go:430` (was printing nanoseconds, now uses `%v`)
- **Impact**: `housekeeping.interval_seconds` YAML config value now correctly parsed. Previously all packer instances ran housekeeping every 10s instead of the configured interval.

## v2.2.6 - Critical Upload Worker Pool Startup Fix (2025-10-29)

### Bug Fixes

#### 🔧 Upload Worker Pool Not Starting
- **Issue**: Upload worker pool was not started during ProcessingOrchestrator initialization
  - `TenantProcessor.Start()` was never called by the orchestrator
  - Upload workers were created but never started processing jobs
  - Files were listed and converted but uploads never completed
  - Caused data processing to stall with files accumulating in source buckets
  - Introduced during orchestrator refactoring in earlier version
- **Fix**: Added proper initialization of tenant processor in orchestrator startup
  - `ProcessingOrchestrator.Start()` now calls `tenantProcessor.Start()` (line 70)
  - This starts the upload worker pool and begins processing upload jobs
  - Upload worker pool reference stored for statistics tracking (line 74)
  - `ProcessingOrchestrator.Stop()` now properly calls `tenantProcessor.Stop()` (line 116)
  - Ensures graceful shutdown of all upload workers
- **Impact**: Packer now properly processes and uploads data files again
  - Upload worker goroutines are started and listening on upload queue
  - Files are successfully converted to Parquet and uploaded to destination
  - Source file cleanup occurs after successful uploads
- **Files Changed**:
  - `services/processing_orchestrator.go:70-74, 116`

### Root Cause Analysis

The orchestrator refactoring created a `TenantProcessor` which contains an `UploadWorkerPool`. The pool is created during `TenantProcessor` construction but requires an explicit `Start()` call to launch the worker goroutines. Without these workers running, upload jobs were queued but never processed, causing the processing pipeline to hang after Parquet conversion.

**Processing Pipeline**:
1. ✅ List source files from S3
2. ✅ Download and merge NDJSON files
3. ✅ Convert to Parquet format
4. ❌ Upload to destination S3 (workers not running)
5. ❌ Cleanup source files (never reached)

---

## v2.2.5 - Dataset Metrics Authentication Fix (2025-10-29)

### Bug Fixes

#### 🔧 Dataset Metrics API Authentication
- **Issue**: Dataset metrics recording was failing with 401 Unauthorized errors
  - DatasetMetricsClient was not sending Authorization header to control service
  - Control service requires `Authorization: Bearer <api_key>` for all non-public endpoints
  - Error logged: "Dataset metrics recording failed with status 401 for customer-1/ebpf-data"
- **Fix**: Added API key authentication to dataset metrics requests
  - Updated `DatasetMetricsClient` struct to store API key
  - Modified `NewDatasetMetricsClient()` to accept API key parameter
  - Added Authorization header with Bearer token to HTTP requests
  - Pattern matches existing health_reporting.go implementation
- **Impact**: Dataset metrics now successfully recorded to control service database
- **Files Changed**:
  - `metrics/dataset_metrics.go:15-17, 40, 96-98`
  - `config/config.go:235`

## v2.2.4 - Root-Level Parquet Metadata Path Fix (2025-10-24)

### Bug Fixes

#### 🔧 Root-Level Parquet Metadata Generation
- **Issue**: Root-level metadata files were generated at incorrect S3 path
  - `constructRootMetadataPath()` function only used S3 prefix, missing tenant_id/dataset_id
  - Example: Generated `ebpf-out/data/parquet/_metadata` instead of `ebpf-out/customer-1/ebpf-data/data/parquet/_metadata`
  - Affected datasets configured with `parquet_metadata: "root"` setting from UI
  - Leaf-level metadata generation was unaffected (working correctly)
- **Fix**: Updated root metadata path construction to include tenant and dataset directories
  - Fixed `constructRootMetadataPath()` to accept tenantID and datasetID parameters
  - Properly constructs path: `PREFIX/TENANT_ID/DATASET_ID/data/parquet`
  - Maintains consistency with leaf-level path structure
- **Impact**: Root-level metadata files (`_metadata` and `_common_metadata`) now generated at correct S3 location

### Implementation Details

**File Changed**: `services/metadata_manager_v2.go:201, 225-241`

**Before (broken)**:
```go
func (mm *MetadataManagerV2) constructRootMetadataPath(prefix string) string {
    cleanPrefix := strings.TrimSuffix(prefix, "/")
    return cleanPrefix + "/data/parquet"
}
// Result: "ebpf-out/data/parquet" (missing tenant/dataset!)
```

**After (fixed)**:
```go
func (mm *MetadataManagerV2) constructRootMetadataPath(prefix, tenantID, datasetID string) string {
    var pathParts []string
    if prefix != "" {
        cleanPrefix := strings.TrimSuffix(prefix, "/")
        pathParts = append(pathParts, cleanPrefix)
    }
    pathParts = append(pathParts, tenantID, datasetID, "data/parquet")
    return strings.Join(pathParts, "/")
}
// Result: "ebpf-out/customer-1/ebpf-data/data/parquet" (correct!)
```

### Metadata Location Setting

The `parquet_metadata` UI setting controls where Parquet metadata files are generated:

- **`root` (default)**: Single `_metadata` and `_common_metadata` at dataset root covering ALL partitions
  - Path: `PREFIX/TENANT_ID/DATASET_ID/data/parquet/_metadata`
  - Example: `ebpf-out/customer-1/ebpf-data/data/parquet/_metadata`
  - Use case: Query engines that prefer centralized metadata for entire dataset

- **`leaf`**: Separate metadata files in EACH partition directory
  - Path: `PREFIX/TENANT_ID/DATASET_ID/data/parquet/PARTITION/_metadata`
  - Example: `ebpf-out/customer-1/ebpf-data/data/parquet/year=2025/month=10/day=24/_metadata`
  - Use case: Partition-pruning query engines or very large datasets

### Configuration Mapping

**UI → Database → Packer**:
- UI field: `parquet_metadata` (values: "root" or "leaf")
- Database: `config.parquet.metadata_level`
- Packer: `dataset.ProcessingConfig.MetadataLevel`

All three levels now working correctly end-to-end.

---

## v2.2.3 - Complete Path Schema & Format Layout Support (2025-10-24)

### Bug Fixes

#### 🔧 Path Schema Configuration (Format Layout)
- **Issue**: Packer ignored the `format_layout` configuration from UI
  - UI provides: 'hive', 'date', 'tenant_dataset', 'custom'
  - Packer only supported: 'date_hour', 'date', 'none'
  - Mismatch between UI options and packer implementation
  - Custom pattern templates not implemented
- **Fix**: Complete rewrite of path schema support to match UI configuration
  - Added support for all UI format_layout options
  - Implemented custom pattern templates with variable substitution
  - Fixed "date" format to generate simple date paths (2025/01/23) instead of hive-style
  - Fixed "hive" format to properly generate hive partitioning (year=2025/month=01/day=23)
  - Added "tenant_dataset" support for flat tenant/dataset structure
  - Added "custom" support with template variables: {tenant}, {dataset}, {year}, {month}, {day}, {hour}
- **Impact**: Output paths now correctly match UI configuration settings

### Implementation Details

**File Changed**: `services/data_layout.go:69-87, 151-206`

**Path Format Examples**:

```go
// Hive partitioning (year=YYYY/month=MM/day=DD)
format_layout: "hive" → "year=2025/month=10/day=24"

// Simple date-based (YYYY/MM/DD)
format_layout: "date" → "2025/10/24"

// Date with hour
format_layout: "date_hour" → "year=2025/month=10/day=24/hour=14"

// Tenant and dataset only (no time partitioning)
format_layout: "tenant_dataset" → "" (uses tenant_id/dataset_id from base path)

// Custom pattern with variables
format_layout: "custom"
partition_layout: "{tenant}/{dataset}/{year}/{month}/{day}"
→ "customer-1/ebpf-data/2025/10/24"
```

**Supported Custom Variables**:
- `{tenant}` - Sanitized tenant ID
- `{dataset}` - Sanitized dataset ID
- `{year}` - 4-digit year (e.g., 2025)
- `{month}` - 2-digit month (e.g., 10)
- `{day}` - 2-digit day (e.g., 24)
- `{hour}` - 2-digit hour (e.g., 14)

### Complete Path Structure

With all settings applied, final S3 keys follow this structure:

```
[s3_prefix]/[tenant_id]/[dataset_id]/data/parquet/[partition_path]/[filename].parquet
```

**Example with all settings**:
```yaml
s3_prefix: "ebpf-out/"
tenant_id: "customer-1"
dataset_id: "ebpf-data"
format_layout: "hive"
```

**Result**:
```
ebpf-out/customer-1/ebpf-data/data/parquet/year=2025/month=10/day=24/customer-1_ebpf-data_20251024_150405_000_abc123def456.parquet
```

### UI Configuration Mapping

**UI Field** → **Packer Field** → **Behavior**
- `s3_bucket` → `S3Destination.BucketName` → Bucket name ✅
- `s3_prefix` → `S3Destination.Prefix` → Prepended to all paths ✅
- `format_layout: "hive"` → `ProcessingConfig.PartitioningScheme` → Hive-style partitioning ✅
- `format_layout: "date"` → `ProcessingConfig.PartitioningScheme` → Simple date paths ✅
- `format_layout: "tenant_dataset"` → `ProcessingConfig.PartitioningScheme` → No time partitioning ✅
- `format_layout: "custom"` → `ProcessingConfig.PartitioningScheme` → Custom template ✅
- `custom_format_pattern` → `ProcessingConfig.PartitionLayout` → Template string ✅
- `parquet_metadata` → `ProcessingConfig.MetadataLevel` → "root" or "leaf" ✅

**Note**: Metadata generation was implemented in v2.1.0 but had a path construction bug for root-level metadata (fixed in v2.2.4)

---

## v2.2.2 - Output Path Prefix Support (2025-10-24)

### Bug Fixes

#### 🔧 S3 Destination Prefix Configuration
- **Issue**: Packer ignored the `destination.connection.prefix` configuration when generating output paths
  - S3 files were written without the configured prefix
  - Example: Dataset configured with `prefix: "ebpf-out/"` but files written to root of tenant/dataset path
  - Affected all datasets with custom output path prefixes
- **Fix**: Updated data layout generation to respect S3 destination prefix configuration
  - `services/data_layout.go:83-95`: Added prefix handling in `GenerateSecureLayout()`
  - Prefix is prepended to generated S3 keys for both parquet and raw paths
  - Properly handles trailing slashes in prefix configuration
- **Impact**: Output files now written to correct S3 prefix paths as configured in dataset settings

### Implementation Details
```go
// Before (line 81):
basePath := fmt.Sprintf("%s/%s", layout.TenantDirectory, layout.DatasetDirectory)
layout.ParquetPath = fmt.Sprintf("%s/data/parquet/%s.parquet", basePath, baseFileName)

// After (lines 83-93):
basePath := fmt.Sprintf("%s/%s", layout.TenantDirectory, layout.DatasetDirectory)

// Apply S3 destination prefix if configured
var prefix string
if dataset.S3Destination != nil && dataset.S3Destination.Prefix != "" {
    prefix = strings.TrimSuffix(dataset.S3Destination.Prefix, "/") + "/"
}

layout.ParquetPath = fmt.Sprintf("%s%s/data/parquet/%s.parquet", prefix, basePath, baseFileName)
```

### Path Generation Examples

**Without Prefix** (old behavior):
```
customer-1/ebpf-data/data/parquet/year=2025/month=10/day=24/customer-1_ebpf-data_20251024_150405_000_abc123def456.parquet
```

**With Prefix** `"ebpf-out/"` (new behavior):
```
ebpf-out/customer-1/ebpf-data/data/parquet/year=2025/month=10/day=24/customer-1_ebpf-data_20251024_150405_000_abc123def456.parquet
```

### Partitioning Scheme Support
- Partitioning schemes (date, date_hour, none) continue to work as configured
- Prefix is applied to all output paths regardless of partitioning scheme
- Both parquet and raw NDJSON paths respect the prefix setting

---

## v2.2.1 - MinIO Support & Codebase Cleanup (2025-10-24)

### Bug Fixes

#### 🔧 MinIO Destination Type Support
- **Issue**: Packer failed to recognize datasets with `destination.type = "minio"` from Control Service
  - Error: "dataset has no S3 destination configured"
  - Affected datasets configured with MinIO instead of direct S3
- **Fix**: Updated dataset loading logic to support both "s3" and "minio" destination types
  - MinIO uses the same S3-compatible API, so both types are handled identically
  - `tenant/tenant_client.go:152`: Added `|| ds.Config.Destination.Type == "minio"` condition
- **Impact**: Packer now correctly processes datasets with MinIO destinations from Control Service database

### Codebase Cleanup

#### 🧹 Removed Deprecated Files & Binaries
- Removed old log files: `packer.log`, `packer_new.log`, `piper_new.log`
- Removed test configuration files:
  - `config-broken.yaml`
  - `config-awx-test.yaml`
  - `config-empty-bucket.yaml`
- Removed old test binaries:
  - `bytefreezer-packer-minimal`
  - `bytefreezer-packer-new`
  - `bytefreezer-packer-optimized`
  - `bytefreezer-packer-root`
  - `test-build`
  - `test_postgres_locks`

#### 📝 Updated Configuration Files
- **config-awx-exact.yaml**: Updated for Control Service integration
  - Added `control_service` configuration section
  - Marked legacy `controller` endpoint as DEPRECATED
  - Commented out deprecated global `s3destination` config
  - Updated `postgres` configuration (replaced `postgreslock`)
  - Set `dev: false` (use Control Service instead of fake data)

- **config.postgres.yaml**: Modernized configuration
  - Added `control_service` section
  - Updated `postgres` configuration format
  - Added proper spool and cache paths
  - Marked dev mode as false

#### 📚 Documentation Updates
- **AWX_DEPLOYMENT_GUIDE.md**: Updated prerequisites and configuration
  - Added Control Service as required prerequisite
  - Added PostgreSQL database as required prerequisite
  - Updated job template examples with `control_service` and `postgres` configuration
  - Removed outdated DynamoDB references

### Implementation Details
- File changed: `tenant/tenant_client.go:152`
- Before: Only checked for `ds.Config.Destination.Type == "s3"`
- After: Checks for both `"s3"` and `"minio"` types

### Migration Notes
- **Deprecated Configs**: The global `s3destination` configuration is now ignored
  - S3 destinations are configured per-dataset in Control Service
  - Legacy configs will not cause errors but will be unused
- **Control Service Required**: Packer now requires Control Service for dataset configuration
  - Dev mode still available as fallback but not recommended for production

---

## v2.2.0 - Database Cleanup & Health Status (2025-10-11)

### New Features

#### 🧹 TTL-Based Database Cleanup
- **Automatic Expiration**: All metadata records now have TTL (Time To Live) set when created
  - File metadata records: 30-day TTL
  - Generation status records: 30-day TTL
  - TTL automatically refreshed on update operations
- **Simple Cleanup**: Single `CleanupExpiredMetadata()` function removes all expired records
  - No configuration needed - TTL is set at record creation time
  - Consistent with other ByteFreezer components (piper uses same approach)
  - Returns counts of deleted records for monitoring

### Database Schema Changes
- **New Migration**: `003_add_ttl_columns.sql` adds TTL support
  - Adds `ttl` column to `packer_parquet_file_metadata` table
  - Adds `ttl` column to `packer_metadata_generation_status` table
  - Creates indexes on TTL columns for fast cleanup queries
  - Migration runs automatically on service startup

### Implementation Details
- `storage/postgres_metadata_client.go:109-143`: UpsertFileMetadata sets 30-day TTL
- `storage/postgres_metadata_client.go:185-218`: UpdateGenerationStatus sets 30-day TTL
- `storage/postgres_metadata_client.go:325-357`: CleanupExpiredMetadata implementation
- `storage/migrations/003_add_ttl_columns.sql`: TTL column migration

### Performance Improvements
- Reduces PostgreSQL storage usage by removing expired metadata
- Improves query performance on smaller tables
- Prevents long-term metadata accumulation
- Indexed TTL columns for fast cleanup queries

### Recommended Usage
Run cleanup periodically from control service housekeeping:
```go
// Clean up all expired metadata records
filesDeleted, statusDeleted, err := metadataClient.CleanupExpiredMetadata(ctx)
```

---

## v2.1.0 - Control Service Integration (2025-10-04)

### New Features

#### 🌐 Control Service Integration (Phase 2)
- **Control Client Library**: Integration with `bytefreezer-control/client` package for Control Service API interaction
- **Configuration Fallback**: 3-tier fallback for tenant configuration (Control Service API → Legacy Controller → Dev Mode)
- **Zero Dependencies**: Control client uses only Go standard library for maximum portability
- **Configuration Caching**: 5-minute cache with automatic invalidation for Control Service responses

#### 🔧 Tenant Management Enhancements
- **Priority-Based Fetching**: Automatic fallback between Control Service, legacy controller, and dev mode
- **Active Tenant Filtering**: Only active tenants from Control Service are loaded
- **Graceful Degradation**: Service continues to function even if Control Service is unavailable
- **Enhanced Logging**: Detailed logging for tenant source (Control Service, legacy, or dev mode)

### Configuration Changes

#### New Control Service Configuration
```yaml
control_service:
  enabled: true
  base_url: "http://bytefreezer-control:8080"
  api_key: "your-api-key"
  timeout_seconds: 30
  account_id: "default-account-id"
  tenant_id: "default-tenant-id"
```

#### Deprecated Configuration (Still Supported)
- `dev: true` - Use `control_service.enabled: false` instead
- `bytefreezer.controller` - Use `control_service.base_url` instead

### Technical Details

#### Control Client Features
- **Account Management**: Create, read, update, delete accounts
- **Tenant Management**: Manage tenants within accounts
- **Type-Safe Helpers**: GetConfigString, GetConfigInt, GetConfigBool
- **HTTP Client**: Proper headers, timeouts, error handling
- **Context Support**: context.Context for all API calls

#### Tenant Fetching Priority
1. **Control Service API** (primary, if enabled and configured)
2. **Legacy Controller** (fallback, if configured)
3. **Dev Mode** (final fallback, fake data)

#### Implementation Details
- `tenant/tenant_client.go:74`: Updated FetchTenants() with 3-tier fallback
- `tenant/tenant_client.go:107`: New fetchTenantsFromControlService() method
- `tenant/tenant_client.go:139`: fetchTenantsFromLegacyController() method
- `config/config.go:27`: New ControlServiceConfig struct
- `go.mod:112`: Local replace directive for bytefreezer-control
- Imports: Using `github.com/bytefreezer/control/client`

### Migration Guide

#### For New Deployments
Use Control Service configuration:
```yaml
control_service:
  enabled: true
  base_url: "http://bytefreezer-control:8080"
  api_key: "your-api-key"
  account_id: "your-account-id"
```

#### For Existing Deployments
Legacy controller configuration continues to work:
```yaml
bytefreezer:
  controller: "http://legacy-controller/tenants"
dev: false
```

Or enable Control Service while keeping legacy as fallback:
```yaml
control_service:
  enabled: true
  base_url: "http://bytefreezer-control:8080"
  api_key: "your-api-key"
  account_id: "your-account-id"
bytefreezer:
  controller: "http://legacy-controller/tenants"  # Used as fallback
```

### Compatibility
- **Backward Compatible**: All existing configurations continue to work
- **No Breaking Changes**: Legacy controller and dev mode still supported
- **Graceful Fallback**: Service continues if Control Service is unavailable

### Testing
✅ Builds successfully with bytefreezer-control/client integration
⏳ End-to-end testing with live Control Service pending

---

## Previous Releases

### v1.x.x Series
- Legacy controller endpoint integration
- Development mode with fake tenant data
- Single-source tenant configuration
