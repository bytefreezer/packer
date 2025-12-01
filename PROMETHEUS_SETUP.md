# ByteFreezer Packer - Prometheus Configuration Guide

This guide shows how to configure ByteFreezer Packer for OpenTelemetry metrics with Prometheus integration.

## Architecture

```
ByteFreezer Packer --SCRAPE-- Prometheus
```

OR

```
ByteFreezer Packer --PUSH--> Push Gateway --SCRAPE--> Prometheus
```

## Available Metrics

The ByteFreezer Packer tracks comprehensive file processing and data transformation metrics per tenant/dataset:

### Processing Metrics
- `bytefreezer_datasets_processed_total` - Total datasets processed per tenant/dataset
- `bytefreezer_files_processed_total` - Total files processed per tenant/dataset
- `bytefreezer_records_processed_total` - Total records processed per tenant/dataset
- `bytefreezer_bytes_processed_total` - Total bytes processed per tenant/dataset
- `bytefreezer_processing_duration` - Processing duration histogram per tenant/dataset
- `bytefreezer_conversion_duration` - Parquet conversion duration per tenant/dataset

### Upload Metrics
- `bytefreezer_uploads_total` - Total uploads per tenant/dataset
- `bytefreezer_upload_bytes_total` - Total bytes uploaded per tenant/dataset
- `bytefreezer_upload_duration` - Upload duration histogram per tenant/dataset
- `bytefreezer_upload_failures_total` - Upload failures per tenant/dataset

### Health & Cache Metrics
- `bytefreezer_dataset_health_status` - Dataset health status (1=healthy, 0=disabled)
- `bytefreezer_active_datasets` - Number of active datasets
- `bytefreezer_cache_size_bytes` - Current cache directory size in bytes
- `bytefreezer_cache_cleanup_total` - Total cache cleanup operations

## Configuration Options

### Option 1: External Scraping (Recommended)

Configure packer for external scraping from K3s Prometheus:

```yaml
otel:
  enabled: true
  prometheus_mode: true           # Enable Prometheus scraping mode
  metrics_port: 9090             # Prometheus metrics port
  metrics_host: "0.0.0.0"        # Bind to all interfaces for external scraping
```

### Option 2: Push Gateway Mode

Configure packer to push to Prometheus Push Gateway:

```yaml
otel:
  enabled: true
  prometheus_mode: false          # Disable scrape mode
  push_gateway_enabled: true      # Enable push mode
  push_gateway_url: "http://YOUR_K3S_PUSHGATEWAY_IP:9091"
  push_interval_seconds: 15       # Push every 15 seconds
```

### Option 3: OTLP Mode

Configure packer to send via OpenTelemetry Collector:

```yaml
otel:
  enabled: true
  prometheus_mode: false
  endpoint: "http://YOUR_OTEL_COLLECTOR_IP:4317"
  scrapeintervalseconds: 30
```

## Setup Steps

### For External Scraping

1. **Update ByteFreezer Packer configuration:**
   ```yaml
   otel:
     enabled: true
     prometheus_mode: true
     metrics_port: 9090
     metrics_host: "0.0.0.0"
   ```

2. **Start ByteFreezer Packer:**
   ```bash
   ./bytefreezer-packer --config config.yaml
   ```

3. **Configure Prometheus to scrape ByteFreezer Packer:**
   Add the following to your Prometheus configuration:
   ```yaml
   scrape_configs:
     - job_name: 'bytefreezer-packer'
       static_configs:
         - targets: ['your-packer-host:9090']
   ```

### For Push Gateway Mode

1. **Install and configure Push Gateway**

2. **Update ByteFreezer Packer configuration with Push Gateway URL**

3. **Configure Prometheus to scrape Push Gateway**

## Configuration Files

- **`config-prometheus-test.yaml`** - Test configuration with Prometheus enabled
- **`prometheus-external-config-packer.yaml`** - Standalone Prometheus config for external scraping
- **`prometheus-packer-targets-example.json`** - File-based service discovery example

## Verification

### Check Metrics Endpoint
```bash
curl http://YOUR_PACKER_HOST:9090/metrics
```

### Query Prometheus
Visit your Prometheus web interface and query:
```
bytefreezer_datasets_processed_total
bytefreezer_bytes_processed_total
bytefreezer_files_processed_total
```

### Check Packer Logs
Look for:
```
OpenTelemetry initialized in Prometheus mode on 0.0.0.0:9090/metrics
Starting Prometheus metrics server on 0.0.0.0:9090
```

## Troubleshooting

### 1. Connection Issues
- Verify packer is binding to `0.0.0.0:9090`
- Check firewall allows traffic on port 9090
- Ensure Prometheus can reach ByteFreezer Packer hosts

### 2. No Metrics Data
- Check if packer is processing any files (metrics are only generated during processing)
- Verify tenant database has datasets configured
- Check S3 source connectivity

### 3. Missing Labels
- Ensure tenant and dataset IDs are properly configured
- Check tenant database population in logs

The ByteFreezer Packer provides detailed metrics for monitoring file processing performance, conversion efficiency, and data throughput per tenant and dataset.