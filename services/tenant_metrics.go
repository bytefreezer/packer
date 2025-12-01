package services

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// TenantMetrics provides comprehensive tenant-specific metrics tracking
type TenantMetrics struct {
	meter metric.Meter

	// Processing metrics
	datasetsProcessedTotal metric.Int64Counter
	tenantsProcessedTotal  metric.Int64Counter // For backward compatibility and tenant-level aggregation
	filesProcessedTotal    metric.Int64Counter
	recordsProcessedTotal  metric.Int64Counter
	bytesProcessedTotal    metric.Int64Counter
	processingDuration     metric.Int64Histogram
	conversionDuration     metric.Int64Histogram

	// Upload metrics
	uploadsTotal        metric.Int64Counter
	uploadFailuresTotal metric.Int64Counter
	uploadBytesTotal    metric.Int64Counter
	uploadDuration      metric.Int64Histogram

	// Error metrics
	processingErrorsTotal metric.Int64Counter
	lockAcquisitionErrors metric.Int64Counter
	datasetFailuresTotal  metric.Int64Counter
	tenantFailuresTotal   metric.Int64Counter // For backward compatibility

	// Health metrics
	datasetHealthStatus metric.Int64Gauge
	tenantHealthStatus  metric.Int64Gauge // For backward compatibility
	activeTenantsGauge  metric.Int64Gauge
	activeDatasetsGauge metric.Int64Gauge

	// Cache metrics
	cacheCleanupTotal  metric.Int64Counter
	cacheCleanupErrors metric.Int64Counter
	cacheSizeBytes     metric.Int64Gauge
}

// NewTenantMetrics creates a new tenant metrics instance
func NewTenantMetrics(meter metric.Meter) (*TenantMetrics, error) {
	// Handle nil meter gracefully - return a stub implementation
	if meter == nil {
		return &TenantMetrics{meter: nil}, nil
	}

	tm := &TenantMetrics{meter: meter}

	var err error

	// Processing metrics
	tm.datasetsProcessedTotal, err = meter.Int64Counter(
		"bytefreezer_datasets_processed_total",
		metric.WithDescription("Total number of datasets processed"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.tenantsProcessedTotal, err = meter.Int64Counter(
		"bytefreezer_tenants_processed_total",
		metric.WithDescription("Total number of tenants processed (aggregated from datasets)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.filesProcessedTotal, err = meter.Int64Counter(
		"bytefreezer_files_processed_total",
		metric.WithDescription("Total number of NDJSON files processed per dataset"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.recordsProcessedTotal, err = meter.Int64Counter(
		"bytefreezer_records_processed_total",
		metric.WithDescription("Total number of JSON records processed per tenant"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.bytesProcessedTotal, err = meter.Int64Counter(
		"bytefreezer_bytes_processed_total",
		metric.WithDescription("Total bytes processed per tenant"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	tm.processingDuration, err = meter.Int64Histogram(
		"bytefreezer_processing_duration_ms",
		metric.WithDescription("Duration of tenant processing in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	tm.conversionDuration, err = meter.Int64Histogram(
		"bytefreezer_conversion_duration_ms",
		metric.WithDescription("Duration of NDJSON to Parquet conversion in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	// Upload metrics
	tm.uploadsTotal, err = meter.Int64Counter(
		"bytefreezer_uploads_total",
		metric.WithDescription("Total number of uploads attempted per tenant"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.uploadFailuresTotal, err = meter.Int64Counter(
		"bytefreezer_upload_failures_total",
		metric.WithDescription("Total number of upload failures per tenant"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.uploadBytesTotal, err = meter.Int64Counter(
		"bytefreezer_upload_bytes_total",
		metric.WithDescription("Total bytes uploaded per tenant"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	tm.uploadDuration, err = meter.Int64Histogram(
		"bytefreezer_upload_duration_ms",
		metric.WithDescription("Duration of uploads in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	// Error metrics
	tm.processingErrorsTotal, err = meter.Int64Counter(
		"bytefreezer_processing_errors_total",
		metric.WithDescription("Total processing errors per tenant"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.lockAcquisitionErrors, err = meter.Int64Counter(
		"bytefreezer_lock_acquisition_errors_total",
		metric.WithDescription("Total lock acquisition errors per tenant"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.datasetFailuresTotal, err = meter.Int64Counter(
		"bytefreezer_dataset_failures_total",
		metric.WithDescription("Total dataset failure count increments"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.tenantFailuresTotal, err = meter.Int64Counter(
		"bytefreezer_tenant_failures_total",
		metric.WithDescription("Total tenant failure count increments (aggregated from datasets)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	// Health metrics
	tm.datasetHealthStatus, err = meter.Int64Gauge(
		"bytefreezer_dataset_health_status",
		metric.WithDescription("Dataset health status (1=healthy, 0=disabled)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.tenantHealthStatus, err = meter.Int64Gauge(
		"bytefreezer_tenant_health_status",
		metric.WithDescription("Tenant health status (1=healthy, 0=disabled, aggregated from datasets)"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.activeTenantsGauge, err = meter.Int64Gauge(
		"bytefreezer_active_tenants",
		metric.WithDescription("Number of active tenants"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.activeDatasetsGauge, err = meter.Int64Gauge(
		"bytefreezer_active_datasets",
		metric.WithDescription("Number of active datasets"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	// Cache metrics
	tm.cacheCleanupTotal, err = meter.Int64Counter(
		"bytefreezer_cache_cleanup_total",
		metric.WithDescription("Total cache cleanup operations"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.cacheCleanupErrors, err = meter.Int64Counter(
		"bytefreezer_cache_cleanup_errors_total",
		metric.WithDescription("Total cache cleanup errors"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}

	tm.cacheSizeBytes, err = meter.Int64Gauge(
		"bytefreezer_cache_size_bytes",
		metric.WithDescription("Current cache directory size in bytes"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	return tm, nil
}

// isEnabled checks if metrics are available and enabled
func (tm *TenantMetrics) isEnabled() bool {
	return tm != nil && tm.meter != nil
}

// RecordDatasetProcessed records a successful dataset processing
func (tm *TenantMetrics) RecordDatasetProcessed(ctx context.Context, tenantID, tenantName, datasetID, datasetName string, duration time.Duration) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.String("dataset_id", datasetID),
		attribute.String("dataset_name", datasetName),
	}

	tm.datasetsProcessedTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	tm.processingDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
}

// RecordTenantProcessed records a successful tenant processing (backward compatibility)
func (tm *TenantMetrics) RecordTenantProcessed(ctx context.Context, tenantID, tenantName string, duration time.Duration) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
	}

	tm.tenantsProcessedTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	tm.processingDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
}

// RecordDatasetFilesProcessed records the number of files processed for a dataset
func (tm *TenantMetrics) RecordDatasetFilesProcessed(ctx context.Context, tenantID, tenantName, datasetID, datasetName string, fileCount int, totalBytes int64) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.String("dataset_id", datasetID),
		attribute.String("dataset_name", datasetName),
	}

	tm.filesProcessedTotal.Add(ctx, int64(fileCount), metric.WithAttributes(attrs...))
	tm.bytesProcessedTotal.Add(ctx, totalBytes, metric.WithAttributes(attrs...))
}

// RecordFilesProcessed records the number of files processed for a tenant (backward compatibility)
func (tm *TenantMetrics) RecordFilesProcessed(ctx context.Context, tenantID, tenantName string, fileCount int, totalBytes int64) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
	}

	tm.filesProcessedTotal.Add(ctx, int64(fileCount), metric.WithAttributes(attrs...))
	tm.bytesProcessedTotal.Add(ctx, totalBytes, metric.WithAttributes(attrs...))
}

// RecordRecordsProcessed records the number of JSON records processed
func (tm *TenantMetrics) RecordRecordsProcessed(ctx context.Context, tenantID, tenantName string, recordCount int64) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
	}

	tm.recordsProcessedTotal.Add(ctx, recordCount, metric.WithAttributes(attrs...))
}

// RecordConversionDuration records the duration of NDJSON to Parquet conversion
func (tm *TenantMetrics) RecordConversionDuration(ctx context.Context, tenantID, tenantName string, duration time.Duration) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
	}

	tm.conversionDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))
}

// RecordUpload records an upload attempt (success or failure)
func (tm *TenantMetrics) RecordUpload(ctx context.Context, tenantID, tenantName, uploadType string, success bool, bytes int64, duration time.Duration) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.String("upload_type", uploadType), // "parquet" or "raw"
		attribute.Bool("success", success),
	}

	tm.uploadsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	tm.uploadDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))

	if success {
		tm.uploadBytesTotal.Add(ctx, bytes, metric.WithAttributes(attrs...))
	} else {
		tm.uploadFailuresTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordDatasetUpload records a dataset upload attempt (success or failure)
func (tm *TenantMetrics) RecordDatasetUpload(ctx context.Context, tenantID, tenantName, datasetID, datasetName, uploadType string, success bool, bytes int64, duration time.Duration) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.String("dataset_id", datasetID),
		attribute.String("dataset_name", datasetName),
		attribute.String("upload_type", uploadType), // "parquet" or "raw"
		attribute.Bool("success", success),
	}

	tm.uploadsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	tm.uploadDuration.Record(ctx, duration.Milliseconds(), metric.WithAttributes(attrs...))

	if success {
		tm.uploadBytesTotal.Add(ctx, bytes, metric.WithAttributes(attrs...))
	} else {
		tm.uploadFailuresTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordProcessingError records a processing error
func (tm *TenantMetrics) RecordProcessingError(ctx context.Context, tenantID, tenantName, errorType string) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.String("error_type", errorType), // "conversion", "download", "merge", etc.
	}

	tm.processingErrorsTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordLockAcquisitionError records a lock acquisition error
func (tm *TenantMetrics) RecordLockAcquisitionError(ctx context.Context, tenantID, tenantName string) {
	if !tm.isEnabled() {
		return
	}

	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
	}

	tm.lockAcquisitionErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordTenantFailure records a tenant failure (when failure counter is incremented)
func (tm *TenantMetrics) RecordTenantFailure(ctx context.Context, tenantID, tenantName string, failureCount int) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.Int("failure_count", failureCount),
	}

	tm.tenantFailuresTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// UpdateTenantHealthStatus updates the health status of a tenant
func (tm *TenantMetrics) UpdateTenantHealthStatus(ctx context.Context, tenantID, tenantName string, isHealthy bool) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
	}

	status := int64(0)
	if isHealthy {
		status = 1
	}

	tm.tenantHealthStatus.Record(ctx, status, metric.WithAttributes(attrs...))
}

// UpdateActiveTenants updates the number of active tenants
func (tm *TenantMetrics) UpdateActiveTenants(ctx context.Context, count int) {
	if !tm.isEnabled() {
		return
	}
	tm.activeTenantsGauge.Record(ctx, int64(count))
}

// RecordCacheCleanup records cache cleanup operations
func (tm *TenantMetrics) RecordCacheCleanup(ctx context.Context, cleanupType string, itemsRemoved int, success bool) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("cleanup_type", cleanupType), // "tenant_dirs", "orphaned_files"
		attribute.Bool("success", success),
		attribute.Int("items_removed", itemsRemoved),
	}

	tm.cacheCleanupTotal.Add(ctx, 1, metric.WithAttributes(attrs...))

	if !success {
		tm.cacheCleanupErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// UpdateCacheSize updates the current cache directory size
func (tm *TenantMetrics) UpdateCacheSize(ctx context.Context, sizeBytes int64) {
	if !tm.isEnabled() {
		return
	}
	tm.cacheSizeBytes.Record(ctx, sizeBytes)
}

// RecordDatasetRecordsProcessed records the number of JSON records processed for a dataset
func (tm *TenantMetrics) RecordDatasetRecordsProcessed(ctx context.Context, tenantID, tenantName, datasetID, datasetName string, recordCount int64) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.String("dataset_id", datasetID),
		attribute.String("dataset_name", datasetName),
	}

	tm.recordsProcessedTotal.Add(ctx, recordCount, metric.WithAttributes(attrs...))
}

// RecordDatasetFailure records a dataset failure (when failure counter is incremented)
func (tm *TenantMetrics) RecordDatasetFailure(ctx context.Context, tenantID, tenantName, datasetID, datasetName string, failureCount int) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.String("dataset_id", datasetID),
		attribute.String("dataset_name", datasetName),
		attribute.Int("failure_count", failureCount),
	}

	tm.datasetFailuresTotal.Add(ctx, 1, metric.WithAttributes(attrs...))

	// Also record at tenant level for aggregation
	tenantAttrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.Int("failure_count", failureCount),
	}
	tm.tenantFailuresTotal.Add(ctx, 1, metric.WithAttributes(tenantAttrs...))
}

// UpdateDatasetHealthStatus updates the health status of a dataset
func (tm *TenantMetrics) UpdateDatasetHealthStatus(ctx context.Context, tenantID, tenantName, datasetID, datasetName string, isHealthy bool) {
	if !tm.isEnabled() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", tenantID),
		attribute.String("tenant_name", tenantName),
		attribute.String("dataset_id", datasetID),
		attribute.String("dataset_name", datasetName),
	}

	status := int64(0)
	if isHealthy {
		status = 1
	}

	tm.datasetHealthStatus.Record(ctx, status, metric.WithAttributes(attrs...))
}

// UpdateActiveDatasets updates the number of active datasets
func (tm *TenantMetrics) UpdateActiveDatasets(ctx context.Context, count int) {
	if !tm.isEnabled() {
		return
	}
	tm.activeDatasetsGauge.Record(ctx, int64(count))
}
