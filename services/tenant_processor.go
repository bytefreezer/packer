// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"github.com/bytedance/sonic"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/config"
	"github.com/bytefreezer/packer/domain"
	"github.com/bytefreezer/packer/storage"
	"github.com/jeremywohl/flatten"
)

type TenantProcessor struct {
	config            *config.Config
	instanceID        string
	parquetConverter  *ParquetConverter
	uploadPool        *UploadWorkerPool
	dataLayoutManager *DataLayoutManager
	metrics           *TenantMetrics
	spoolManager      *SpoolManager
	lockClient        storage.LockClient
	activityReporter  *ActivityReporter
}

type ProcessingResult struct {
	TenantID         string
	DatasetID        string
	DatasetName      string
	FilesProcessed   int
	RecordsProcessed int64
	TotalSize        int64
	OutputSize       int64 // Size of output Parquet file
	MergedFile       string
	ParquetFile      string
	ParquetUploadKey string
	RawUploadKey     string
	UploadLocation   string
	ProcessingTime   time.Duration
	ConversionTime   time.Duration
	UploadTime       time.Duration
	ProcessedFiles   []storage.S3Object
	Error            error
}

func NewTenantProcessor(cfg *config.Config, spoolManager *SpoolManager, lockClient storage.LockClient) (*TenantProcessor, error) {
	// Initialize tenant metrics
	tenantMetrics, err := NewTenantMetrics(nil) // Pass nil for now, metrics are optional
	if err != nil {
		log.Warnf("Failed to initialize tenant metrics: %v", err)
		tenantMetrics = nil
	}
	// Create metadata manager for Parquet metadata file generation
	instanceID := generateInstanceID()

	// Initialize activity reporter if control service is configured
	var activityReporter *ActivityReporter
	if cfg.ControlService.Enabled && cfg.ControlService.BaseURL != "" {
		activityReporter = NewActivityReporter(
			cfg.ControlService.BaseURL,
			cfg.ControlService.APIKey,
			instanceID,
		)
		log.Info("Activity reporter initialized for operation tracking")
	} else {
		log.Info("Activity reporter disabled (control service not configured)")
	}

	var metadataManager MetadataManagerInterface
	if cfg.MetadataClient != nil {
		// Use Control API-based incremental metadata manager
		metadataManager = NewMetadataManagerV2(cfg.S3DestinationManager, lockClient, cfg.MetadataClient, instanceID)
		log.Info("Using Control API-based incremental metadata manager")
	} else {
		// Fallback to original metadata manager
		metadataManager = NewMetadataManager(cfg.S3DestinationManager, lockClient, instanceID)
		log.Info("Using fallback metadata manager (Control API metadata not configured)")
	}

	// Get worker count from config (default to 5)
	workerCount := 5
	if cfg.UploadPool.WorkerCount > 0 {
		workerCount = cfg.UploadPool.WorkerCount
	}

	uploadPool := NewUploadWorkerPool(cfg, cfg.S3DestinationManager, metadataManager, workerCount, tenantMetrics)

	return &TenantProcessor{
		config:            cfg,
		instanceID:        instanceID,
		parquetConverter:  NewParquetConverter(),
		uploadPool:        uploadPool,
		dataLayoutManager: NewDataLayoutManager(),
		metrics:           tenantMetrics,
		spoolManager:      spoolManager,
		lockClient:        lockClient,
		activityReporter:  activityReporter,
	}, nil
}

// generateInstanceID creates a stable identifier for this service instance based on hostname
// This remains constant across restarts, allowing detection and cleanup of abandoned locks
func generateInstanceID() string {
	hostname, err := os.Hostname()
	if err != nil {
		// Fallback to a default if hostname can't be determined
		return "packer-unknown"
	}
	return hostname
}

// Start starts the upload worker pool
func (tp *TenantProcessor) Start() {
	tp.uploadPool.Start()
}

// Stop stops the upload worker pool
func (tp *TenantProcessor) Stop() {
	tp.uploadPool.Stop()
}

// ProcessAvailableTenants processes all unlocked datasets within tenants
func (tp *TenantProcessor) ProcessAvailableTenants(ctx context.Context) ([]*ProcessingResult, error) {
	if tp.config.TenantDatabase == nil {
		return nil, fmt.Errorf("tenant database not initialized")
	}

	tenants := tp.config.TenantDatabase.GetAllTenants()
	if len(tenants) == 0 {
		log.Info("No tenants available for processing")
		return []*ProcessingResult{}, nil
	}

	var results []*ProcessingResult
	lockDuration := 30 * time.Minute // Default lock duration

	// Process datasets within each tenant
	for _, tenant := range tenants {
		select {
		case <-ctx.Done():
			log.Info("Context cancelled, stopping tenant processing")
			return results, ctx.Err()
		default:
		}

		log.Infof("Processing tenant: %s (%s) with %d datasets", tenant.ID, tenant.Name, len(tenant.Datasets))

		// Process each dataset within the tenant
		for _, dataset := range tenant.Datasets {
			select {
			case <-ctx.Done():
				log.Info("Context cancelled, stopping dataset processing")
				return results, ctx.Err()
			default:
			}

			log.Infof("Processing dataset: %s (%s) for tenant %s", dataset.ID, dataset.Name, tenant.ID)
			result := tp.processDataset(ctx, &tenant, dataset, lockDuration)
			results = append(results, result)

			// Handle dataset processing results
			if result.Error != nil {
				log.Errorf("Failed to process dataset %s for tenant %s: %v", dataset.ID, tenant.ID, result.Error)

				// Send standard SOC alert for dataset processing failure
				if tp.config.SOCAlertClient != nil {
					alertErr := tp.config.SOCAlertClient.SendTenantProcessingFailureAlert(
						fmt.Sprintf("%s/%s", tenant.ID, dataset.ID), result.Error, result.FilesProcessed, result.TotalSize,
					)
					if alertErr != nil {
						log.Errorf("Failed to send SOC alert for dataset processing failure: %v", alertErr)
					}
				}
			} else {
				log.Infof("Successfully processed dataset %s for tenant %s: %d files, %d bytes, processing=%v, conversion=%v, upload=%v",
					result.DatasetID, result.TenantID, result.FilesProcessed, result.TotalSize,
					result.ProcessingTime, result.ConversionTime, result.UploadTime)

				// Record dataset metrics asynchronously (non-blocking)
				if tp.config.DatasetMetricsClient != nil {
					go func(tenantID, datasetID string, inputBytes, outputBytes, linesProcessed int64) {
						// Use background context for async metrics recording
						metricsCtx := context.Background()
						log.Infof("Recording dataset metrics for %s/%s: input_bytes=%d, output_bytes=%d, lines=%d",
							tenantID, datasetID, inputBytes, outputBytes, linesProcessed)
						if err := tp.config.DatasetMetricsClient.RecordMetric(metricsCtx, tenantID, datasetID,
							inputBytes, outputBytes, linesProcessed, 0, nil); err != nil {
							log.Errorf("Failed to record dataset metrics for %s/%s: %v", tenantID, datasetID, err)
						}
					}(tenant.ID, dataset.ID, result.TotalSize, result.OutputSize, result.RecordsProcessed)
				}
			}
		}
	}

	return results, nil
}

// processDataset processes a single dataset with locking
func (tp *TenantProcessor) processDataset(ctx context.Context, tenant *domain.Tenant, dataset *domain.Dataset, lockDuration time.Duration) *ProcessingResult {
	startTime := time.Now()
	result := &ProcessingResult{
		TenantID:         tenant.ID,
		DatasetID:        dataset.ID,
		DatasetName:      dataset.Name,
		FilesProcessed:   0,
		RecordsProcessed: 0,
		TotalSize:        0,
		ProcessingTime:   0,
		ConversionTime:   0,
		UploadTime:       0,
		ProcessedFiles:   []storage.S3Object{},
	}

	// Check if dataset has S3 destination configured
	if dataset.S3Destination == nil {
		result.Error = fmt.Errorf("dataset %s in tenant %s has no S3 destination configured", dataset.ID, tenant.ID)
		result.ProcessingTime = time.Since(startTime)
		return result
	}

	// Check if locking is configured and available
	if tp.lockClient == nil {
		log.Warnf("Lock client not configured, processing dataset %s without locking", dataset.ID)
		return tp.processDatasetInternal(ctx, tenant, dataset, result)
	}

	// Create unique lock key for dataset (tenant_id/dataset_id)
	lockKey := fmt.Sprintf("%s/%s", tenant.ID, dataset.ID)

	// Try to acquire lock
	_, err := tp.lockClient.AcquireLock(ctx, lockKey, tp.instanceID, lockDuration)
	if err != nil {
		log.Debugf("Could not acquire lock for dataset %s in tenant %s: %v", dataset.ID, tenant.ID, err)

		// Record lock acquisition error
		if tp.metrics != nil {
			tp.metrics.RecordLockAcquisitionError(ctx, tenant.ID, tenant.Name)
		}

		result.Error = fmt.Errorf("dataset locked or lock acquisition failed: %w", err)
		result.ProcessingTime = time.Since(startTime)
		return result
	}

	log.Infof("Acquired lock for dataset %s in tenant %s", dataset.ID, tenant.ID)

	// Start heartbeat goroutine to maintain the lock
	heartbeatStop := make(chan struct{})
	defer close(heartbeatStop)

	// Check if lock client supports heartbeat (PostgreSQL does, others might not)
	if heartbeatClient, ok := tp.lockClient.(storage.HeartbeatLockClient); ok {
		go tp.maintainLockHeartbeat(ctx, heartbeatClient, lockKey, tp.instanceID, heartbeatStop)
	}

	// Ensure lock is released when we're done
	defer func() {
		if releaseErr := tp.lockClient.ReleaseLock(context.Background(), lockKey, tp.instanceID); releaseErr != nil {
			log.Errorf("Failed to release lock for dataset %s in tenant %s: %v", dataset.ID, tenant.ID, releaseErr)
		} else {
			log.Info(fmt.Sprintf("Released lock for dataset %s in tenant %s", dataset.ID, tenant.ID))
		}
	}()

	// Process the dataset
	return tp.processDatasetInternal(ctx, tenant, dataset, result)
}

// processDatasetStreaming processes a dataset using streaming mode (no intermediate files)
func (tp *TenantProcessor) processDatasetStreaming(ctx context.Context, tenant *domain.Tenant, dataset *domain.Dataset, result *ProcessingResult) *ProcessingResult {
	startTime := time.Now()

	// Step 1: List .ndjson.gz files in dataset directory (tenant/dataset/)
	datasetPrefix := fmt.Sprintf("%s/%s/", tenant.ID, dataset.ID)
	files, err := tp.config.S3SourceClient.ListDataFiles(ctx, datasetPrefix)
	if err != nil {
		result.Error = fmt.Errorf("failed to list NDJSON files for dataset %s in tenant %s: %w", dataset.ID, tenant.ID, err)
		result.ProcessingTime = time.Since(startTime)
		if tp.metrics != nil {
			tp.metrics.RecordProcessingError(ctx, tenant.ID, tenant.Name, "list_files")
		}
		return result
	}

	if len(files) == 0 {
		log.Infof("No NDJSON files found for dataset %s in tenant %s", dataset.ID, tenant.ID)
		result.ProcessingTime = time.Since(startTime)
		return result
	}

	log.Infof("Found %d NDJSON files for streaming processing of dataset %s in tenant %s", len(files), dataset.ID, tenant.ID)

	// Step 2: Start activity reporting for this operation
	operationID := fmt.Sprintf("%s/%s-%d", tenant.ID, dataset.ID, time.Now().Unix())
	if tp.activityReporter != nil {
		tp.activityReporter.StartOperation(operationID, tenant.ID, dataset.ID, "converting", int64(len(files)), "files")
	}

	// Step 3: Generate secure data layout for this dataset
	layout, err := tp.dataLayoutManager.GenerateSecureLayout(tenant, dataset, time.Now(), "parquet")
	if err != nil {
		result.Error = fmt.Errorf("failed to generate secure layout for dataset %s in tenant %s: %w", dataset.ID, tenant.ID, err)
		result.ProcessingTime = time.Since(startTime)
		if tp.activityReporter != nil {
			tp.activityReporter.FailOperation(operationID, fmt.Sprintf("Failed to generate layout: %v", err))
		}
		return result
	}

	// Step 4: Stream NDJSON files directly to Parquet using parquet converter
	conversionStart := time.Now()
	parquetFilePath, conversionResult, err := tp.parquetConverter.StreamNDJSONToParquet(ctx, files, layout.ParquetPath, &ParquetConversionOptions{
		Compression:          tp.config.Parquet.Compression,
		MaxFileSizeMB:        tp.config.Parquet.MaxFileSizeMB,
		MemoryBufferMB:       tp.config.Parquet.MemoryBufferMB,
		CheckpointRecords:    tp.config.Parquet.CheckpointRecords,
		AtomicUpload:         tp.config.Parquet.AtomicUpload,
		S3SourceClient:       tp.config.S3SourceClient,
		S3DestinationManager: tp.config.S3DestinationManager,
		Dataset:              dataset,
		ActivityReporter:     tp.activityReporter,
		OperationID:          operationID,
	})

	conversionTime := time.Since(conversionStart)

	if err != nil {
		result.Error = fmt.Errorf("failed to stream convert NDJSON to Parquet for dataset %s in tenant %s: %w", dataset.ID, tenant.ID, err)
		result.ProcessingTime = time.Since(startTime)
		result.ConversionTime = conversionTime
		if tp.metrics != nil {
			tp.metrics.RecordProcessingError(ctx, tenant.ID, tenant.Name, "parquet_conversion")
		}
		if tp.activityReporter != nil {
			tp.activityReporter.FailOperation(operationID, fmt.Sprintf("Conversion failed: %v", err))
		}
		return result
	}

	// Step 5: Complete activity reporting
	if tp.activityReporter != nil {
		tp.activityReporter.CompleteOperation(operationID, conversionResult.TotalSize, conversionResult.FileSize, conversionResult.RecordsProcessed)
	}

	// Step 6: Record success metrics
	result.FilesProcessed = len(files)
	result.RecordsProcessed = conversionResult.RecordsProcessed
	result.TotalSize = conversionResult.TotalSize
	result.OutputSize = conversionResult.FileSize
	result.ParquetFile = parquetFilePath
	result.ParquetUploadKey = layout.ParquetPath
	result.ProcessingTime = time.Since(startTime)
	result.ConversionTime = conversionTime
	result.ProcessedFiles = files

	log.Infof("Successfully streamed %d files (%d records, %d bytes) to Parquet for dataset %s in tenant %s in %v",
		len(files), conversionResult.RecordsProcessed, conversionResult.TotalSize, dataset.ID, tenant.ID, result.ProcessingTime)

	if tp.metrics != nil {
		tp.metrics.RecordFilesProcessed(ctx, tenant.ID, tenant.Name, len(files), conversionResult.TotalSize)
		tp.metrics.RecordRecordsProcessed(ctx, tenant.ID, tenant.Name, conversionResult.RecordsProcessed)
	}

	// Step 5: Update Parquet metadata after streaming upload
	if tp.uploadPool != nil && tp.uploadPool.metadataManager != nil && layout.ParquetPath != "" {
		log.Debugf("Updating metadata after streaming upload for tenant:%s dataset:%s file:%s", tenant.ID, dataset.ID, layout.ParquetPath)

		metadataCtx, metadataCancel := context.WithTimeout(context.Background(), time.Minute*2)
		defer metadataCancel()

		if err := tp.uploadPool.metadataManager.UpdateMetadataAfterUpload(metadataCtx, tenant, dataset, layout.ParquetPath); err != nil {
			log.Errorf("Failed to update metadata for tenant:%s dataset:%s: %v", tenant.ID, dataset.ID, err)
		} else {
			log.Infof("Successfully updated metadata for tenant:%s dataset:%s", tenant.ID, dataset.ID)
		}
	}

	// Step 6: Cleanup source files (if configured)
	if tp.config.UploadPool.CleanupSourceFiles {
		if err := tp.cleanupSourceFiles(ctx, tenant.ID, files); err != nil {
			log.Errorf("Failed to cleanup source files for tenant %s: %v", tenant.ID, err)
			// Send SOC alert for cleanup failure
			if tp.config.SOCAlertClient != nil {
				if alertErr := tp.config.SOCAlertClient.SendAlert(
					"WARN",
					"Source File Cleanup Failed",
					fmt.Sprintf("Failed to cleanup source files for tenant %s - files may be reprocessed", tenant.ID),
					fmt.Sprintf("Error: %v", err),
				); alertErr != nil {
					log.Warnf("Failed to send SOC alert: %v", alertErr)
				}
			}
		}
	}

	return result
}

// processDatasetInternal performs the actual dataset processing pipeline
func (tp *TenantProcessor) processDatasetInternal(ctx context.Context, tenant *domain.Tenant, dataset *domain.Dataset, result *ProcessingResult) *ProcessingResult {
	startTime := time.Now()

	// Check if streaming mode is enabled in configuration
	if tp.config.Parquet.StreamingMode {
		log.Infof("Using streaming mode for dataset %s in tenant %s - no intermediate files", dataset.ID, tenant.ID)
		return tp.processDatasetStreaming(ctx, tenant, dataset, result)
	}

	// Step 1: List .ndjson.gz files in dataset directory (tenant/dataset/)
	datasetPrefix := fmt.Sprintf("%s/%s/", tenant.ID, dataset.ID)
	files, err := tp.config.S3SourceClient.ListDataFiles(ctx, datasetPrefix)
	if err != nil {
		result.Error = fmt.Errorf("failed to list NDJSON files for dataset %s in tenant %s: %w", dataset.ID, tenant.ID, err)
		result.ProcessingTime = time.Since(startTime)

		// Record processing error
		if tp.metrics != nil {
			tp.metrics.RecordProcessingError(ctx, tenant.ID, tenant.Name, "list_files")
		}

		return result
	}

	if len(files) == 0 {
		log.Infof("No NDJSON files found for dataset %s in tenant %s", dataset.ID, tenant.ID)
		result.ProcessingTime = time.Since(startTime)
		return result
	}

	log.Infof("Found %d NDJSON files for dataset %s in tenant %s", len(files), dataset.ID, tenant.ID)

	// Step 2: Create temporary merged file
	mergedFile, err := tp.createMergedFile(ctx, tenant, dataset, files)
	if err != nil {
		result.Error = fmt.Errorf("failed to create merged file for dataset %s in tenant %s: %w", dataset.ID, tenant.ID, err)
		result.ProcessingTime = time.Since(startTime)

		// Record processing error
		if tp.metrics != nil {
			tp.metrics.RecordProcessingError(ctx, tenant.ID, tenant.Name, "merge_files")
		}

		// Mark files as failed if tracker is available
		tp.markFilesAsFailed(ctx, files, tenant.ID, dataset.ID, err)

		return result
	}

	result.FilesProcessed = len(files)
	result.MergedFile = mergedFile
	result.ProcessedFiles = files

	// Calculate total size
	for _, file := range files {
		result.TotalSize += file.Size
	}

	processingTime := time.Since(startTime)

	// Step 3: Convert to Parquet
	conversionStart := time.Now()
	conversionResult, err := tp.parquetConverter.ConvertNDJSONToParquet(mergedFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to convert to parquet for tenant %s: %w", tenant.ID, err)
		result.ProcessingTime = processingTime
		result.ConversionTime = time.Since(conversionStart)

		// Record processing error
		if tp.metrics != nil {
			tp.metrics.RecordProcessingError(ctx, tenant.ID, tenant.Name, "conversion")
		}

		return result
	}

	result.ParquetFile = conversionResult.OutputFile
	result.OutputSize = conversionResult.FileSize
	result.ConversionTime = conversionResult.ConvertTime
	result.RecordsProcessed = conversionResult.RecordsCount

	log.Infof("Successfully converted %d records to parquet for tenant %s: %s",
		conversionResult.RecordsCount, tenant.ID, result.ParquetFile)

	// Step 4: Upload to tenant's S3 destination with data lake partitioning
	uploadStart := time.Now()
	parquetLocation, rawLocation, err := tp.uploadToDestination(ctx, tenant, dataset, result.ParquetFile, mergedFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to upload files for tenant %s: %w", tenant.ID, err)
		result.ProcessingTime = processingTime
		result.ConversionTime = conversionResult.ConvertTime
		result.UploadTime = time.Since(uploadStart)

		// Record processing error
		if tp.metrics != nil {
			tp.metrics.RecordProcessingError(ctx, tenant.ID, tenant.Name, "upload")
		}

		return result
	}

	result.UploadLocation = parquetLocation
	result.ParquetUploadKey = parquetLocation
	result.RawUploadKey = rawLocation
	result.UploadTime = time.Since(uploadStart)

	if rawLocation != "" {
		log.Infof("Successfully uploaded both parquet and raw data for tenant %s", tenant.ID)
	} else {
		log.Infof("Successfully uploaded parquet data for tenant %s", tenant.ID)
	}

	// Step 5: Cleanup local files (include compressed raw file)
	filesToClean := []string{mergedFile, result.ParquetFile}

	// Add compressed raw file to cleanup (it's always created if raw storage was enabled)
	compressedRawFile := mergedFile + ".gz"
	filesToClean = append(filesToClean, compressedRawFile)

	tp.cleanupLocalFiles(filesToClean...)

	// Step 5.1: Cleanup tenant/dataset directory
	tempDir := filepath.Join(tp.config.Bytefreezer.CachePath, "processing", tenant.ID, dataset.ID)
	tp.cleanupTenantDirectory(tempDir)

	// Step 6: Cleanup source files (optional - could be made configurable)
	if tp.config.UploadPool.CleanupSourceFiles {
		if err := tp.cleanupSourceFiles(ctx, tenant.ID, files); err != nil {
			log.Errorf("Failed to cleanup source files for tenant %s: %v", tenant.ID, err)

			// Send SOC alert for cleanup failure
			if tp.config.SOCAlertClient != nil {
				alertErr := tp.config.SOCAlertClient.SendAlert(
					"CRITICAL",
					"Source File Cleanup Failed",
					fmt.Sprintf("Failed to cleanup source files for tenant %s - files may be reprocessed", tenant.ID),
					fmt.Sprintf("Error: %v, Files: %v", err, func() []string {
						var keys []string
						for _, f := range files {
							keys = append(keys, f.Key)
						}
						return keys
					}()),
				)
				if alertErr != nil {
					log.Errorf("Failed to send cleanup failure SOC alert: %v", alertErr)
				}
			}

			// Don't fail the entire process for cleanup errors since files are marked complete
		}
	}

	result.ProcessingTime = processingTime

	// Record successful tenant processing metrics
	if tp.metrics != nil {
		tp.metrics.RecordTenantProcessed(ctx, tenant.ID, tenant.Name, processingTime)
		tp.metrics.RecordFilesProcessed(ctx, tenant.ID, tenant.Name, result.FilesProcessed, result.TotalSize)
		// Record conversion duration if available
		if result.ConversionTime > 0 {
			tp.metrics.RecordConversionDuration(ctx, tenant.ID, tenant.Name, result.ConversionTime)
		}
		// Record records processed
		if result.RecordsProcessed > 0 {
			tp.metrics.RecordRecordsProcessed(ctx, tenant.ID, tenant.Name, result.RecordsProcessed)
		}
	}

	log.Infof("Successfully completed processing pipeline for tenant %s", tenant.ID)
	return result
}

// createMergedFile downloads, unzips, and merges NDJSON files
func (tp *TenantProcessor) createMergedFile(ctx context.Context, tenant *domain.Tenant, dataset *domain.Dataset, files []storage.S3Object) (string, error) {
	// Create temporary directory for this tenant and dataset
	tempDir := filepath.Join(tp.config.Bytefreezer.CachePath, "processing", tenant.ID, dataset.ID)
	if err := os.MkdirAll(tempDir, 0750); err != nil {
		return "", fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Create merged file
	mergedFilePath := filepath.Join(tempDir, fmt.Sprintf("merged_%d.ndjson", time.Now().Unix()))
	mergedFile, err := os.Create(mergedFilePath) // #nosec G304 - mergedFilePath is controlled by application logic
	if err != nil {
		return "", fmt.Errorf("failed to create merged file: %w", err)
	}
	defer mergedFile.Close()

	writer := bufio.NewWriter(mergedFile)
	defer writer.Flush()

	// Get flattening configuration
	var shouldFlatten bool
	var flattenDelimiter string = "."
	if dataset.ProcessingConfig != nil {
		shouldFlatten = dataset.ProcessingConfig.FlattenJSON
		if dataset.ProcessingConfig.FlattenDelimiter != "" {
			flattenDelimiter = dataset.ProcessingConfig.FlattenDelimiter
		}
	}

	if shouldFlatten {
		log.Infof("JSON flattening enabled for tenant %s (delimiter: %s)", tenant.ID, flattenDelimiter)
	}

	// Process each file
	for i, file := range files {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}

		log.Debugf("Processing file %d/%d for tenant %s: %s", i+1, len(files), tenant.ID, file.Key)

		if err := tp.processFile(ctx, file, writer, shouldFlatten, flattenDelimiter); err != nil {
			log.Errorf("Failed to process file %s for tenant %s: %v", file.Key, tenant.ID, err)
			continue // Continue with other files
		}
	}

	log.Infof("Successfully created merged file for tenant %s: %s", tenant.ID, mergedFilePath)
	return mergedFilePath, nil
}

// processFile downloads, decompresses (if needed), and writes file content to the merged file
func (tp *TenantProcessor) processFile(ctx context.Context, file storage.S3Object, writer *bufio.Writer, shouldFlatten bool, flattenDelimiter string) error {
	// Only handle .ndjson.gz files
	return tp.processRegularFile(ctx, file, writer, shouldFlatten, flattenDelimiter)
}

// processRegularFile handles .ndjson.gz files
func (tp *TenantProcessor) processRegularFile(ctx context.Context, file storage.S3Object, writer *bufio.Writer, shouldFlatten bool, flattenDelimiter string) error {
	// Download file from S3
	reader, err := tp.config.S3SourceClient.GetObject(ctx, file.Key)
	if err != nil {
		return fmt.Errorf("failed to download file %s: %w", file.Key, err)
	}
	defer reader.Close()

	return tp.processFileContent(reader, writer, shouldFlatten, flattenDelimiter, file.Key)
}

// processFileContent processes the actual content of a file (handles gzip decompression and JSON flattening)
func (tp *TenantProcessor) processFileContent(reader io.Reader, writer *bufio.Writer, shouldFlatten bool, flattenDelimiter string, fileName string) error {
	// Determine if file is compressed
	var contentReader io.Reader = reader

	// All files are expected to be gzipped .ndjson.gz
	if strings.HasSuffix(strings.ToLower(fileName), ".gz") {

		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader for %s: %w", fileName, err)
		}
		defer gzipReader.Close()
		contentReader = gzipReader
	}

	// Copy content line by line to ensure proper NDJSON format
	scanner := bufio.NewScanner(contentReader)
	lineCount := 0

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue // Skip empty lines
		}

		// Flatten JSON if enabled
		if shouldFlatten {
			var jsonData map[string]interface{}
			if err := sonic.Unmarshal([]byte(line), &jsonData); err != nil {
				// If JSON parsing fails, write original line and continue
				log.Warnf("Failed to parse JSON line, writing as-is: %v", err)
			} else {
				// Flatten the JSON using custom delimiter
				flattened, err := flatten.FlattenString(line, "", flatten.DotStyle)
				if err != nil {
					log.Warnf("Failed to flatten JSON line, writing as-is: %v", err)
				} else {
					// Replace dots with custom delimiter if specified
					if flattenDelimiter != "." && flattenDelimiter != "" {
						// Replace dots with custom delimiter
						flattened = strings.ReplaceAll(flattened, ".", flattenDelimiter)
					}
					line = flattened
				}
			}
		}

		// Write line to merged file
		if _, err := writer.WriteString(line); err != nil {
			return fmt.Errorf("failed to write line to merged file: %w", err)
		}

		// Add newline
		if _, err := writer.WriteString("\n"); err != nil {
			return fmt.Errorf("failed to write newline to merged file: %w", err)
		}

		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file %s: %w", fileName, err)
	}

	log.Debugf("Processed %d lines from file %s", lineCount, fileName)
	return nil
}

// uploadToDestination uploads both parquet and raw files to dataset's S3 destination using secure data layout
func (tp *TenantProcessor) uploadToDestination(ctx context.Context, tenant *domain.Tenant, dataset *domain.Dataset, parquetFile, mergedFile string) (parquetLocation, rawLocation string, err error) {
	// Generate secure data layout with cross-tenant protection
	processingTime := time.Now()
	layout, err := tp.dataLayoutManager.GenerateSecureLayout(tenant, dataset, processingTime, "ndjson")
	if err != nil {
		return "", "", fmt.Errorf("failed to generate secure data layout: %w", err)
	}

	log.Infof("Using secure data layout for tenant %s, dataset %s: parquet=%s, raw=%s",
		tenant.ID, dataset.ID, layout.ParquetPath, layout.RawPath)

	// Upload parquet file using secure layout
	parquetJob := tp.uploadPool.CreateUploadJobForDataset(tenant, dataset, parquetFile, layout.ParquetPath)
	parquetJob.ContentType = "application/octet-stream"
	parquetJob.Metadata = map[string]string{
		"tenant-id":       tenant.ID,
		"tenant-name":     tenant.Name,
		"dataset-id":      dataset.ID,
		"dataset-name":    dataset.Name,
		"data-format":     "parquet",
		"source-format":   "ndjson",
		"processing-time": processingTime.Format(time.RFC3339),
		"compression":     "zstd",
		"partitioning":    layout.PartitionScheme,
		"layout-secure":   fmt.Sprintf("%t", layout.IsSecure),
	}

	if err := tp.uploadPool.SubmitJob(parquetJob); err != nil {
		return "", "", fmt.Errorf("failed to submit parquet upload job: %w", err)
	}

	// Upload raw file if enabled (compressed)
	var rawJob *UploadJob
	enableRawStorage := dataset.ProcessingConfig != nil && dataset.ProcessingConfig.EnableRawStorage
	if enableRawStorage {
		// Compress the merged file before uploading
		compressedRawFile, err := tp.compressFile(mergedFile)
		if err != nil {
			log.Errorf("Failed to compress raw file for dataset %s in tenant %s: %v", dataset.ID, tenant.ID, err)
			// Don't fail the entire process, just skip raw storage
		} else {
			// Update raw path to reflect gzip compression
			rawPathWithGz := layout.RawPath + ".gz"

			rawJob = tp.uploadPool.CreateUploadJobForDataset(tenant, dataset, compressedRawFile, rawPathWithGz)
			rawJob.ContentType = "application/gzip"
			rawJob.Metadata = map[string]string{
				"tenant-id":       tenant.ID,
				"tenant-name":     tenant.Name,
				"dataset-id":      dataset.ID,
				"dataset-name":    dataset.Name,
				"data-format":     "ndjson",
				"compression":     "gzip",
				"purpose":         "raw-lineage",
				"processing-time": processingTime.Format(time.RFC3339),
				"partitioning":    layout.PartitionScheme,
				"layout-secure":   fmt.Sprintf("%t", layout.IsSecure),
			}

			if err := tp.uploadPool.SubmitJob(rawJob); err != nil {
				return "", "", fmt.Errorf("failed to submit raw upload job: %w", err)
			}
			log.Infof("Raw NDJSON storage enabled for tenant %s (gzip compressed)", tenant.ID)
		}
	} else {
		log.Debugf("Raw NDJSON storage disabled for tenant %s", tenant.ID)
	}

	// Wait for parquet upload result
	select {
	case result := <-parquetJob.ResultChan:
		if result.Success {
			parquetLocation = result.Location
		} else {
			return "", "", fmt.Errorf("parquet upload failed: %w", result.Error)
		}
	case <-ctx.Done():
		return "", "", ctx.Err()
	case <-time.After(10 * time.Minute):
		return "", "", fmt.Errorf("parquet upload timeout")
	}

	// Wait for raw upload result if enabled
	if enableRawStorage && rawJob != nil {
		select {
		case result := <-rawJob.ResultChan:
			if result.Success {
				rawLocation = result.Location
			} else {
				log.Errorf("Raw upload failed (non-critical): %v", result.Error)
				// Don't fail the entire process if raw upload fails
			}
		case <-ctx.Done():
			return parquetLocation, "", ctx.Err()
		case <-time.After(5 * time.Minute):
			log.Error("Raw upload timeout (non-critical)")
			// Don't fail the entire process if raw upload times out
		}
	}

	return parquetLocation, rawLocation, nil
}

// compressFile compresses a file using gzip and returns the path to the compressed file
func (tp *TenantProcessor) compressFile(inputFile string) (string, error) {
	// Create compressed file path
	compressedFile := inputFile + ".gz"

	// Open input file
	input, err := os.Open(inputFile) // #nosec G304 - inputFile is controlled by application logic
	if err != nil {
		return "", fmt.Errorf("failed to open input file: %w", err)
	}
	defer input.Close()

	// Create compressed output file
	output, err := os.Create(compressedFile) // #nosec G304 - compressedFile is controlled by application logic
	if err != nil {
		return "", fmt.Errorf("failed to create compressed file: %w", err)
	}
	defer output.Close()

	// Create gzip writer
	gzipWriter := gzip.NewWriter(output)
	defer gzipWriter.Close()

	// Copy data with compression
	_, err = io.Copy(gzipWriter, input)
	if err != nil {
		return "", fmt.Errorf("failed to compress file: %w", err)
	}

	// Ensure all data is flushed
	if err := gzipWriter.Close(); err != nil {
		return "", fmt.Errorf("failed to close gzip writer: %w", err)
	}

	log.Debugf("Successfully compressed %s to %s", inputFile, compressedFile)
	return compressedFile, nil
}

// cleanupLocalFiles removes temporary local files
func (tp *TenantProcessor) cleanupLocalFiles(files ...string) {
	for _, file := range files {
		if file == "" {
			continue
		}
		if err := os.Remove(file); err != nil {
			log.Warnf("Failed to cleanup local file %s: %v", file, err)
		} else {
			log.Debugf("Cleaned up local file: %s", file)
		}
	}
}

// cleanupSourceFiles removes processed source files from S3
func (tp *TenantProcessor) cleanupSourceFiles(ctx context.Context, tenantID string, files []storage.S3Object) error {
	log.Infof("Cleaning up %d processed source files for tenant %s", len(files), tenantID)

	var errors []string
	for _, file := range files {
		if err := tp.config.S3SourceClient.DeleteObject(ctx, file.Key); err != nil {
			errors = append(errors, fmt.Sprintf("failed to delete %s: %v", file.Key, err))
			log.Errorf("Failed to delete processed file %s: %v", file.Key, err)
		} else {
			log.Debugf("Successfully deleted processed file %s", file.Key)
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("cleanup errors: %s", strings.Join(errors, "; "))
	}

	return nil
}

// GetProcessingStats returns current processing statistics
func (tp *TenantProcessor) GetProcessingStats() map[string]interface{} {
	uploadStats := tp.uploadPool.GetStats()

	return map[string]interface{}{
		"instance_id": tp.instanceID,
		"cache_path":  tp.config.Bytefreezer.CachePath,
		"upload_stats": map[string]interface{}{
			"total_jobs":      uploadStats.TotalJobs,
			"successful_jobs": uploadStats.SuccessfulJobs,
			"failed_jobs":     uploadStats.FailedJobs,
			"retried_jobs":    uploadStats.RetriedJobs,
			"total_bytes":     uploadStats.TotalBytes,
			"average_time":    uploadStats.AverageTime.String(),
			"workers_active":  uploadStats.WorkersActive,
		},
		"destination_client": "S3Client", // Simple S3 client in use
	}
}

// cleanupTenantDirectory removes the tenant's temporary directory after processing
func (tp *TenantProcessor) cleanupTenantDirectory(tenantDir string) {
	if tenantDir == "" {
		return
	}

	// Remove the tenant directory (should be empty after file cleanup)
	if err := os.Remove(tenantDir); err != nil {
		if !os.IsNotExist(err) {
			log.Debugf("Could not remove tenant directory %s: %v", tenantDir, err)
		}
	} else {
		log.Debugf("Cleaned up tenant directory: %s", tenantDir)
	}
}

// CleanupOldProcessingDirectories removes old/orphaned tenant processing directories
// This should be called periodically during housekeeping
func (tp *TenantProcessor) CleanupOldProcessingDirectories(maxAge time.Duration) error {
	processingDir := filepath.Join(tp.config.Bytefreezer.CachePath, "processing")

	// Check if processing directory exists
	if _, err := os.Stat(processingDir); os.IsNotExist(err) {
		return nil // Nothing to clean up
	}

	entries, err := os.ReadDir(processingDir)
	if err != nil {
		return fmt.Errorf("failed to read processing directory: %w", err)
	}

	now := time.Now()
	removedCount := 0

	for _, entry := range entries {
		if !entry.IsDir() {
			continue // Skip files
		}

		dirPath := filepath.Join(processingDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			log.Warnf("Could not get info for directory %s: %v", dirPath, err)
			continue
		}

		// Remove directories older than maxAge
		if now.Sub(info.ModTime()) > maxAge {
			if err := os.RemoveAll(dirPath); err != nil {
				log.Warnf("Failed to remove old processing directory %s: %v", dirPath, err)
			} else {
				log.Debugf("Removed old processing directory: %s (age: %v)", dirPath, now.Sub(info.ModTime()))
				removedCount++
			}
		}
	}

	if removedCount > 0 {
		log.Infof("Cleaned up %d old tenant processing directories", removedCount)
	}

	// Record cleanup metrics
	if tp.metrics != nil {
		tp.metrics.RecordCacheCleanup(context.Background(), "tenant_dirs", removedCount, true)
	}

	// Try to remove the processing directory itself if it's empty
	if err := os.Remove(processingDir); err == nil {
		log.Debugf("Removed empty processing directory: %s", processingDir)
	}

	return nil
}

// CleanupCache performs comprehensive cache cleanup
func (tp *TenantProcessor) CleanupCache() error {
	cacheDir := tp.config.Bytefreezer.CachePath
	if cacheDir == "" {
		return nil
	}

	// Check if cleanup is enabled
	if !tp.config.Housekeeping.Cleanup.Enabled {
		return nil
	}

	log.Debugf("Starting comprehensive cache cleanup in: %s", cacheDir)

	// Get configured max ages or use defaults
	processingDirMaxAge := 2 * time.Hour  // Default: 2 hours
	orphanedFilesMaxAge := 24 * time.Hour // Default: 24 hours

	if tp.config.Housekeeping.Cleanup.ProcessingDirMaxAgeHours > 0 {
		processingDirMaxAge = time.Duration(tp.config.Housekeeping.Cleanup.ProcessingDirMaxAgeHours) * time.Hour
	}
	if tp.config.Housekeeping.Cleanup.OrphanedFilesMaxAgeHours > 0 {
		orphanedFilesMaxAge = time.Duration(tp.config.Housekeeping.Cleanup.OrphanedFilesMaxAgeHours) * time.Hour
	}

	// Clean up old processing directories
	if err := tp.CleanupOldProcessingDirectories(processingDirMaxAge); err != nil {
		log.Warnf("Failed to cleanup old processing directories: %v", err)
	}

	// Clean up any orphaned files in the cache root
	if err := tp.cleanupOrphanedFiles(cacheDir, orphanedFilesMaxAge); err != nil {
		log.Warnf("Failed to cleanup orphaned files: %v", err)
	}

	return nil
}

// markFilesAsFailed marks files as failed in the processed file tracker
func (tp *TenantProcessor) markFilesAsFailed(ctx context.Context, files []storage.S3Object, tenantID, datasetID string, processingError error) {
	// ProcessedFileTracker has been removed - no-op
}

// ProcessSingleDataset processes a specific dataset for a tenant
func (tp *TenantProcessor) ProcessSingleDataset(ctx context.Context, tenantID, datasetID string) error {
	// Get tenant and dataset information
	if tp.config.TenantDatabase == nil {
		return fmt.Errorf("tenant database not initialized")
	}

	tenant := tp.config.TenantDatabase.GetTenantByID(tenantID)
	if tenant == nil {
		return fmt.Errorf("tenant %s not found", tenantID)
	}

	// Find the dataset
	var targetDataset *domain.Dataset
	for i := range tenant.Datasets {
		if tenant.Datasets[i].ID == datasetID {
			targetDataset = tenant.Datasets[i]
			break
		}
	}

	if targetDataset == nil {
		return fmt.Errorf("dataset %s not found for tenant %s", datasetID, tenantID)
	}

	log.Infof("Processing single dataset %s (%s) for tenant %s",
		targetDataset.ID, targetDataset.Name, tenant.ID)

	// Process the dataset with standard lock duration
	lockDuration := 30 * time.Minute
	result := tp.processDataset(ctx, tenant, targetDataset, lockDuration)

	if result.Error != nil {
		return fmt.Errorf("failed to process dataset %s for tenant %s: %w",
			datasetID, tenantID, result.Error)
	}

	log.Infof("Successfully processed dataset %s for tenant %s: %d files, %d bytes",
		result.DatasetID, result.TenantID, result.FilesProcessed, result.TotalSize)

	// Record dataset metrics asynchronously (non-blocking)
	if tp.config.DatasetMetricsClient != nil {
		go func(tenantID, datasetID string, inputBytes, outputBytes, linesProcessed int64) {
			// Use background context for async metrics recording
			metricsCtx := context.Background()
			log.Infof("Recording dataset metrics for %s/%s: input_bytes=%d, output_bytes=%d, lines=%d",
				tenantID, datasetID, inputBytes, outputBytes, linesProcessed)
			if err := tp.config.DatasetMetricsClient.RecordMetric(metricsCtx, tenantID, datasetID,
				inputBytes, outputBytes, linesProcessed, 0, nil); err != nil {
				log.Errorf("Failed to record dataset metrics for %s/%s: %v", tenantID, datasetID, err)
			}
		}(result.TenantID, result.DatasetID, result.TotalSize, result.OutputSize, result.RecordsProcessed)
	}

	return nil
}

// cleanupOrphanedFiles removes old files directly in cache directory
func (tp *TenantProcessor) cleanupOrphanedFiles(cacheDir string, maxAge time.Duration) error {
	entries, err := os.ReadDir(cacheDir)
	if err != nil {
		return fmt.Errorf("failed to read cache directory: %w", err)
	}

	now := time.Now()
	removedCount := 0

	for _, entry := range entries {
		if entry.IsDir() {
			continue // Skip directories (handled separately)
		}

		filePath := filepath.Join(cacheDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		// Remove old files
		if now.Sub(info.ModTime()) > maxAge {
			if err := os.Remove(filePath); err != nil {
				log.Warnf("Failed to remove orphaned file %s: %v", filePath, err)
			} else {
				log.Debugf("Removed orphaned file: %s", filePath)
				removedCount++
			}
		}
	}

	if removedCount > 0 {
		log.Infof("Cleaned up %d orphaned files from cache directory", removedCount)
	}

	// Record cleanup metrics
	if tp.metrics != nil {
		tp.metrics.RecordCacheCleanup(context.Background(), "orphaned_files", removedCount, true)
	}

	return nil
}

// maintainLockHeartbeat runs a goroutine that periodically updates the lock heartbeat
func (tp *TenantProcessor) maintainLockHeartbeat(ctx context.Context, heartbeatClient storage.HeartbeatLockClient,
	lockKey, instanceID string, stop <-chan struct{}) {

	ticker := time.NewTicker(2 * time.Minute) // Update heartbeat every 2 minutes
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Debugf("Stopping heartbeat for lock %s: context cancelled", lockKey)
			return
		case <-stop:
			log.Debugf("Stopping heartbeat for lock %s: stop signal received", lockKey)
			return
		case <-ticker.C:
			if err := heartbeatClient.UpdateHeartbeat(ctx, lockKey, instanceID); err != nil {
				log.Warnf("Failed to update heartbeat for lock %s: %v", lockKey, err)
				// Continue running even if individual heartbeat fails
			} else {
				log.Debugf("Updated heartbeat for lock %s", lockKey)
			}
		}
	}
}
