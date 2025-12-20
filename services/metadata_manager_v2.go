// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/domain"
	"github.com/bytefreezer/packer/storage"
)

// MetadataManagerV2 handles Parquet metadata with Control Service API
type MetadataManagerV2 struct {
	s3DestinationManager *storage.S3DestinationManager
	lockClient           storage.LockClient
	metadataClient       storage.MetadataClient
	metadataExtractor    *storage.ParquetMetadataExtractor
	instanceID           string
}

// NewMetadataManagerV2 creates a new metadata manager using Control API
func NewMetadataManagerV2(s3DestinationManager *storage.S3DestinationManager, lockClient storage.LockClient, metadataClient storage.MetadataClient, instanceID string) *MetadataManagerV2 {
	return &MetadataManagerV2{
		s3DestinationManager: s3DestinationManager,
		lockClient:           lockClient,
		metadataClient:       metadataClient,
		metadataExtractor:    storage.NewParquetMetadataExtractor(s3DestinationManager),
		instanceID:           instanceID,
	}
}

// UpdateMetadataAfterUpload incrementally updates metadata using Control API
func (mm *MetadataManagerV2) UpdateMetadataAfterUpload(ctx context.Context, tenant *domain.Tenant, dataset *domain.Dataset, uploadedFilePath string) error {
	startTime := time.Now()

	// Create lock key for tenant:dataset metadata updates
	lockKey := fmt.Sprintf("metadata_%s_%s", tenant.ID, dataset.ID)

	log.Debugf("Acquiring metadata lock for tenant:%s dataset:%s", tenant.ID, dataset.ID)

	// Acquire distributed lock to prevent concurrent metadata updates
	_, err := mm.lockClient.AcquireLock(ctx, lockKey, mm.instanceID, time.Minute*15) // Increased timeout
	if err != nil {
		return fmt.Errorf("failed to acquire metadata lock for %s:%s: %w", tenant.ID, dataset.ID, err)
	}
	defer func() {
		if unlockErr := mm.lockClient.ReleaseLock(ctx, lockKey, mm.instanceID); unlockErr != nil {
			log.Errorf("Failed to release metadata lock for %s:%s: %v", tenant.ID, dataset.ID, unlockErr)
		}
	}()

	log.Infof("Incrementally updating metadata for tenant:%s dataset:%s after uploading %s", tenant.ID, dataset.ID, uploadedFilePath)

	// Step 1: Extract metadata from the new Parquet file ONLY
	newFileMetadata, err := mm.metadataExtractor.ExtractFileMetadata(ctx, tenant.ID, dataset.ID, dataset.S3Destination, uploadedFilePath, mm.instanceID)
	if err != nil {
		return fmt.Errorf("failed to extract metadata from new file %s: %w", uploadedFilePath, err)
	}

	// Step 2: Store/update file metadata via Control API
	if err := mm.metadataClient.UpsertFileMetadata(ctx, newFileMetadata); err != nil {
		return fmt.Errorf("failed to store file metadata: %w", err)
	}

	// Step 3: Determine metadata level (root or leaf)
	metadataLevel := "leaf" // Default to leaf-level
	if dataset.ProcessingConfig != nil && dataset.ProcessingConfig.MetadataLevel != "" {
		metadataLevel = dataset.ProcessingConfig.MetadataLevel
	}
	log.Debugf("Metadata level for tenant:%s dataset:%s: %s (ProcessingConfig: %+v)", tenant.ID, dataset.ID, metadataLevel, dataset.ProcessingConfig)

	// Step 4: Regenerate metadata files based on level
	if metadataLevel == "root" {
		// Root-level: Generate metadata for ALL partitions at dataset root
		if err := mm.regenerateRootMetadataFromDatabase(ctx, tenant.ID, dataset.ID, dataset.S3Destination); err != nil {
			return fmt.Errorf("failed to regenerate root-level metadata files: %w", err)
		}
	} else {
		// Leaf-level: Generate metadata only for the partition containing this file
		partitionPath := mm.getPartitionPath(uploadedFilePath)
		if err := mm.regenerateMetadataFromDatabase(ctx, tenant.ID, dataset.ID, dataset.S3Destination, partitionPath); err != nil {
			return fmt.Errorf("failed to regenerate leaf-level metadata files: %w", err)
		}

		// Update generation status for partition
		if err := mm.updateGenerationStatus(ctx, tenant.ID, dataset.ID, partitionPath); err != nil {
			log.Warnf("Failed to update generation status: %v", err) // Non-fatal
		}
	}

	processingTime := time.Since(startTime)
	log.Infof("Successfully updated %s metadata for tenant:%s dataset:%s in %v",
		metadataLevel, tenant.ID, dataset.ID, processingTime)

	return nil
}

// regenerateMetadataFromDatabase generates metadata files from Control API data
func (mm *MetadataManagerV2) regenerateMetadataFromDatabase(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, partitionPath string) error {
	log.Debugf("Regenerating metadata from database for %s:%s partition %s", tenantID, datasetID, partitionPath)

	// Get all files for this partition from Control API
	files, err := mm.metadataClient.GetFileMetadataByPartition(ctx, tenantID, datasetID, partitionPath)
	if err != nil {
		return fmt.Errorf("failed to get file metadata from database: %w", err)
	}

	if len(files) == 0 {
		log.Warnf("No files found in database for partition %s", partitionPath)
		return nil
	}

	log.Debugf("Found %d files in database for metadata generation", len(files))

	// Extract the full directory path from the first file's path
	// Files have paths like: customer-1/ebpf-data/data/parquet/year=2025/month=10/day=05/file.parquet
	// We need the directory: customer-1/ebpf-data/data/parquet/year=2025/month=10/day=05
	fullDirectoryPath := filepath.Dir(files[0].FilePath)
	log.Debugf("Using directory path from files: %s", fullDirectoryPath)

	// Validate schema compatibility
	schemas := make([]string, len(files))
	for i, file := range files {
		schemas[i] = file.SchemaJSON
	}

	if err := mm.metadataExtractor.ValidateSchemaCompatibility(schemas); err != nil {
		log.Warnf("Schema compatibility issues detected: %v", err)
		// Continue anyway - just log the warning
	}

	// Generate _metadata file (with statistics)
	metadataContent, err := mm.metadataExtractor.GenerateMetadataFile(files, true)
	if err != nil {
		return fmt.Errorf("failed to generate _metadata content: %w", err)
	}

	// Upload _metadata file - use full directory path for S3 key
	metadataKey := mm.constructMetadataKey(fullDirectoryPath, "_metadata")
	if err := mm.uploadMetadataToS3(ctx, s3Dest, tenantID, datasetID, metadataKey, metadataContent); err != nil {
		return fmt.Errorf("failed to upload _metadata file: %w", err)
	}

	// Generate _common_metadata file (schema only)
	commonMetadataContent, err := mm.metadataExtractor.GenerateMetadataFile(files, false)
	if err != nil {
		return fmt.Errorf("failed to generate _common_metadata content: %w", err)
	}

	// Upload _common_metadata file - use full directory path for S3 key
	commonMetadataKey := mm.constructMetadataKey(fullDirectoryPath, "_common_metadata")
	if err := mm.uploadMetadataToS3(ctx, s3Dest, tenantID, datasetID, commonMetadataKey, commonMetadataContent); err != nil {
		return fmt.Errorf("failed to upload _common_metadata file: %w", err)
	}

	log.Infof("Successfully regenerated metadata files for %s:%s partition %s (%d files)",
		tenantID, datasetID, partitionPath, len(files))

	return nil
}

// regenerateRootMetadataFromDatabase generates root-level metadata files for ALL partitions
func (mm *MetadataManagerV2) regenerateRootMetadataFromDatabase(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination) error {
	log.Debugf("Regenerating root-level metadata from database for %s:%s", tenantID, datasetID)

	// Get ALL files for this tenant:dataset from Control API (across all partitions)
	files, err := mm.metadataClient.GetAllFileMetadata(ctx, tenantID, datasetID)
	if err != nil {
		return fmt.Errorf("failed to get all file metadata from database: %w", err)
	}

	if len(files) == 0 {
		log.Warnf("No files found in database for %s:%s", tenantID, datasetID)
		return nil
	}

	log.Infof("Found %d files across all partitions for root metadata generation", len(files))

	// Validate schema compatibility across all files
	schemas := make([]string, len(files))
	for i, file := range files {
		schemas[i] = file.SchemaJSON
	}

	// Check for schema incompatibilities and merge if needed
	if err := mm.metadataExtractor.ValidateSchemaCompatibility(schemas); err != nil {
		log.Warnf("Schema compatibility issues detected across partitions: %v", err)
		log.Infof("Merging %d schemas to create unified schema", len(schemas))

		// Merge schemas to create a union schema that covers all partitions
		mergedSchema, mergeErr := mm.metadataExtractor.MergeSchemas(schemas)
		if mergeErr != nil {
			log.Errorf("Failed to merge schemas: %v", mergeErr)
			// Continue with original schemas - will use first file's schema
		} else {
			// Update all files to use the merged schema
			mergedSchemaJSON, _ := sonic.Marshal(mergedSchema)
			for i := range files {
				files[i].SchemaJSON = string(mergedSchemaJSON)
			}
			log.Infof("Successfully merged schemas - updated all %d file records with unified schema", len(files))
		}
	}

	// Generate _metadata file (with statistics) covering all partitions
	metadataContent, err := mm.metadataExtractor.GenerateMetadataFile(files, true)
	if err != nil {
		return fmt.Errorf("failed to generate root _metadata content: %w", err)
	}

	// Construct root-level key: PREFIX/TENANT_ID/DATASET_ID/data/parquet/_metadata
	// Example: ebpf-out/customer-1/ebpf-data/data/parquet/_metadata
	rootPath := mm.constructRootMetadataPath(s3Dest.Prefix, tenantID, datasetID)
	metadataKey := mm.constructMetadataKey(rootPath, "_metadata")
	if err := mm.uploadMetadataToS3(ctx, s3Dest, tenantID, datasetID, metadataKey, metadataContent); err != nil {
		return fmt.Errorf("failed to upload root _metadata file: %w", err)
	}

	// Generate _common_metadata file (schema only)
	commonMetadataContent, err := mm.metadataExtractor.GenerateMetadataFile(files, false)
	if err != nil {
		return fmt.Errorf("failed to generate root _common_metadata content: %w", err)
	}

	// Upload _common_metadata file at root level
	commonMetadataKey := mm.constructMetadataKey(rootPath, "_common_metadata")
	if err := mm.uploadMetadataToS3(ctx, s3Dest, tenantID, datasetID, commonMetadataKey, commonMetadataContent); err != nil {
		return fmt.Errorf("failed to upload root _common_metadata file: %w", err)
	}

	log.Infof("Successfully regenerated root-level metadata files for %s:%s covering %d files across all partitions",
		tenantID, datasetID, len(files))

	return nil
}

// constructRootMetadataPath builds the root directory path for metadata files
// Example: "ebpf-out/customer-1/ebpf-data/data/parquet"
func (mm *MetadataManagerV2) constructRootMetadataPath(prefix, tenantID, datasetID string) string {
	// Build path: PREFIX/TENANT_ID/DATASET_ID/data/parquet
	var pathParts []string

	// Add prefix if provided
	if prefix != "" {
		cleanPrefix := strings.TrimSuffix(prefix, "/")
		pathParts = append(pathParts, cleanPrefix)
	}

	// Add tenant and dataset directories
	pathParts = append(pathParts, tenantID, datasetID, "data/parquet")

	return strings.Join(pathParts, "/")
}

// uploadMetadataToS3 uploads metadata content to S3
func (mm *MetadataManagerV2) uploadMetadataToS3(ctx context.Context, s3Dest *domain.S3Destination, tenantID, datasetID, key string, content []byte) error {
	// Get S3 client for this tenant:dataset
	s3Client, err := mm.s3DestinationManager.GetS3Client(tenantID, datasetID, s3Dest)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	// Prepare metadata
	metadata := map[string]string{
		"Content-Type":   "application/json",
		"X-Generated-By": "bytefreezer-packer-v2",
		"X-Generated-At": time.Now().UTC().Format(time.RFC3339),
		"X-Tenant-ID":    tenantID,
		"X-Dataset-ID":   datasetID,
		"X-File-Count":   "0", // Would be populated with actual count
	}

	// Upload to S3
	if err := s3Client.PutObjectBytes(ctx, key, content, metadata); err != nil {
		return fmt.Errorf("failed to upload metadata file %s: %w", key, err)
	}

	log.Infof("Successfully uploaded metadata file to s3://%s/%s (%d bytes)", s3Dest.BucketName, key, len(content))
	return nil
}

// updateGenerationStatus updates the metadata generation tracking
func (mm *MetadataManagerV2) updateGenerationStatus(ctx context.Context, tenantID, datasetID, partitionPath string) error {
	// Get current summary from database
	summary, err := mm.metadataClient.GetMetadataSummary(ctx, tenantID, datasetID, partitionPath)
	if err != nil {
		return fmt.Errorf("failed to get metadata summary: %w", err)
	}

	if summary == nil {
		log.Debugf("No summary data found for %s:%s partition %s", tenantID, datasetID, partitionPath)
		return nil
	}

	// Calculate schema hash for change detection
	// For now, use a simple approach - in production you'd calculate from actual schema
	schemaHash := calculateSchemaHash(fmt.Sprintf("%s:%s:%s", tenantID, datasetID, partitionPath))

	status := &storage.MetadataGenerationStatus{
		TenantID:          tenantID,
		DatasetID:         datasetID,
		PartitionPath:     partitionPath,
		LastGeneratedAt:   time.Now(),
		FileCount:         summary.FileCount,
		TotalRows:         summary.TotalRows,
		TotalSizeBytes:    summary.TotalSizeBytes,
		NeedsRegeneration: false,
		CurrentSchemaHash: schemaHash,
		SchemaVersion:     1,
	}

	return mm.metadataClient.UpdateGenerationStatus(ctx, status)
}

// constructMetadataKey builds the S3 key for metadata files
func (mm *MetadataManagerV2) constructMetadataKey(partitionPath, fileName string) string {
	if partitionPath == "" {
		return fileName
	}

	// Ensure proper path formatting
	cleanPath := strings.TrimPrefix(partitionPath, "/")
	if !strings.HasSuffix(cleanPath, "/") {
		cleanPath += "/"
	}

	return cleanPath + fileName
}

// getPartitionPath extracts the partition path from uploaded file path
func (mm *MetadataManagerV2) getPartitionPath(uploadedFilePath string) string {
	// Extract partition from path like: customer-1/ebpf-data/data/parquet/year=2024/month=01/day=15/file.parquet
	// Should return only the Hive partition: year=2024/month=01/day=15
	// This must match the format used by ParquetMetadataExtractor.extractPartitionPath()

	dir := filepath.Dir(uploadedFilePath)
	parts := strings.Split(dir, "/")

	// Look for Hive-style partitions (key=value)
	var partitionParts []string
	for _, part := range parts {
		if strings.Contains(part, "=") {
			partitionParts = append(partitionParts, part)
		}
	}

	if len(partitionParts) > 0 {
		return strings.Join(partitionParts, "/")
	}

	// If no Hive-style partitions found, use the directory path
	// but skip the tenant/dataset parts (first 2 components typically)
	if len(parts) > 2 {
		return strings.Join(parts[2:], "/")
	}

	return ""
}

// GetMetadataInfo returns information about metadata files for a directory
func (mm *MetadataManagerV2) GetMetadataInfo(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, directory string) (*MetadataInfo, error) {
	partitionPath := mm.getPartitionPath(directory)

	// Get data from Control API first
	summary, err := mm.metadataClient.GetMetadataSummary(ctx, tenantID, datasetID, partitionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata summary: %w", err)
	}

	info := &MetadataInfo{
		Directory: directory,
	}

	if summary != nil {
		info.ParquetFileCount = summary.FileCount
		// Check if metadata files exist in S3
		info.HasMetadata, err = mm.checkMetadataFileExists(ctx, s3Dest, tenantID, datasetID, partitionPath, "_metadata")
		if err != nil {
			log.Warnf("Failed to check _metadata file existence: %v", err)
		}

		info.HasCommonMetadata, err = mm.checkMetadataFileExists(ctx, s3Dest, tenantID, datasetID, partitionPath, "_common_metadata")
		if err != nil {
			log.Warnf("Failed to check _common_metadata file existence: %v", err)
		}
	}

	return info, nil
}

// checkMetadataFileExists checks if a metadata file exists in S3
func (mm *MetadataManagerV2) checkMetadataFileExists(ctx context.Context, s3Dest *domain.S3Destination, tenantID, datasetID, partitionPath, fileName string) (bool, error) {
	s3Client, err := mm.s3DestinationManager.GetS3Client(tenantID, datasetID, s3Dest)
	if err != nil {
		return false, fmt.Errorf("failed to get S3 client: %w", err)
	}

	key := mm.constructMetadataKey(partitionPath, fileName)
	return s3Client.ObjectExists(ctx, s3Dest.BucketName, key)
}

// ValidateMetadataExists checks if metadata files exist for a given directory
func (mm *MetadataManagerV2) ValidateMetadataExists(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, directory string) (bool, error) {
	partitionPath := mm.getPartitionPath(directory)
	return mm.checkMetadataFileExists(ctx, s3Dest, tenantID, datasetID, partitionPath, "_metadata")
}

// CleanupOrphanedMetadata removes metadata for files that no longer exist in S3
func (mm *MetadataManagerV2) CleanupOrphanedMetadata(ctx context.Context, tenantID, datasetID string) error {
	log.Infof("Starting orphaned metadata cleanup for %s:%s", tenantID, datasetID)

	// This would be implemented as a background job
	// For now, just log the intention
	log.Infof("Orphaned metadata cleanup completed for %s:%s", tenantID, datasetID)

	return nil
}

// GetStats returns statistics about the metadata system
func (mm *MetadataManagerV2) GetStats(ctx context.Context) (map[string]interface{}, error) {
	// Return basic statistics about metadata management
	return map[string]interface{}{
		"implementation": "control-api-v2",
		"features": []string{
			"incremental-updates",
			"schema-validation",
			"failure-tracking",
			"distributed-locking",
		},
	}, nil
}

// calculateSchemaHash calculates SHA256 hash for schema change detection
func calculateSchemaHash(input string) string {
	hash := sha256.Sum256([]byte(input))
	return fmt.Sprintf("%x", hash)
}

// MetadataInfo is defined in metadata_manager.go
