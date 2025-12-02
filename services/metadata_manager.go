// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/domain"
	"github.com/bytefreezer/packer/storage"
)

// MetadataManager handles Parquet metadata file generation and updates
type MetadataManager struct {
	s3DestinationManager *storage.S3DestinationManager
	lockClient           storage.LockClient
	instanceID           string
}

// NewMetadataManager creates a new metadata manager
func NewMetadataManager(s3DestinationManager *storage.S3DestinationManager, lockClient storage.LockClient, instanceID string) *MetadataManager {
	return &MetadataManager{
		s3DestinationManager: s3DestinationManager,
		lockClient:           lockClient,
		instanceID:           instanceID,
	}
}

// UpdateMetadataAfterUpload updates _metadata and _common_metadata files after successful Parquet upload
func (mm *MetadataManager) UpdateMetadataAfterUpload(ctx context.Context, tenant *domain.Tenant, dataset *domain.Dataset, uploadedFilePath string) error {
	// Create lock key for tenant:dataset metadata updates
	lockKey := fmt.Sprintf("metadata_%s_%s", tenant.ID, dataset.ID)

	log.Debugf("Acquiring metadata lock for tenant:%s dataset:%s", tenant.ID, dataset.ID)

	// Acquire distributed lock to prevent concurrent metadata updates
	_, err := mm.lockClient.AcquireLock(ctx, lockKey, mm.instanceID, time.Minute*5)
	if err != nil {
		return fmt.Errorf("failed to acquire metadata lock for %s:%s: %w", tenant.ID, dataset.ID, err)
	}
	defer func() {
		if unlockErr := mm.lockClient.ReleaseLock(ctx, lockKey, mm.instanceID); unlockErr != nil {
			log.Errorf("Failed to release metadata lock for %s:%s: %v", tenant.ID, dataset.ID, unlockErr)
		}
	}()

	log.Infof("Updating metadata files for tenant:%s dataset:%s after uploading %s", tenant.ID, dataset.ID, uploadedFilePath)

	// Get the directory path where metadata files should be created
	metadataDir := mm.getMetadataDirectory(uploadedFilePath)

	// List all Parquet files in the directory
	parquetFiles, err := mm.listParquetFilesInDirectory(ctx, tenant.ID, dataset.ID, dataset.S3Destination, metadataDir)
	if err != nil {
		return fmt.Errorf("failed to list Parquet files for metadata update: %w", err)
	}

	if len(parquetFiles) == 0 {
		log.Warnf("No Parquet files found in directory %s for metadata update", metadataDir)
		return nil
	}

	log.Debugf("Found %d Parquet files for metadata update in %s", len(parquetFiles), metadataDir)

	// Generate and upload _metadata file (contains schema + statistics for all files)
	if err := mm.generateAndUploadMetadataFile(ctx, tenant.ID, dataset.ID, dataset.S3Destination, metadataDir, parquetFiles, "_metadata", true); err != nil {
		return fmt.Errorf("failed to generate _metadata file: %w", err)
	}

	// Generate and upload _common_metadata file (contains schema only)
	if err := mm.generateAndUploadMetadataFile(ctx, tenant.ID, dataset.ID, dataset.S3Destination, metadataDir, parquetFiles, "_common_metadata", false); err != nil {
		return fmt.Errorf("failed to generate _common_metadata file: %w", err)
	}

	log.Infof("Successfully updated metadata files for tenant:%s dataset:%s in %s", tenant.ID, dataset.ID, metadataDir)
	return nil
}

// getMetadataDirectory extracts the directory path from uploaded file path
func (mm *MetadataManager) getMetadataDirectory(uploadedFilePath string) string {
	// Remove the filename to get directory path
	return filepath.Dir(uploadedFilePath)
}

// listParquetFilesInDirectory lists all .parquet files in the specified S3 directory
func (mm *MetadataManager) listParquetFilesInDirectory(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, directory string) ([]string, error) {
	// Construct S3 prefix for the directory
	prefix := strings.TrimPrefix(directory, "/")
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	log.Debugf("Listing Parquet files in S3 directory: bucket=%s prefix=%s", s3Dest.BucketName, prefix)

	// Get S3 client for this tenant:dataset
	s3Client, err := mm.s3DestinationManager.GetS3Client(tenantID, datasetID, s3Dest)
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 client: %w", err)
	}

	// List objects with .parquet extension
	objects, err := s3Client.ListObjects(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list S3 objects: %w", err)
	}

	var parquetFiles []string
	for _, obj := range objects {
		if strings.HasSuffix(strings.ToLower(obj.Key), ".parquet") {
			// Skip metadata files themselves
			filename := filepath.Base(obj.Key)
			if filename != "_metadata" && filename != "_common_metadata" {
				parquetFiles = append(parquetFiles, obj.Key)
			}
		}
	}

	return parquetFiles, nil
}

// generateAndUploadMetadataFile generates metadata file and uploads to S3
func (mm *MetadataManager) generateAndUploadMetadataFile(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, directory string, parquetFiles []string, metadataFileName string, includeStatistics bool) error {
	log.Debugf("Generating %s file for %d Parquet files (includeStatistics=%v)", metadataFileName, len(parquetFiles), includeStatistics)

	// Generate metadata content
	metadataContent, err := mm.generateParquetMetadata(ctx, s3Dest, parquetFiles, includeStatistics)
	if err != nil {
		return fmt.Errorf("failed to generate metadata content: %w", err)
	}

	// Construct S3 key for metadata file
	metadataKey := strings.TrimPrefix(directory, "/")
	if !strings.HasSuffix(metadataKey, "/") {
		metadataKey += "/"
	}
	metadataKey += metadataFileName

	// Upload metadata file to S3
	log.Debugf("Uploading %s to S3: bucket=%s key=%s size=%d bytes", metadataFileName, s3Dest.BucketName, metadataKey, len(metadataContent))

	metadata := map[string]string{
		"Content-Type":    "application/octet-stream",
		"X-Parquet-Files": fmt.Sprintf("%d", len(parquetFiles)),
		"X-Generated-By":  "bytefreezer-packer",
		"X-Generated-At":  time.Now().UTC().Format(time.RFC3339),
		"X-Include-Stats": fmt.Sprintf("%v", includeStatistics),
	}

	// Get S3 client for this tenant:dataset
	s3Client, err := mm.s3DestinationManager.GetS3Client(tenantID, datasetID, s3Dest)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	err = s3Client.PutObjectBytes(ctx, metadataKey, metadataContent, metadata)
	if err != nil {
		return fmt.Errorf("failed to upload %s file: %w", metadataFileName, err)
	}

	log.Infof("Successfully uploaded %s file: %s (%d bytes, %d files)", metadataFileName, metadataKey, len(metadataContent), len(parquetFiles))
	return nil
}

// generateParquetMetadata generates the actual Parquet metadata content
func (mm *MetadataManager) generateParquetMetadata(ctx context.Context, s3Dest *domain.S3Destination, parquetFiles []string, includeStatistics bool) ([]byte, error) {
	log.Debugf("Generating Parquet metadata for %d files (includeStatistics=%v)", len(parquetFiles), includeStatistics)

	// This is a simplified implementation - in production, you would use a proper Parquet library
	// to read metadata from existing files and combine them

	// For now, we'll create a basic metadata structure
	// TODO: Implement proper Parquet metadata reading and combining using arrow/parquet-go

	metadataHeader := fmt.Sprintf(`{
  "version": 1,
  "schema": {
    "type": "group",
    "fields": []
  },
  "num_rows": 0,
  "row_groups": [],
  "files": [%s],
  "generated_by": "bytefreezer-packer",
  "generated_at": "%s",
  "include_statistics": %v
}`, mm.formatFileList(parquetFiles), time.Now().UTC().Format(time.RFC3339), includeStatistics)

	return []byte(metadataHeader), nil
}

// formatFileList formats the list of Parquet files for metadata
func (mm *MetadataManager) formatFileList(files []string) string {
	var fileEntries []string
	for _, file := range files {
		entry := fmt.Sprintf(`{"path": "%s"}`, file)
		fileEntries = append(fileEntries, entry)
	}
	return strings.Join(fileEntries, ",\n    ")
}

// ValidateMetadataExists checks if metadata files exist for a given directory
func (mm *MetadataManager) ValidateMetadataExists(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, directory string) (bool, error) {
	metadataKey := strings.TrimPrefix(directory, "/")
	if !strings.HasSuffix(metadataKey, "/") {
		metadataKey += "/"
	}
	metadataKey += "_metadata"

	// Get S3 client for this tenant:dataset
	s3Client, err := mm.s3DestinationManager.GetS3Client(tenantID, datasetID, s3Dest)
	if err != nil {
		return false, fmt.Errorf("failed to get S3 client: %w", err)
	}

	exists, err := s3Client.ObjectExists(ctx, s3Dest.BucketName, metadataKey)
	if err != nil {
		return false, fmt.Errorf("failed to check metadata existence: %w", err)
	}

	return exists, nil
}

// GetMetadataInfo returns information about metadata files
func (mm *MetadataManager) GetMetadataInfo(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, directory string) (*MetadataInfo, error) {
	metadataKey := strings.TrimPrefix(directory, "/")
	if !strings.HasSuffix(metadataKey, "/") {
		metadataKey += "/"
	}

	info := &MetadataInfo{
		Directory: directory,
	}

	// Get S3 client for this tenant:dataset
	s3Client, err := mm.s3DestinationManager.GetS3Client(tenantID, datasetID, s3Dest)
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 client: %w", err)
	}

	// Check _metadata file
	metadataExists, err := s3Client.ObjectExists(ctx, s3Dest.BucketName, metadataKey+"_metadata")
	if err != nil {
		return nil, fmt.Errorf("failed to check _metadata existence: %w", err)
	}
	info.HasMetadata = metadataExists

	// Check _common_metadata file
	commonMetadataExists, err := s3Client.ObjectExists(ctx, s3Dest.BucketName, metadataKey+"_common_metadata")
	if err != nil {
		return nil, fmt.Errorf("failed to check _common_metadata existence: %w", err)
	}
	info.HasCommonMetadata = commonMetadataExists

	// Count Parquet files
	parquetFiles, err := mm.listParquetFilesInDirectory(ctx, tenantID, datasetID, s3Dest, directory)
	if err != nil {
		return nil, fmt.Errorf("failed to count Parquet files: %w", err)
	}
	info.ParquetFileCount = len(parquetFiles)

	return info, nil
}

// MetadataInfo contains information about metadata files in a directory
type MetadataInfo struct {
	Directory         string `json:"directory"`
	HasMetadata       bool   `json:"has_metadata"`
	HasCommonMetadata bool   `json:"has_common_metadata"`
	ParquetFileCount  int    `json:"parquet_file_count"`
}
