// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package storage

import (
	"context"
	"time"
)

// MetadataClient defines the interface for Parquet metadata management
type MetadataClient interface {
	// UpsertFileMetadata inserts or updates metadata for a Parquet file
	UpsertFileMetadata(ctx context.Context, metadata *ParquetFileMetadata) error

	// GetFileMetadataByPartition retrieves all file metadata for a specific partition
	GetFileMetadataByPartition(ctx context.Context, tenantID, datasetID, partitionPath string) ([]*ParquetFileMetadata, error)

	// GetAllFileMetadata retrieves ALL file metadata for a tenant:dataset (across all partitions)
	GetAllFileMetadata(ctx context.Context, tenantID, datasetID string) ([]*ParquetFileMetadata, error)

	// UpdateGenerationStatus updates the metadata generation status for a partition
	UpdateGenerationStatus(ctx context.Context, status *MetadataGenerationStatus) error

	// GetGenerationStatus retrieves the metadata generation status for a partition
	GetGenerationStatus(ctx context.Context, tenantID, datasetID, partitionPath string) (*MetadataGenerationStatus, error)

	// GetMetadataSummary retrieves aggregated metadata for a partition
	GetMetadataSummary(ctx context.Context, tenantID, datasetID, partitionPath string) (*ParquetMetadataSummary, error)

	// DeleteFileMetadata removes metadata for a specific file (for cleanup)
	DeleteFileMetadata(ctx context.Context, tenantID, datasetID, filePath string) error

	// CleanupOrphanedMetadata removes metadata for files that no longer exist
	CleanupOrphanedMetadata(ctx context.Context, tenantID, datasetID string, existingFiles []string) (int, error)

	// CleanupExpiredMetadata removes metadata records that have exceeded their TTL
	CleanupExpiredMetadata(ctx context.Context) (int, int, error)

	// UpsertFieldTrackingBatch updates or creates field tracking entries for schema evolution
	// fields is a map of field_name -> field_type
	UpsertFieldTrackingBatch(ctx context.Context, tenantID, datasetID string, fields map[string]string) error

	// Close closes any resources
	Close() error
}

// ParquetFileMetadata represents metadata for a single Parquet file
type ParquetFileMetadata struct {
	ID            int64     `json:"id"`
	TenantID      string    `json:"tenant_id"`
	DatasetID     string    `json:"dataset_id"`
	FilePath      string    `json:"file_path"`
	PartitionPath string    `json:"partition_path"`
	FileSizeBytes int64     `json:"file_size_bytes"`
	RowCount      int64     `json:"row_count"`
	CreatedAt     time.Time `json:"created_at"`
	LastModified  time.Time `json:"last_modified"`
	SchemaJSON    string    `json:"schema_json"`
	ColumnStats   string    `json:"column_stats"`
	FileChecksum  string    `json:"file_checksum"`
	InstanceID    string    `json:"instance_id"`
	Version       int       `json:"metadata_version"`
}

// MetadataGenerationStatus tracks metadata generation for partitions
type MetadataGenerationStatus struct {
	TenantID          string    `json:"tenant_id"`
	DatasetID         string    `json:"dataset_id"`
	PartitionPath     string    `json:"partition_path"`
	LastGeneratedAt   time.Time `json:"last_generated_at"`
	FileCount         int       `json:"file_count"`
	TotalRows         int64     `json:"total_rows"`
	TotalSizeBytes    int64     `json:"total_size_bytes"`
	NeedsRegeneration bool      `json:"needs_regeneration"`
	CurrentSchemaHash string    `json:"current_schema_hash"`
	SchemaVersion     int       `json:"schema_version"`
}

// ParquetMetadataSummary provides aggregated metadata information
type ParquetMetadataSummary struct {
	TenantID            string    `json:"tenant_id"`
	DatasetID           string    `json:"dataset_id"`
	PartitionPath       string    `json:"partition_path"`
	FileCount           int       `json:"file_count"`
	TotalRows           int64     `json:"total_rows"`
	TotalSizeBytes      int64     `json:"total_size_bytes"`
	FirstFileCreated    time.Time `json:"first_file_created"`
	LastFileModified    time.Time `json:"last_file_modified"`
	MetadataLastUpdated time.Time `json:"metadata_last_updated"`
}
