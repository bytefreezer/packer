package services

import (
	"context"

	"github.com/bytefreezer/packer/domain"
)

// MetadataManagerInterface defines the contract for metadata managers
type MetadataManagerInterface interface {
	// UpdateMetadataAfterUpload updates metadata files after successful Parquet upload
	UpdateMetadataAfterUpload(ctx context.Context, tenant *domain.Tenant, dataset *domain.Dataset, uploadedFilePath string) error

	// GetMetadataInfo returns information about metadata files for a directory
	GetMetadataInfo(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, directory string) (*MetadataInfo, error)

	// ValidateMetadataExists checks if metadata files exist for a given directory
	ValidateMetadataExists(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, directory string) (bool, error)
}

// Ensure both implementations satisfy the interface
var _ MetadataManagerInterface = (*MetadataManager)(nil)
var _ MetadataManagerInterface = (*MetadataManagerV2)(nil)
