// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/bytefreezer/packer/domain"
)

// DataLayoutManager handles secure data layout and prevents cross-tenant contamination
type DataLayoutManager struct {
	globalBucketRegistry map[string]*BucketOwnership // bucket -> ownership info
}

// BucketOwnership tracks which tenant owns a bucket to prevent conflicts
type BucketOwnership struct {
	OwnerTenantID   string
	OwnerTenantName string
	RegisteredAt    time.Time
}

// DataLayout represents the complete data layout for a dataset
type DataLayout struct {
	// Bucket information
	BucketName    string
	OwnerTenantID string

	// Directory structure
	TenantDirectory  string // tenant-xxx/
	DatasetDirectory string // dataset-yyy/

	// Data paths
	ParquetPath string // Full S3 key for parquet data
	RawPath     string // Full S3 key for raw NDJSON data

	// Metadata
	PartitionScheme string
	Timestamp       time.Time
	IsSecure        bool // Whether this layout passes security checks
}

// NewDataLayoutManager creates a new data layout manager
func NewDataLayoutManager() *DataLayoutManager {
	return &DataLayoutManager{
		globalBucketRegistry: make(map[string]*BucketOwnership),
	}
}

// GenerateSecureLayout generates a secure data layout for a dataset with cross-tenant protection
func (dlm *DataLayoutManager) GenerateSecureLayout(tenant *domain.Tenant, dataset *domain.Dataset, timestamp time.Time, fileType string) (*DataLayout, error) {
	layout := &DataLayout{
		BucketName:    dataset.S3Destination.BucketName,
		OwnerTenantID: tenant.ID,
		Timestamp:     timestamp,
		IsSecure:      false,
	}

	// Step 1: Verify bucket ownership and prevent cross-tenant contamination
	if err := dlm.validateBucketOwnership(tenant.ID, tenant.Name, dataset.S3Destination.BucketName); err != nil {
		return nil, fmt.Errorf("bucket ownership validation failed: %w", err)
	}

	// Step 2: Generate secure directory structure
	layout.TenantDirectory = dlm.generateSecureTenantDirectory(tenant)
	layout.DatasetDirectory = dlm.generateSecureDatasetDirectory(dataset)

	// Step 3: Get partitioning scheme
	partitionScheme := "hive" // Default to hive for backward compatibility
	var customPattern string
	if dataset.ProcessingConfig != nil && dataset.ProcessingConfig.PartitioningScheme != "" {
		partitionScheme = dataset.ProcessingConfig.PartitioningScheme
		if partitionScheme == "custom" && dataset.ProcessingConfig.PartitionLayout != "" {
			customPattern = dataset.ProcessingConfig.PartitionLayout
		}
	}
	layout.PartitionScheme = partitionScheme

	// Step 4: Generate full data paths
	var partitionPath string
	if partitionScheme == "custom" && customPattern != "" {
		partitionPath = dlm.applyCustomPattern(customPattern, tenant, dataset, timestamp)
	} else {
		partitionPath = dlm.generatePartitionPath(partitionScheme, timestamp)
	}
	baseFileName := dlm.generateSecureFileName(tenant, dataset, timestamp)

	// Step 5: Build complete S3 keys with tenant/dataset isolation
	basePath := fmt.Sprintf("%s/%s", layout.TenantDirectory, layout.DatasetDirectory)

	// Apply S3 destination prefix if configured
	var prefix string
	if dataset.S3Destination != nil && dataset.S3Destination.Prefix != "" {
		prefix = strings.TrimSuffix(dataset.S3Destination.Prefix, "/") + "/"
	}

	if partitionPath != "" {
		layout.ParquetPath = fmt.Sprintf("%s%s/data/parquet/%s/%s.parquet", prefix, basePath, partitionPath, baseFileName)
		layout.RawPath = fmt.Sprintf("%s%s/data/raw/%s/%s.ndjson", prefix, basePath, partitionPath, baseFileName)
	} else {
		layout.ParquetPath = fmt.Sprintf("%s%s/data/parquet/%s.parquet", prefix, basePath, baseFileName)
		layout.RawPath = fmt.Sprintf("%s%s/data/raw/%s.ndjson", prefix, basePath, baseFileName)
	}

	// Step 6: Final security validation
	if err := dlm.validateLayoutSecurity(layout); err != nil {
		return nil, fmt.Errorf("layout security validation failed: %w", err)
	}

	layout.IsSecure = true
	return layout, nil
}

// validateBucketOwnership ensures a bucket is only used by one tenant
func (dlm *DataLayoutManager) validateBucketOwnership(tenantID, tenantName, bucketName string) error {
	// Normalize bucket name to prevent case-based attacks
	bucketName = strings.ToLower(strings.TrimSpace(bucketName))

	if bucketName == "" {
		return fmt.Errorf("bucket name cannot be empty")
	}

	// Check if bucket is already registered to a different tenant
	if ownership, exists := dlm.globalBucketRegistry[bucketName]; exists {
		if ownership.OwnerTenantID != tenantID {
			return fmt.Errorf("SECURITY VIOLATION: bucket '%s' is already owned by tenant '%s' (requested by tenant '%s')",
				bucketName, ownership.OwnerTenantID, tenantID)
		}
		// Bucket already registered to this tenant - OK
		return nil
	}

	// Register bucket to this tenant
	dlm.globalBucketRegistry[bucketName] = &BucketOwnership{
		OwnerTenantID:   tenantID,
		OwnerTenantName: tenantName,
		RegisteredAt:    time.Now(),
	}

	return nil
}

// generateSecureTenantDirectory creates a secure tenant directory name
func (dlm *DataLayoutManager) generateSecureTenantDirectory(tenant *domain.Tenant) string {
	// Use simple tenant ID for directory naming
	// Sanitize tenant ID for filesystem safety
	safeTenantID := dlm.sanitizeDirectoryName(tenant.ID)
	return safeTenantID
}

// generateSecureDatasetDirectory creates a dataset directory name
func (dlm *DataLayoutManager) generateSecureDatasetDirectory(dataset *domain.Dataset) string {
	// Use simple dataset ID for directory naming
	// Sanitize dataset ID for filesystem safety
	safeDatasetID := dlm.sanitizeDirectoryName(dataset.ID)
	return safeDatasetID
}

// generatePartitionPath creates partition path based on scheme
func (dlm *DataLayoutManager) generatePartitionPath(scheme string, timestamp time.Time) string {
	switch scheme {
	case "hive":
		// Hive-style partitioning with key=value format
		return fmt.Sprintf("year=%04d/month=%02d/day=%02d",
			timestamp.Year(), timestamp.Month(), timestamp.Day())
	case "date":
		// Simple date-based partitioning (no key=value format)
		return fmt.Sprintf("%04d/%02d/%02d",
			timestamp.Year(), timestamp.Month(), timestamp.Day())
	case "date_hour":
		// Hive-style with hour partitioning
		return fmt.Sprintf("year=%04d/month=%02d/day=%02d/hour=%02d",
			timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour())
	case "tenant_dataset", "none":
		// No time-based partitioning (tenant/dataset handled separately)
		return ""
	default:
		// Default to hive partitioning for backward compatibility
		return fmt.Sprintf("year=%04d/month=%02d/day=%02d",
			timestamp.Year(), timestamp.Month(), timestamp.Day())
	}
}

// applyCustomPattern applies a custom pattern template with variable substitution
// Supported variables: {tenant}, {dataset}, {year}, {month}, {day}, {hour}
func (dlm *DataLayoutManager) applyCustomPattern(pattern string, tenant *domain.Tenant, dataset *domain.Dataset, timestamp time.Time) string {
	result := pattern

	// Sanitize tenant and dataset IDs for path safety
	safeTenantID := dlm.sanitizeDirectoryName(tenant.ID)
	safeDatasetID := dlm.sanitizeDirectoryName(dataset.ID)

	// Replace variables
	result = strings.ReplaceAll(result, "{tenant}", safeTenantID)
	result = strings.ReplaceAll(result, "{dataset}", safeDatasetID)
	result = strings.ReplaceAll(result, "{year}", fmt.Sprintf("%04d", timestamp.Year()))
	result = strings.ReplaceAll(result, "{month}", fmt.Sprintf("%02d", timestamp.Month()))
	result = strings.ReplaceAll(result, "{day}", fmt.Sprintf("%02d", timestamp.Day()))
	result = strings.ReplaceAll(result, "{hour}", fmt.Sprintf("%02d", timestamp.Hour()))

	// Remove leading/trailing slashes
	result = strings.Trim(result, "/")

	return result
}

// generateSecureFileName creates a secure, unique filename
func (dlm *DataLayoutManager) generateSecureFileName(tenant *domain.Tenant, dataset *domain.Dataset, timestamp time.Time) string {
	fileTimestamp := timestamp.Format("20060102_150405_000") // Include milliseconds

	// Create a content hash for additional uniqueness
	hasher := sha256.New()
	hasher.Write([]byte(fmt.Sprintf("%s:%s:%d", tenant.ID, dataset.ID, timestamp.UnixNano())))
	contentHash := fmt.Sprintf("%x", hasher.Sum(nil))[:12]

	return fmt.Sprintf("%s_%s_%s_%s",
		dlm.sanitizeDirectoryName(tenant.ID),
		dlm.sanitizeDirectoryName(dataset.ID),
		fileTimestamp,
		contentHash)
}

// sanitizeDirectoryName ensures directory names are filesystem-safe
func (dlm *DataLayoutManager) sanitizeDirectoryName(name string) string {
	// Replace unsafe characters with underscores
	unsafe := []string{"/", "\\", ":", "*", "?", "\"", "<", ">", "|", " ", "..", "~", "#", "%"}
	sanitized := name

	for _, char := range unsafe {
		sanitized = strings.ReplaceAll(sanitized, char, "_")
	}

	// Limit length to prevent filesystem issues
	if len(sanitized) > 50 {
		sanitized = sanitized[:50]
	}

	// Ensure not empty
	if sanitized == "" {
		sanitized = "unknown"
	}

	return sanitized
}

// validateLayoutSecurity performs final security validation on the layout
func (dlm *DataLayoutManager) validateLayoutSecurity(layout *DataLayout) error {
	// Check for path traversal attempts
	if strings.Contains(layout.ParquetPath, "..") || strings.Contains(layout.RawPath, "..") {
		return fmt.Errorf("SECURITY VIOLATION: path traversal detected in layout")
	}

	// Ensure tenant directory is present in all paths
	if !strings.Contains(layout.ParquetPath, layout.TenantDirectory) {
		return fmt.Errorf("SECURITY VIOLATION: parquet path missing tenant directory isolation")
	}

	if !strings.Contains(layout.RawPath, layout.TenantDirectory) {
		return fmt.Errorf("SECURITY VIOLATION: raw path missing tenant directory isolation")
	}

	// Ensure dataset directory is present in all paths
	if !strings.Contains(layout.ParquetPath, layout.DatasetDirectory) {
		return fmt.Errorf("SECURITY VIOLATION: parquet path missing dataset directory isolation")
	}

	if !strings.Contains(layout.RawPath, layout.DatasetDirectory) {
		return fmt.Errorf("SECURITY VIOLATION: raw path missing dataset directory isolation")
	}

	// Check path length limits
	if len(layout.ParquetPath) > 1024 {
		return fmt.Errorf("SECURITY VIOLATION: parquet path exceeds maximum length")
	}

	if len(layout.RawPath) > 1024 {
		return fmt.Errorf("SECURITY VIOLATION: raw path exceeds maximum length")
	}

	return nil
}
