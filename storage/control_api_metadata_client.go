// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bytefreezer/goodies/log"
)

// ControlAPIMetadataClient implements MetadataClient using Control Service HTTP APIs
type ControlAPIMetadataClient struct {
	client  *http.Client
	baseURL string
	apiKey  string
}

// ControlAPIMetadataConfig holds configuration for Control API metadata client
type ControlAPIMetadataConfig struct {
	BaseURL        string
	APIKey         string
	TimeoutSeconds int
}

// NewControlAPIMetadataClient creates a new Control API metadata client
func NewControlAPIMetadataClient(conf *ControlAPIMetadataConfig) (*ControlAPIMetadataClient, error) {
	if conf.BaseURL == "" {
		return nil, fmt.Errorf("control service base_url is required")
	}

	timeout := time.Duration(conf.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &ControlAPIMetadataClient{
		client: &http.Client{
			Timeout: timeout,
		},
		baseURL: conf.BaseURL,
		apiKey:  conf.APIKey,
	}, nil
}

// doRequest performs an HTTP request to the control service
func (c *ControlAPIMetadataClient) doRequest(ctx context.Context, method, endpoint string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	url := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return resp, nil
}

// UpsertFileMetadata inserts or updates metadata for a Parquet file
func (c *ControlAPIMetadataClient) UpsertFileMetadata(ctx context.Context, metadata *ParquetFileMetadata) error {
	// Parse SchemaJSON string into map
	var schemaMap map[string]interface{}
	if metadata.SchemaJSON != "" {
		if err := json.Unmarshal([]byte(metadata.SchemaJSON), &schemaMap); err != nil {
			return fmt.Errorf("failed to unmarshal schema_json: %w", err)
		}
	}

	// Parse ColumnStats string (can be array or map)
	var columnStats interface{}
	if metadata.ColumnStats != "" {
		if err := json.Unmarshal([]byte(metadata.ColumnStats), &columnStats); err != nil {
			return fmt.Errorf("failed to unmarshal column_stats: %w", err)
		}
	}

	body := map[string]interface{}{
		"tenant_id":       metadata.TenantID,
		"dataset_id":      metadata.DatasetID,
		"file_path":       metadata.FilePath,
		"partition_path":  metadata.PartitionPath,
		"file_size_bytes": metadata.FileSizeBytes,
		"row_count":       metadata.RowCount,
		"created_at":      metadata.CreatedAt,
		"last_modified":   metadata.LastModified,
		"schema_json":     schemaMap,
		"column_stats":    columnStats,
		"file_checksum":   metadata.FileChecksum,
		"instance_id":     metadata.InstanceID,
	}

	resp, err := c.doRequest(ctx, "POST", "/api/v1/packer/metadata/files", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to upsert file metadata: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	// Decode response to get ID and version
	var result struct {
		ID      int64 `json:"id"`
		Version int   `json:"metadata_version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		// Non-fatal, metadata was stored successfully
		log.Debugf("Could not decode upsert response: %v", err)
	} else {
		metadata.ID = result.ID
		metadata.Version = result.Version
	}

	log.Debugf("Upserted metadata for file %s (ID: %d, Version: %d)", metadata.FilePath, metadata.ID, metadata.Version)
	return nil
}

// GetFileMetadataByPartition retrieves all file metadata for a specific partition
func (c *ControlAPIMetadataClient) GetFileMetadataByPartition(ctx context.Context, tenantID, datasetID, partitionPath string) ([]*ParquetFileMetadata, error) {
	endpoint := fmt.Sprintf("/api/v1/packer/metadata/files/%s/%s/partition/%s", tenantID, datasetID, partitionPath)

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// No metadata found - return empty slice
		return []*ParquetFileMetadata{}, nil
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get file metadata: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var files []*ParquetFileMetadata
	if err := json.NewDecoder(resp.Body).Decode(&files); err != nil {
		return nil, fmt.Errorf("failed to decode file metadata response: %w", err)
	}

	return files, nil
}

// GetAllFileMetadata retrieves ALL file metadata for a tenant:dataset (across all partitions)
func (c *ControlAPIMetadataClient) GetAllFileMetadata(ctx context.Context, tenantID, datasetID string) ([]*ParquetFileMetadata, error) {
	endpoint := fmt.Sprintf("/api/v1/packer/metadata/files/%s/%s/all", tenantID, datasetID)

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// No metadata found - return empty slice
		return []*ParquetFileMetadata{}, nil
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get all file metadata: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	// Decode response wrapper
	var response struct {
		Files []*ParquetFileMetadata `json:"files"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode file metadata response: %w", err)
	}

	return response.Files, nil
}

// UpdateGenerationStatus updates the metadata generation status for a partition
func (c *ControlAPIMetadataClient) UpdateGenerationStatus(ctx context.Context, status *MetadataGenerationStatus) error {
	body := map[string]interface{}{
		"tenant_id":           status.TenantID,
		"dataset_id":          status.DatasetID,
		"partition_path":      status.PartitionPath,
		"last_generated_at":   status.LastGeneratedAt,
		"file_count":          status.FileCount,
		"total_rows":          status.TotalRows,
		"total_size_bytes":    status.TotalSizeBytes,
		"needs_regeneration":  status.NeedsRegeneration,
		"current_schema_hash": status.CurrentSchemaHash,
		"schema_version":      status.SchemaVersion,
	}

	resp, err := c.doRequest(ctx, "POST", "/api/v1/packer/metadata/generation-status", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update generation status: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// GetGenerationStatus retrieves the metadata generation status for a partition
func (c *ControlAPIMetadataClient) GetGenerationStatus(ctx context.Context, tenantID, datasetID, partitionPath string) (*MetadataGenerationStatus, error) {
	endpoint := fmt.Sprintf("/api/v1/packer/metadata/generation-status/%s/%s/%s", tenantID, datasetID, partitionPath)

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// Return default status if not found
		return &MetadataGenerationStatus{
			TenantID:      tenantID,
			DatasetID:     datasetID,
			PartitionPath: partitionPath,
		}, nil
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get generation status: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var status MetadataGenerationStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode generation status response: %w", err)
	}

	return &status, nil
}

// GetMetadataSummary retrieves aggregated metadata for a partition
func (c *ControlAPIMetadataClient) GetMetadataSummary(ctx context.Context, tenantID, datasetID, partitionPath string) (*ParquetMetadataSummary, error) {
	endpoint := fmt.Sprintf("/api/v1/packer/metadata/summary/%s/%s/%s", tenantID, datasetID, partitionPath)

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// No data found
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get metadata summary: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var summary ParquetMetadataSummary
	if err := json.NewDecoder(resp.Body).Decode(&summary); err != nil {
		return nil, fmt.Errorf("failed to decode metadata summary response: %w", err)
	}

	return &summary, nil
}

// DeleteFileMetadata removes metadata for a specific file (for cleanup)
func (c *ControlAPIMetadataClient) DeleteFileMetadata(ctx context.Context, tenantID, datasetID, filePath string) error {
	endpoint := fmt.Sprintf("/api/v1/packer/metadata/files/%s/%s/%s", tenantID, datasetID, filePath)

	resp, err := c.doRequest(ctx, "DELETE", endpoint, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to delete file metadata: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// CleanupOrphanedMetadata removes metadata for files that no longer exist
func (c *ControlAPIMetadataClient) CleanupOrphanedMetadata(ctx context.Context, tenantID, datasetID string, existingFiles []string) (int, error) {
	body := map[string]interface{}{
		"tenant_id":      tenantID,
		"dataset_id":     datasetID,
		"existing_files": existingFiles,
	}

	resp, err := c.doRequest(ctx, "POST", "/api/v1/packer/metadata/cleanup/orphaned", body)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("failed to cleanup orphaned metadata: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var result struct {
		DeletedCount int `json:"deleted_count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("failed to decode cleanup response: %w", err)
	}

	return result.DeletedCount, nil
}

// CleanupExpiredMetadata removes metadata records that have exceeded their TTL
func (c *ControlAPIMetadataClient) CleanupExpiredMetadata(ctx context.Context) (int, int, error) {
	resp, err := c.doRequest(ctx, "DELETE", "/api/v1/packer/metadata/cleanup/expired", nil)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Warnf("Failed to cleanup expired metadata: status %d, body: %s", resp.StatusCode, string(bodyBytes))
		return 0, 0, nil
	}

	var result struct {
		FilesDeleted  int `json:"files_deleted"`
		StatusDeleted int `json:"status_deleted"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Warnf("Failed to decode cleanup response: %v", err)
		return 0, 0, nil
	}

	if result.FilesDeleted > 0 || result.StatusDeleted > 0 {
		log.Infof("Cleaned up %d expired file metadata records and %d expired generation status records",
			result.FilesDeleted, result.StatusDeleted)
	}

	return result.FilesDeleted, result.StatusDeleted, nil
}

// UpsertFieldTrackingBatch updates or creates field tracking entries for schema evolution
// fields is a map of field_name -> field_type
func (c *ControlAPIMetadataClient) UpsertFieldTrackingBatch(ctx context.Context, tenantID, datasetID string, fields map[string]string) error {
	if len(fields) == 0 {
		return nil
	}

	body := map[string]interface{}{
		"tenant_id":  tenantID,
		"dataset_id": datasetID,
		"fields":     fields,
	}

	resp, err := c.doRequest(ctx, "POST", "/api/v1/packer/field-tracking/batch", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to upsert field tracking: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	log.Debugf("Upserted %d fields for %s/%s", len(fields), tenantID, datasetID)
	return nil
}

// Close closes any resources (no-op for HTTP client)
func (c *ControlAPIMetadataClient) Close() error {
	// HTTP client doesn't need explicit closing
	return nil
}
