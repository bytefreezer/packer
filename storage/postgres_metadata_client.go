// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// PostgreSQLMetadataClient manages Parquet metadata in PostgreSQL
type PostgreSQLMetadataClient struct {
	db *sql.DB
}

// NewPostgreSQLMetadataClient creates a new PostgreSQL metadata client
func NewPostgreSQLMetadataClient(config PostgreSQLLockConfig) (*PostgreSQLMetadataClient, error) {
	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.Username, config.Password, config.Database, config.SSLMode)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)

	client := &PostgreSQLMetadataClient{
		db: db,
	}

	// Run migrations
	if err := client.runMigrations(); err != nil {
		log.Warnf("Failed to run metadata migrations (tables may already exist): %v", err)
	}

	return client, nil
}

// Close closes the database connection
func (pmc *PostgreSQLMetadataClient) Close() error {
	if pmc.db != nil {
		return pmc.db.Close()
	}
	return nil
}

// UpsertFileMetadata inserts or updates metadata for a Parquet file
func (pmc *PostgreSQLMetadataClient) UpsertFileMetadata(ctx context.Context, metadata *ParquetFileMetadata) error {
	// Set TTL to 30 days from now
	ttl := time.Now().Add(30 * 24 * time.Hour)

	query := `
		INSERT INTO packer_parquet_file_metadata (
			tenant_id, dataset_id, file_path, partition_path,
			file_size_bytes, row_count, created_at, last_modified,
			schema_json, column_stats, file_checksum, instance_id, ttl
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (tenant_id, dataset_id, file_path)
		DO UPDATE SET
			file_size_bytes = EXCLUDED.file_size_bytes,
			row_count = EXCLUDED.row_count,
			last_modified = EXCLUDED.last_modified,
			schema_json = EXCLUDED.schema_json,
			column_stats = EXCLUDED.column_stats,
			file_checksum = EXCLUDED.file_checksum,
			instance_id = EXCLUDED.instance_id,
			ttl = EXCLUDED.ttl,
			metadata_version = packer_parquet_file_metadata.metadata_version + 1
		RETURNING id, metadata_version
	`

	err := pmc.db.QueryRowContext(ctx, query,
		metadata.TenantID, metadata.DatasetID, metadata.FilePath, metadata.PartitionPath,
		metadata.FileSizeBytes, metadata.RowCount, metadata.CreatedAt, metadata.LastModified,
		metadata.SchemaJSON, metadata.ColumnStats, metadata.FileChecksum, metadata.InstanceID, ttl,
	).Scan(&metadata.ID, &metadata.Version)

	if err != nil {
		return fmt.Errorf("failed to upsert file metadata: %w", err)
	}

	log.Debugf("Upserted metadata for file %s (ID: %d, Version: %d)", metadata.FilePath, metadata.ID, metadata.Version)
	return nil
}

// GetFileMetadataByPartition retrieves all file metadata for a specific partition
func (pmc *PostgreSQLMetadataClient) GetFileMetadataByPartition(ctx context.Context, tenantID, datasetID, partitionPath string) ([]*ParquetFileMetadata, error) {
	query := `
		SELECT id, tenant_id, dataset_id, file_path, partition_path,
			   file_size_bytes, row_count, created_at, last_modified,
			   schema_json, column_stats, file_checksum, instance_id, metadata_version
		FROM packer_parquet_file_metadata
		WHERE tenant_id = $1 AND dataset_id = $2 AND partition_path = $3
		ORDER BY file_path
	`

	rows, err := pmc.db.QueryContext(ctx, query, tenantID, datasetID, partitionPath)
	if err != nil {
		return nil, fmt.Errorf("failed to query file metadata: %w", err)
	}
	defer rows.Close()

	var files []*ParquetFileMetadata
	for rows.Next() {
		file := &ParquetFileMetadata{}
		var instanceID sql.NullString
		err := rows.Scan(
			&file.ID, &file.TenantID, &file.DatasetID, &file.FilePath, &file.PartitionPath,
			&file.FileSizeBytes, &file.RowCount, &file.CreatedAt, &file.LastModified,
			&file.SchemaJSON, &file.ColumnStats, &file.FileChecksum, &instanceID, &file.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file metadata: %w", err)
		}
		if instanceID.Valid {
			file.InstanceID = instanceID.String
		}
		files = append(files, file)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating file metadata rows: %w", err)
	}

	return files, nil
}

// GetAllFileMetadata retrieves ALL file metadata for a tenant:dataset (across all partitions)
func (pmc *PostgreSQLMetadataClient) GetAllFileMetadata(ctx context.Context, tenantID, datasetID string) ([]*ParquetFileMetadata, error) {
	query := `
		SELECT id, tenant_id, dataset_id, file_path, partition_path,
			   file_size_bytes, row_count, created_at, last_modified,
			   schema_json, column_stats, file_checksum, instance_id, metadata_version
		FROM packer_parquet_file_metadata
		WHERE tenant_id = $1 AND dataset_id = $2
		ORDER BY partition_path, file_path
	`

	rows, err := pmc.db.QueryContext(ctx, query, tenantID, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to query all file metadata: %w", err)
	}
	defer rows.Close()

	var files []*ParquetFileMetadata
	for rows.Next() {
		file := &ParquetFileMetadata{}
		var instanceID sql.NullString
		err := rows.Scan(
			&file.ID, &file.TenantID, &file.DatasetID, &file.FilePath, &file.PartitionPath,
			&file.FileSizeBytes, &file.RowCount, &file.CreatedAt, &file.LastModified,
			&file.SchemaJSON, &file.ColumnStats, &file.FileChecksum, &instanceID, &file.Version,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan file metadata: %w", err)
		}
		if instanceID.Valid {
			file.InstanceID = instanceID.String
		}
		files = append(files, file)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating file metadata rows: %w", err)
	}

	return files, nil
}

// UpdateGenerationStatus updates the metadata generation status for a partition
func (pmc *PostgreSQLMetadataClient) UpdateGenerationStatus(ctx context.Context, status *MetadataGenerationStatus) error {
	// Set TTL to 30 days from now
	ttl := time.Now().Add(30 * 24 * time.Hour)

	query := `
		INSERT INTO packer_metadata_generation_status (
			tenant_id, dataset_id, partition_path, last_generated_at,
			file_count, total_rows, total_size_bytes, needs_regeneration,
			current_schema_hash, schema_version, ttl
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (tenant_id, dataset_id, partition_path)
		DO UPDATE SET
			last_generated_at = EXCLUDED.last_generated_at,
			file_count = EXCLUDED.file_count,
			total_rows = EXCLUDED.total_rows,
			total_size_bytes = EXCLUDED.total_size_bytes,
			needs_regeneration = EXCLUDED.needs_regeneration,
			current_schema_hash = EXCLUDED.current_schema_hash,
			schema_version = EXCLUDED.schema_version,
			ttl = EXCLUDED.ttl
	`

	_, err := pmc.db.ExecContext(ctx, query,
		status.TenantID, status.DatasetID, status.PartitionPath, status.LastGeneratedAt,
		status.FileCount, status.TotalRows, status.TotalSizeBytes, status.NeedsRegeneration,
		status.CurrentSchemaHash, status.SchemaVersion, ttl,
	)

	if err != nil {
		return fmt.Errorf("failed to update generation status: %w", err)
	}

	return nil
}

// GetGenerationStatus retrieves the metadata generation status for a partition
func (pmc *PostgreSQLMetadataClient) GetGenerationStatus(ctx context.Context, tenantID, datasetID, partitionPath string) (*MetadataGenerationStatus, error) {
	query := `
		SELECT tenant_id, dataset_id, partition_path, last_generated_at,
			   file_count, total_rows, total_size_bytes, needs_regeneration,
			   current_schema_hash, schema_version
		FROM packer_metadata_generation_status
		WHERE tenant_id = $1 AND dataset_id = $2 AND partition_path = $3
	`

	status := &MetadataGenerationStatus{}
	err := pmc.db.QueryRowContext(ctx, query, tenantID, datasetID, partitionPath).Scan(
		&status.TenantID, &status.DatasetID, &status.PartitionPath, &status.LastGeneratedAt,
		&status.FileCount, &status.TotalRows, &status.TotalSizeBytes, &status.NeedsRegeneration,
		&status.CurrentSchemaHash, &status.SchemaVersion,
	)

	if err == sql.ErrNoRows {
		// Return default status if not found
		return &MetadataGenerationStatus{
			TenantID:      tenantID,
			DatasetID:     datasetID,
			PartitionPath: partitionPath,
		}, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to get generation status: %w", err)
	}

	return status, nil
}

// GetMetadataSummary retrieves aggregated metadata for a partition
func (pmc *PostgreSQLMetadataClient) GetMetadataSummary(ctx context.Context, tenantID, datasetID, partitionPath string) (*ParquetMetadataSummary, error) {
	query := `
		SELECT tenant_id, dataset_id, partition_path, file_count, total_rows, total_size_bytes,
			   first_file_created, last_file_modified, metadata_last_updated
		FROM packer_parquet_metadata_summary
		WHERE tenant_id = $1 AND dataset_id = $2 AND partition_path = $3
	`

	summary := &ParquetMetadataSummary{}
	err := pmc.db.QueryRowContext(ctx, query, tenantID, datasetID, partitionPath).Scan(
		&summary.TenantID, &summary.DatasetID, &summary.PartitionPath,
		&summary.FileCount, &summary.TotalRows, &summary.TotalSizeBytes,
		&summary.FirstFileCreated, &summary.LastFileModified, &summary.MetadataLastUpdated,
	)

	if err == sql.ErrNoRows {
		return nil, nil // No data found
	} else if err != nil {
		return nil, fmt.Errorf("failed to get metadata summary: %w", err)
	}

	return summary, nil
}

// DeleteFileMetadata removes metadata for a specific file (for cleanup)
func (pmc *PostgreSQLMetadataClient) DeleteFileMetadata(ctx context.Context, tenantID, datasetID, filePath string) error {
	query := `DELETE FROM packer_parquet_file_metadata WHERE tenant_id = $1 AND dataset_id = $2 AND file_path = $3`
	_, err := pmc.db.ExecContext(ctx, query, tenantID, datasetID, filePath)
	if err != nil {
		return fmt.Errorf("failed to delete file metadata: %w", err)
	}
	return nil
}

// CleanupOrphanedMetadata removes metadata for files that no longer exist in S3
func (pmc *PostgreSQLMetadataClient) CleanupOrphanedMetadata(ctx context.Context, tenantID, datasetID string, existingFiles []string) (int, error) {
	if len(existingFiles) == 0 {
		return 0, nil
	}

	// Build a safe parameterized query for the IN clause
	// This is safe from SQL injection because we only use parameter placeholders
	inClausePlaceholders := make([]string, len(existingFiles))
	args := make([]interface{}, len(existingFiles)+2)

	// First two parameters are tenant_id and dataset_id
	args[0] = tenantID
	args[1] = datasetID

	// Build placeholders for the IN clause
	for i, file := range existingFiles {
		inClausePlaceholders[i] = fmt.Sprintf("$%d", i+3) // Start from $3 since $1,$2 are used
		args[i+2] = file
	}

	// Build the complete query with safe parameter placeholders
	inClause := strings.Join(inClausePlaceholders, ",")
	// #nosec G202 - Safe: inClause contains only validated parameter placeholders like $3,$4,$5
	query := "DELETE FROM packer_parquet_file_metadata WHERE tenant_id = $1 AND dataset_id = $2 AND file_path NOT IN (" + inClause + ")"

	result, err := pmc.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup orphaned metadata: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	return int(rowsAffected), nil
}

// CleanupExpiredMetadata removes metadata records that have exceeded their TTL
// This prevents unbounded table growth by removing expired metadata
func (pmc *PostgreSQLMetadataClient) CleanupExpiredMetadata(ctx context.Context) (int, int, error) {
	// Cleanup file metadata
	fileQuery := `DELETE FROM packer_parquet_file_metadata WHERE ttl < NOW()`
	fileResult, err := pmc.db.ExecContext(ctx, fileQuery)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to cleanup expired file metadata: %w", err)
	}

	filesDeleted, err := fileResult.RowsAffected()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get file metadata rows affected: %w", err)
	}

	// Cleanup generation status
	statusQuery := `DELETE FROM packer_metadata_generation_status WHERE ttl < NOW()`
	statusResult, err := pmc.db.ExecContext(ctx, statusQuery)
	if err != nil {
		return int(filesDeleted), 0, fmt.Errorf("failed to cleanup expired generation status: %w", err)
	}

	statusDeleted, err := statusResult.RowsAffected()
	if err != nil {
		return int(filesDeleted), 0, fmt.Errorf("failed to get generation status rows affected: %w", err)
	}

	if filesDeleted > 0 || statusDeleted > 0 {
		log.Infof("Cleaned up %d expired file metadata records and %d expired generation status records", filesDeleted, statusDeleted)
	}

	return int(filesDeleted), int(statusDeleted), nil
}

// runMigrations creates the necessary tables if they don't exist
func (pmc *PostgreSQLMetadataClient) runMigrations() error {
	// This is a simplified migration - in production you'd use a proper migration tool
	migrationSQL := `
		CREATE TABLE IF NOT EXISTS packer_parquet_file_metadata (
			id SERIAL PRIMARY KEY,
			tenant_id VARCHAR(255) NOT NULL,
			dataset_id VARCHAR(255) NOT NULL,
			file_path VARCHAR(1000) NOT NULL,
			partition_path VARCHAR(500),
			file_size_bytes BIGINT NOT NULL,
			row_count BIGINT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
			last_modified TIMESTAMP WITH TIME ZONE NOT NULL,
			schema_json JSONB NOT NULL,
			column_stats JSONB,
			file_checksum VARCHAR(64),
			metadata_version INTEGER DEFAULT 1,
			inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			UNIQUE(tenant_id, dataset_id, file_path)
		);

		CREATE INDEX IF NOT EXISTS idx_parquet_metadata_tenant_dataset
			ON packer_parquet_file_metadata(tenant_id, dataset_id);
		CREATE INDEX IF NOT EXISTS idx_parquet_metadata_partition
			ON packer_parquet_file_metadata(tenant_id, dataset_id, partition_path);
	`

	_, err := pmc.db.Exec(migrationSQL)
	return err
}

// CalculateSchemaHash calculates SHA256 hash of schema for change detection
func CalculateSchemaHash(schemaJSON string) string {
	hash := sha256.Sum256([]byte(schemaJSON))
	return fmt.Sprintf("%x", hash)
}

// MergeColumnStats merges column statistics from multiple files
func MergeColumnStats(stats1, stats2 string) (string, error) {
	if stats1 == "" {
		return stats2, nil
	}
	if stats2 == "" {
		return stats1, nil
	}

	var map1, map2 map[string]interface{}
	if err := sonic.Unmarshal([]byte(stats1), &map1); err != nil {
		return "", fmt.Errorf("failed to unmarshal stats1: %w", err)
	}
	if err := sonic.Unmarshal([]byte(stats2), &map2); err != nil {
		return "", fmt.Errorf("failed to unmarshal stats2: %w", err)
	}

	// Simple merge - in production you'd do proper min/max/count aggregation
	for k, v := range map2 {
		map1[k] = v
	}

	result, err := sonic.Marshal(map1)
	if err != nil {
		return "", fmt.Errorf("failed to marshal merged stats: %w", err)
	}

	return string(result), nil
}
