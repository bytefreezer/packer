// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/bytedance/sonic"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/domain"

	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/file"
)

// Compiled regex for performance - avoid regexp.MatchString in loops
var hivePartitionRegex = regexp.MustCompile(`\w+=\w+`)

// ParquetMetadataExtractor extracts metadata from Parquet files
type ParquetMetadataExtractor struct {
	s3DestinationManager *S3DestinationManager
}

// ParquetSchema represents a simplified Parquet schema
type ParquetSchema struct {
	Type     string            `json:"type"`
	Fields   []ParquetField    `json:"fields"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ParquetField represents a field in the Parquet schema
type ParquetField struct {
	Name     string            `json:"name"`
	Type     string            `json:"type"`
	Nullable bool              `json:"nullable"`
	Children []ParquetField    `json:"children,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ParquetColumnStats represents statistics for a Parquet column
type ParquetColumnStats struct {
	ColumnName    string      `json:"column_name"`
	MinValue      interface{} `json:"min_value,omitempty"`
	MaxValue      interface{} `json:"max_value,omitempty"`
	NullCount     int64       `json:"null_count"`
	DistinctCount int64       `json:"distinct_count,omitempty"`
	DataType      string      `json:"data_type"`
}

// NewParquetMetadataExtractor creates a new metadata extractor
func NewParquetMetadataExtractor(s3DestinationManager *S3DestinationManager) *ParquetMetadataExtractor {
	return &ParquetMetadataExtractor{
		s3DestinationManager: s3DestinationManager,
	}
}

// ExtractFileMetadata extracts metadata from a single Parquet file
func (pme *ParquetMetadataExtractor) ExtractFileMetadata(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, filePath, instanceID string) (*ParquetFileMetadata, error) {
	log.Debugf("Extracting metadata from Parquet file: %s", filePath)

	// Get S3 client for this tenant:dataset
	s3Client, err := pme.s3DestinationManager.GetS3Client(tenantID, datasetID, s3Dest)
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 client: %w", err)
	}

	// Get file info first
	fileInfo, err := s3Client.GetObjectInfo(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get file info for %s: %w", filePath, err)
	}

	// Extract partition path
	partitionPath := pme.extractPartitionPath(filePath)

	// Download and read the Parquet file to extract real metadata
	reader, err := s3Client.GetObjectReader(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open Parquet file %s: %w", filePath, err)
	}
	defer reader.Close()

	// Read all data into memory for Apache Arrow Parquet (it needs io.ReaderAt)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read Parquet file data: %w", err)
	}

	// Create Parquet file reader using bytes.NewReader (implements io.ReaderAt)
	parquetFile, err := file.NewParquetReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to open Parquet file: %w", err)
	}
	defer parquetFile.Close()

	// Extract actual schema from Parquet file
	schema, err := pme.extractSchemaFromParquetFile(parquetFile)
	if err != nil {
		return nil, fmt.Errorf("failed to extract schema: %w", err)
	}

	schemaJSON, err := sonic.Marshal(schema)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal schema: %w", err)
	}

	// Extract actual column statistics from Parquet file
	columnStats, err := pme.extractColumnStatsFromParquetFile(parquetFile)
	if err != nil {
		log.Warnf("Failed to extract column stats: %v", err)
		columnStats = []ParquetColumnStats{} // Use empty stats on error
	}

	columnStatsJSON, err := sonic.Marshal(columnStats)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal column stats: %w", err)
	}

	// Get actual row count from Parquet file
	actualRowCount := parquetFile.NumRows()

	// Calculate file checksum
	checksum := pme.calculateFileChecksum(filePath, fileInfo.Size, fileInfo.LastModified)

	metadata := &ParquetFileMetadata{
		TenantID:      tenantID,
		DatasetID:     datasetID,
		FilePath:      filePath,
		PartitionPath: partitionPath,
		FileSizeBytes: fileInfo.Size,
		RowCount:      actualRowCount,
		CreatedAt:     time.Now(),
		LastModified:  fileInfo.LastModified,
		SchemaJSON:    string(schemaJSON),
		ColumnStats:   string(columnStatsJSON),
		FileChecksum:  checksum,
		InstanceID:    instanceID,
	}

	log.Debugf("Extracted metadata for %s: %d bytes, %d rows, %d columns, partition: %s",
		filePath, metadata.FileSizeBytes, metadata.RowCount, len(schema.Fields), metadata.PartitionPath)

	return metadata, nil
}

// ExtractFileMetadataWithParquetReader extracts metadata using actual Parquet reading
// TODO: Implement this with a proper Parquet library
func (pme *ParquetMetadataExtractor) ExtractFileMetadataWithParquetReader(ctx context.Context, tenantID, datasetID string, s3Dest *domain.S3Destination, filePath, instanceID string) (*ParquetFileMetadata, error) {
	// This would be the production implementation using a proper Parquet library
	// Example with segmentio/parquet-go:

	/*
		// Get S3 client
		s3Client, err := pme.s3DestinationManager.GetS3Client(tenantID, datasetID, s3Dest)
		if err != nil {
			return nil, fmt.Errorf("failed to get S3 client: %w", err)
		}

		// Open the Parquet file
		reader, err := s3Client.GetObjectReader(ctx, filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to open Parquet file: %w", err)
		}
		defer reader.Close()

		// Create Parquet reader
		parquetReader, err := parquet.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to create Parquet reader: %w", err)
		}

		// Extract schema
		schema := parquetReader.Schema()

		// Extract row count
		numRows := parquetReader.NumRows()

		// Extract column statistics
		columnStats := make([]ParquetColumnStats, 0)
		for _, field := range schema.Fields() {
			stats := ParquetColumnStats{
				ColumnName: field.Name(),
				DataType:   field.Type().String(),
				NullCount:  0, // Would extract from Parquet metadata
			}
			columnStats = append(columnStats, stats)
		}

		// Continue with rest of implementation...
	*/

	// For now, fall back to the simplified version
	return pme.ExtractFileMetadata(ctx, tenantID, datasetID, s3Dest, filePath, instanceID)
}

// extractPartitionPath extracts the partition path from a file path
func (pme *ParquetMetadataExtractor) extractPartitionPath(filePath string) string {
	// Extract partition from path like: tenant1/dataset1/year=2024/month=01/day=15/file.parquet
	// Should return: year=2024/month=01/day=15

	dir := filepath.Dir(filePath)
	parts := strings.Split(dir, "/")

	var partitionParts []string
	for _, part := range parts {
		// Look for Hive-style partitions (key=value) using compiled regex
		if hivePartitionRegex.MatchString(part) {
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

// calculateFileChecksum creates a secure checksum based on file attributes
func (pme *ParquetMetadataExtractor) calculateFileChecksum(filePath string, size int64, modTime time.Time) string {
	// Secure checksum based on path, size, and modification time using SHA256
	data := fmt.Sprintf("%s|%d|%d", filePath, size, modTime.Unix())
	hash := sha256.Sum256([]byte(data))
	return fmt.Sprintf("%x", hash)
}

// GenerateMetadataFile generates a Parquet metadata file from database metadata
func (pme *ParquetMetadataExtractor) GenerateMetadataFile(files []*ParquetFileMetadata, includeStatistics bool) ([]byte, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files provided for metadata generation")
	}

	// Calculate totals
	var totalRows int64
	var totalSize int64
	fileList := make([]map[string]interface{}, 0, len(files))

	for _, file := range files {
		totalRows += file.RowCount
		totalSize += file.FileSizeBytes

		fileEntry := map[string]interface{}{
			"path":      file.FilePath,
			"size":      file.FileSizeBytes,
			"row_count": file.RowCount,
		}

		if includeStatistics {
			// Parse and include column statistics
			var columnStats []ParquetColumnStats
			if err := sonic.Unmarshal([]byte(file.ColumnStats), &columnStats); err == nil {
				fileEntry["column_stats"] = columnStats
			}
		}

		fileList = append(fileList, fileEntry)
	}

	// Parse schema from the first file (assume all files have compatible schemas)
	var schema ParquetSchema
	if err := sonic.Unmarshal([]byte(files[0].SchemaJSON), &schema); err != nil {
		return nil, fmt.Errorf("failed to parse schema: %w", err)
	}

	// Create metadata structure
	metadata := map[string]interface{}{
		"version":            1,
		"schema":             schema,
		"num_rows":           totalRows,
		"total_size_bytes":   totalSize,
		"file_count":         len(files),
		"files":              fileList,
		"generated_by":       "bytefreezer-packer",
		"generated_at":       time.Now().UTC().Format(time.RFC3339),
		"include_statistics": includeStatistics,
	}

	// Add partition information if available
	if files[0].PartitionPath != "" {
		metadata["partition_path"] = files[0].PartitionPath
	}

	// Marshal to JSON
	result, err := sonic.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	return result, nil
}

// ValidateSchemaCompatibility checks if schemas from multiple files are compatible
func (pme *ParquetMetadataExtractor) ValidateSchemaCompatibility(schemas []string) error {
	if len(schemas) <= 1 {
		return nil // Single schema or no schemas are always compatible
	}

	var baseSchema ParquetSchema
	if err := sonic.Unmarshal([]byte(schemas[0]), &baseSchema); err != nil {
		return fmt.Errorf("failed to parse base schema: %w", err)
	}

	for i, schemaJSON := range schemas[1:] {
		var currentSchema ParquetSchema
		if err := sonic.Unmarshal([]byte(schemaJSON), &currentSchema); err != nil {
			return fmt.Errorf("failed to parse schema %d: %w", i+1, err)
		}

		// Simple compatibility check - compare field names and types
		if len(baseSchema.Fields) != len(currentSchema.Fields) {
			return fmt.Errorf("schema %d has different number of fields", i+1)
		}

		for j, baseField := range baseSchema.Fields {
			currentField := currentSchema.Fields[j]
			if baseField.Name != currentField.Name {
				return fmt.Errorf("schema %d field %d name mismatch: %s vs %s",
					i+1, j, baseField.Name, currentField.Name)
			}
			if baseField.Type != currentField.Type {
				log.Warnf("Schema %d field %s type mismatch: %s vs %s (might be compatible)",
					i+1, baseField.Name, baseField.Type, currentField.Type)
			}
		}
	}

	return nil
}

// MergeSchemas merges multiple schemas into a single compatible schema
// This creates a union schema that includes all fields from all input schemas
// Duplicates are detected by normalizing field names (lowercase, underscores)
func (pme *ParquetMetadataExtractor) MergeSchemas(schemas []string) (*ParquetSchema, error) {
	if len(schemas) == 0 {
		return nil, fmt.Errorf("no schemas to merge")
	}

	// Parse base schema
	var baseSchema ParquetSchema
	if err := sonic.Unmarshal([]byte(schemas[0]), &baseSchema); err != nil {
		return nil, fmt.Errorf("failed to parse base schema: %w", err)
	}

	if len(schemas) == 1 {
		// Still deduplicate single schema
		baseSchema.Fields = pme.deduplicateFields(baseSchema.Fields)
		return &baseSchema, nil
	}

	// Create maps for deduplication:
	// - mergedFields: exact field name -> field
	// - normalizedFields: normalized name -> original field name (for duplicate detection)
	mergedFields := make(map[string]*ParquetField)
	normalizedFields := make(map[string]string) // normalized -> original name

	// Track field order - use first occurrence order
	fieldOrder := make([]string, 0, len(baseSchema.Fields))

	// Process base schema fields
	for i := range baseSchema.Fields {
		field := &baseSchema.Fields[i]
		normalized := pme.normalizeFieldName(field.Name)

		if existingName, exists := normalizedFields[normalized]; exists {
			// Duplicate detected - skip this field
			log.Warnf("Skipping duplicate field %s (normalized: %s, already have: %s)",
				field.Name, normalized, existingName)
			continue
		}

		mergedFields[field.Name] = field
		normalizedFields[normalized] = field.Name
		fieldOrder = append(fieldOrder, field.Name)
	}

	// Merge each additional schema
	for i, schemaJSON := range schemas[1:] {
		var currentSchema ParquetSchema
		if err := sonic.Unmarshal([]byte(schemaJSON), &currentSchema); err != nil {
			log.Warnf("Failed to parse schema %d, skipping: %v", i+1, err)
			continue
		}

		// Process each field in current schema
		for _, currentField := range currentSchema.Fields {
			normalized := pme.normalizeFieldName(currentField.Name)

			// Check for duplicate by normalized name
			if existingName, exists := normalizedFields[normalized]; exists {
				// Duplicate detected
				if existingName != currentField.Name {
					log.Debugf("Skipping duplicate field %s (normalized: %s, already have: %s)",
						currentField.Name, normalized, existingName)
				}
				// Check type compatibility with existing field
				if existingField, ok := mergedFields[existingName]; ok {
					if existingField.Type != currentField.Type {
						log.Warnf("Field %s has different types: %s vs %s (using %s)",
							existingName, existingField.Type, currentField.Type, existingField.Type)
					}
				}
				continue
			}

			// New field - add it to merged schema
			log.Debugf("Adding new field %s from schema %d (type: %s)",
				currentField.Name, i+1, currentField.Type)
			newField := currentField
			mergedFields[currentField.Name] = &newField
			normalizedFields[normalized] = currentField.Name
			fieldOrder = append(fieldOrder, currentField.Name)
		}
	}

	// Build final schema with fields in order
	finalFields := make([]ParquetField, 0, len(fieldOrder))
	for _, fieldName := range fieldOrder {
		if field, ok := mergedFields[fieldName]; ok {
			finalFields = append(finalFields, *field)
		}
	}

	mergedSchema := &ParquetSchema{
		Type:     baseSchema.Type,
		Fields:   finalFields,
		Metadata: baseSchema.Metadata,
	}

	if mergedSchema.Metadata == nil {
		mergedSchema.Metadata = make(map[string]string)
	}
	mergedSchema.Metadata["schema_merged"] = "true"
	mergedSchema.Metadata["source_schema_count"] = fmt.Sprintf("%d", len(schemas))

	log.Infof("Merged %d schemas into unified schema with %d fields", len(schemas), len(finalFields))

	return mergedSchema, nil
}

// normalizeFieldName normalizes field names for duplicate detection
// Converts to lowercase and replaces hyphens with underscores
func (pme *ParquetMetadataExtractor) normalizeFieldName(name string) string {
	normalized := strings.ToLower(name)
	normalized = strings.ReplaceAll(normalized, "-", "_")
	// Remove trailing numbers that might indicate duplicates (e.g., BfTs_1 -> bfts)
	for strings.HasSuffix(normalized, "_1") || strings.HasSuffix(normalized, "_2") {
		normalized = normalized[:len(normalized)-2]
	}
	return normalized
}

// deduplicateFields removes duplicate fields from a field list
func (pme *ParquetMetadataExtractor) deduplicateFields(fields []ParquetField) []ParquetField {
	seen := make(map[string]bool)
	result := make([]ParquetField, 0, len(fields))

	for _, field := range fields {
		normalized := pme.normalizeFieldName(field.Name)
		if !seen[normalized] {
			seen[normalized] = true
			result = append(result, field)
		} else {
			log.Debugf("Removed duplicate field: %s (normalized: %s)", field.Name, normalized)
		}
	}

	return result
}

// extractSchemaFromParquetFile extracts the actual schema from a Parquet file
func (pme *ParquetMetadataExtractor) extractSchemaFromParquetFile(reader *file.Reader) (*ParquetSchema, error) {
	schema := &ParquetSchema{
		Type:     "group",
		Fields:   make([]ParquetField, 0),
		Metadata: map[string]string{"created_by": "bytefreezer-packer"},
	}

	// Get column descriptors which contain the actual field information
	// Only process first row group to get schema
	if reader.NumRowGroups() > 0 {
		rowGroup := reader.RowGroup(0)

		for j := 0; j < rowGroup.NumColumns(); j++ {
			columnChunk, err := rowGroup.MetaData().ColumnChunk(j)
			if err != nil {
				log.Warnf("Failed to get column chunk %d: %v", j, err)
				continue
			}

			// Extract field name from path (last component)
			pathInSchema := columnChunk.PathInSchema()
			if len(pathInSchema) > 0 {
				fieldName := pathInSchema[len(pathInSchema)-1]

				// Get physical type
				fieldType := pme.convertPhysicalType(columnChunk.Type())

				parquetField := ParquetField{
					Name:     fieldName,
					Type:     fieldType,
					Nullable: true, // Conservative assumption
				}
				schema.Fields = append(schema.Fields, parquetField)
			}
		}
	}

	return schema, nil
}

// extractColumnStatsFromParquetFile extracts column statistics from Parquet file
func (pme *ParquetMetadataExtractor) extractColumnStatsFromParquetFile(reader *file.Reader) ([]ParquetColumnStats, error) {
	stats := make([]ParquetColumnStats, 0)

	// Get column stats from first row group
	if reader.NumRowGroups() > 0 {
		rowGroup := reader.RowGroup(0)

		for j := 0; j < rowGroup.NumColumns(); j++ {
			columnChunk, err := rowGroup.MetaData().ColumnChunk(j)
			if err != nil {
				log.Warnf("Failed to get column chunk %d: %v", j, err)
				continue
			}

			// Extract field name from path (last component)
			pathInSchema := columnChunk.PathInSchema()
			if len(pathInSchema) > 0 {
				fieldName := pathInSchema[len(pathInSchema)-1]

				stats = append(stats, ParquetColumnStats{
					ColumnName: fieldName,
					DataType:   pme.convertPhysicalType(columnChunk.Type()),
					NullCount:  0, // We'd need to read row group stats to get actual null counts
				})
			}
		}
	}

	return stats, nil
}

// convertPhysicalType converts Parquet physical type to string representation
func (pme *ParquetMetadataExtractor) convertPhysicalType(physicalType parquet.Type) string {
	switch physicalType {
	case parquet.Types.Boolean:
		return "boolean"
	case parquet.Types.Int32:
		return "int32"
	case parquet.Types.Int64:
		return "int64"
	case parquet.Types.Int96:
		return "timestamp"
	case parquet.Types.Float:
		return "float"
	case parquet.Types.Double:
		return "double"
	case parquet.Types.ByteArray:
		return "string" // Most ByteArray fields are strings (UTF8)
	case parquet.Types.FixedLenByteArray:
		return "fixed_len_byte_array"
	default:
		return "unknown"
	}
}
