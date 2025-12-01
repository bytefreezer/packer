package services

import (
	"bufio"
	"compress/gzip"
	"context"
	"github.com/bytedance/sonic"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/compress"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/bytefreezer/packer/domain"
	"github.com/bytefreezer/packer/storage"
	"github.com/bytefreezer/goodies/log"
)

type ParquetConverter struct {
	memPool memory.Allocator
}

type ConversionResult struct {
	InputFile        string
	OutputFile       string
	RecordsCount     int64
	RecordsProcessed int64
	TotalSize        int64
	FileSize         int64
	ConvertTime      time.Duration
	Error            error
}

type ParquetConversionOptions struct {
	Compression          string
	MaxFileSizeMB        int
	MemoryBufferMB       int
	CheckpointRecords    int
	AtomicUpload         bool
	S3SourceClient       *storage.S3SourceClient
	S3DestinationManager *storage.S3DestinationManager
	Dataset              *domain.Dataset
	ActivityReporter     *ActivityReporter
	OperationID          string
}

func NewParquetConverter() *ParquetConverter {
	return &ParquetConverter{
		memPool: memory.NewGoAllocator(),
	}
}

// ConvertNDJSONToParquet converts an NDJSON file to Parquet format
func (pc *ParquetConverter) ConvertNDJSONToParquet(inputFilePath string) (*ConversionResult, error) {
	startTime := time.Now()

	result := &ConversionResult{
		InputFile: inputFilePath,
	}

	// Generate output file path
	outputFilePath := strings.TrimSuffix(inputFilePath, filepath.Ext(inputFilePath)) + ".parquet"
	result.OutputFile = outputFilePath

	log.Infof("Starting NDJSON to Parquet conversion: %s -> %s", inputFilePath, outputFilePath)

	// Step 1: Analyze schema from NDJSON
	schema, recordCount, err := pc.analyzeNDJSONSchema(inputFilePath)
	if err != nil {
		result.Error = fmt.Errorf("failed to analyze NDJSON schema: %w", err)
		result.ConvertTime = time.Since(startTime)
		return result, result.Error
	}

	result.RecordsCount = recordCount
	log.Debugf("Analyzed schema with %d fields, %d records", len(schema.Fields()), recordCount)

	// Step 2: Convert to Parquet
	err = pc.convertToParquet(inputFilePath, outputFilePath, schema)
	if err != nil {
		result.Error = fmt.Errorf("failed to convert to parquet: %w", err)
		result.ConvertTime = time.Since(startTime)
		return result, result.Error
	}

	// Get file size
	if stat, err := os.Stat(outputFilePath); err == nil {
		result.FileSize = stat.Size()
	}

	result.ConvertTime = time.Since(startTime)
	log.Infof("Successfully converted %s to %s (%d records, %d bytes, took %v)",
		inputFilePath, outputFilePath, result.RecordsCount, result.FileSize, result.ConvertTime)

	return result, nil
}

// StreamNDJSONToParquet streams NDJSON files directly to Parquet without intermediate files
func (pc *ParquetConverter) StreamNDJSONToParquet(ctx context.Context, files []storage.S3Object, s3Key string, options *ParquetConversionOptions) (string, *ConversionResult, error) {
	startTime := time.Now()

	result := &ConversionResult{
		InputFile: fmt.Sprintf("streaming-%d-files", len(files)),
	}

	if len(files) == 0 {
		return "", result, fmt.Errorf("no files provided for streaming conversion")
	}

	log.Infof("Starting streaming conversion of %d files to S3 key: %s", len(files), s3Key)

	// Step 1: Analyze schema from first few files
	schema, err := pc.analyzeSchemaFromS3Files(ctx, files[:min(len(files), 3)], options.S3SourceClient)
	if err != nil {
		result.Error = fmt.Errorf("failed to analyze schema: %w", err)
		return "", result, result.Error
	}

	// Step 2: Get S3 destination client
	destClient, err := options.S3DestinationManager.GetS3Client(options.Dataset.TenantID, options.Dataset.ID, options.Dataset.S3Destination)
	if err != nil {
		result.Error = fmt.Errorf("failed to get S3 destination client: %w", err)
		return "", result, result.Error
	}

	// Step 3: Create temporary file for atomic upload if enabled
	finalKey := s3Key
	uploadKey := s3Key
	if options.AtomicUpload {
		uploadKey = s3Key + ".tmp"
	}

	// Step 4: Create S3 writer for direct upload
	s3Writer := &S3Writer{
		client: destClient.GetRawS3Client(),
		bucket: options.Dataset.S3Destination.BucketName,
		key:    uploadKey,
	}

	// Step 5: Create Parquet writer
	compression := compress.Codecs.Uncompressed
	if options.Compression == "zstd" {
		compression = compress.Codecs.Zstd
	} else if options.Compression == "snappy" {
		compression = compress.Codecs.Snappy
	}

	parquetProps := parquet.NewWriterProperties(parquet.WithCompression(compression))
	arrowProps := pqarrow.DefaultWriterProps()

	writer, err := pqarrow.NewFileWriter(schema, s3Writer, parquetProps, arrowProps)
	if err != nil {
		result.Error = fmt.Errorf("failed to create Parquet writer: %w", err)
		return "", result, result.Error
	}

	// Step 6: Stream process all files
	recordsProcessed := int64(0)
	totalSize := int64(0)
	batch := make([]map[string]interface{}, 0, options.CheckpointRecords)
	lastProgressUpdate := time.Now()

	for i, file := range files {
		select {
		case <-ctx.Done():
			writer.Close()
			return "", result, ctx.Err()
		default:
		}

		log.Debugf("Streaming file %d/%d: %s", i+1, len(files), file.Key)

		fileRecords, fileSize, err := pc.processS3FileStreaming(ctx, file, options.S3SourceClient, &batch, schema, writer, options.CheckpointRecords)
		if err != nil {
			log.Warnf("Failed to process file %s: %v", file.Key, err)
			continue
		}

		recordsProcessed += fileRecords
		totalSize += fileSize

		// Report progress every 100 files or 30 seconds
		if options.ActivityReporter != nil && options.OperationID != "" {
			shouldReport := (i+1)%100 == 0 || time.Since(lastProgressUpdate) > 30*time.Second
			if shouldReport {
				options.ActivityReporter.UpdateProgress(
					options.OperationID,
					int64(i+1),
					fmt.Sprintf("Converting %d/%d files", i+1, len(files)),
				)
				options.ActivityReporter.UpdateMetrics(options.OperationID, totalSize, s3Writer.BytesWritten(), recordsProcessed)
				lastProgressUpdate = time.Now()
			}
		}
	}

	// Step 7: Write final batch if any records remain
	if len(batch) > 0 {
		if err := pc.writeBatch(writer, schema, batch); err != nil {
			writer.Close()
			result.Error = fmt.Errorf("failed to write final batch: %w", err)
			return "", result, result.Error
		}
	}

	// Step 8: Close writer and capture output size
	// Get bytes written BEFORE closing (buffer is available before close)
	outputSize := s3Writer.BytesWritten()

	if err := writer.Close(); err != nil {
		result.Error = fmt.Errorf("failed to close Parquet writer: %w", err)
		return "", result, result.Error
	}

	// Step 9: Atomic rename if enabled
	if options.AtomicUpload {
		if err := s3Writer.AtomicRename(finalKey); err != nil {
			result.Error = fmt.Errorf("failed to perform atomic rename: %w", err)
			return "", result, result.Error
		}
	}

	result.RecordsProcessed = recordsProcessed
	result.TotalSize = totalSize
	result.FileSize = outputSize
	result.ConvertTime = time.Since(startTime)
	result.OutputFile = finalKey

	log.Infof("Streaming conversion completed: %d records, input: %d bytes, output: %d bytes, time: %v",
		recordsProcessed, totalSize, outputSize, result.ConvertTime)
	return finalKey, result, nil
}

// analyzeNDJSONSchema reads the NDJSON file and infers the Arrow schema
func (pc *ParquetConverter) analyzeNDJSONSchema(filePath string) (*arrow.Schema, int64, error) {
	file, err := os.Open(filePath) // #nosec G304 - filePath is controlled by application logic
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	fieldTypes := make(map[string]arrow.DataType)
	var recordCount int64

	// Sample first few records to infer schema
	sampleCount := 0
	maxSamples := 1000 // Analyze first 1000 records for schema

	for scanner.Scan() && sampleCount < maxSamples {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var jsonObj map[string]interface{}
		if err := sonic.Unmarshal([]byte(line), &jsonObj); err != nil {
			log.Warnf("Skipping invalid JSON line: %s", err)
			continue
		}

		// Infer field types
		for key, value := range jsonObj {
			inferredType := pc.inferArrowType(value)
			if existingType, exists := fieldTypes[key]; exists {
				// Try to reconcile types if they differ
				fieldTypes[key] = pc.reconcileTypes(existingType, inferredType)
			} else {
				fieldTypes[key] = inferredType
			}
		}

		sampleCount++
	}

	// Count total records
	if _, err := file.Seek(0, 0); err != nil {
		return nil, 0, err // Reset file pointer
	}
	scanner = bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			recordCount++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, 0, fmt.Errorf("error reading file: %w", err)
	}

	// Build Arrow schema
	fields := make([]arrow.Field, 0, len(fieldTypes))
	for name, dataType := range fieldTypes {
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     dataType,
			Nullable: true, // NDJSON fields are typically nullable
		})
	}

	schema := arrow.NewSchema(fields, nil)
	return schema, recordCount, nil
}

// inferArrowType infers Arrow data type from Go interface{}
func (pc *ParquetConverter) inferArrowType(value interface{}) arrow.DataType {
	if value == nil {
		return &arrow.StringType{} // Default to string for null values
	}

	switch v := value.(type) {
	case bool:
		return &arrow.BooleanType{}
	case int, int8, int16, int32:
		return &arrow.Int32Type{}
	case int64:
		return &arrow.Int64Type{}
	case float32:
		return &arrow.Float32Type{}
	case float64:
		return &arrow.Float64Type{}
	case string:
		// Try to parse as timestamp
		if pc.isTimestamp(v) {
			return &arrow.TimestampType{Unit: arrow.Millisecond}
		}
		return &arrow.StringType{}
	case []interface{}:
		// For arrays, use the first element to infer type
		if len(v) > 0 {
			elemType := pc.inferArrowType(v[0])
			return arrow.ListOf(elemType)
		}
		return arrow.ListOf(&arrow.StringType{})
	case map[string]interface{}:
		// For nested objects, convert to JSON string
		return &arrow.StringType{}
	default:
		// Fallback to string
		return &arrow.StringType{}
	}
}

// reconcileTypes attempts to find a common type between two Arrow types
func (pc *ParquetConverter) reconcileTypes(type1, type2 arrow.DataType) arrow.DataType {
	if type1.ID() == type2.ID() {
		return type1
	}

	// If one is string, use string (most permissive)
	if type1.ID() == arrow.STRING || type2.ID() == arrow.STRING {
		return &arrow.StringType{}
	}

	// Numeric type reconciliation
	if pc.isNumericType(type1) && pc.isNumericType(type2) {
		return pc.promoteNumericType(type1, type2)
	}

	// Default to string for incompatible types
	return &arrow.StringType{}
}

// isNumericType checks if a type is numeric
func (pc *ParquetConverter) isNumericType(t arrow.DataType) bool {
	switch t.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT32, arrow.FLOAT64:
		return true
	}
	return false
}

// promoteNumericType promotes to the more general numeric type
func (pc *ParquetConverter) promoteNumericType(type1, type2 arrow.DataType) arrow.DataType {
	// Simple promotion rules
	if type1.ID() == arrow.FLOAT64 || type2.ID() == arrow.FLOAT64 {
		return &arrow.Float64Type{}
	}
	if type1.ID() == arrow.FLOAT32 || type2.ID() == arrow.FLOAT32 {
		return &arrow.Float32Type{}
	}
	if type1.ID() == arrow.INT64 || type2.ID() == arrow.INT64 {
		return &arrow.Int64Type{}
	}
	return &arrow.Int32Type{}
}

// isTimestamp attempts to detect timestamp strings
func (pc *ParquetConverter) isTimestamp(s string) bool {
	// Common timestamp formats
	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05.000Z",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, format := range formats {
		if _, err := time.Parse(format, s); err == nil {
			return true
		}
	}
	return false
}

// convertToParquet performs the actual conversion
func (pc *ParquetConverter) convertToParquet(inputPath, outputPath string, schema *arrow.Schema) error {
	// Open input file
	inputFile, err := os.Open(inputPath) // #nosec G304 - inputPath is controlled by application logic
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inputFile.Close()

	// Create output file
	outputFile, err := os.Create(outputPath) // #nosec G304 - outputPath is controlled by application logic
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	// Create Parquet writer with optimized properties for data lakes
	// Target: 256 MiB files (128-512 MiB range)
	// Row group size: 128 MiB target
	// Codec: ZSTD for better compression
	props := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithDictionaryDefault(true),
	)

	log.Debug("Using ZSTD compression for parquet file optimization")

	arrowProps := pqarrow.DefaultWriterProps()

	writer, err := pqarrow.NewFileWriter(schema, outputFile, props, arrowProps)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}
	defer writer.Close()

	// Process records in larger batches for better compression
	batchSize := 10000
	scanner := bufio.NewScanner(inputFile)

	var records []map[string]interface{}

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var jsonObj map[string]interface{}
		if err := sonic.Unmarshal([]byte(line), &jsonObj); err != nil {
			log.Warnf("Skipping invalid JSON line: %s", err)
			continue
		}

		records = append(records, jsonObj)

		// Write batch when it reaches the desired size
		if len(records) >= batchSize {
			if err := pc.writeBatch(writer, schema, records); err != nil {
				return fmt.Errorf("failed to write batch: %w", err)
			}
			records = records[:0] // Reset slice
		}
	}

	// Write remaining records
	if len(records) > 0 {
		if err := pc.writeBatch(writer, schema, records); err != nil {
			return fmt.Errorf("failed to write final batch: %w", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input file: %w", err)
	}

	return nil
}

// writeBatch writes a batch of records to the Parquet file
func (pc *ParquetConverter) writeBatch(writer *pqarrow.FileWriter, schema *arrow.Schema, records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	// Build arrays for each field
	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(pc.memPool, field.Type)
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()

	// Populate builders
	for _, record := range records {
		for i, field := range schema.Fields() {
			value, exists := record[field.Name]
			if err := pc.appendValue(builders[i], field.Type, value, exists); err != nil {
				return fmt.Errorf("failed to append value for field %s: %w", field.Name, err)
			}
		}
	}

	// Build arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
	}

	// Create record batch
	batch := array.NewRecord(schema, arrays, int64(len(records)))
	defer batch.Release()

	// Write batch
	if err := writer.Write(batch); err != nil {
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	return nil
}

// appendValue appends a value to an array builder
func (pc *ParquetConverter) appendValue(builder array.Builder, dataType arrow.DataType, value interface{}, exists bool) error {
	if !exists || value == nil {
		builder.AppendNull()
		return nil
	}

	switch dataType.ID() {
	case arrow.BOOL:
		if v, ok := value.(bool); ok {
			builder.(*array.BooleanBuilder).Append(v)
		} else {
			builder.AppendNull()
		}
	case arrow.INT32:
		if v, ok := pc.toInt32(value); ok {
			builder.(*array.Int32Builder).Append(v)
		} else {
			builder.AppendNull()
		}
	case arrow.INT64:
		if v, ok := pc.toInt64(value); ok {
			builder.(*array.Int64Builder).Append(v)
		} else {
			builder.AppendNull()
		}
	case arrow.FLOAT32:
		if v, ok := pc.toFloat32(value); ok {
			builder.(*array.Float32Builder).Append(v)
		} else {
			builder.AppendNull()
		}
	case arrow.FLOAT64:
		if v, ok := pc.toFloat64(value); ok {
			builder.(*array.Float64Builder).Append(v)
		} else {
			builder.AppendNull()
		}
	case arrow.STRING:
		str := pc.toString(value)
		builder.(*array.StringBuilder).Append(str)
	case arrow.TIMESTAMP:
		if ts, ok := pc.toTimestamp(value); ok {
			builder.(*array.TimestampBuilder).Append(ts)
		} else {
			builder.AppendNull()
		}
	default:
		// Fallback to string representation
		str := pc.toString(value)
		if sb, ok := builder.(*array.StringBuilder); ok {
			sb.Append(str)
		} else {
			builder.AppendNull()
		}
	}

	return nil
}

// Helper functions for type conversion
func (pc *ParquetConverter) toInt32(value interface{}) (int32, bool) {
	switch v := value.(type) {
	case int:
		// Check for overflow when converting int to int32
		if v > 2147483647 || v < -2147483648 {
			return 0, false
		}
		return int32(v), true // #nosec G115 - overflow check performed above
	case int32:
		return v, true
	case int64:
		// Check for overflow when converting int64 to int32
		if v > 2147483647 || v < -2147483648 {
			return 0, false
		}
		return int32(v), true // #nosec G115 - overflow check performed above
	case float64:
		// Check for overflow when converting float64 to int32
		if v > 2147483647 || v < -2147483648 {
			return 0, false
		}
		return int32(v), true
	}
	return 0, false
}

func (pc *ParquetConverter) toInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case float64:
		return int64(v), true
	}
	return 0, false
}

func (pc *ParquetConverter) toFloat32(value interface{}) (float32, bool) {
	switch v := value.(type) {
	case float32:
		return v, true
	case float64:
		return float32(v), true
	case int:
		return float32(v), true
	case int64:
		return float32(v), true
	}
	return 0, false
}

func (pc *ParquetConverter) toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float32:
		return float64(v), true
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	}
	return 0, false
}

func (pc *ParquetConverter) toString(value interface{}) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case []interface{}, map[string]interface{}:
		// Convert complex types to JSON
		if jsonBytes, err := sonic.Marshal(v); err == nil {
			return string(jsonBytes)
		}
		return fmt.Sprintf("%v", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (pc *ParquetConverter) toTimestamp(value interface{}) (arrow.Timestamp, bool) {
	str := pc.toString(value)
	if str == "" {
		return 0, false
	}

	formats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05.000Z",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, str); err == nil {
			return arrow.Timestamp(t.UnixMilli()), true
		}
	}
	return 0, false
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// analyzeSchemaFromS3Files analyzes schema from S3 files directly
func (pc *ParquetConverter) analyzeSchemaFromS3Files(ctx context.Context, files []storage.S3Object, s3Client *storage.S3SourceClient) (*arrow.Schema, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files provided for schema analysis")
	}

	// Analyze first file to get initial schema
	reader, err := s3Client.GetObject(ctx, files[0].Key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object %s: %w", files[0].Key, err)
	}
	defer reader.Close()

	schema, _, err := pc.analyzeNDJSONSchemaFromReader(reader, files[0].Key)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze schema from %s: %w", files[0].Key, err)
	}

	return schema, nil
}

// analyzeNDJSONSchemaFromReader analyzes schema from an io.Reader
func (pc *ParquetConverter) analyzeNDJSONSchemaFromReader(reader io.Reader, filename string) (*arrow.Schema, int64, error) {
	// Handle gzip decompression if needed
	var contentReader io.Reader = reader
	if strings.HasSuffix(strings.ToLower(filename), ".gz") {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		contentReader = gzipReader
	}

	scanner := bufio.NewScanner(contentReader)
	fields := make(map[string]arrow.DataType)
	recordCount := int64(0)

	// Sample first 1000 records for schema inference
	for scanner.Scan() && recordCount < 1000 {
		var record map[string]interface{}
		if err := sonic.Unmarshal(scanner.Bytes(), &record); err != nil {
			continue // Skip invalid JSON lines
		}

		recordCount++

		// Infer types for each field
		for fieldName, value := range record {
			inferredType := pc.inferArrowType(value)

			if existingType, exists := fields[fieldName]; exists {
				// Reconcile types if field already exists
				fields[fieldName] = pc.reconcileTypes(existingType, inferredType)
			} else {
				fields[fieldName] = inferredType
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, recordCount, fmt.Errorf("error reading file: %w", err)
	}

	// Convert to Arrow schema
	arrowFields := make([]arrow.Field, 0, len(fields))
	for fieldName, fieldType := range fields {
		arrowFields = append(arrowFields, arrow.Field{
			Name:     fieldName,
			Type:     fieldType,
			Nullable: true,
		})
	}

	schema := arrow.NewSchema(arrowFields, nil)
	return schema, recordCount, nil
}

// processS3FileStreaming processes a single S3 file in streaming mode
func (pc *ParquetConverter) processS3FileStreaming(ctx context.Context, file storage.S3Object, s3Client *storage.S3SourceClient, batch *[]map[string]interface{}, schema *arrow.Schema, writer *pqarrow.FileWriter, checkpointRecords int) (int64, int64, error) {
	reader, err := s3Client.GetObject(ctx, file.Key)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get object: %w", err)
	}
	defer reader.Close()

	// Handle gzip decompression
	var contentReader io.Reader = reader
	if strings.HasSuffix(strings.ToLower(file.Key), ".gz") {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return 0, 0, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		contentReader = gzipReader
	}

	scanner := bufio.NewScanner(contentReader)
	recordsProcessed := int64(0)
	totalSize := int64(0)

	for scanner.Scan() {
		var record map[string]interface{}
		line := scanner.Bytes()
		if err := sonic.Unmarshal(line, &record); err != nil {
			continue // Skip invalid JSON lines
		}

		*batch = append(*batch, record)
		recordsProcessed++
		totalSize += int64(len(line))

		// Write batch if checkpoint reached
		if len(*batch) >= checkpointRecords {
			if err := pc.writeBatch(writer, schema, *batch); err != nil {
				return recordsProcessed, totalSize, fmt.Errorf("failed to write batch: %w", err)
			}
			*batch = (*batch)[:0] // Reset batch
		}
	}

	if err := scanner.Err(); err != nil {
		return recordsProcessed, totalSize, fmt.Errorf("error reading file: %w", err)
	}

	return recordsProcessed, totalSize, nil
}
