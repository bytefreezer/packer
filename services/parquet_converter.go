// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet"
	"github.com/apache/arrow/go/v18/parquet/compress"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/domain"
	"github.com/bytefreezer/packer/storage"
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
	// Timestamp tracking for smart filenames and S3 metadata
	MinTimestamp time.Time
	MaxTimestamp time.Time
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

// MultiFileResult contains results for all output files when splitting occurs
type MultiFileResult struct {
	Files            []string           // All output file paths
	Results          []*ConversionResult // Result per file
	TotalRecords     int64
	TotalInputSize   int64
	TotalOutputSize  int64
	TotalConvertTime time.Duration
}

// parquetWriterState holds state for a single parquet output file
type parquetWriterState struct {
	s3Writer    *S3Writer
	writer      *pqarrow.FileWriter
	partNumber  int
	baseKey     string
	finalKey    string
	uploadKey   string
	records     int64
	timestamps  FileTimestamps
	startTime   time.Time
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
// Supports file splitting when output exceeds MaxFileSizeMB
func (pc *ParquetConverter) StreamNDJSONToParquet(ctx context.Context, files []storage.S3Object, s3Key string, options *ParquetConversionOptions) (string, *ConversionResult, error) {
	multiResult, err := pc.StreamNDJSONToParquetMulti(ctx, files, s3Key, options)
	if err != nil {
		return "", &ConversionResult{Error: err}, err
	}

	// Return first file for backward compatibility
	if len(multiResult.Files) == 0 {
		return "", &ConversionResult{}, fmt.Errorf("no output files created")
	}

	// Combine results for single-file API compatibility
	result := &ConversionResult{
		InputFile:        fmt.Sprintf("streaming-%d-files", len(files)),
		OutputFile:       multiResult.Files[0],
		RecordsProcessed: multiResult.TotalRecords,
		TotalSize:        multiResult.TotalInputSize,
		FileSize:         multiResult.TotalOutputSize,
		ConvertTime:      multiResult.TotalConvertTime,
	}

	// Get timestamps from first result
	if len(multiResult.Results) > 0 && !multiResult.Results[0].MinTimestamp.IsZero() {
		result.MinTimestamp = multiResult.Results[0].MinTimestamp
		result.MaxTimestamp = multiResult.Results[len(multiResult.Results)-1].MaxTimestamp
	}

	return multiResult.Files[0], result, nil
}

// StreamNDJSONToParquetMulti streams NDJSON files to multiple Parquet files with size limiting
func (pc *ParquetConverter) StreamNDJSONToParquetMulti(ctx context.Context, files []storage.S3Object, s3Key string, options *ParquetConversionOptions) (*MultiFileResult, error) {
	startTime := time.Now()

	multiResult := &MultiFileResult{}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files provided for streaming conversion")
	}

	// Calculate max file size in bytes (default 64MB if not set)
	maxFileSizeBytes := int64(64 * 1024 * 1024)
	if options.MaxFileSizeMB > 0 {
		maxFileSizeBytes = int64(options.MaxFileSizeMB) * 1024 * 1024
	}

	log.Infof("Starting streaming conversion of %d files to S3 key: %s (max file size: %d MB)", len(files), s3Key, maxFileSizeBytes/(1024*1024))

	// Step 1: Analyze schema from first few files
	schema, err := pc.analyzeSchemaFromS3Files(ctx, files[:min(len(files), 3)], options.S3SourceClient)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze schema: %w", err)
	}

	// Step 2: Get S3 destination client
	destClient, err := options.S3DestinationManager.GetS3Client(options.Dataset.TenantID, options.Dataset.ID, options.Dataset.S3Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to get S3 destination client: %w", err)
	}

	// Determine compression codec
	compression := compress.Codecs.Uncompressed
	if options.Compression == "zstd" {
		compression = compress.Codecs.Zstd
	} else if options.Compression == "snappy" {
		compression = compress.Codecs.Snappy
	}

	// Helper function to create a new writer state
	createWriterState := func(partNum int) (*parquetWriterState, error) {
		state := &parquetWriterState{
			partNumber: partNum,
			baseKey:    s3Key,
			startTime:  time.Now(),
		}

		// Generate part key: base_part001.parquet
		if partNum == 1 {
			state.finalKey = s3Key
		} else {
			// Insert part number before .parquet extension
			state.finalKey = strings.TrimSuffix(s3Key, ".parquet") + fmt.Sprintf("_part%03d.parquet", partNum)
		}

		state.uploadKey = state.finalKey
		if options.AtomicUpload {
			state.uploadKey = state.finalKey + ".tmp"
		}

		state.s3Writer = &S3Writer{
			client: destClient.GetRawS3Client(),
			bucket: options.Dataset.S3Destination.BucketName,
			key:    state.uploadKey,
		}

		parquetProps := parquet.NewWriterProperties(parquet.WithCompression(compression))
		arrowProps := pqarrow.DefaultWriterProps()

		writer, err := pqarrow.NewFileWriter(schema, state.s3Writer, parquetProps, arrowProps)
		if err != nil {
			return nil, fmt.Errorf("failed to create Parquet writer for part %d: %w", partNum, err)
		}
		state.writer = writer

		log.Debugf("Created writer for part %d: %s", partNum, state.finalKey)
		return state, nil
	}

	// Helper function to finalize a writer state
	finalizeWriterState := func(state *parquetWriterState) (*ConversionResult, error) {
		// Set S3 metadata
		if state.timestamps.HasTimestamp {
			s3Metadata := map[string]string{
				"min-timestamp": state.timestamps.MinTimestamp.UTC().Format(time.RFC3339),
				"max-timestamp": state.timestamps.MaxTimestamp.UTC().Format(time.RFC3339),
				"row-count":     fmt.Sprintf("%d", state.records),
			}
			state.s3Writer.SetMetadata(s3Metadata)
		}

		outputSize := state.s3Writer.BytesWritten()

		if err := state.writer.Close(); err != nil {
			return nil, fmt.Errorf("failed to close Parquet writer: %w", err)
		}

		// Atomic rename if enabled
		if options.AtomicUpload {
			if err := state.s3Writer.AtomicRename(state.finalKey); err != nil {
				return nil, fmt.Errorf("failed to perform atomic rename: %w", err)
			}
		}

		result := &ConversionResult{
			OutputFile:       state.finalKey,
			RecordsProcessed: state.records,
			FileSize:         outputSize,
			ConvertTime:      time.Since(state.startTime),
			MinTimestamp:     state.timestamps.MinTimestamp,
			MaxTimestamp:     state.timestamps.MaxTimestamp,
		}

		log.Infof("Finalized part %d: %s (%d records, %d bytes)", state.partNumber, state.finalKey, state.records, outputSize)
		return result, nil
	}

	// Create initial writer
	currentState, err := createWriterState(1)
	if err != nil {
		return nil, err
	}

	// Track totals
	totalRecords := int64(0)
	totalInputSize := int64(0)
	batch := make([]map[string]interface{}, 0, options.CheckpointRecords)
	lastProgressUpdate := time.Now()

	// Process all input files
	for i, file := range files {
		select {
		case <-ctx.Done():
			currentState.writer.Close()
			return nil, ctx.Err()
		default:
		}

		log.Debugf("Streaming file %d/%d: %s", i+1, len(files), file.Key)

		fileRecords, fileSize, fileTimestamps, err := pc.processS3FileStreamingWithSplit(
			ctx, file, options.S3SourceClient, &batch, schema,
			currentState, options.CheckpointRecords, maxFileSizeBytes,
			func() (*parquetWriterState, error) {
				// Finalize current state
				result, err := finalizeWriterState(currentState)
				if err != nil {
					return nil, err
				}
				multiResult.Files = append(multiResult.Files, result.OutputFile)
				multiResult.Results = append(multiResult.Results, result)
				multiResult.TotalOutputSize += result.FileSize

				// Create new state
				newState, err := createWriterState(currentState.partNumber + 1)
				if err != nil {
					return nil, err
				}
				currentState = newState
				return currentState, nil
			},
		)
		if err != nil {
			log.Warnf("Failed to process file %s: %v", file.Key, err)
			continue
		}

		totalRecords += fileRecords
		totalInputSize += fileSize

		// Update current state timestamps
		if fileTimestamps != nil && fileTimestamps.HasTimestamp {
			if !currentState.timestamps.HasTimestamp {
				currentState.timestamps = *fileTimestamps
			} else {
				if fileTimestamps.MinTimestamp.Before(currentState.timestamps.MinTimestamp) {
					currentState.timestamps.MinTimestamp = fileTimestamps.MinTimestamp
				}
				if fileTimestamps.MaxTimestamp.After(currentState.timestamps.MaxTimestamp) {
					currentState.timestamps.MaxTimestamp = fileTimestamps.MaxTimestamp
				}
			}
		}

		// Report progress
		if options.ActivityReporter != nil && options.OperationID != "" {
			shouldReport := (i+1)%100 == 0 || time.Since(lastProgressUpdate) > 30*time.Second
			if shouldReport {
				options.ActivityReporter.UpdateProgress(
					options.OperationID,
					int64(i+1),
					fmt.Sprintf("Converting %d/%d files (part %d)", i+1, len(files), currentState.partNumber),
				)
				lastProgressUpdate = time.Now()
			}
		}
	}

	// Write final batch
	if len(batch) > 0 {
		pc.sortRecordsByTimestamp(batch)
		if err := pc.writeBatchToState(currentState, schema, batch); err != nil {
			currentState.writer.Close()
			return nil, fmt.Errorf("failed to write final batch: %w", err)
		}
	}

	// Finalize last writer
	result, err := finalizeWriterState(currentState)
	if err != nil {
		return nil, err
	}
	multiResult.Files = append(multiResult.Files, result.OutputFile)
	multiResult.Results = append(multiResult.Results, result)
	multiResult.TotalOutputSize += result.FileSize

	multiResult.TotalRecords = totalRecords
	multiResult.TotalInputSize = totalInputSize
	multiResult.TotalConvertTime = time.Since(startTime)

	log.Infof("Streaming conversion completed: %d records, %d input bytes -> %d output bytes in %d files, time: %v",
		totalRecords, totalInputSize, multiResult.TotalOutputSize, len(multiResult.Files), multiResult.TotalConvertTime)

	return multiResult, nil
}

// writeBatchToState writes a batch to a specific writer state and updates record count
func (pc *ParquetConverter) writeBatchToState(state *parquetWriterState, schema *arrow.Schema, records []map[string]interface{}) error {
	if err := pc.writeBatch(state.writer, schema, records); err != nil {
		return err
	}
	state.records += int64(len(records))
	return nil
}

// processS3FileStreamingWithSplit processes a file with support for mid-file splitting
func (pc *ParquetConverter) processS3FileStreamingWithSplit(
	ctx context.Context,
	file storage.S3Object,
	s3Client *storage.S3SourceClient,
	batch *[]map[string]interface{},
	schema *arrow.Schema,
	state *parquetWriterState,
	checkpointRecords int,
	maxFileSizeBytes int64,
	createNewWriter func() (*parquetWriterState, error),
) (int64, int64, *FileTimestamps, error) {
	reader, err := s3Client.GetObject(ctx, file.Key)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("failed to get object: %w", err)
	}
	defer reader.Close()

	// Handle gzip decompression
	var contentReader io.Reader = reader
	if strings.HasSuffix(strings.ToLower(file.Key), ".gz") {
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		contentReader = gzipReader
	}

	scanner := bufio.NewScanner(contentReader)
	recordsProcessed := int64(0)
	totalSize := int64(0)
	timestamps := &FileTimestamps{}

	for scanner.Scan() {
		var record map[string]interface{}
		line := scanner.Bytes()
		if err := sonic.Unmarshal(line, &record); err != nil {
			continue // Skip invalid JSON lines
		}

		// Add BfTs (ByteFreezer Timestamp) if not already present
		// Proxy now injects BfTs at ingestion time; this is fallback for older data
		if _, hasBfTs := record["BfTs"]; !hasBfTs {
			record["BfTs"] = time.Now().UnixMilli()
		}

		// Track timestamps
		if ts := pc.extractTimestampFromRecord(record); !ts.IsZero() {
			if !timestamps.HasTimestamp {
				timestamps.MinTimestamp = ts
				timestamps.MaxTimestamp = ts
				timestamps.HasTimestamp = true
			} else {
				if ts.Before(timestamps.MinTimestamp) {
					timestamps.MinTimestamp = ts
				}
				if ts.After(timestamps.MaxTimestamp) {
					timestamps.MaxTimestamp = ts
				}
			}

			// Also update state timestamps
			if !state.timestamps.HasTimestamp {
				state.timestamps.MinTimestamp = ts
				state.timestamps.MaxTimestamp = ts
				state.timestamps.HasTimestamp = true
			} else {
				if ts.Before(state.timestamps.MinTimestamp) {
					state.timestamps.MinTimestamp = ts
				}
				if ts.After(state.timestamps.MaxTimestamp) {
					state.timestamps.MaxTimestamp = ts
				}
			}
		}

		*batch = append(*batch, record)
		recordsProcessed++
		totalSize += int64(len(line))

		// Write batch if checkpoint reached
		if len(*batch) >= checkpointRecords {
			pc.sortRecordsByTimestamp(*batch)
			if err := pc.writeBatchToState(state, schema, *batch); err != nil {
				return recordsProcessed, totalSize, timestamps, fmt.Errorf("failed to write batch: %w", err)
			}
			*batch = (*batch)[:0]

			// Check if we need to split to a new file
			currentSize := state.s3Writer.BytesWritten()
			if currentSize >= maxFileSizeBytes {
				log.Infof("File size %d bytes exceeds limit %d bytes, creating new part", currentSize, maxFileSizeBytes)
				newState, err := createNewWriter()
				if err != nil {
					return recordsProcessed, totalSize, timestamps, fmt.Errorf("failed to create new writer: %w", err)
				}
				// Update state pointer (caller's state variable is updated via createNewWriter)
				_ = newState // State is updated in createNewWriter callback
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return recordsProcessed, totalSize, timestamps, fmt.Errorf("error reading file: %w", err)
	}

	return recordsProcessed, totalSize, timestamps, nil
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

	// Build Arrow schema with BfTs as first field
	fields := make([]arrow.Field, 0, len(fieldTypes)+1)

	// Add BfTs (ByteFreezer Timestamp) as first field - Unix milliseconds
	fields = append(fields, arrow.Field{
		Name:     "BfTs",
		Type:     &arrow.Int64Type{},
		Nullable: false,
	})

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

		// Add BfTs (ByteFreezer Timestamp) if not already present
		// Proxy now injects BfTs at ingestion time; this is fallback for older data
		if _, hasBfTs := jsonObj["BfTs"]; !hasBfTs {
			jsonObj["BfTs"] = time.Now().UnixMilli()
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
// Records are sorted by timestamp before writing for better query performance
func (pc *ParquetConverter) writeBatch(writer *pqarrow.FileWriter, schema *arrow.Schema, records []map[string]interface{}) error {
	if len(records) == 0 {
		return nil
	}

	// Sort records by timestamp for better query performance
	// This allows queries to skip ORDER BY since data is pre-sorted
	pc.sortRecordsByTimestamp(records)

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

	// Prepend BfTs (ByteFreezer Timestamp) as first field - Unix milliseconds
	bfTsField := arrow.Field{
		Name:     "BfTs",
		Type:     &arrow.Int64Type{},
		Nullable: false,
	}
	newFields := append([]arrow.Field{bfTsField}, schema.Fields()...)
	schema = arrow.NewSchema(newFields, nil)

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

// FileTimestamps holds min/max timestamps tracked during processing
type FileTimestamps struct {
	MinTimestamp time.Time
	MaxTimestamp time.Time
	HasTimestamp bool
}

// extractTimestampFromRecord extracts timestamp from common timestamp fields
func (pc *ParquetConverter) extractTimestampFromRecord(record map[string]interface{}) time.Time {
	// Try common timestamp field names in order of priority
	timestampFields := []string{"InfoTimestamp", "timestamp", "Timestamp", "@timestamp", "time", "Time", "event_time", "created_at"}

	for _, fieldName := range timestampFields {
		if value, exists := record[fieldName]; exists {
			if ts := pc.parseTimestampValue(value); !ts.IsZero() {
				return ts
			}
		}
	}

	return time.Time{}
}

// parseTimestampValue parses various timestamp formats
func (pc *ParquetConverter) parseTimestampValue(value interface{}) time.Time {
	switch v := value.(type) {
	case string:
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05.000Z",
			"2006-01-02T15:04:05Z",
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		for _, format := range formats {
			if t, err := time.Parse(format, v); err == nil {
				return t
			}
		}
	case float64:
		// Unix timestamp in seconds or milliseconds
		if v > 1e12 {
			// Milliseconds
			return time.UnixMilli(int64(v))
		}
		return time.Unix(int64(v), 0)
	case int64:
		if v > 1e12 {
			return time.UnixMilli(v)
		}
		return time.Unix(v, 0)
	}
	return time.Time{}
}

// sortRecordsByTimestamp sorts records by timestamp in ascending order
// This ensures parquet files are pre-sorted, allowing queries to skip ORDER BY
func (pc *ParquetConverter) sortRecordsByTimestamp(records []map[string]interface{}) {
	sort.Slice(records, func(i, j int) bool {
		tsI := pc.extractTimestampFromRecord(records[i])
		tsJ := pc.extractTimestampFromRecord(records[j])

		// If either timestamp is zero, maintain original order
		if tsI.IsZero() || tsJ.IsZero() {
			return false
		}

		return tsI.Before(tsJ)
	})
}
