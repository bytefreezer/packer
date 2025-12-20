// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package models

type EmptyResponse struct{}
type EmptyRequest struct{}
type HealthResponse struct {
	Status    string            `json:"status"`
	Timestamp string            `json:"timestamp"`
	Services  map[string]string `json:"services"`
}

// ConfigResponse represents the current system configuration
type ConfigResponse struct {
	App             AppConfig                  `json:"app"`
	Server          ServerConfig               `json:"server"`
	Bytefreezer     BytefreezerConfig          `json:"bytefreezer"`
	SOC             SOCConfig                  `json:"soc"`
	S3Source        S3ConfigMasked             `json:"s3_source"`
	UploadPool      UploadPoolConfig           `json:"upload_pool"`
	TenantFailures  TenantFailuresConfig       `json:"tenant_failures"`
	DatasetFailures DatasetFailuresConfig      `json:"dataset_failures"`
	ProcessedFiles  ProcessedFilesConfigMasked `json:"processed_files"`
	Otel            OtelConfig                 `json:"otel"`
	Housekeeping    HousekeepingConfig         `json:"housekeeping"`
	Dev             bool                       `json:"dev"`
	ComponentStatus ComponentStatusResponse    `json:"component_status"`
}

// Individual config sections for response
type AppConfig struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type ServerConfig struct {
	ApiPort int `json:"api_port"`
}

type BytefreezerConfig struct {
	CachePath string `json:"cache_path"`
}

type SOCConfig struct {
	Enabled  bool   `json:"enabled"`
	Endpoint string `json:"endpoint"`
	Timeout  int    `json:"timeout"`
}

type OtelConfig struct {
	Enabled               bool   `json:"enabled"`
	Endpoint              string `json:"endpoint"`
	ScrapeIntervalSeconds int    `json:"scrape_interval_seconds"`
}

type UploadPoolConfig struct {
	WorkerCount        int  `json:"worker_count"`
	CleanupSourceFiles bool `json:"cleanup_source_files"`
}

type TenantFailuresConfig struct {
	TableName string `json:"table_name"`
	Threshold int    `json:"threshold"`
}

type DatasetFailuresConfig struct {
	TableName string `json:"table_name"`
	Threshold int    `json:"threshold"`
}

type HousekeepingConfig struct {
	Enabled         bool                      `json:"enabled"`
	IntervalSeconds int                       `json:"interval_seconds"`
	Cleanup         HousekeepingCleanupConfig `json:"cleanup"`
}

type HousekeepingCleanupConfig struct {
	Enabled                  bool `json:"enabled"`
	ProcessingDirMaxAgeHours int  `json:"processing_dir_max_age_hours"`
	OrphanedFilesMaxAgeHours int  `json:"orphaned_files_max_age_hours"`
}

// Masked config sections (sensitive data redacted)
type S3ConfigMasked struct {
	BucketName string `json:"bucket_name"`
	Region     string `json:"region"`
	AccessKey  string `json:"access_key"` // This will be masked
	SecretKey  string `json:"secret_key"` // This will be masked
	Endpoint   string `json:"endpoint"`
	Ssl        bool   `json:"ssl"`
}

type ProcessedFilesConfigMasked struct {
	TableName string    `json:"table_name"`
	Region    string    `json:"region"`
	Endpoint  string    `json:"endpoint"`
	SSL       bool      `json:"ssl"`
	AccessKey string    `json:"access_key"` // This will be masked
	SecretKey string    `json:"secret_key"` // This will be masked
	TTLDays   int       `json:"ttl_days"`
	DLQConfig DLQConfig `json:"dlq"`
}

type DLQConfig struct {
	Enabled          bool   `json:"enabled"`
	BucketName       string `json:"bucket_name"`
	MaxAgeHours      int    `json:"max_age_hours"`
	CheckIntervalMin int    `json:"check_interval_minutes"`
}

type ComponentStatusResponse struct {
	TenantDatabase bool `json:"tenant_database"`
	SOCAlertClient bool `json:"soc_alert_client"`
	S3SourceClient bool `json:"s3_source_client"`
}

// Processing API Models

// ProcessingRequest represents a request to schedule processing
type ProcessingRequest struct {
	TenantID  string `json:"tenant_id" validate:"required"`
	DatasetID string `json:"dataset_id" validate:"required"`
	Priority  int    `json:"priority,omitempty"` // Optional, defaults to 1
}

// ProcessingResponse represents the response after scheduling
type ProcessingResponse struct {
	Message   string `json:"message"`
	TenantID  string `json:"tenant_id"`
	DatasetID string `json:"dataset_id"`
	Priority  int    `json:"priority"`
	JobID     string `json:"job_id,omitempty"`
}

// SpoolStatsResponse represents spool system statistics
type SpoolStatsResponse struct {
	Queue        int                    `json:"queue"`
	Retry        int                    `json:"retry"`
	DLQ          int                    `json:"dlq"`
	ReadyRetries int                    `json:"ready_retries"`
	UploadPool   map[string]interface{} `json:"upload_pool,omitempty"`
	Timestamp    string                 `json:"timestamp"`
}

// JobRequest represents a request for job operations
type JobRequest struct {
	JobID string `json:"job_id" validate:"required"`
}

// ProcessingStatsResponse represents comprehensive processing statistics
type ProcessingStatsResponse struct {
	Spool      map[string]int         `json:"spool"`
	UploadPool map[string]interface{} `json:"upload_pool,omitempty"`
	Timestamp  string                 `json:"timestamp"`
}

// JobListResponse represents a list of jobs
type JobListResponse struct {
	Message   string         `json:"message"`
	Filter    string         `json:"filter,omitempty"`
	Stats     map[string]int `json:"stats"`
	Timestamp string         `json:"timestamp"`
	Note      string         `json:"note,omitempty"`
}

// DLQJobsResponse represents jobs in the dead letter queue
type DLQJobsResponse struct {
	Message     string `json:"message"`
	DLQCount    int    `json:"dlq_count"`
	Description string `json:"description"`
	Timestamp   string `json:"timestamp"`
}

// ProcessingHealthResponse represents the health status of the processing system
type ProcessingHealthResponse struct {
	Status      string         `json:"status"`
	Message     string         `json:"message"`
	SpoolStats  map[string]int `json:"spool_stats"`
	Timestamp   string         `json:"timestamp"`
	QueueActive bool           `json:"queue_active"`
}

// ClearLockRequest represents a request to clear a lock for tenant/dataset
type ClearLockRequest struct {
	TenantID  string `json:"tenant_id" validate:"required"`
	DatasetID string `json:"dataset_id" validate:"required"`
}

// ClearLockResponse represents the response after clearing a lock
type ClearLockResponse struct {
	Message   string `json:"message"`
	TenantID  string `json:"tenant_id"`
	DatasetID string `json:"dataset_id"`
	LockKey   string `json:"lock_key"`
	Success   bool   `json:"success"`
}
