// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package domain

type Tenant struct {
	ID       string     `json:"id"`
	Name     string     `json:"name"`
	Datasets []*Dataset `json:"datasets"`
}

type Dataset struct {
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	TenantID         string            `json:"tenant_id"`
	Testing          bool              `json:"testing"`
	S3Destination    *S3Destination    `json:"s3_destination"`
	ProcessingConfig *ProcessingConfig `json:"processing_config,omitempty"`
}

type ProcessingConfig struct {
	FlattenJSON        bool   `json:"flatten_json"`
	FlattenDelimiter   string `json:"flatten_delimiter,omitempty"`
	EnableRawStorage   bool   `json:"enable_raw_storage"`
	PartitioningScheme string `json:"partitioning_scheme,omitempty"` // "hive", "date", "date_hour", "columnar", "iceberg", "none"
	PartitionLayout    string `json:"partition_layout,omitempty"`    // Template for partition path layout
	MetadataLevel      string `json:"metadata_level,omitempty"`      // "root" or "leaf" (default: "leaf")
}

type S3Destination struct {
	BucketName   string `mapstructure:"bucket_name" json:"bucket_name"`
	Prefix       string `mapstructure:"prefix" json:"prefix"`
	Region       string `mapstructure:"region" json:"region"`
	AccessKey    string `mapstructure:"access_key" json:"access_key"`
	SecretKey    string `mapstructure:"secret_key" json:"secret_key"`
	Endpoint     string `mapstructure:"endpoint" json:"endpoint"`
	Ssl          bool   `mapstructure:"ssl" json:"ssl"`
	UseIamRole   bool   `mapstructure:"use_iam_role" json:"use_iam_role"`
	Active       bool   `mapstructure:"active" json:"active"`
	FailureCount int    `mapstructure:"failure_count" json:"failure_count"`
	LastError    string `mapstructure:"last_error" json:"last_error"`
}
