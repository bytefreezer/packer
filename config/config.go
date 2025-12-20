// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/bytefreezer/goodies/log"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	pkgerrors "github.com/pkg/errors"

	"github.com/bytefreezer/packer/alerts"
	"github.com/bytefreezer/packer/errors"
	"github.com/bytefreezer/packer/metrics"
	"github.com/bytefreezer/packer/storage"
	"github.com/bytefreezer/packer/tenant"
)

var k = koanf.New(".")

type Config struct {
	App            App                  `mapstructure:"app"`
	Logging        LoggingConfig        `mapstructure:"logging"`
	Server         Server               `mapstructure:"server"`
	Bytefreezer    Bytefreezer          `mapstructure:"bytefreezer"`
	ControlService ControlServiceConfig `mapstructure:"control_service"`
	Otel           Otel                 `mapstructure:"otel"`
	Housekeeping   Housekeeping         `mapstructure:"housekeeping"`
	S3Source       S3Config             `mapstructure:"s3source"`
	// S3Destination removed - now using per-tenant:dataset destinations
	// PostgreSQL removed - now using Control Service API
	Parquet              ParquetConfig                 `mapstructure:"parquet"`
	UploadPool           UploadPoolConfig              `mapstructure:"uploadpool"`
	SOC                  SOCAlert                      `mapstructure:"soc"`
	HealthReporting      HealthReportingConfig         `mapstructure:"health_reporting"`
	ErrorTracking        ErrorTrackingConfig           `mapstructure:"error_tracking"`
	Dev                  bool                          `mapstructure:"dev"` // Deprecated - use control_service instead
	TenantClient         *tenant.TenantClient          `mapstructure:"-"`
	TenantDatabase       *tenant.TenantDatabase        `mapstructure:"-"`
	SOCAlertClient       *alerts.SOCAlertClient        `mapstructure:"-"`
	S3SourceClient       *storage.S3SourceClient       `mapstructure:"-"`
	S3DestinationManager *storage.S3DestinationManager `mapstructure:"-"`
	LockClient           storage.LockClient            `mapstructure:"-"`
	MetadataClient       storage.MetadataClient        `mapstructure:"-"`
	DatasetMetricsClient *metrics.DatasetMetricsClient `mapstructure:"-"`
	ErrorReporter        *errors.ErrorReporter         `mapstructure:"-"`
	AccumulationClient   *storage.AccumulationClient   `mapstructure:"-"`
}

type Bytefreezer struct {
	Controller string `mapstructure:"controller"` // Deprecated - use control_service instead
	SpoolPath  string `mapstructure:"spool_path"`
	CachePath  string `mapstructure:"cache_path"`
}

type ControlServiceConfig struct {
	Enabled        bool   `mapstructure:"enabled"`
	BaseURL        string `mapstructure:"base_url"`
	APIKey         string `mapstructure:"api_key"`
	TimeoutSeconds int    `mapstructure:"timeout_seconds"`
}

type S3Config struct {
	BucketName string `mapstructure:"bucket_name"`
	Region     string `mapstructure:"region"`
	AccessKey  string `mapstructure:"access_key"`
	SecretKey  string `mapstructure:"secret_key"`
	Endpoint   string `mapstructure:"endpoint"`
	Ssl        bool   `mapstructure:"ssl"`
	UseIamRole bool   `mapstructure:"use_iam_role"`
}

type ParquetConfig struct {
	MaxFileSizeMB     int    `mapstructure:"max_file_size_mb"`
	TimeoutSeconds    int    `mapstructure:"timeout_seconds"`
	Compression       string `mapstructure:"compression"`
	KeepSource        bool   `mapstructure:"keep_source"`
	StreamingMode     bool   `mapstructure:"streaming_mode"`
	MemoryBufferMB    int    `mapstructure:"memory_buffer_mb"`
	CheckpointRecords int    `mapstructure:"checkpoint_records"`
	AtomicUpload      bool   `mapstructure:"atomic_upload"`
}

type Housekeeping struct {
	Enabled         bool                `mapstructure:"enabled"`
	IntervalSeconds int                 `mapstructure:"intervalseconds"`
	Cleanup         HousekeepingCleanup `mapstructure:"cleanup"`
}

type HousekeepingCleanup struct {
	Enabled                  bool                    `mapstructure:"enabled"`
	ProcessingDirMaxAgeHours int                     `mapstructure:"processing_dir_max_age_hours"`
	OrphanedFilesMaxAgeHours int                     `mapstructure:"orphaned_files_max_age_hours"`
	LockCleanup              HousekeepingLockCleanup `mapstructure:"lock_cleanup"`
}

type HousekeepingLockCleanup struct {
	Enabled               bool `mapstructure:"enabled"`
	StaleThresholdMinutes int  `mapstructure:"stale_threshold_minutes"`
}

type App struct {
	Name           string `mapstructure:"name"`
	Version        string `mapstructure:"version"`
	DeploymentType string `mapstructure:"deployment_type"` // "managed" or "onprem"
}

// LoggingConfig stores global logging configurations
type LoggingConfig struct {
	Level    string `mapstructure:"level"`
	Encoding string `mapstructure:"encoding"`
}

type Server struct {
	ApiPort int `mapstructure:"api_port"`
}

type Otel struct {
	Enabled     bool   `mapstructure:"enabled"`
	MetricsPort int    `mapstructure:"metrics_port"`
	MetricsHost string `mapstructure:"metrics_host"`
}

type SOCAlert struct {
	Enabled  bool   `mapstructure:"enabled"`
	Endpoint string `mapstructure:"endpoint"`
	Timeout  int    `mapstructure:"timeout"`
}

type UploadPoolConfig struct {
	WorkerCount        int  `mapstructure:"worker_count"`
	CleanupSourceFiles bool `mapstructure:"cleanup_source_files"`
}

type HealthReportingConfig struct {
	Enabled           bool `mapstructure:"enabled"`
	ReportInterval    int  `mapstructure:"report_interval"` // Interval in seconds
	TimeoutSeconds    int  `mapstructure:"timeout_seconds"`
	RegisterOnStartup bool `mapstructure:"register_on_startup"`
}

// ErrorTrackingConfig represents error tracking configuration
// Note: Uses control_service.base_url for the control service endpoint
type ErrorTrackingConfig struct {
	Enabled bool `mapstructure:"enabled"`
}

func LoadConfig(cfgFile, envPrefix string, cfg *Config) error {
	if cfgFile == "" {
		cfgFile = "config.yaml"
	}

	err := k.Load(file.Provider(cfgFile), yaml.Parser())
	if err != nil {
		return pkgerrors.Wrapf(err, "failed to parse %s", cfgFile)
	}

	log.Infof("Loading environment variables with prefix: %s", envPrefix)
	envVars := make(map[string]string)
	for _, env := range os.Environ() {
		if strings.HasPrefix(env, envPrefix) {
			envVars[env] = os.Getenv(strings.Split(env, "=")[0])
		}
	}
	if len(envVars) > 0 {
		log.Infof("Found environment variables: %+v", envVars)
	} else {
		log.Infof("No environment variables found with prefix %s", envPrefix)
	}

	if err := k.Load(env.Provider(envPrefix, ".", func(s string) string {
		return strings.Replace(strings.ToLower(strings.TrimPrefix(s, envPrefix)), "_", ".", -1)
	}), nil); err != nil {
		return pkgerrors.Wrapf(err, "error loading config from env")
	}

	err = k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{Tag: "mapstructure"})
	if err != nil {
		return pkgerrors.Wrapf(err, "failed to unmarshal %s", cfgFile)
	}

	// Debug logging to show loaded S3 configuration
	log.Infof("=== LOADED CONFIG DEBUG ===")
	log.Infof("S3Source config loaded: BucketName='%s', Region='%s', Endpoint='%s', AccessKey='%s', SSL=%v",
		cfg.S3Source.BucketName, cfg.S3Source.Region, cfg.S3Source.Endpoint, cfg.S3Source.AccessKey, cfg.S3Source.Ssl)
	log.Infof("S3Destination config removed - now using per-tenant:dataset destinations in fake tenant data")
	log.Infof("Raw YAML parse check - checking s3source section in config file")

	// Also check if the raw YAML contains s3source
	rawConfig := k.Raw()
	if s3sourceData, exists := rawConfig["s3source"]; exists {
		log.Infof("Raw s3source data found: %+v", s3sourceData)
	} else {
		log.Errorf("No s3source section found in raw config!")
	}

	// s3destination section removed - destinations are now per-tenant:dataset in fake tenant data
	log.Infof("s3destination section removed from config - using per-tenant:dataset destinations")

	log.Infof("Available keys in raw config: %+v", k.Keys())

	return nil
}

// InitializeComponents initializes all components (tenant, SOC, S3, Control API)
func (cfg *Config) InitializeComponents() error {
	// Initialize SOC alert client
	cfg.SOCAlertClient = alerts.NewSOCAlertClient(alerts.AlertClientConfig{
		SOC: alerts.SOCConfig{
			Enabled:  cfg.SOC.Enabled,
			Endpoint: cfg.SOC.Endpoint,
			Timeout:  cfg.SOC.Timeout,
		},
		App: alerts.AppConfig{
			Name:    cfg.App.Name,
			Version: cfg.App.Version,
		},
		Dev: cfg.Dev,
	})

	// Initialize dataset metrics client
	cfg.DatasetMetricsClient = metrics.NewDatasetMetricsClient(
		cfg.ControlService.BaseURL,
		cfg.ControlService.APIKey,
		cfg.ControlService.TimeoutSeconds,
		cfg.ControlService.Enabled,
	)
	log.Infof("Dataset metrics client initialized (enabled: %v, endpoint: %s)",
		cfg.ControlService.Enabled, cfg.ControlService.BaseURL)

	// Initialize error reporting client if configured
	if cfg.ErrorTracking.Enabled && cfg.ControlService.BaseURL != "" {
		cfg.ErrorReporter = errors.NewErrorReporter(
			cfg.ControlService.BaseURL,
			cfg.ControlService.APIKey,
			"packer",
			true,
		)
		log.Infof("Error reporter initialized - reporting to control service at %s", cfg.ControlService.BaseURL)
	} else {
		log.Infof("Error reporting disabled (enabled: %v, control_service: %s)", cfg.ErrorTracking.Enabled, cfg.ControlService.BaseURL)
	}

	// Initialize Control API lock client
	if cfg.ControlService.BaseURL != "" {
		log.Infof("Initializing Control API lock client: %s", cfg.ControlService.BaseURL)
		lockClient, err := storage.NewControlAPILockClient(&storage.ControlAPILockConfig{
			BaseURL:        cfg.ControlService.BaseURL,
			APIKey:         cfg.ControlService.APIKey,
			TimeoutSeconds: cfg.ControlService.TimeoutSeconds,
		})
		if err != nil {
			// Send critical alert for lock client failure
			if alertErr := cfg.SOCAlertClient.SendCriticalAlert(
				"Control API Lock Client Initialization Failed",
				"Failed to initialize Control API lock client - service cannot start",
				fmt.Sprintf("Error: %v", err),
			); alertErr != nil {
				log.Warnf("Failed to send SOC alert: %v", alertErr)
			}
			return fmt.Errorf("failed to initialize Control API lock client: %w", err)
		}
		cfg.LockClient = lockClient

		// Test Control API lock connection
		if err := cfg.LockClient.TestConnection(); err != nil {
			// Send critical alert for lock connection failure
			if alertErr := cfg.SOCAlertClient.SendCriticalAlert(
				"Control API Lock Connection Failed",
				"Failed to connect to Control API lock service - service cannot start",
				fmt.Sprintf("Error: %v", err),
			); alertErr != nil {
				log.Warnf("Failed to send SOC alert: %v", alertErr)
			}
			return fmt.Errorf("failed to connect to Control API lock service: %w", err)
		}
		log.Info("Using Control API for distributed locking")
	} else {
		log.Warnf("Control API not configured - missing 'control_service:' section in config file")
		log.Warnf("Without distributed locking, multiple instances may process the same datasets")
	}

	// Initialize Control API metadata client
	if cfg.ControlService.BaseURL != "" {
		metadataClient, err := storage.NewControlAPIMetadataClient(&storage.ControlAPIMetadataConfig{
			BaseURL:        cfg.ControlService.BaseURL,
			APIKey:         cfg.ControlService.APIKey,
			TimeoutSeconds: cfg.ControlService.TimeoutSeconds,
		})
		if err != nil {
			// Send critical alert for metadata client failure
			if alertErr := cfg.SOCAlertClient.SendCriticalAlert(
				"Control API Metadata Client Initialization Failed",
				"Failed to initialize Control API metadata client - using fallback metadata system",
				fmt.Sprintf("Error: %v", err),
			); alertErr != nil {
				log.Warnf("Failed to send SOC alert: %v", alertErr)
			}
			log.Warnf("Failed to initialize Control API metadata client, using fallback: %v", err)
		} else {
			cfg.MetadataClient = metadataClient
			log.Info("Using Control API for metadata management")
		}
	} else {
		log.Info("Control API metadata not configured, using fallback metadata system")
	}

	// Initialize Control API accumulation client
	if cfg.ControlService.BaseURL != "" {
		accumulationClient, err := storage.NewAccumulationClient(&storage.AccumulationClientConfig{
			BaseURL:        cfg.ControlService.BaseURL,
			APIKey:         cfg.ControlService.APIKey,
			TimeoutSeconds: cfg.ControlService.TimeoutSeconds,
		})
		if err != nil {
			log.Warnf("Failed to initialize accumulation client: %v", err)
		} else {
			cfg.AccumulationClient = accumulationClient
			log.Info("Using Control API for accumulation state management")
		}
	} else {
		log.Info("Accumulation client not configured (control service not available)")
	}

	// Initialize S3 source client
	log.Debugf("Initializing S3 source client with bucket: '%s', region: '%s', endpoint: '%s'",
		cfg.S3Source.BucketName, cfg.S3Source.Region, cfg.S3Source.Endpoint)
	s3SourceClient, err := storage.NewS3SourceClient(storage.S3SourceConfig{
		BucketName: cfg.S3Source.BucketName,
		Region:     cfg.S3Source.Region,
		AccessKey:  cfg.S3Source.AccessKey,
		SecretKey:  cfg.S3Source.SecretKey,
		Endpoint:   cfg.S3Source.Endpoint,
		Ssl:        cfg.S3Source.Ssl,
		UseIamRole: cfg.S3Source.UseIamRole,
	})
	if err != nil {
		// Send critical alert for S3 source failure
		if alertErr := cfg.SOCAlertClient.SendCriticalAlert(
			"S3 Source Client Initialization Failed",
			"Failed to initialize S3 source client - service cannot start",
			fmt.Sprintf("Error: %v", err),
		); alertErr != nil {
			fmt.Printf("Failed to send SOC alert: %v\n", alertErr)
		}
		return fmt.Errorf("failed to initialize S3 source client: %w", err)
	}
	cfg.S3SourceClient = s3SourceClient

	// Test S3 source connection
	if err := cfg.S3SourceClient.TestConnection(); err != nil {
		// Send critical alert for S3 connection failure
		if alertErr := cfg.SOCAlertClient.SendCriticalAlert(
			"S3 Source Connection Failed",
			"Failed to connect to S3 source bucket - service cannot start",
			fmt.Sprintf("Error: %v", err),
		); alertErr != nil {
			fmt.Printf("Failed to send SOC alert: %v\n", alertErr)
		}
		return fmt.Errorf("failed to connect to S3 source: %w", err)
	}

	// Initialize S3 destination manager for lazy per-tenant:dataset connections
	cfg.S3DestinationManager = storage.NewS3DestinationManager()

	// Initialize tenant client
	cfg.TenantClient = tenant.NewTenantClient(tenant.TenantClientConfig{
		App: tenant.AppConfig{
			Name:           cfg.App.Name,
			Version:        cfg.App.Version,
			DeploymentType: cfg.App.DeploymentType,
		},
		Bytefreezer: tenant.BytefreezerConfig{
			Controller: cfg.Bytefreezer.Controller,
			CachePath:  cfg.Bytefreezer.SpoolPath, // Use spool path for caching
		},
		ControlService: tenant.ControlServiceConfig{
			Enabled:        cfg.ControlService.Enabled,
			BaseURL:        cfg.ControlService.BaseURL,
			APIKey:         cfg.ControlService.APIKey,
			TimeoutSeconds: cfg.ControlService.TimeoutSeconds,
		},
		Dev: cfg.Dev,
	})

	// Initialize tenant database
	cfg.TenantDatabase = tenant.NewTenantDatabase()

	// Populate tenant database with retry logic
	// PopulateDatabase will retry with exponential backoff and start with empty database if control service is unavailable
	// Service will continue to retry fetching tenants in background via UpdateDatabase
	if err := cfg.TenantDatabase.PopulateDatabase(cfg.TenantClient); err != nil {
		// This should never happen now since PopulateDatabase returns nil to allow service to start
		// Keeping error handling for safety
		fmt.Printf("Unexpected error during tenant database population: %v\n", err)
	}

	return nil
}

// UpdateTenantDatabase refreshes the tenant database
func (cfg *Config) UpdateTenantDatabase() error {
	if cfg.TenantDatabase == nil || cfg.TenantClient == nil {
		return fmt.Errorf("tenant components not initialized")
	}
	return cfg.TenantDatabase.UpdateDatabase(cfg.TenantClient)
}

// GetLockClient returns the active lock client (Control API)
func (cfg *Config) GetLockClient() storage.LockClient {
	return cfg.LockClient
}

// GetErrorReporter returns the error reporter instance
func (cfg *Config) GetErrorReporter() *errors.ErrorReporter {
	return cfg.ErrorReporter
}
