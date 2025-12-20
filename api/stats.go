// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package api

import (
	"context"
	"time"

	"github.com/bytefreezer/packer/api/models"
	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"
)

const (
	SUCCESS  = "success"
	HEALTHY  = "healthy"
	UNHEALTY = "unhealthy"
)

func (api *API) HealthCheck() usecase.IOInteractorOf[models.EmptyRequest, models.HealthResponse] {

	u := usecase.NewInteractor(func(ctx context.Context, req models.EmptyRequest, resp *models.HealthResponse) error {

		resp.Timestamp = time.Now().UTC().Format(time.RFC3339)
		resp.Services = make(map[string]string)
		allHealthy := true

		// Check tenant database health
		tenantDBHealthy := api.Services.IsTenantDatabaseHealthy()
		tenantCount := api.Services.GetTenantCount()
		if tenantDBHealthy && tenantCount > 0 {
			resp.Services["tenant_database"] = HEALTHY
		} else {
			resp.Services["tenant_database"] = UNHEALTY
			allHealthy = false
		}

		// Check S3 source connectivity
		if err := api.Services.CheckS3SourceConnectivity(); err != nil {
			resp.Services["s3_source"] = UNHEALTY + ": " + err.Error()
			allHealthy = false
		} else {
			resp.Services["s3_source"] = HEALTHY
		}

		// Check locking backend connectivity (Control API)
		if err := api.Services.CheckLockingConnectivity(); err != nil {
			resp.Services["locking"] = UNHEALTY + ": " + err.Error()
			allHealthy = false
		} else {
			resp.Services["locking"] = HEALTHY
		}

		if allHealthy {
			resp.Status = HEALTHY
		} else {
			resp.Status = UNHEALTY
		}

		return nil
	})
	u.SetTags("Internal")
	u.SetExpectedErrors(status.Internal)
	u.SetDescription("Check status of the service including connectivity to S3, DynamoDB, and tenant database.")
	return u
}

// maskSensitiveValue masks sensitive configuration values
func maskSensitiveValue(value string) string {
	if value == "" {
		return ""
	}
	if len(value) <= 8 {
		return "***"
	}
	return value[:4] + "***" + value[len(value)-4:]
}

// GetConfig returns a handler for getting current system configuration
func (api *API) GetConfig() usecase.IOInteractorOf[models.EmptyRequest, models.ConfigResponse] {
	u := usecase.NewInteractor(func(ctx context.Context, req models.EmptyRequest, resp *models.ConfigResponse) error {
		cfg := api.Config

		// Basic app configuration
		resp.App = models.AppConfig{
			Name:    cfg.App.Name,
			Version: cfg.App.Version,
		}

		// Server configuration
		resp.Server = models.ServerConfig{
			ApiPort: cfg.Server.ApiPort,
		}

		// ByteFreezer configuration
		resp.Bytefreezer = models.BytefreezerConfig{
			CachePath: cfg.Bytefreezer.CachePath,
		}

		// SOC configuration
		resp.SOC = models.SOCConfig{
			Enabled:  cfg.SOC.Enabled,
			Endpoint: cfg.SOC.Endpoint,
			Timeout:  cfg.SOC.Timeout,
		}

		// S3 source configuration (with masked secrets)
		resp.S3Source = models.S3ConfigMasked{
			BucketName: cfg.S3Source.BucketName,
			Region:     cfg.S3Source.Region,
			AccessKey:  maskSensitiveValue(cfg.S3Source.AccessKey),
			SecretKey:  maskSensitiveValue(cfg.S3Source.SecretKey),
			Endpoint:   cfg.S3Source.Endpoint,
			Ssl:        cfg.S3Source.Ssl,
		}

		// Secrets Manager is not used in packer

		// Locking is handled via Control API

		// Upload pool configuration
		resp.UploadPool = models.UploadPoolConfig{
			WorkerCount:        cfg.UploadPool.WorkerCount,
			CleanupSourceFiles: cfg.UploadPool.CleanupSourceFiles,
		}

		// Tenant failures tracking is disabled

		// Dataset failures tracking is disabled

		// Processed files tracking is disabled

		// OTEL configuration
		resp.Otel = models.OtelConfig{
			Enabled:               cfg.Otel.Enabled,
			Endpoint:              "", // Not used in packer
			ScrapeIntervalSeconds: 0,  // Not used in packer
		}

		// Housekeeping configuration
		resp.Housekeeping = models.HousekeepingConfig{
			Enabled:         cfg.Housekeeping.Enabled,
			IntervalSeconds: cfg.Housekeeping.IntervalSeconds,
			Cleanup: models.HousekeepingCleanupConfig{
				Enabled:                  cfg.Housekeeping.Cleanup.Enabled,
				ProcessingDirMaxAgeHours: cfg.Housekeeping.Cleanup.ProcessingDirMaxAgeHours,
				OrphanedFilesMaxAgeHours: cfg.Housekeeping.Cleanup.OrphanedFilesMaxAgeHours,
			},
		}

		// Dev flag
		resp.Dev = cfg.Dev

		// Component status
		resp.ComponentStatus = models.ComponentStatusResponse{
			TenantDatabase: cfg.TenantDatabase != nil,
			SOCAlertClient: cfg.SOCAlertClient != nil,
			S3SourceClient: cfg.S3SourceClient != nil,
		}

		return nil
	})

	u.SetTags("Configuration")
	u.SetExpectedErrors(status.Internal)
	u.SetDescription("Retrieve the current system configuration (sensitive values are masked)")
	return u
}
