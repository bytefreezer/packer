package api

import (
	"context"
	"fmt"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/api/models"
	"github.com/bytefreezer/packer/services"
	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"
)

// SetProcessingOrchestrator sets the processing orchestrator for the API
func (api *API) SetProcessingOrchestrator(orchestrator *services.ProcessingOrchestrator) {
	api.orchestrator = orchestrator
}

// ScheduleDatasetProcessing schedules a specific dataset for processing
func (api *API) ScheduleDatasetProcessing() usecase.IOInteractorOf[models.ProcessingRequest, models.ProcessingResponse] {
	u := usecase.NewInteractor(func(ctx context.Context, req models.ProcessingRequest, resp *models.ProcessingResponse) error {
		// Validate required fields
		if req.TenantID == "" || req.DatasetID == "" {
			return status.Wrap(fmt.Errorf("tenant_id and dataset_id are required"), status.InvalidArgument)
		}

		// Set default priority if not specified
		if req.Priority == 0 {
			req.Priority = 1
		}

		// Schedule the processing
		err := api.orchestrator.ScheduleDatasetProcessing(req.TenantID, req.DatasetID, req.Priority)
		if err != nil {
			log.Errorf("Failed to schedule dataset processing: %v", err)
			return status.Wrap(err, status.Internal)
		}

		resp.Message = "Dataset processing scheduled successfully"
		resp.TenantID = req.TenantID
		resp.DatasetID = req.DatasetID
		resp.Priority = req.Priority

		log.Infof("API: Scheduled dataset processing for tenant %s, dataset %s (priority %d)",
			req.TenantID, req.DatasetID, req.Priority)

		return nil
	})

	u.SetTags("Processing")
	u.SetExpectedErrors(status.InvalidArgument, status.Internal)
	u.SetDescription("Schedule a specific dataset for processing")
	return u
}

// GetProcessingStats returns comprehensive processing statistics
func (api *API) GetProcessingStats() usecase.IOInteractorOf[models.EmptyRequest, models.ProcessingStatsResponse] {
	u := usecase.NewInteractor(func(ctx context.Context, req models.EmptyRequest, resp *models.ProcessingStatsResponse) error {
		stats, err := api.orchestrator.GetProcessingStats()
		if err != nil {
			log.Errorf("Failed to get processing stats: %v", err)
			return status.Wrap(err, status.Internal)
		}

		// Extract spool stats
		if spoolStats, ok := stats["spool"].(map[string]int); ok {
			resp.Spool = spoolStats
		}

		// Add upload pool stats if available
		if uploadPool, exists := stats["upload_pool"]; exists {
			resp.UploadPool = uploadPool.(map[string]interface{})
		}

		resp.Timestamp = fmt.Sprintf("%v", stats["timestamp"])

		return nil
	})

	u.SetTags("Processing")
	u.SetExpectedErrors(status.Internal)
	u.SetDescription("Get comprehensive processing statistics")
	return u
}

// GetSpoolStats returns spool system statistics in a simplified format
func (api *API) GetSpoolStats() usecase.IOInteractorOf[models.EmptyRequest, models.SpoolStatsResponse] {
	u := usecase.NewInteractor(func(ctx context.Context, req models.EmptyRequest, resp *models.SpoolStatsResponse) error {
		stats, err := api.orchestrator.GetProcessingStats()
		if err != nil {
			log.Errorf("Failed to get spool stats: %v", err)
			return status.Wrap(err, status.Internal)
		}

		// Extract spool stats and format response
		spoolStats, ok := stats["spool"].(map[string]int)
		if !ok {
			return status.Wrap(fmt.Errorf("spool stats not available"), status.Internal)
		}

		resp.Queue = spoolStats["queue"]
		resp.Retry = spoolStats["retry"]
		resp.DLQ = spoolStats["dlq"]
		resp.ReadyRetries = spoolStats["ready_retries"]
		resp.Timestamp = fmt.Sprintf("%v", stats["timestamp"])

		// Add upload pool stats if available
		if uploadPool, exists := stats["upload_pool"]; exists {
			resp.UploadPool = uploadPool.(map[string]interface{})
		}

		return nil
	})

	u.SetTags("Processing")
	u.SetExpectedErrors(status.Internal)
	u.SetDescription("Get spool system statistics in a simplified format")
	return u
}

// GetDLQJobs returns jobs in the dead letter queue
func (api *API) GetDLQJobs() usecase.IOInteractorOf[models.EmptyRequest, models.DLQJobsResponse] {
	u := usecase.NewInteractor(func(ctx context.Context, req models.EmptyRequest, resp *models.DLQJobsResponse) error {
		stats, err := api.orchestrator.GetProcessingStats()
		if err != nil {
			return status.Wrap(err, status.Internal)
		}

		spoolStats, ok := stats["spool"].(map[string]int)
		if !ok {
			return status.Wrap(fmt.Errorf("spool stats not available"), status.Internal)
		}

		resp.Message = "Dead Letter Queue jobs"
		resp.DLQCount = spoolStats["dlq"]
		resp.Description = "Jobs that have exceeded max retry attempts"
		resp.Timestamp = fmt.Sprintf("%v", stats["timestamp"])

		return nil
	})

	u.SetTags("Processing")
	u.SetExpectedErrors(status.Internal)
	u.SetDescription("Get jobs in the dead letter queue")
	return u
}

// GetProcessingHealth returns the health status of the processing system
func (api *API) GetProcessingHealth() usecase.IOInteractorOf[models.EmptyRequest, models.ProcessingHealthResponse] {
	u := usecase.NewInteractor(func(ctx context.Context, req models.EmptyRequest, resp *models.ProcessingHealthResponse) error {
		stats, err := api.orchestrator.GetProcessingStats()
		if err != nil {
			resp.Status = "unhealthy"
			resp.Message = "Failed to get processing stats"
			return status.Wrap(err, status.Internal)
		}

		spoolStats, ok := stats["spool"].(map[string]int)
		if !ok {
			resp.Status = "unhealthy"
			resp.Message = "Spool stats not available"
			return status.Wrap(fmt.Errorf("spool stats not available"), status.Internal)
		}

		// Determine health based on DLQ size and system responsiveness
		resp.Status = "healthy"
		resp.Message = "Processing system is operating normally"

		if spoolStats["dlq"] > 100 {
			resp.Status = "warning"
			resp.Message = fmt.Sprintf("High DLQ count: %d jobs failed permanently", spoolStats["dlq"])
		}

		if spoolStats["dlq"] > 500 {
			resp.Status = "unhealthy"
			resp.Message = fmt.Sprintf("Critical DLQ count: %d jobs failed permanently", spoolStats["dlq"])
			return status.Wrap(fmt.Errorf("critical DLQ count"), status.Internal)
		}

		resp.SpoolStats = spoolStats
		resp.Timestamp = fmt.Sprintf("%v", stats["timestamp"])
		resp.QueueActive = spoolStats["queue"] > 0 || spoolStats["ready_retries"] > 0

		return nil
	})

	u.SetTags("Processing")
	u.SetExpectedErrors(status.Internal)
	u.SetDescription("Get the health status of the processing system")
	return u
}

// ClearLock clears a lock for a specific tenant/dataset
func (api *API) ClearLock() usecase.IOInteractorOf[models.ClearLockRequest, models.ClearLockResponse] {
	u := usecase.NewInteractor(func(ctx context.Context, req models.ClearLockRequest, resp *models.ClearLockResponse) error {
		// Validate required fields
		if req.TenantID == "" || req.DatasetID == "" {
			return status.Wrap(fmt.Errorf("tenant_id and dataset_id are required"), status.InvalidArgument)
		}

		// Get lock client from config
		lockClient := api.Config.GetLockClient()
		if lockClient == nil {
			return status.Wrap(fmt.Errorf("lock client not available"), status.Internal)
		}

		// Create lock key (same format as tenant processor: tenant_id/dataset_id)
		lockKey := fmt.Sprintf("%s/%s", req.TenantID, req.DatasetID)

		// Check if the lock exists first
		isLocked, currentLock, err := lockClient.IsLocked(ctx, lockKey)
		if err != nil {
			log.Errorf("Failed to check lock status for %s: %v", lockKey, err)
			return status.Wrap(fmt.Errorf("failed to check lock status: %w", err), status.Internal)
		}

		if !isLocked {
			resp.Message = "No lock found for the specified tenant/dataset"
			resp.TenantID = req.TenantID
			resp.DatasetID = req.DatasetID
			resp.LockKey = lockKey
			resp.Success = true
			log.Infof("API: No lock to clear for %s", lockKey)
			return nil
		}

		// Release the lock using the instance ID from the current lock
		err = lockClient.ReleaseLock(ctx, lockKey, currentLock.LockedBy)
		if err != nil {
			log.Errorf("Failed to release lock for %s: %v", lockKey, err)
			return status.Wrap(fmt.Errorf("failed to release lock: %w", err), status.Internal)
		}

		resp.Message = fmt.Sprintf("Successfully cleared lock for tenant %s, dataset %s (was locked by %s)", req.TenantID, req.DatasetID, currentLock.LockedBy)
		resp.TenantID = req.TenantID
		resp.DatasetID = req.DatasetID
		resp.LockKey = lockKey
		resp.Success = true

		log.Infof("API: Successfully cleared lock for %s (was locked by %s)", lockKey, currentLock.LockedBy)

		return nil
	})

	u.SetTags("Processing")
	u.SetExpectedErrors(status.InvalidArgument, status.Internal)
	u.SetDescription("Clear a lock for a specific tenant/dataset")
	return u
}
