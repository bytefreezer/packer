// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/config"
	"github.com/bytefreezer/packer/storage"
)

// ProcessingOrchestrator manages the overall processing workflow with spool-based reliability
type ProcessingOrchestrator struct {
	config           *config.Config
	spoolManager     *SpoolManager
	tenantProcessor  *TenantProcessor
	uploadWorkerPool *UploadWorkerPool
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup
	running          bool
	mutex            sync.RWMutex
}

// ProcessingJobType defines the types of jobs that can be processed
const (
	JobTypeDatasetProcessing = "dataset_processing"
	JobTypeParquetConversion = "parquet_conversion"
	JobTypeUpload            = "upload"
)

// NewProcessingOrchestrator creates a new processing orchestrator
func NewProcessingOrchestrator(cfg *config.Config) (*ProcessingOrchestrator, error) {
	spoolManager, err := NewSpoolManager(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create spool manager: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	orchestrator := &ProcessingOrchestrator{
		config:       cfg,
		spoolManager: spoolManager,
		ctx:          ctx,
		cancel:       cancel,
	}

	return orchestrator, nil
}

// Start begins the processing orchestrator
func (po *ProcessingOrchestrator) Start() error {
	po.mutex.Lock()
	defer po.mutex.Unlock()

	if po.running {
		return fmt.Errorf("processing orchestrator is already running")
	}

	// Initialize components
	var err error
	po.tenantProcessor, err = NewTenantProcessor(po.config, po.spoolManager, po.config.GetLockClient())
	if err != nil {
		return fmt.Errorf("failed to create tenant processor: %w", err)
	}

	// Start the tenant processor (this starts the upload worker pool)
	po.tenantProcessor.Start()
	log.Info("Tenant processor and upload worker pool started")

	// Store reference to upload worker pool for stats
	po.uploadWorkerPool = po.tenantProcessor.uploadPool

	// Scan for and move any existing malformed jobs to DLQ before starting processing
	log.Info("Scanning for malformed jobs on startup...")
	if moved, err := po.spoolManager.ScanAndMoveMalformedJobs(); err != nil {
		log.Errorf("Failed to scan for malformed jobs on startup: %v", err)
	} else if moved > 0 {
		log.Infof("Moved %d malformed jobs to DLQ on startup", moved)
	} else {
		log.Info("No malformed jobs found on startup")
	}

	// Start background workers
	po.wg.Add(4)
	go po.mainProcessingLoop()
	go po.retryProcessingLoop()
	go po.housekeepingLoop()
	go po.monitoringLoop()

	po.running = true
	log.Info("Processing orchestrator started successfully")

	return nil
}

// Stop gracefully shuts down the processing orchestrator
func (po *ProcessingOrchestrator) Stop() error {
	po.mutex.Lock()
	defer po.mutex.Unlock()

	if !po.running {
		return nil
	}

	log.Info("Stopping processing orchestrator...")

	// Cancel context and wait for goroutines
	po.cancel()
	po.wg.Wait()

	// Stop tenant processor (this stops the upload worker pool)
	if po.tenantProcessor != nil {
		po.tenantProcessor.Stop()
	}

	// Cleanup expired locks on shutdown (best effort)
	log.Info("Cleaning up expired locks on shutdown...")
	if lockClient := po.config.GetLockClient(); lockClient != nil {
		if heartbeatClient, ok := lockClient.(storage.HeartbeatLockClient); ok {
			// Cleanup locks that have expired (TTL passed)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := heartbeatClient.CleanupExpiredLocks(ctx); err != nil {
				log.Warnf("Failed to cleanup expired locks on shutdown: %v", err)
			} else {
				log.Info("Expired lock cleanup completed on shutdown")
			}
		}
	}

	po.running = false
	log.Info("Processing orchestrator stopped")

	return nil
}

// ScheduleDatasetProcessing schedules a dataset for processing
func (po *ProcessingOrchestrator) ScheduleDatasetProcessing(tenantID, datasetID string, priority int) error {
	job := &SpoolJob{
		TenantID:    tenantID,
		DatasetID:   datasetID,
		JobType:     JobTypeDatasetProcessing,
		Priority:    priority,
		MaxAttempts: 0, // Infinite retry - "all or nothing" approach
		Payload: map[string]interface{}{
			"tenant_id":  tenantID,
			"dataset_id": datasetID,
		},
	}

	if err := po.spoolManager.EnqueueJob(job); err != nil {
		return fmt.Errorf("failed to enqueue dataset processing job: %w", err)
	}

	log.Infof("Scheduled dataset processing for tenant %s, dataset %s", tenantID, datasetID)
	return nil
}

// GetProcessingStats returns current processing statistics
func (po *ProcessingOrchestrator) GetProcessingStats() (map[string]interface{}, error) {
	spoolStats, err := po.spoolManager.GetStats()
	if err != nil {
		return nil, fmt.Errorf("failed to get spool stats: %w", err)
	}

	stats := map[string]interface{}{
		"spool":     spoolStats,
		"timestamp": time.Now(),
	}

	if po.uploadWorkerPool != nil {
		uploadStats := po.uploadWorkerPool.GetStats()
		stats["upload_pool"] = uploadStats
	}

	return stats, nil
}

// mainProcessingLoop handles jobs from the main queue
func (po *ProcessingOrchestrator) mainProcessingLoop() {
	defer po.wg.Done()
	log.Debug("Started main processing loop")

	ticker := time.NewTicker(5 * time.Second) // Check for new jobs every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case <-po.ctx.Done():
			log.Debug("Main processing loop stopping")
			return
		case <-ticker.C:
			po.processNextJob()
		}
	}
}

// retryProcessingLoop handles jobs that need to be retried
func (po *ProcessingOrchestrator) retryProcessingLoop() {
	defer po.wg.Done()
	log.Debug("Started retry processing loop")

	ticker := time.NewTicker(30 * time.Second) // Check for retry jobs every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-po.ctx.Done():
			log.Debug("Retry processing loop stopping")
			return
		case <-ticker.C:
			po.processRetryJobs()
		}
	}
}

// housekeepingLoop performs maintenance tasks
func (po *ProcessingOrchestrator) housekeepingLoop() {
	defer po.wg.Done()
	log.Debug("Started housekeeping loop")

	ticker := time.NewTicker(1 * time.Hour) // Run housekeeping every hour
	defer ticker.Stop()

	for {
		select {
		case <-po.ctx.Done():
			log.Debug("Housekeeping loop stopping")
			return
		case <-ticker.C:
			po.performHousekeeping()
		}
	}
}

// monitoringLoop provides periodic monitoring and alerting
func (po *ProcessingOrchestrator) monitoringLoop() {
	defer po.wg.Done()
	log.Debug("Started monitoring loop")

	ticker := time.NewTicker(5 * time.Minute) // Monitor every 5 minutes
	defer ticker.Stop()

	for {
		select {
		case <-po.ctx.Done():
			log.Debug("Monitoring loop stopping")
			return
		case <-ticker.C:
			po.performMonitoring()
		}
	}
}

// processNextJob processes the next available job from the queue
func (po *ProcessingOrchestrator) processNextJob() {
	job, err := po.spoolManager.GetNextJob(po.ctx)
	if err != nil {
		log.Errorf("Failed to get next job: %v", err)
		return
	}

	if job == nil {
		return // No jobs available
	}

	// Validate job before processing
	if validationErr := po.spoolManager.ValidateJob(job); validationErr != nil {
		log.Warnf("Job %s failed validation: %v", job.ID, validationErr)
		if moveErr := po.spoolManager.MoveToDLQ(job, validationErr.Error()); moveErr != nil {
			log.Errorf("Failed to move invalid job %s to DLQ: %v", job.ID, moveErr)
		}
		return
	}

	log.Infof("Processing job %s (type: %s, tenant: %s, dataset: %s)",
		job.ID, job.JobType, job.TenantID, job.DatasetID)

	// Process the job based on its type
	var processErr error
	switch job.JobType {
	case JobTypeDatasetProcessing:
		processErr = po.processDatasetJob(job)
	case JobTypeParquetConversion:
		processErr = po.processParquetJob(job)
	case JobTypeUpload:
		processErr = po.processUploadJob(job)
	default:
		processErr = fmt.Errorf("unknown job type: %s", job.JobType)
	}

	// Handle job result
	if processErr != nil {
		// Check if this is a lock acquisition failure
		if isLockAcquisitionFailure(processErr) {
			log.Infof("Job %s failed due to lock acquisition (dataset already locked by another process), will retry later: %v", job.ID, processErr)
			// For lock failures, reset attempts and queue for retry with longer backoff
			// This allows other nodes to potentially acquire the lock
			job.Attempts = 0 // Reset attempts - lock conflicts shouldn't count as failures
			if retryErr := po.spoolManager.RequeueForRetry(job, "lock acquisition conflict - another process has the lock"); retryErr != nil {
				log.Errorf("Failed to requeue job %s for lock retry: %v", job.ID, retryErr)
			}
		} else {
			log.Errorf("Job %s failed: %v", job.ID, processErr)
			if retryErr := po.spoolManager.RequeueForRetry(job, processErr.Error()); retryErr != nil {
				log.Errorf("Failed to requeue job %s for retry: %v", job.ID, retryErr)
			}
		}
	} else {
		log.Infof("Job %s completed successfully", job.ID)
		if completeErr := po.spoolManager.CompleteJob(job); completeErr != nil {
			log.Errorf("Failed to mark job %s as complete: %v", job.ID, completeErr)
		}
	}
}

// processRetryJobs handles jobs that are ready for retry
func (po *ProcessingOrchestrator) processRetryJobs() {
	retryJobs, err := po.spoolManager.GetRetryJobs(po.ctx)
	if err != nil {
		log.Errorf("Failed to get retry jobs: %v", err)
		return
	}

	for _, job := range retryJobs {
		log.Infof("Promoting retry job %s back to queue (attempt %d/%d)",
			job.ID, job.Attempts+1, job.MaxAttempts)

		if err := po.spoolManager.PromoteRetryJob(job); err != nil {
			log.Errorf("Failed to promote retry job %s: %v", job.ID, err)
		}
	}
}

// performHousekeeping runs maintenance tasks
func (po *ProcessingOrchestrator) performHousekeeping() {
	log.Debug("Running housekeeping tasks")

	// Scan for and move malformed jobs to DLQ
	if moved, err := po.spoolManager.ScanAndMoveMalformedJobs(); err != nil {
		log.Errorf("Failed to scan for malformed jobs: %v", err)
	} else if moved > 0 {
		log.Infof("Moved %d malformed jobs to DLQ during housekeeping", moved)
	}

	// Cleanup old DLQ jobs (keep for 7 days)
	if err := po.spoolManager.CleanupOldJobs(7 * 24 * time.Hour); err != nil {
		log.Errorf("Failed to cleanup old DLQ jobs: %v", err)
	}

	// Additional housekeeping tasks can be added here
	log.Debug("Housekeeping tasks completed")
}

// performMonitoring checks system health and sends alerts if needed
func (po *ProcessingOrchestrator) performMonitoring() {
	stats, err := po.GetProcessingStats()
	if err != nil {
		log.Errorf("Failed to get processing stats for monitoring: %v", err)
		return
	}

	spoolStats, ok := stats["spool"].(map[string]int)
	if !ok {
		return
	}

	// Alert if retry queue is getting too large
	if retryCount := spoolStats["retry"]; retryCount > 100 {
		if po.config.SOCAlertClient != nil {
			alertErr := po.config.SOCAlertClient.SendCriticalAlert(
				"ByteFreezer Packer Retry Queue Alert",
				fmt.Sprintf("Retry queue has %d jobs", retryCount),
				"High number of retrying jobs may indicate a system issue",
			)
			if alertErr != nil {
				log.Errorf("Failed to send retry queue alert: %v", alertErr)
			}
		}
	}

	// Alert if DLQ is accumulating invalid jobs
	if dlqCount := spoolStats["dlq"]; dlqCount > 50 {
		if po.config.SOCAlertClient != nil {
			alertErr := po.config.SOCAlertClient.SendCriticalAlert(
				"ByteFreezer Packer DLQ Alert",
				fmt.Sprintf("Dead letter queue has %d invalid jobs", dlqCount),
				"High number of invalid jobs may indicate data quality or configuration issues",
			)
			if alertErr != nil {
				log.Errorf("Failed to send DLQ alert: %v", alertErr)
			}
		}
	}

	log.Debugf("Monitoring stats: %+v", spoolStats)
}

// Job processing methods

func (po *ProcessingOrchestrator) processDatasetJob(job *SpoolJob) error {
	tenantID := job.TenantID
	datasetID := job.DatasetID

	// Use the tenant processor to handle the dataset
	return po.tenantProcessor.ProcessSingleDataset(po.ctx, tenantID, datasetID)
}

func (po *ProcessingOrchestrator) processParquetJob(job *SpoolJob) error {
	// Placeholder for Parquet-specific processing
	return fmt.Errorf("parquet job processing not implemented yet")
}

func (po *ProcessingOrchestrator) processUploadJob(job *SpoolJob) error {
	// Placeholder for upload job processing
	return fmt.Errorf("upload job processing not implemented yet")
}

// isLockAcquisitionFailure checks if an error is due to lock acquisition failure
func isLockAcquisitionFailure(err error) bool {
	if err == nil {
		return false
	}

	errorStr := strings.ToLower(err.Error())
	// Check for lock-related error messages
	return strings.Contains(errorStr, "already locked") ||
		strings.Contains(errorStr, "lock acquisition failed") ||
		strings.Contains(errorStr, "dataset locked") ||
		strings.Contains(errorStr, "failed to acquire lock")
}
