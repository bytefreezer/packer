package services

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/config"
	"github.com/bytefreezer/packer/domain"
	"github.com/bytefreezer/packer/storage"
)

type UploadWorkerPool struct {
	config               *config.Config
	s3DestinationManager *storage.S3DestinationManager
	metadataManager      MetadataManagerInterface
	workerCount          int
	uploadQueue          chan *UploadJob
	retryQueue           chan *UploadJob
	workers              []*UploadWorker
	ctx                  context.Context
	cancel               context.CancelFunc
	wg                   sync.WaitGroup
	stats                *UploadStats
	statsLock            sync.RWMutex
	metrics              *TenantMetrics
}

type UploadJob struct {
	ID          string
	TenantID    string
	DatasetID   string
	Tenant      *domain.Tenant
	Dataset     *domain.Dataset
	LocalFile   string
	RemoteKey   string
	ContentType string
	Metadata    map[string]string
	Attempts    int
	MaxAttempts int
	CreatedAt   time.Time
	LastAttempt time.Time
	ResultChan  chan *UploadResult
}

type UploadResult struct {
	Job        *UploadJob
	Success    bool
	Error      error
	FileSize   int64
	UploadTime time.Duration
	Location   string
}

type UploadWorker struct {
	id          int
	pool        *UploadWorkerPool
	uploadQueue <-chan *UploadJob
	retryQueue  <-chan *UploadJob
}

type UploadStats struct {
	TotalJobs      int64
	SuccessfulJobs int64
	FailedJobs     int64
	RetriedJobs    int64
	TotalBytes     int64
	AverageTime    time.Duration
	WorkersActive  int
}

func NewUploadWorkerPool(cfg *config.Config, s3DestinationManager *storage.S3DestinationManager, metadataManager MetadataManagerInterface, workerCount int, metrics *TenantMetrics) *UploadWorkerPool {
	if workerCount <= 0 {
		workerCount = 5 // Default worker count
	}

	ctx, cancel := context.WithCancel(context.Background())

	pool := &UploadWorkerPool{
		config:               cfg,
		s3DestinationManager: s3DestinationManager,
		metadataManager:      metadataManager,
		workerCount:          workerCount,
		uploadQueue:          make(chan *UploadJob, workerCount*10), // Buffer for jobs
		retryQueue:           make(chan *UploadJob, workerCount*5),  // Smaller retry buffer
		workers:              make([]*UploadWorker, workerCount),
		ctx:                  ctx,
		cancel:               cancel,
		stats: &UploadStats{
			WorkersActive: workerCount,
		},
		metrics: metrics,
	}

	// Initialize workers
	for i := 0; i < workerCount; i++ {
		pool.workers[i] = &UploadWorker{
			id:          i,
			pool:        pool,
			uploadQueue: pool.uploadQueue,
			retryQueue:  pool.retryQueue,
		}
	}

	return pool
}

// Start starts all workers in the pool
func (up *UploadWorkerPool) Start() {
	log.Infof("Starting upload worker pool with %d workers", up.workerCount)

	for i, worker := range up.workers {
		up.wg.Add(1)
		go worker.run(up.ctx, &up.wg)
		log.Debugf("Started upload worker %d", i)
	}

	// Start retry processor
	up.wg.Add(1)
	go up.retryProcessor(up.ctx, &up.wg)

	log.Info("Upload worker pool started successfully")
}

// Stop stops all workers and waits for them to finish
func (up *UploadWorkerPool) Stop() {
	log.Info("Stopping upload worker pool")

	up.cancel()
	close(up.uploadQueue)
	close(up.retryQueue)

	up.wg.Wait()
	log.Info("Upload worker pool stopped")
}

// SubmitJob submits an upload job to the pool
func (up *UploadWorkerPool) SubmitJob(job *UploadJob) error {
	select {
	case up.uploadQueue <- job:
		up.updateStats(func(s *UploadStats) {
			s.TotalJobs++
		})
		log.Debugf("Submitted upload job %s for tenant %s", job.ID, job.TenantID)
		return nil
	case <-up.ctx.Done():
		return fmt.Errorf("upload worker pool is shutting down")
	default:
		return fmt.Errorf("upload queue is full, try again later")
	}
}

// CreateUploadJob creates a new upload job
func (up *UploadWorkerPool) CreateUploadJob(tenant *domain.Tenant, localFile, remoteKey string) *UploadJob {
	return &UploadJob{
		ID:          fmt.Sprintf("%s-%d", tenant.ID, time.Now().UnixNano()),
		TenantID:    tenant.ID,
		Tenant:      tenant,
		LocalFile:   localFile,
		RemoteKey:   remoteKey,
		ContentType: "application/octet-stream", // Default, can be overridden
		Metadata:    make(map[string]string),
		MaxAttempts: 0, // Infinite retry - "all or nothing" approach
		CreatedAt:   time.Now(),
		ResultChan:  make(chan *UploadResult, 1),
	}
}

// CreateUploadJobForDataset creates a new upload job with dataset context
func (up *UploadWorkerPool) CreateUploadJobForDataset(tenant *domain.Tenant, dataset *domain.Dataset, localFile, remoteKey string) *UploadJob {
	return &UploadJob{
		ID:          fmt.Sprintf("%s-%s-%d", tenant.ID, dataset.ID, time.Now().UnixNano()),
		TenantID:    tenant.ID,
		DatasetID:   dataset.ID,
		Tenant:      tenant,
		Dataset:     dataset,
		LocalFile:   localFile,
		RemoteKey:   remoteKey,
		ContentType: "application/octet-stream", // Default, can be overridden
		Metadata:    make(map[string]string),
		MaxAttempts: 0, // Infinite retry - "all or nothing" approach
		CreatedAt:   time.Now(),
		ResultChan:  make(chan *UploadResult, 1),
	}
}

// GetStats returns current upload statistics
func (up *UploadWorkerPool) GetStats() *UploadStats {
	up.statsLock.RLock()
	defer up.statsLock.RUnlock()

	// Return a copy to avoid race conditions
	statsCopy := *up.stats
	return &statsCopy
}

// updateStats safely updates statistics
func (up *UploadWorkerPool) updateStats(updater func(*UploadStats)) {
	up.statsLock.Lock()
	defer up.statsLock.Unlock()
	updater(up.stats)
}

// retryProcessor handles failed uploads that need to be retried
func (up *UploadWorkerPool) retryProcessor(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Debug("Started retry processor")

	retryTimer := time.NewTicker(30 * time.Second) // Process retries every 30 seconds
	defer retryTimer.Stop()

	for {
		select {
		case job := <-up.retryQueue:
			if job == nil {
				continue
			}

			// Check if we should retry based on backoff
			backoffDuration := time.Duration(job.Attempts*job.Attempts) * 10 * time.Second
			if time.Since(job.LastAttempt) < backoffDuration {
				// Put it back in retry queue after a delay
				go func(j *UploadJob) {
					time.Sleep(backoffDuration - time.Since(j.LastAttempt))
					select {
					case up.retryQueue <- j:
					case <-ctx.Done():
					}
				}(job)
				continue
			}

			// Submit for retry
			select {
			case up.uploadQueue <- job:
				up.updateStats(func(s *UploadStats) {
					s.RetriedJobs++
				})
				log.Debugf("Resubmitted job %s for retry (attempt %d)", job.ID, job.Attempts+1)
			case <-ctx.Done():
				return
			}

		case <-retryTimer.C:
			// Periodic cleanup or monitoring can go here

		case <-ctx.Done():
			log.Debug("Retry processor stopping")
			return
		}
	}
}

// run executes the worker loop
func (w *UploadWorker) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	log.Debugf("Upload worker %d started", w.id)

	for {
		select {
		case job := <-w.uploadQueue:
			if job == nil {
				continue
			}
			w.processJob(ctx, job)

		case job := <-w.retryQueue:
			if job == nil {
				continue
			}
			w.processJob(ctx, job)

		case <-ctx.Done():
			log.Debugf("Upload worker %d stopping", w.id)
			return
		}
	}
}

// processJob processes a single upload job
func (w *UploadWorker) processJob(ctx context.Context, job *UploadJob) {
	startTime := time.Now()
	job.Attempts++
	job.LastAttempt = startTime

	log.Debugf("Worker %d processing job %s (attempt %d)", w.id, job.ID, job.Attempts)

	result := &UploadResult{
		Job: job,
	}

	// Get S3 client for this tenant:dataset destination
	s3Client, err := w.pool.s3DestinationManager.GetS3Client(job.TenantID, job.DatasetID, job.Dataset.S3Destination)
	if err != nil {
		// Check if destination is inactive due to repeated failures
		failureData := w.pool.s3DestinationManager.GetFailureData(job.TenantID, job.DatasetID)
		if !failureData.Active {
			// Destination is inactive - implement graceful error handling
			log.Warnf("Destination %s:%s is inactive due to repeated failures - deleting file and continuing",
				job.TenantID, job.DatasetID)

			// Delete the local file to prevent buildup
			if removeErr := os.Remove(job.LocalFile); removeErr != nil {
				log.Errorf("Failed to delete local file %s after destination failure: %v", job.LocalFile, removeErr)
			} else {
				log.Debugf("Successfully deleted local file %s after destination marked inactive", job.LocalFile)
			}

			// Send SOC alert about inactive destination
			if w.pool.config.SOCAlertClient != nil {
				alertErr := w.pool.config.SOCAlertClient.SendAlert(
					"WARN",
					"S3 Destination Inactive",
					fmt.Sprintf("Destination %s:%s is inactive after %d failures", job.TenantID, job.DatasetID, failureData.FailureCount),
					fmt.Sprintf("Last error: %s, File deleted: %s", failureData.LastError, job.LocalFile),
				)
				if alertErr != nil {
					log.Errorf("Failed to send inactive destination SOC alert: %v", alertErr)
				}
			}

			// Report critical error to control service - destination is now inactive
			if w.pool.config.ErrorReporter != nil {
				w.pool.config.ErrorReporter.ReportErrorSimple(
					ctx,
					"s3_destination_inactive",
					fmt.Sprintf("S3 destination inactive after %d failures. Last error: %s. Data is being dropped.", failureData.FailureCount, failureData.LastError),
					"critical",
					job.TenantID,
					job.DatasetID,
				)
			}

			// Return success to avoid retries - we've handled the error gracefully
			result.Success = true
			result.UploadTime = time.Since(startTime)
			result.Location = fmt.Sprintf("destination-inactive://%s:%s", job.TenantID, job.DatasetID)
			w.handleJobResult(ctx, job, result)
			return
		}

		// Destination is still active but failed to get client - this is a temporary error
		result.Error = fmt.Errorf("failed to get S3 client for tenant %s dataset %s: %w", job.TenantID, job.DatasetID, err)
		w.handleJobResult(ctx, job, result)
		return
	}

	// Open local file
	file, err := os.Open(job.LocalFile)
	if err != nil {
		result.Error = fmt.Errorf("failed to open local file: %w", err)
		w.handleJobResult(ctx, job, result)
		return
	}
	defer file.Close()

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		result.Error = fmt.Errorf("failed to get file info: %w", err)
		w.handleJobResult(ctx, job, result)
		return
	}
	result.FileSize = fileInfo.Size()

	// Prepare metadata
	metadata := map[string]string{
		"tenant-id":   job.TenantID,
		"uploaded-by": "bytefreezer-packer",
		"uploaded-at": startTime.UTC().Format(time.RFC3339),
		"source-file": filepath.Base(job.LocalFile),
	}

	// Add dataset metadata if available
	if job.Dataset != nil {
		metadata["dataset-id"] = job.DatasetID
		metadata["dataset-name"] = job.Dataset.Name
	}

	// Merge with any existing metadata
	for k, v := range job.Metadata {
		metadata[k] = v
	}

	// Perform upload using S3Client PutObject method
	err = s3Client.PutObject(ctx, job.RemoteKey, file, metadata)
	if err != nil {
		// Record failure in S3 destination manager
		errorMsg := fmt.Sprintf("S3 upload failed: %v", err)
		w.pool.s3DestinationManager.RecordFailure(job.TenantID, job.DatasetID, errorMsg)

		// Report error to control service for visibility in dashboard
		if w.pool.config.ErrorReporter != nil {
			w.pool.config.ErrorReporter.ReportErrorSimple(
				ctx,
				"s3_upload_failure",
				errorMsg,
				"error",
				job.TenantID,
				job.DatasetID,
			)
		}

		result.Error = fmt.Errorf("S3 upload failed: %w", err)
		w.handleJobResult(ctx, job, result)
		return
	}

	// Success
	result.Success = true
	result.UploadTime = time.Since(startTime)
	result.Location = fmt.Sprintf("s3://[bucket]/%s", job.RemoteKey) // Simplified location

	// Job completed successfully
	log.Debugf("Job %s completed successfully for tenant %s", job.ID, job.TenantID)

	// Update Parquet metadata files after successful upload (for Parquet files only)
	if job.Dataset != nil && job.Tenant != nil && strings.HasSuffix(strings.ToLower(job.RemoteKey), ".parquet") {
		if w.pool.metadataManager != nil {
			log.Debugf("Updating metadata files for tenant:%s dataset:%s after upload: %s", job.TenantID, job.DatasetID, job.RemoteKey)

			// Use background context with timeout for metadata update to avoid blocking upload pipeline
			metadataCtx, metadataCancel := context.WithTimeout(context.Background(), time.Minute*2)
			defer metadataCancel()

			if err := w.pool.metadataManager.UpdateMetadataAfterUpload(metadataCtx, job.Tenant, job.Dataset, job.RemoteKey); err != nil {
				// Log error but don't fail the upload - metadata update is best-effort
				log.Errorf("Failed to update metadata files for tenant:%s dataset:%s: %v", job.TenantID, job.DatasetID, err)
			} else {
				log.Debugf("Successfully updated metadata files for tenant:%s dataset:%s", job.TenantID, job.DatasetID)
			}
		}
	}

	w.handleJobResult(ctx, job, result)

	// Update stats
	w.pool.updateStats(func(s *UploadStats) {
		s.SuccessfulJobs++
		s.TotalBytes += result.FileSize
		// Update average time (simple moving average approximation)
		if s.SuccessfulJobs == 1 {
			s.AverageTime = result.UploadTime
		} else {
			s.AverageTime = (s.AverageTime + result.UploadTime) / 2
		}
	})

	// Record upload metrics
	if w.pool.metrics != nil {
		uploadType := "parquet"
		if job.Metadata != nil {
			if dataFormat, ok := job.Metadata["data-format"]; ok {
				uploadType = dataFormat
			}
		}

		if job.Dataset != nil {
			// Use dataset-aware metrics recording
			w.pool.metrics.RecordDatasetUpload(ctx, job.TenantID, job.Tenant.Name, job.DatasetID, job.Dataset.Name, uploadType, true, result.FileSize, result.UploadTime)
		} else {
			// Fallback to tenant-level metrics for backward compatibility
			w.pool.metrics.RecordUpload(ctx, job.TenantID, job.Tenant.Name, uploadType, true, result.FileSize, result.UploadTime)
		}
	}

	log.Infof("Worker %d successfully uploaded %s (%d bytes) to %s in %v",
		w.id, job.LocalFile, result.FileSize, result.Location, result.UploadTime)
}

// handleJobResult handles the result of a job (success or failure)
func (w *UploadWorker) handleJobResult(ctx context.Context, job *UploadJob, result *UploadResult) {
	if result.Success {
		// Send result back to caller
		select {
		case job.ResultChan <- result:
		default:
			// Channel might be closed or not being read
		}
		return
	}

	// Handle failure
	w.pool.updateStats(func(s *UploadStats) {
		s.FailedJobs++
	})

	// Record upload failure metrics
	if w.pool.metrics != nil {
		uploadType := "parquet"
		if job.Metadata != nil {
			if dataFormat, ok := job.Metadata["data-format"]; ok {
				uploadType = dataFormat
			}
		}
		fileSize := int64(0)
		if fileInfo, err := os.Stat(job.LocalFile); err == nil {
			fileSize = fileInfo.Size()
		}
		duration := time.Duration(0)
		if result != nil {
			duration = result.UploadTime
		}

		if job.Dataset != nil {
			// Use dataset-aware metrics recording
			w.pool.metrics.RecordDatasetUpload(ctx, job.TenantID, job.Tenant.Name, job.DatasetID, job.Dataset.Name, uploadType, false, fileSize, duration)
		} else {
			// Fallback to tenant-level metrics for backward compatibility
			w.pool.metrics.RecordUpload(ctx, job.TenantID, job.Tenant.Name, uploadType, false, fileSize, duration)
		}
	}

	log.Errorf("Worker %d failed to upload %s (attempt %d): %v",
		w.id, job.LocalFile, job.Attempts, result.Error)

	// Send SOC alert for upload retry
	if w.pool.config.SOCAlertClient != nil {
		alertErr := w.pool.config.SOCAlertClient.SendTenantUploadRetryAlert(
			job.TenantID, result.Error, job.Attempts, job.MaxAttempts, job.LocalFile,
		)
		if alertErr != nil {
			log.Errorf("Failed to send SOC alert for upload retry: %v", alertErr)
		}
	}

	// Infinite retry logic - "all or nothing" approach
	if job.MaxAttempts == 0 {
		// Infinite retry - queue for retry regardless of attempt count
		log.Infof("Queuing job %s for infinite retry (attempt %d)", job.ID, job.Attempts)
		select {
		case w.pool.retryQueue <- job:
		case <-w.pool.ctx.Done():
			// Send failure result if shutting down
			select {
			case job.ResultChan <- result:
			default:
			}
		}
		return
	}

	// Legacy finite retry logic (for backward compatibility if MaxAttempts > 0)
	if job.Attempts < job.MaxAttempts {
		log.Infof("Queuing job %s for retry (%d/%d)", job.ID, job.Attempts, job.MaxAttempts)
		select {
		case w.pool.retryQueue <- job:
		case <-w.pool.ctx.Done():
			// Send failure result if shutting down
			select {
			case job.ResultChan <- result:
			default:
			}
		}
	} else {
		// Max attempts reached, send failure result (only for finite retry jobs)
		log.Errorf("Job %s failed permanently after %d attempts", job.ID, job.Attempts)

		// TODO: Implement failure tracking with PostgreSQL
		// Increment tenant failure counter
		/*if w.pool.config.TenantFailureTracker != nil {
			thresholdReached, failureCount, err := w.pool.config.TenantFailureTracker.IncrementFailureCount(
				ctx, job.TenantID, result.Error.Error(),
			)
			if err != nil {
				log.Errorf("Failed to increment failure counter for tenant %s: %v", job.TenantID, err)
			} else {
				log.Infof("Incremented failure count for tenant %s to %d (threshold reached: %v)",
					job.TenantID, failureCount, thresholdReached)

				// Record tenant failure metrics
				if w.pool.metrics != nil {
					if job.Dataset != nil {
						w.pool.metrics.RecordDatasetFailure(ctx, job.TenantID, job.Tenant.Name, job.DatasetID, job.Dataset.Name, int(failureCount))
					} else {
						w.pool.metrics.RecordTenantFailure(ctx, job.TenantID, job.Tenant.Name, int(failureCount))
					}
				}

				// Disable tenant if threshold reached
				if thresholdReached {
					log.Warnf("Tenant %s has reached failure threshold (%d), attempting to disable",
						job.TenantID, failureCount)

					disableReason := fmt.Sprintf("Automatic disable after %d consecutive upload failures. Last error: %v",
						failureCount, result.Error)

					if disableErr := w.pool.config.TenantClient.DisableTenant(job.TenantID, disableReason); disableErr != nil {
						log.Errorf("Failed to disable tenant %s: %v", job.TenantID, disableErr)

						// Send critical alert about disable failure
						if w.pool.config.SOCAlertClient != nil {
							alertErr := w.pool.config.SOCAlertClient.SendCriticalAlert(
								"Failed to Disable Problematic Tenant",
								fmt.Sprintf("Tenant %s reached failure threshold but could not be disabled", job.TenantID),
								fmt.Sprintf("Disable error: %v, Original failure: %v", disableErr, result.Error),
							)
							if alertErr != nil {
								log.Errorf("Failed to send critical disable failure alert: %v", alertErr)
							}
						}
					} else {
						log.Infof("Successfully disabled tenant %s via controller", job.TenantID)

						// Send info alert about successful disable
						if w.pool.config.SOCAlertClient != nil {
							alertErr := w.pool.config.SOCAlertClient.SendAlert(
								"INFO",
								"Tenant Automatically Disabled",
								fmt.Sprintf("Tenant %s has been automatically disabled due to repeated failures", job.TenantID),
								fmt.Sprintf("Failure count: %d, Reason: %s", failureCount, disableReason),
							)
							if alertErr != nil {
								log.Errorf("Failed to send tenant disable info alert: %v", alertErr)
							}
						}
					}
				}
			}
		}*/

		// Send critical SOC alert for permanent failures
		if w.pool.config.SOCAlertClient != nil {
			alertErr := w.pool.config.SOCAlertClient.SendTenantUploadFailureAlert(
				job.TenantID, result.Error, job.MaxAttempts, job.LocalFile,
			)
			if alertErr != nil {
				log.Errorf("Failed to send critical SOC alert for permanent upload failure: %v", alertErr)
			}
		}

		select {
		case job.ResultChan <- result:
		default:
		}
	}
}
