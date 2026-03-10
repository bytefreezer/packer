// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"fmt"
	"github.com/bytedance/sonic"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/config"
)

// SpoolJob represents a job that can be spooled to disk
type SpoolJob struct {
	ID           string                 `json:"id"`
	TenantID     string                 `json:"tenant_id"`
	DatasetID    string                 `json:"dataset_id"`
	JobType      string                 `json:"job_type"`
	Payload      map[string]interface{} `json:"payload"`
	CreatedAt    time.Time              `json:"created_at"`
	LastAttempt  time.Time              `json:"last_attempt"`
	Attempts     int                    `json:"attempts"`
	MaxAttempts  int                    `json:"max_attempts"`
	Priority     int                    `json:"priority"` // Higher number = higher priority
	RetryAfter   time.Time              `json:"retry_after"`
	ErrorMessage string                 `json:"error_message,omitempty"`
}

// SpoolManager handles persistent job queuing with infinite retry and DLQ for invalid jobs
type SpoolManager struct {
	config   *config.Config
	basePath string
	queueDir string
	retryDir string
	dlqDir   string
}

const (
	QueueDirName  = "queue"
	RetryDirName  = "retry"
	DLQDirName    = "dlq"
	JobExtension  = ".job"
	MetaExtension = ".meta"
)

// NewSpoolManager creates a new spool manager
func NewSpoolManager(cfg *config.Config) (*SpoolManager, error) {
	basePath := cfg.Bytefreezer.SpoolPath
	if basePath == "" {
		return nil, fmt.Errorf("spool path not configured")
	}

	sm := &SpoolManager{
		config:   cfg,
		basePath: basePath,
		queueDir: filepath.Join(basePath, QueueDirName),
		retryDir: filepath.Join(basePath, RetryDirName),
		dlqDir:   filepath.Join(basePath, DLQDirName),
	}

	// Create directories
	if err := sm.createDirectories(); err != nil {
		return nil, fmt.Errorf("failed to create spool directories: %w", err)
	}

	return sm, nil
}

// createDirectories ensures all spool directories exist
func (sm *SpoolManager) createDirectories() error {
	dirs := []string{sm.queueDir, sm.retryDir, sm.dlqDir}

	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0750); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	log.Infof("Created spool directories at %s (queue, retry, dlq)", sm.basePath)
	return nil
}

// EnqueueJob adds a new job to the queue
func (sm *SpoolManager) EnqueueJob(job *SpoolJob) error {
	if job.ID == "" {
		job.ID = generateJobID(job.TenantID, job.DatasetID, job.JobType)
	}

	job.CreatedAt = time.Now()
	job.LastAttempt = time.Time{} // Not attempted yet
	job.Attempts = 0

	// No max attempts limit - infinite retry for "all or nothing" approach

	return sm.writeJobToDir(job, sm.queueDir)
}

// GetNextJob retrieves the next job from queue, ordered by priority and age
func (sm *SpoolManager) GetNextJob(ctx context.Context) (*SpoolJob, error) {
	jobs, err := sm.listJobsInDir(sm.queueDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list queue jobs: %w", err)
	}

	if len(jobs) == 0 {
		return nil, nil // No jobs available
	}

	// Sort by priority (higher first), then by creation time (older first)
	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].Priority != jobs[j].Priority {
			return jobs[i].Priority > jobs[j].Priority
		}
		return jobs[i].CreatedAt.Before(jobs[j].CreatedAt)
	})

	job := jobs[0]

	// Remove from queue
	if err := sm.removeJobFromDir(job.ID, sm.queueDir); err != nil {
		return nil, fmt.Errorf("failed to remove job from queue: %w", err)
	}

	return job, nil
}

// RequeueForRetry moves a failed job to retry directory with backoff
func (sm *SpoolManager) RequeueForRetry(job *SpoolJob, errorMsg string) error {
	job.Attempts++
	job.LastAttempt = time.Now()
	job.ErrorMessage = errorMsg

	// Calculate exponential backoff: 2^attempts * 30 seconds
	backoffSeconds := 30 * (1 << job.Attempts) // 30s, 60s, 120s, 240s...
	if backoffSeconds > 1800 {                 // Cap at 30 minutes
		backoffSeconds = 1800
	}
	job.RetryAfter = time.Now().Add(time.Duration(backoffSeconds) * time.Second)

	// Infinite retry - no DLQ, "all or nothing" approach
	// Different nodes will keep retrying until success or destination is marked as down
	log.Infof("Job %s failed (attempt %d), scheduling retry at %v",
		job.ID, job.Attempts, job.RetryAfter)
	return sm.writeJobToDir(job, sm.retryDir)
}

// GetRetryJobs returns jobs ready for retry
func (sm *SpoolManager) GetRetryJobs(ctx context.Context) ([]*SpoolJob, error) {
	jobs, err := sm.listJobsInDir(sm.retryDir)
	if err != nil {
		return nil, fmt.Errorf("failed to list retry jobs: %w", err)
	}

	var readyJobs []*SpoolJob
	now := time.Now()

	for _, job := range jobs {
		if now.After(job.RetryAfter) {
			readyJobs = append(readyJobs, job)
		}
	}

	// Sort by priority and retry time
	sort.Slice(readyJobs, func(i, j int) bool {
		if readyJobs[i].Priority != readyJobs[j].Priority {
			return readyJobs[i].Priority > readyJobs[j].Priority
		}
		return readyJobs[i].RetryAfter.Before(readyJobs[j].RetryAfter)
	})

	return readyJobs, nil
}

// PromoteRetryJob moves a retry job back to the main queue
func (sm *SpoolManager) PromoteRetryJob(job *SpoolJob) error {
	// Remove from retry directory
	if err := sm.removeJobFromDir(job.ID, sm.retryDir); err != nil {
		return fmt.Errorf("failed to remove job from retry dir: %w", err)
	}

	// Add to queue directory
	return sm.writeJobToDir(job, sm.queueDir)
}

// CompleteJob removes a successfully completed job
func (sm *SpoolManager) CompleteJob(job *SpoolJob) error {
	log.Debugf("Completing job %s", job.ID)
	// Job might be in queue dir if it was being processed
	// Just ensure it's removed (ignore errors if not found)
	if err := sm.removeJobFromDir(job.ID, sm.queueDir); err != nil {
		log.Debugf("Job %s not found in queue dir (this is normal): %v", job.ID, err)
	}
	if err := sm.removeJobFromDir(job.ID, sm.retryDir); err != nil {
		log.Debugf("Job %s not found in retry dir (this is normal): %v", job.ID, err)
	}
	return nil
}

// DLQ system removed - using "all or nothing" approach with infinite retry

// GetStats returns statistics about the spool system
func (sm *SpoolManager) GetStats() (map[string]int, error) {
	stats := make(map[string]int)

	queueJobs, err := sm.listJobsInDir(sm.queueDir)
	if err != nil {
		return nil, fmt.Errorf("failed to count queue jobs: %w", err)
	}
	stats["queue"] = len(queueJobs)

	retryJobs, err := sm.listJobsInDir(sm.retryDir)
	if err != nil {
		return nil, fmt.Errorf("failed to count retry jobs: %w", err)
	}
	stats["retry"] = len(retryJobs)

	dlqJobs, err := sm.listJobsInDir(sm.dlqDir)
	if err != nil {
		return nil, fmt.Errorf("failed to count DLQ jobs: %w", err)
	}
	stats["dlq"] = len(dlqJobs)

	// Count ready retries
	now := time.Now()
	readyRetries := 0
	for _, job := range retryJobs {
		if now.After(job.RetryAfter) {
			readyRetries++
		}
	}
	stats["ready_retries"] = readyRetries

	return stats, nil
}

// CleanupOldJobs removes old completed/DLQ jobs
func (sm *SpoolManager) CleanupOldJobs(maxAge time.Duration) error {
	// Clean up old DLQ jobs (keep for maxAge, then remove)
	dlqJobs, err := sm.listJobsInDir(sm.dlqDir)
	if err != nil {
		log.Warnf("Failed to list DLQ jobs for cleanup: %v", err)
		return err
	}

	now := time.Now()
	removed := 0
	for _, job := range dlqJobs {
		if now.Sub(job.CreatedAt) > maxAge {
			if err := sm.removeJobFromDir(job.ID, sm.dlqDir); err != nil {
				log.Errorf("Failed to remove old DLQ job %s: %v", job.ID, err)
			} else {
				removed++
			}
		}
	}

	if removed > 0 {
		log.Infof("Cleaned up %d old DLQ jobs (older than %v)", removed, maxAge)
	}

	return nil
}

// ValidateJob checks if a job has valid data
func (sm *SpoolManager) ValidateJob(job *SpoolJob) error {
	// Check for literal "string" values (test data)
	if job.TenantID == "string" || job.DatasetID == "string" {
		return fmt.Errorf("invalid job: literal 'string' value detected (tenant_id=%s, dataset_id=%s)", job.TenantID, job.DatasetID)
	}

	// Check for empty required fields
	if job.TenantID == "" {
		return fmt.Errorf("invalid job: tenant_id is empty")
	}
	if job.DatasetID == "" {
		return fmt.Errorf("invalid job: dataset_id is empty")
	}
	if job.JobType == "" {
		return fmt.Errorf("invalid job: job_type is empty")
	}

	// Check if tenant exists in database
	if sm.config.TenantDatabase != nil {
		tenant := sm.config.TenantDatabase.GetTenantByID(job.TenantID)
		if tenant == nil {
			return fmt.Errorf("invalid job: tenant '%s' not found in database", job.TenantID)
		}

		// Check if dataset exists for this tenant
		datasetExists := false
		for _, dataset := range tenant.Datasets {
			if dataset.ID == job.DatasetID {
				datasetExists = true
				break
			}
		}
		if !datasetExists {
			return fmt.Errorf("invalid job: dataset '%s' not found for tenant '%s'", job.DatasetID, job.TenantID)
		}
	}

	return nil
}

// MoveToDLQ moves an invalid job to the dead letter queue with metadata
func (sm *SpoolManager) MoveToDLQ(job *SpoolJob, reason string) error {
	job.ErrorMessage = reason
	job.LastAttempt = time.Now()

	// Write the job to DLQ
	if err := sm.writeJobToDir(job, sm.dlqDir); err != nil {
		return fmt.Errorf("failed to write job to DLQ: %w", err)
	}

	// Create metadata file with additional context
	metadata := map[string]interface{}{
		"moved_to_dlq_at": time.Now().Format(time.RFC3339),
		"reason":          reason,
		"job_id":          job.ID,
		"tenant_id":       job.TenantID,
		"dataset_id":      job.DatasetID,
		"job_type":        job.JobType,
		"attempts":        job.Attempts,
		"created_at":      job.CreatedAt.Format(time.RFC3339),
	}

	metaPath := filepath.Join(sm.dlqDir, job.ID+MetaExtension)
	metaData, err := sonic.MarshalIndent(metadata, "", "  ")
	if err != nil {
		log.Warnf("Failed to create DLQ metadata for job %s: %v", job.ID, err)
	} else {
		if err := os.WriteFile(metaPath, metaData, 0600); err != nil {
			log.Warnf("Failed to write DLQ metadata for job %s: %v", job.ID, err)
		}
	}

	// Remove from queue/retry directories
	sm.removeJobFromDir(job.ID, sm.queueDir)
	sm.removeJobFromDir(job.ID, sm.retryDir)

	log.Warnf("Moved invalid job %s to DLQ: %s", job.ID, reason)
	return nil
}

// GetDLQJobs returns all jobs in the dead letter queue
func (sm *SpoolManager) GetDLQJobs() ([]*SpoolJob, error) {
	return sm.listJobsInDir(sm.dlqDir)
}

// ScanAndMoveMalformedJobs scans all directories for malformed jobs and moves them to DLQ
func (sm *SpoolManager) ScanAndMoveMalformedJobs() (int, error) {
	moved := 0

	// Check queue directory
	queueJobs, err := sm.listJobsInDir(sm.queueDir)
	if err != nil {
		return moved, fmt.Errorf("failed to list queue jobs: %w", err)
	}

	for _, job := range queueJobs {
		if err := sm.ValidateJob(job); err != nil {
			if moveErr := sm.MoveToDLQ(job, err.Error()); moveErr != nil {
				log.Errorf("Failed to move invalid queue job %s to DLQ: %v", job.ID, moveErr)
			} else {
				moved++
			}
		}
	}

	// Check retry directory
	retryJobs, err := sm.listJobsInDir(sm.retryDir)
	if err != nil {
		return moved, fmt.Errorf("failed to list retry jobs: %w", err)
	}

	for _, job := range retryJobs {
		if err := sm.ValidateJob(job); err != nil {
			if moveErr := sm.MoveToDLQ(job, err.Error()); moveErr != nil {
				log.Errorf("Failed to move invalid retry job %s to DLQ: %v", job.ID, moveErr)
			} else {
				moved++
			}
		}
	}

	if moved > 0 {
		log.Infof("Moved %d malformed jobs to DLQ", moved)
	}

	return moved, nil
}

// Helper functions

func (sm *SpoolManager) writeJobToDir(job *SpoolJob, dir string) error {
	jobPath := filepath.Join(dir, job.ID+JobExtension)

	data, err := sonic.MarshalIndent(job, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Write atomically with temp file
	tempPath := jobPath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write job file: %w", err)
	}

	if err := os.Rename(tempPath, jobPath); err != nil {
		if removeErr := os.Remove(tempPath); removeErr != nil {
			log.Errorf("Failed to cleanup temp file %s: %v", tempPath, removeErr)
		}
		return fmt.Errorf("failed to rename job file: %w", err)
	}

	return nil
}

func (sm *SpoolManager) listJobsInDir(dir string) ([]*SpoolJob, error) {
	var jobs []*SpoolJob

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, JobExtension) {
			return nil
		}

		job, loadErr := sm.loadJobFromFile(path)
		if loadErr != nil {
			log.Errorf("Failed to load job from %s: %v", path, loadErr)
			return nil // Continue processing other jobs
		}

		jobs = append(jobs, job)
		return nil
	})

	return jobs, err
}

func (sm *SpoolManager) loadJobFromFile(filePath string) (*SpoolJob, error) {
	// Validate that the file path is within one of our spool directories
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	validDirs := []string{sm.queueDir, sm.retryDir, sm.dlqDir}
	isValid := false
	for _, dir := range validDirs {
		absDir, err := filepath.Abs(dir)
		if err != nil {
			continue
		}
		if strings.HasPrefix(absPath, absDir+string(filepath.Separator)) || absPath == absDir {
			isValid = true
			break
		}
	}

	if !isValid {
		return nil, fmt.Errorf("file path is outside of allowed spool directories")
	}

	data, err := os.ReadFile(filepath.Clean(filePath)) // #nosec G304 - path is validated above
	if err != nil {
		return nil, fmt.Errorf("failed to read job file: %w", err)
	}

	var job SpoolJob
	if err := sonic.Unmarshal(data, &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	return &job, nil
}

func (sm *SpoolManager) removeJobFromDir(jobID, dir string) error {
	jobPath := filepath.Join(dir, jobID+JobExtension)
	err := os.Remove(jobPath)
	if os.IsNotExist(err) {
		return nil // Already removed, that's fine
	}
	return err
}

// HasJobForDataset checks if a job for the given tenant/dataset already exists
// in either the queue or retry directory. Used for deduplication.
func (sm *SpoolManager) HasJobForDataset(tenantID, datasetID string) bool {
	prefix := tenantID + "_" + datasetID + "_"
	for _, dir := range []string{sm.queueDir, sm.retryDir} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
				return true
			}
		}
	}
	return false
}

// generateJobID creates a unique job ID
func generateJobID(tenantID, datasetID, jobType string) string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("%s_%s_%s_%d", tenantID, datasetID, jobType, timestamp)
}
