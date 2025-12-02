// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/bytefreezer/goodies/log"
)

// ActivityReporter reports operation progress to control service
type ActivityReporter struct {
	controlURL string
	apiKey     string
	instanceID string
	client     *http.Client

	// In-memory tracking of current operations
	operations map[string]*OperationProgress
	mutex      sync.RWMutex
}

// OperationProgress tracks a single operation's progress
type OperationProgress struct {
	OperationID      string
	TenantID         string
	DatasetID        string
	OperationType    string
	ProgressCurrent  int64
	ProgressTotal    int64
	ProgressUnit     string
	ProgressMessage  string
	InputBytes       int64
	OutputBytes      int64
	RecordsProcessed int64
	ErrorCount       int
	StartedAt        time.Time
	LastReported     time.Time
}

// OperationUpdate is sent to control service
type OperationUpdate struct {
	OperationID      string                 `json:"operation_id"`
	ServiceType      string                 `json:"service_type"`
	InstanceID       string                 `json:"instance_id"`
	AccountID        string                 `json:"account_id,omitempty"`
	TenantID         string                 `json:"tenant_id,omitempty"`
	DatasetID        string                 `json:"dataset_id,omitempty"`
	OperationType    string                 `json:"operation_type"`
	Status           string                 `json:"status"`
	ProgressCurrent  int64                  `json:"progress_current,omitempty"`
	ProgressTotal    int64                  `json:"progress_total,omitempty"`
	ProgressUnit     string                 `json:"progress_unit,omitempty"`
	ProgressMessage  string                 `json:"progress_message,omitempty"`
	InputBytes       int64                  `json:"input_bytes,omitempty"`
	OutputBytes      int64                  `json:"output_bytes,omitempty"`
	RecordsProcessed int64                  `json:"records_processed,omitempty"`
	ErrorCount       int                    `json:"error_count"`
	Details          map[string]interface{} `json:"details,omitempty"`
	StartedAt        *time.Time             `json:"started_at,omitempty"`
}

// NewActivityReporter creates a new activity reporter
func NewActivityReporter(controlURL, apiKey, instanceID string) *ActivityReporter {
	return &ActivityReporter{
		controlURL: controlURL,
		apiKey:     apiKey,
		instanceID: instanceID,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		operations: make(map[string]*OperationProgress),
	}
}

// StartOperation registers a new operation
func (r *ActivityReporter) StartOperation(operationID, tenantID, datasetID, operationType string, total int64, unit string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.operations[operationID] = &OperationProgress{
		OperationID:   operationID,
		TenantID:      tenantID,
		DatasetID:     datasetID,
		OperationType: operationType,
		ProgressTotal: total,
		ProgressUnit:  unit,
		StartedAt:     time.Now(),
		LastReported:  time.Now(),
	}

	// Report immediately
	go r.reportOperation(operationID, "in_progress")
}

// UpdateProgress updates operation progress
func (r *ActivityReporter) UpdateProgress(operationID string, current int64, message string) {
	r.mutex.Lock()
	op, exists := r.operations[operationID]
	if !exists {
		r.mutex.Unlock()
		return
	}

	op.ProgressCurrent = current
	op.ProgressMessage = message

	// Report if it's been more than 30 seconds since last report
	shouldReport := time.Since(op.LastReported) > 30*time.Second
	r.mutex.Unlock()

	if shouldReport {
		r.reportOperation(operationID, "in_progress")
	}
}

// UpdateMetrics updates operation metrics (bytes, records)
func (r *ActivityReporter) UpdateMetrics(operationID string, inputBytes, outputBytes, records int64) {
	r.mutex.Lock()
	op, exists := r.operations[operationID]
	if !exists {
		r.mutex.Unlock()
		return
	}

	op.InputBytes = inputBytes
	op.OutputBytes = outputBytes
	op.RecordsProcessed = records
	r.mutex.Unlock()
}

// CompleteOperation marks operation as completed
func (r *ActivityReporter) CompleteOperation(operationID string, inputBytes, outputBytes, records int64) {
	r.mutex.Lock()
	op, exists := r.operations[operationID]
	if !exists {
		r.mutex.Unlock()
		return
	}

	op.InputBytes = inputBytes
	op.OutputBytes = outputBytes
	op.RecordsProcessed = records
	op.ProgressCurrent = op.ProgressTotal
	r.mutex.Unlock()

	r.reportOperation(operationID, "completed")

	// Clean up after reporting
	r.mutex.Lock()
	delete(r.operations, operationID)
	r.mutex.Unlock()
}

// FailOperation marks operation as failed
func (r *ActivityReporter) FailOperation(operationID string, errorMsg string) {
	r.mutex.Lock()
	op, exists := r.operations[operationID]
	if !exists {
		r.mutex.Unlock()
		return
	}

	op.ErrorCount++
	op.ProgressMessage = errorMsg
	r.mutex.Unlock()

	r.reportOperation(operationID, "failed")

	// Clean up after reporting
	r.mutex.Lock()
	delete(r.operations, operationID)
	r.mutex.Unlock()
}

// reportOperation sends operation update to control service
func (r *ActivityReporter) reportOperation(operationID, status string) {
	r.mutex.RLock()
	op, exists := r.operations[operationID]
	if !exists {
		r.mutex.RUnlock()
		return
	}

	// Create copy to avoid holding lock during HTTP request
	update := OperationUpdate{
		OperationID:      op.OperationID,
		ServiceType:      "packer",
		InstanceID:       r.instanceID,
		TenantID:         op.TenantID,
		DatasetID:        op.DatasetID,
		OperationType:    op.OperationType,
		Status:           status,
		ProgressCurrent:  op.ProgressCurrent,
		ProgressTotal:    op.ProgressTotal,
		ProgressUnit:     op.ProgressUnit,
		ProgressMessage:  op.ProgressMessage,
		InputBytes:       op.InputBytes,
		OutputBytes:      op.OutputBytes,
		RecordsProcessed: op.RecordsProcessed,
		ErrorCount:       op.ErrorCount,
		StartedAt:        &op.StartedAt,
	}

	op.LastReported = time.Now()
	r.mutex.RUnlock()

	// Send to control service
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.sendUpdate(ctx, &update); err != nil {
		log.Debugf("Failed to report operation %s: %v", operationID, err)
	} else {
		log.Debugf("Reported operation %s: %s (%d/%d %s)",
			operationID, status, update.ProgressCurrent, update.ProgressTotal, update.ProgressUnit)
	}
}

// sendUpdate sends update to control service
func (r *ActivityReporter) sendUpdate(ctx context.Context, update *OperationUpdate) error {
	url := r.controlURL + "/api/v1/activity/operations"

	jsonData, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("failed to marshal update: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+r.apiKey)

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}
