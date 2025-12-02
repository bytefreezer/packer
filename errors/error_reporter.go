// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package errors

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bytedance/sonic"
	"net/http"
	"sync"
	"time"

	"github.com/bytefreezer/goodies/log"
)

// ErrorReporter handles reporting errors to the control service with self-healing
type ErrorReporter struct {
	controlURL   string
	apiKey       string
	httpClient   *http.Client
	component    string
	enabled      bool
	queue        []ErrorReport
	queueMutex   sync.RWMutex
	maxQueueSize int
	retryTicker  *time.Ticker
	stopChan     chan struct{}
}

// ErrorReport represents an error to be reported
type ErrorReport struct {
	ErrorType    string                 `json:"error_type"`
	Component    string                 `json:"component"`
	TenantID     string                 `json:"tenant_id,omitempty"`
	DatasetID    string                 `json:"dataset_id,omitempty"`
	ErrorMessage string                 `json:"error_message"`
	ErrorSample  map[string]interface{} `json:"error_sample,omitempty"`
	Severity     string                 `json:"severity,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// NewErrorReporter creates a new error reporter with self-healing retry logic
func NewErrorReporter(controlURL, apiKey, component string, enabled bool) *ErrorReporter {
	er := &ErrorReporter{
		controlURL:   controlURL,
		apiKey:       apiKey,
		component:    component,
		enabled:      enabled,
		maxQueueSize: 1000, // Keep up to 1000 errors in queue
		stopChan:     make(chan struct{}),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	// Start background retry worker if enabled
	if enabled {
		er.retryTicker = time.NewTicker(30 * time.Second)
		go er.retryWorker()
		log.Info("Error reporter started with self-healing retry")
	}

	return er
}

// Stop stops the error reporter retry worker
func (er *ErrorReporter) Stop() {
	if er.retryTicker != nil {
		er.retryTicker.Stop()
		close(er.stopChan)
		log.Info("Error reporter stopped")
	}
}

// retryWorker periodically retries queued errors
func (er *ErrorReporter) retryWorker() {
	for {
		select {
		case <-er.stopChan:
			return
		case <-er.retryTicker.C:
			er.processQueue()
		}
	}
}

// processQueue attempts to send all queued errors
func (er *ErrorReporter) processQueue() {
	er.queueMutex.Lock()
	defer er.queueMutex.Unlock()

	if len(er.queue) == 0 {
		return
	}

	log.Infof("Processing error queue: %d errors pending", len(er.queue))

	// Try to send each error
	var remaining []ErrorReport
	successCount := 0

	for _, report := range er.queue {
		err := er.sendError(context.Background(), report)
		if err != nil {
			// Keep in queue for next retry
			remaining = append(remaining, report)
		} else {
			successCount++
		}
	}

	er.queue = remaining

	if successCount > 0 {
		log.Infof("Successfully sent %d queued errors, %d remaining", successCount, len(remaining))
	}
	if len(remaining) > 0 {
		log.Warnf("Failed to send %d errors, will retry later", len(remaining))
	}
}

// ReportError reports an error to the control service with self-healing
// If control service is unavailable, the error is queued for later retry
func (er *ErrorReporter) ReportError(ctx context.Context, report ErrorReport) error {
	if !er.enabled {
		log.Debug("Error reporting is disabled, skipping report")
		return nil
	}

	// Set component from reporter if not specified
	if report.Component == "" {
		report.Component = er.component
	}

	// Default severity to "error" if not specified
	if report.Severity == "" {
		report.Severity = "error"
	}

	// Try to send immediately
	err := er.sendError(ctx, report)
	if err != nil {
		// Control service unavailable - queue for later retry
		er.queueError(report)
		log.Warnf("Failed to report error, queued for retry: %v", err)
		// Return nil - don't fail the calling service
		return nil
	}

	return nil
}

// sendError sends an error to the control service (internal method)
func (er *ErrorReporter) sendError(ctx context.Context, report ErrorReport) error {
	// Marshal the report
	body, err := sonic.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to marshal error report: %w", err)
	}

	// Create the request
	url := fmt.Sprintf("%s/api/v1/errors", er.controlURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create error report request: %w", err)
	}

	// Add headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", er.apiKey))

	// Send the request
	resp, err := er.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send error report: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error report failed with status %d", resp.StatusCode)
	}

	log.Debugf("Error reported successfully: type=%s, component=%s", report.ErrorType, report.Component)
	return nil
}

// queueError adds an error to the retry queue
func (er *ErrorReporter) queueError(report ErrorReport) {
	er.queueMutex.Lock()
	defer er.queueMutex.Unlock()

	// Check queue size limit
	if len(er.queue) >= er.maxQueueSize {
		// Drop oldest error
		log.Warnf("Error queue full (%d), dropping oldest error", er.maxQueueSize)
		er.queue = er.queue[1:]
	}

	er.queue = append(er.queue, report)
	log.Debugf("Error queued for retry: %s (queue size: %d)", report.ErrorType, len(er.queue))
}

// ReportErrorSimple is a convenience method for reporting simple errors
func (er *ErrorReporter) ReportErrorSimple(ctx context.Context, errorType, errorMessage, severity string, tenantID, datasetID string) error {
	report := ErrorReport{
		ErrorType:    errorType,
		ErrorMessage: errorMessage,
		Severity:     severity,
		TenantID:     tenantID,
		DatasetID:    datasetID,
	}
	return er.ReportError(ctx, report)
}

// ReportErrorWithSample reports an error with a sample
func (er *ErrorReporter) ReportErrorWithSample(ctx context.Context, errorType, errorMessage, severity string, tenantID, datasetID string, sample map[string]interface{}) error {
	report := ErrorReport{
		ErrorType:    errorType,
		ErrorMessage: errorMessage,
		Severity:     severity,
		TenantID:     tenantID,
		DatasetID:    datasetID,
		ErrorSample:  sample,
	}
	return er.ReportError(ctx, report)
}

// ReportCritical reports a critical error
func (er *ErrorReporter) ReportCritical(ctx context.Context, errorType, errorMessage string, tenantID, datasetID string) error {
	return er.ReportErrorSimple(ctx, errorType, errorMessage, "critical", tenantID, datasetID)
}

// ReportWarning reports a warning
func (er *ErrorReporter) ReportWarning(ctx context.Context, errorType, errorMessage string, tenantID, datasetID string) error {
	return er.ReportErrorSimple(ctx, errorType, errorMessage, "warning", tenantID, datasetID)
}
