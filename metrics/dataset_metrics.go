package metrics

import (
	"bytes"
	"context"
	"fmt"
	"github.com/bytedance/sonic"
	"net/http"
	"time"

	"github.com/bytefreezer/goodies/log"
)

// DatasetMetricsClient handles sending dataset metrics to the control service
type DatasetMetricsClient struct {
	controlURL string
	apiKey     string
	httpClient *http.Client
	enabled    bool
}

// DatasetMetricRequest represents a metrics record request to control service
type DatasetMetricRequest struct {
	Component      string                 `json:"component"`
	InputBytes     int64                  `json:"input_bytes"`
	OutputBytes    int64                  `json:"output_bytes"`
	LinesProcessed int64                  `json:"lines_processed"`
	ErrorCount     int64                  `json:"error_count"`
	CustomMetrics  map[string]interface{} `json:"custom_metrics,omitempty"`
}

// DatasetMetricResponse represents the response from recording a metric
type DatasetMetricResponse struct {
	Success    bool      `json:"success"`
	MetricID   int64     `json:"metric_id"`
	RecordedAt time.Time `json:"recorded_at"`
}

// NewDatasetMetricsClient creates a new dataset metrics client
func NewDatasetMetricsClient(controlURL string, apiKey string, timeoutSeconds int, enabled bool) *DatasetMetricsClient {
	if timeoutSeconds <= 0 {
		timeoutSeconds = 5
	}

	return &DatasetMetricsClient{
		controlURL: controlURL,
		apiKey:     apiKey,
		httpClient: &http.Client{
			Timeout: time.Duration(timeoutSeconds) * time.Second,
		},
		enabled: enabled,
	}
}

// RecordMetric sends dataset metrics to the control service
// This is called after successful data packing
func (c *DatasetMetricsClient) RecordMetric(ctx context.Context, tenantID, datasetID string,
	inputBytes, outputBytes, linesProcessed, errorCount int64,
	customMetrics map[string]interface{}) error {

	if !c.enabled {
		log.Debug("Dataset metrics recording is disabled")
		return nil
	}

	// Build the metrics request
	metricReq := DatasetMetricRequest{
		Component:      "packer-to-storage",
		InputBytes:     inputBytes,
		OutputBytes:    outputBytes,
		LinesProcessed: linesProcessed,
		ErrorCount:     errorCount,
		CustomMetrics:  customMetrics,
	}

	reqBody, err := sonic.Marshal(metricReq)
	if err != nil {
		return fmt.Errorf("failed to marshal metrics request: %w", err)
	}

	// Build the URL - matches control service API
	url := fmt.Sprintf("%s/api/v1/tenants/%s/datasets/%s/metrics",
		c.controlURL, tenantID, datasetID)

	log.Infof("Sending dataset metrics to %s: component=%s, input=%d, output=%d, lines=%d",
		url, metricReq.Component, inputBytes, outputBytes, linesProcessed)
	log.Debugf("Dataset metrics request body: %s", string(reqBody))

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		log.Errorf("Failed to create metrics request: %v", err)
		return fmt.Errorf("failed to create metrics request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		// Don't fail the packing request if metrics recording fails
		log.Errorf("Failed to send dataset metrics to control service at %s: %v", url, err)
		return nil // Return nil so packing request still succeeds
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Dataset metrics recording failed with status %d for %s/%s at URL %s",
			resp.StatusCode, tenantID, datasetID, url)
		return nil // Return nil so packing request still succeeds
	}

	var metricResp DatasetMetricResponse
	if err := sonic.ConfigDefault.NewDecoder(resp.Body).Decode(&metricResp); err != nil {
		log.Warnf("Failed to decode metrics response: %v", err)
		return nil
	}

	if metricResp.Success {
		log.Debugf("Successfully recorded dataset metrics for %s/%s (metric_id: %d)",
			tenantID, datasetID, metricResp.MetricID)
	}

	return nil
}
