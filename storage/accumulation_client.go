// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/bytefreezer/goodies/log"
)

// AccumulationClient calls Control API to manage accumulation state
type AccumulationClient struct {
	client  *http.Client
	baseURL string
	apiKey  string
}

// AccumulationClientConfig holds configuration for accumulation client
type AccumulationClientConfig struct {
	BaseURL        string
	APIKey         string
	TimeoutSeconds int
}

// AccumulationState represents the state returned from Control API
type AccumulationState struct {
	TenantID         string     `json:"tenant_id"`
	DatasetID        string     `json:"dataset_id"`
	AccumulatedBytes int64      `json:"accumulated_bytes"`
	FileCount        int        `json:"file_count"`
	OldestFileTime   *time.Time `json:"oldest_file_time,omitempty"`
	NewestFileTime   *time.Time `json:"newest_file_time,omitempty"`
	LastCheckedAt    *time.Time `json:"last_checked_at,omitempty"`
	LastFlushedAt    *time.Time `json:"last_flushed_at,omitempty"`
	FlushCount       int        `json:"flush_count"`

	// Computed fields from Control API
	ThresholdBytes   int64   `json:"threshold_bytes,omitempty"`
	ThresholdSeconds int     `json:"threshold_seconds,omitempty"`
	CompressionRatio float64 `json:"compression_ratio,omitempty"`
	ProgressPercent  float64 `json:"progress_percent,omitempty"`
	TimeUntilFlush   int     `json:"time_until_flush_seconds,omitempty"`
	ShouldFlush      bool    `json:"should_flush,omitempty"`
	FlushReason      string  `json:"flush_reason,omitempty"`
	EstimatedOutputMB float64 `json:"estimated_output_mb,omitempty"`
}

// NewAccumulationClient creates a new accumulation client
func NewAccumulationClient(conf *AccumulationClientConfig) (*AccumulationClient, error) {
	if conf.BaseURL == "" {
		return nil, fmt.Errorf("control service base_url is required")
	}

	timeout := time.Duration(conf.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &AccumulationClient{
		client: &http.Client{
			Timeout: timeout,
		},
		baseURL: conf.BaseURL,
		apiKey:  conf.APIKey,
	}, nil
}

// doRequest performs an HTTP request to the control service
func (c *AccumulationClient) doRequest(ctx context.Context, method, endpoint string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	reqURL := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, method, reqURL, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}

	return resp, nil
}

// UpdateState updates the accumulation state for a dataset and returns whether to flush
func (c *AccumulationClient) UpdateState(ctx context.Context, tenantID, datasetID string,
	accumulatedBytes int64, fileCount int, oldestFileTime, newestFileTime *time.Time) (*AccumulationState, error) {

	body := map[string]interface{}{
		"tenant_id":         tenantID,
		"dataset_id":        datasetID,
		"accumulated_bytes": accumulatedBytes,
		"file_count":        fileCount,
	}
	if oldestFileTime != nil {
		body["oldest_file_time"] = oldestFileTime.Format(time.RFC3339)
	}
	if newestFileTime != nil {
		body["newest_file_time"] = newestFileTime.Format(time.RFC3339)
	}

	resp, err := c.doRequest(ctx, "POST", "/api/v1/packer/accumulation/state", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to update accumulation state: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var response struct {
		State       *AccumulationState `json:"state"`
		ShouldFlush bool               `json:"should_flush"`
		FlushReason string             `json:"flush_reason,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Copy top-level fields to state
	if response.State != nil {
		response.State.ShouldFlush = response.ShouldFlush
		response.State.FlushReason = response.FlushReason
	}

	log.Debugf("Updated accumulation state for %s/%s: %d bytes, %d files, should_flush=%v (%s)",
		tenantID, datasetID, accumulatedBytes, fileCount, response.ShouldFlush, response.FlushReason)

	return response.State, nil
}

// CheckFlush checks if a dataset should be flushed (lightweight poll)
func (c *AccumulationClient) CheckFlush(ctx context.Context, tenantID, datasetID string) (*AccumulationState, error) {
	endpoint := fmt.Sprintf("/api/v1/packer/accumulation/check?tenant_id=%s&dataset_id=%s",
		url.QueryEscape(tenantID), url.QueryEscape(datasetID))

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to check accumulation flush: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var state AccumulationState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &state, nil
}

// MarkFlushed marks a dataset's accumulation as flushed (resets counters)
func (c *AccumulationClient) MarkFlushed(ctx context.Context, tenantID, datasetID string) (*AccumulationState, error) {
	body := map[string]interface{}{
		"tenant_id":  tenantID,
		"dataset_id": datasetID,
	}

	resp, err := c.doRequest(ctx, "POST", "/api/v1/packer/accumulation/flush-complete", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to mark accumulation flushed: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var response struct {
		Success    bool               `json:"success"`
		State      *AccumulationState `json:"state"`
		FlushCount int                `json:"flush_count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	log.Infof("Marked accumulation flushed for %s/%s, total flushes: %d",
		tenantID, datasetID, response.FlushCount)

	return response.State, nil
}

// GetState gets the current accumulation state for a dataset
func (c *AccumulationClient) GetState(ctx context.Context, tenantID, datasetID string) (*AccumulationState, error) {
	endpoint := fmt.Sprintf("/api/v1/tenants/%s/datasets/%s/accumulation",
		url.PathEscape(tenantID), url.PathEscape(datasetID))

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// Return empty state for new datasets
		return &AccumulationState{
			TenantID:  tenantID,
			DatasetID: datasetID,
		}, nil
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to get accumulation state: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var response struct {
		State *AccumulationState `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return response.State, nil
}

// TestConnection tests the connection to the control service
func (c *AccumulationClient) TestConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.doRequest(ctx, "GET", "/api/v1/health", nil)
	if err != nil {
		return fmt.Errorf("failed to connect to control service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("control service health check failed with status %d", resp.StatusCode)
	}

	return nil
}
