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

// ControlAPILockClient implements LockClient using Control Service HTTP APIs
type ControlAPILockClient struct {
	client  *http.Client
	baseURL string
	apiKey  string
}

// ControlAPILockConfig holds configuration for Control API lock client
type ControlAPILockConfig struct {
	BaseURL        string
	APIKey         string
	TimeoutSeconds int
}

// NewControlAPILockClient creates a new Control API lock client
func NewControlAPILockClient(conf *ControlAPILockConfig) (*ControlAPILockClient, error) {
	if conf.BaseURL == "" {
		return nil, fmt.Errorf("control service base_url is required")
	}

	timeout := time.Duration(conf.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &ControlAPILockClient{
		client: &http.Client{
			Timeout: timeout,
		},
		baseURL: conf.BaseURL,
		apiKey:  conf.APIKey,
	}, nil
}

// doRequest performs an HTTP request to the control service
func (c *ControlAPILockClient) doRequest(ctx context.Context, method, endpoint string, body interface{}) (*http.Response, error) {
	var reqBody io.Reader
	if body != nil {
		jsonData, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonData)
	}

	url := c.baseURL + endpoint
	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
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

// TestConnection tests the connection to the control service
func (c *ControlAPILockClient) TestConnection() error {
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

// AcquireLock attempts to acquire a lock for a tenant
func (c *ControlAPILockClient) AcquireLock(ctx context.Context, tenantID, instanceID string, lockDuration time.Duration) (*TenantLock, error) {
	body := map[string]interface{}{
		"tenant_id":              tenantID,
		"locked_by":              instanceID,
		"lock_duration_seconds":  int(lockDuration.Seconds()),
	}

	resp, err := c.doRequest(ctx, "POST", "/api/v1/packer/locks/tenants", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		// Lock already held by another instance
		return nil, fmt.Errorf("tenant %s is already locked by another instance", tenantID)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to acquire lock: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var lock TenantLock
	if err := json.NewDecoder(resp.Body).Decode(&lock); err != nil {
		// If decode fails, create lock object manually
		lock = TenantLock{
			TenantID:      tenantID,
			LockedBy:      instanceID,
			LockTimestamp: time.Now(),
			TTL:           int64(lockDuration.Seconds()),
		}
	}

	log.Infof("Successfully acquired lock for tenant %s (instance: %s)", tenantID, instanceID)
	return &lock, nil
}

// ReleaseLock releases a lock for a tenant
func (c *ControlAPILockClient) ReleaseLock(ctx context.Context, tenantID, instanceID string) error {
	endpoint := fmt.Sprintf("/api/v1/packer/locks/tenants/%s/release", url.PathEscape(tenantID))
	body := map[string]interface{}{
		"locked_by": instanceID,
	}

	resp, err := c.doRequest(ctx, "POST", endpoint, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to release lock: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	log.Infof("Successfully released lock for tenant %s (instance: %s)", tenantID, instanceID)
	return nil
}

// UpdateHeartbeat updates the last_heartbeat timestamp for an active lock
func (c *ControlAPILockClient) UpdateHeartbeat(ctx context.Context, tenantID, instanceID string) error {
	endpoint := fmt.Sprintf("/api/v1/packer/locks/tenants/%s/heartbeat", url.PathEscape(tenantID))
	body := map[string]interface{}{
		"locked_by": instanceID,
	}

	resp, err := c.doRequest(ctx, "PUT", endpoint, body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to update heartbeat: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	log.Debugf("Updated heartbeat for tenant %s (instance: %s)", tenantID, instanceID)
	return nil
}

// IsLocked checks if a tenant is currently locked
func (c *ControlAPILockClient) IsLocked(ctx context.Context, tenantID string) (bool, *TenantLock, error) {
	endpoint := fmt.Sprintf("/api/v1/packer/locks/tenants/%s", url.PathEscape(tenantID))

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return false, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// Not locked
		return false, nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return false, nil, fmt.Errorf("failed to check lock status: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	var lock TenantLock
	if err := json.NewDecoder(resp.Body).Decode(&lock); err != nil {
		return false, nil, fmt.Errorf("failed to decode lock response: %w", err)
	}

	return true, &lock, nil
}

// CleanupStaleLocks removes locks that haven't been updated recently
func (c *ControlAPILockClient) CleanupStaleLocks(ctx context.Context, staleThresholdMinutes int) error {
	body := map[string]interface{}{
		"stale_threshold_minutes": staleThresholdMinutes,
	}

	resp, err := c.doRequest(ctx, "DELETE", "/api/v1/packer/locks/tenants/cleanup/stale", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Warnf("Failed to cleanup stale locks: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// CleanupExpiredLocks removes expired locks
func (c *ControlAPILockClient) CleanupExpiredLocks(ctx context.Context) error {
	resp, err := c.doRequest(ctx, "DELETE", "/api/v1/packer/locks/tenants/cleanup/expired", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Warnf("Failed to cleanup expired locks: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// CleanupInstanceLocks removes all locks held by a specific instance
func (c *ControlAPILockClient) CleanupInstanceLocks(ctx context.Context, instanceID string) error {
	body := map[string]interface{}{
		"instance_id": instanceID,
	}

	resp, err := c.doRequest(ctx, "DELETE", "/api/v1/packer/locks/tenants/cleanup/instance", body)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to cleanup instance locks: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	log.Infof("Successfully cleaned up locks for instance %s", instanceID)
	return nil
}

// ClearAllLocks removes all locks (use with caution)
func (c *ControlAPILockClient) ClearAllLocks(ctx context.Context) error {
	resp, err := c.doRequest(ctx, "DELETE", "/api/v1/packer/locks/tenants/cleanup/all", nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Warnf("Failed to clear all locks: status %d, body: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}

// Close closes any resources (no-op for HTTP client)
func (c *ControlAPILockClient) Close() error {
	// HTTP client doesn't need explicit closing
	return nil
}
