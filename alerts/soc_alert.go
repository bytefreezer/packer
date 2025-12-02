// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package alerts

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
)

// SOCConfig represents SOC alert configuration
type SOCConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	Endpoint string `mapstructure:"endpoint"`
	Timeout  int    `mapstructure:"timeout"`
}

// AppConfig represents application configuration
type AppConfig struct {
	Name    string `mapstructure:"name"`
	Version string `mapstructure:"version"`
}

// AlertClientConfig represents the full config needed by SOCAlertClient
type AlertClientConfig struct {
	SOC SOCConfig `mapstructure:"soc"`
	App AppConfig `mapstructure:"app"`
	Dev bool      `mapstructure:"dev"`
}

type SOCAlertClient struct {
	config     AlertClientConfig
	httpClient *http.Client
}

type AlertPayload struct {
	Service     string    `json:"service"`
	Severity    string    `json:"severity"`
	Title       string    `json:"title"`
	Message     string    `json:"message"`
	Timestamp   time.Time `json:"timestamp"`
	Environment string    `json:"environment"`
	Host        string    `json:"host,omitempty"`
	Details     string    `json:"details,omitempty"`
}

const (
	SEVERITY_CRITICAL = "critical"
	SEVERITY_HIGH     = "high"
	SEVERITY_MEDIUM   = "medium"
	SEVERITY_LOW      = "low"
)

func NewSOCAlertClient(conf AlertClientConfig) *SOCAlertClient {
	timeout := time.Duration(conf.SOC.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	return &SOCAlertClient{
		config: conf,
		httpClient: &http.Client{
			Timeout: timeout,
		},
	}
}

// SendAlert sends an alert to the SOC endpoint
func (sac *SOCAlertClient) SendAlert(severity, title, message, details string) error {
	if !sac.config.SOC.Enabled {
		log.Debug("SOC alerts disabled, skipping alert")
		return nil
	}

	if sac.config.SOC.Endpoint == "" {
		log.Warn("SOC alert endpoint not configured")
		return nil
	}

	// Skip alerts in development mode unless it's critical
	if sac.config.Dev && severity != SEVERITY_CRITICAL {
		log.Debugf("Development mode - skipping %s alert: %s", severity, title)
		return nil
	}

	environment := "production"
	if sac.config.Dev {
		environment = "development"
	}

	payload := AlertPayload{
		Service:     sac.config.App.Name,
		Severity:    severity,
		Title:       title,
		Message:     message,
		Timestamp:   time.Now().UTC(),
		Environment: environment,
		Details:     details,
	}

	return sac.sendAlertPayload(payload)
}

// SendCriticalAlert sends a critical alert (always sent, even in dev mode)
func (sac *SOCAlertClient) SendCriticalAlert(title, message, details string) error {
	return sac.SendAlert(SEVERITY_CRITICAL, title, message, details)
}

// SendHighAlert sends a high severity alert
func (sac *SOCAlertClient) SendHighAlert(title, message, details string) error {
	return sac.SendAlert(SEVERITY_HIGH, title, message, details)
}

// SendMediumAlert sends a medium severity alert
func (sac *SOCAlertClient) SendMediumAlert(title, message, details string) error {
	return sac.SendAlert(SEVERITY_MEDIUM, title, message, details)
}

// sendAlertPayload sends the actual HTTP request
func (sac *SOCAlertClient) sendAlertPayload(payload AlertPayload) error {
	jsonData, err := sonic.Marshal(payload)
	if err != nil {
		log.Errorf("Failed to marshal SOC alert payload: %v", err)
		return fmt.Errorf("failed to marshal alert payload: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), sac.httpClient.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", sac.config.SOC.Endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Errorf("Failed to create SOC alert request: %v", err)
		return fmt.Errorf("failed to create alert request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", fmt.Sprintf("%s/%s", sac.config.App.Name, sac.config.App.Version))

	log.Debugf("Sending SOC alert to %s: %s - %s", sac.config.SOC.Endpoint, payload.Severity, payload.Title)

	resp, err := sac.httpClient.Do(req)
	if err != nil {
		log.Errorf("Failed to send SOC alert: %v", err)
		return fmt.Errorf("failed to send alert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Errorf("SOC alert endpoint returned status %d", resp.StatusCode)
		return fmt.Errorf("SOC alert endpoint returned status %d", resp.StatusCode)
	}

	log.Debugf("SOC alert sent successfully (status: %d)", resp.StatusCode)
	return nil
}

// SendServiceStartAlert sends an alert when service starts
func (sac *SOCAlertClient) SendServiceStartAlert() error {
	return sac.SendMediumAlert(
		"Service Started",
		fmt.Sprintf("%s service has started successfully", sac.config.App.Name),
		fmt.Sprintf("Version: %s, Environment: %s", sac.config.App.Version, func() string {
			if sac.config.Dev {
				return "development"
			}
			return "production"
		}()),
	)
}

// SendTenantDatabaseFailureAlert sends alert when tenant database fails to initialize
func (sac *SOCAlertClient) SendTenantDatabaseFailureAlert(err error) error {
	return sac.SendCriticalAlert(
		"Tenant Database Initialization Failed",
		"Failed to initialize tenant database - service cannot start",
		fmt.Sprintf("Error: %v", err),
	)
}

// SendTenantUpdateFailureAlert sends alert when tenant updates fail repeatedly
func (sac *SOCAlertClient) SendTenantUpdateFailureAlert(err error, failureCount int) error {
	severity := SEVERITY_MEDIUM
	if failureCount >= 5 {
		severity = SEVERITY_HIGH
	}
	if failureCount >= 10 {
		severity = SEVERITY_CRITICAL
	}

	return sac.SendAlert(
		severity,
		"Tenant Database Update Failures",
		fmt.Sprintf("Tenant database update has failed %d consecutive times", failureCount),
		fmt.Sprintf("Latest error: %v", err),
	)
}

// SendTenantProcessingFailureAlert sends alert when tenant processing fails
func (sac *SOCAlertClient) SendTenantProcessingFailureAlert(tenantID string, err error, filesCount int, totalSize int64) error {
	return sac.SendAlert(
		SEVERITY_HIGH,
		fmt.Sprintf("Tenant Processing Failed - %s", tenantID),
		fmt.Sprintf("Failed to process tenant %s during housekeeping cycle", tenantID),
		fmt.Sprintf("Error: %v, Files: %d, Total Size: %d bytes", err, filesCount, totalSize),
	)
}

// SendTenantUploadFailureAlert sends alert when tenant upload fails permanently
func (sac *SOCAlertClient) SendTenantUploadFailureAlert(tenantID string, err error, attempts int, filePath string) error {
	return sac.SendAlert(
		SEVERITY_CRITICAL,
		fmt.Sprintf("Tenant Upload Permanently Failed - %s", tenantID),
		fmt.Sprintf("File upload permanently failed for tenant %s after %d attempts", tenantID, attempts),
		fmt.Sprintf("Error: %v, File: %s", err, filePath),
	)
}

// SendTenantUploadRetryAlert sends alert when tenant upload is being retried
func (sac *SOCAlertClient) SendTenantUploadRetryAlert(tenantID string, err error, attempt int, maxAttempts int, filePath string) error {
	return sac.SendAlert(
		SEVERITY_MEDIUM,
		fmt.Sprintf("Tenant Upload Failed (Retrying) - %s", tenantID),
		fmt.Sprintf("File upload failed for tenant %s (attempt %d/%d)", tenantID, attempt, maxAttempts),
		fmt.Sprintf("Error: %v, File: %s", err, filePath),
	)
}
