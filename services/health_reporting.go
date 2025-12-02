// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bytes"
	"fmt"
	"github.com/bytedance/sonic"
	"net/http"
	"os"
	"time"

	"github.com/bytefreezer/goodies/log"
)

// HealthReportingService handles health reporting to the control service
type HealthReportingService struct {
	controlURL     string
	serviceType    string
	instanceID     string
	instanceAPI    string
	reportInterval time.Duration
	timeout        time.Duration
	httpClient     *http.Client
	apiKey         string // Bearer token for authentication
	enabled        bool
	stopChan       chan bool
	config         map[string]interface{} // Full configuration data to report
}

// ServiceRegistrationRequest represents a service registration request
type ServiceRegistrationRequest struct {
	ServiceType   string                 `json:"service_type"`
	InstanceID    string                 `json:"instance_id,omitempty"`
	InstanceAPI   string                 `json:"instance_api"`
	Status        string                 `json:"status,omitempty"`
	Configuration map[string]interface{} `json:"configuration,omitempty"`
}

// ServiceRegistrationResponse represents a service registration response
type ServiceRegistrationResponse struct {
	Success     bool   `json:"success"`
	Message     string `json:"message"`
	InstanceID  string `json:"instance_id"`
	ServiceType string `json:"service_type"`
}

// HealthReportRequest represents a health report request
type HealthReportRequest struct {
	ServiceName   string                 `json:"service_name"`
	ServiceID     string                 `json:"service_id"`
	InstanceAPI   string                 `json:"instance_api"`
	Healthy       bool                   `json:"healthy"`
	Configuration map[string]interface{} `json:"configuration,omitempty"`
	Metrics       map[string]interface{} `json:"metrics,omitempty"`
}

// HealthReportResponse represents a health report response
type HealthReportResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// NewHealthReportingService creates a new health reporting service
func NewHealthReportingService(controlURL, serviceType, instanceAPI, apiKey string, reportInterval, timeout time.Duration, config map[string]interface{}) *HealthReportingService {
	// Get hostname for instance ID
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return &HealthReportingService{
		controlURL:     controlURL,
		serviceType:    serviceType,
		instanceID:     hostname,
		instanceAPI:    instanceAPI,
		reportInterval: reportInterval,
		timeout:        timeout,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		apiKey:   apiKey,
		enabled:  true,
		stopChan: make(chan bool),
		config:   config,
	}
}

// Start begins health reporting
func (h *HealthReportingService) Start() {
	if !h.enabled {
		log.Info("Health reporting is disabled")
		return
	}

	log.Infof("Starting health reporting service - reporting to %s every %v", h.controlURL, h.reportInterval)

	// Register service on startup
	if err := h.RegisterService(); err != nil {
		log.Warnf("Failed to register service on startup: %v", err)
	}

	// Start reporting goroutine
	go h.reportingLoop()
}

// Stop stops health reporting
func (h *HealthReportingService) Stop() {
	if h.enabled {
		close(h.stopChan)
		log.Info("Health reporting service stopped")
	}
}

// RegisterService registers this service instance with the control service
func (h *HealthReportingService) RegisterService() error {
	if !h.enabled {
		return nil
	}

	registrationReq := ServiceRegistrationRequest{
		ServiceType:   h.serviceType,
		InstanceID:    h.instanceID,
		InstanceAPI:   h.instanceAPI,
		Status:        "Starting",
		Configuration: h.config, // Use full configuration data
	}

	reqBody, err := sonic.Marshal(registrationReq)
	if err != nil {
		return fmt.Errorf("failed to marshal registration request: %w", err)
	}

	req, err := http.NewRequest("POST", h.controlURL+"/api/v1/health/register", bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create registration request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if h.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.apiKey)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to register service: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("service registration failed with status %d", resp.StatusCode)
	}

	var registrationResp ServiceRegistrationResponse
	if err := sonic.ConfigDefault.NewDecoder(resp.Body).Decode(&registrationResp); err != nil {
		return fmt.Errorf("failed to decode registration response: %w", err)
	}

	if !registrationResp.Success {
		return fmt.Errorf("service registration failed: %s", registrationResp.Message)
	}

	log.Infof("Successfully registered service %s with instance ID %s", h.serviceType, registrationResp.InstanceID)
	return nil
}

// SendHealthReport sends a health report to the control service
func (h *HealthReportingService) SendHealthReport(healthy bool, configuration map[string]interface{}, metrics map[string]interface{}) error {
	if !h.enabled {
		return nil
	}

	healthReq := HealthReportRequest{
		ServiceName:   h.serviceType,
		ServiceID:     h.instanceID,
		InstanceAPI:   h.instanceAPI,
		Healthy:       healthy,
		Configuration: configuration,
		Metrics:       metrics,
	}

	reqBody, err := sonic.Marshal(healthReq)
	if err != nil {
		return fmt.Errorf("failed to marshal health report: %w", err)
	}

	req, err := http.NewRequest("POST", h.controlURL+"/api/v1/services/report", bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create health report request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if h.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+h.apiKey)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send health report: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("health report failed with status %d", resp.StatusCode)
	}

	log.Debugf("Successfully sent health report for %s (healthy: %v)", h.serviceType, healthy)
	return nil
}

// reportingLoop runs the periodic health reporting
func (h *HealthReportingService) reportingLoop() {
	ticker := time.NewTicker(h.reportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// Generate health report with basic metrics
			healthy := h.isServiceHealthy()
			configuration := h.config
			metrics := h.generateMetrics()

			if err := h.SendHealthReport(healthy, configuration, metrics); err != nil {
				log.Warnf("Failed to send health report: %v", err)
			}

		case <-h.stopChan:
			return
		}
	}
}

// isServiceHealthy performs basic health checks
func (h *HealthReportingService) isServiceHealthy() bool {
	// Basic health check - service is healthy if we can execute this function
	// In a real implementation, this would check:
	// - UDP listener health
	// - Receiver connectivity
	// - Spooling directory availability
	// - Memory usage
	// - Disk space
	return true
}

// generateMetrics generates service metrics
func (h *HealthReportingService) generateMetrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"timestamp":         time.Now().Unix(),
		"last_health_check": time.Now().UTC().Format(time.RFC3339),
	}

	return metrics
}
