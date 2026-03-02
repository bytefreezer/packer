// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"bytes"
	"fmt"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/domain"
	"github.com/bytefreezer/packer/storage"
)

// DestinationTester tests S3 destination connectivity for local_storage datasets
// and reports results back to the control plane.
type DestinationTester struct {
	controlURL           string
	apiKey               string
	httpClient           *http.Client
	s3DestinationManager *storage.S3DestinationManager
}

// NewDestinationTester creates a new destination tester
func NewDestinationTester(controlURL, apiKey string, s3DestMgr *storage.S3DestinationManager) *DestinationTester {
	return &DestinationTester{
		controlURL:           controlURL,
		apiKey:               apiKey,
		httpClient:           &http.Client{Timeout: 30 * time.Second},
		s3DestinationManager: s3DestMgr,
	}
}

// TestAndReportAll iterates all tenants and tests destination for local_storage datasets
func (dt *DestinationTester) TestAndReportAll(tenants []domain.Tenant) {
	tested := 0
	for _, tenant := range tenants {
		for _, dataset := range tenant.Datasets {
			if !dataset.LocalStorage {
				continue
			}
			if dataset.S3Destination == nil {
				continue
			}
			dt.testAndReport(tenant.ID, dataset)
			tested++
		}
	}
	if tested > 0 {
		log.Infof("Destination tester: tested %d local_storage datasets", tested)
	}
}

// testAndReport tests a single dataset's S3 destination and reports to control
func (dt *DestinationTester) testAndReport(tenantID string, dataset *domain.Dataset) {
	reachable := true
	message := ""

	// Get or create S3 client (reuses cached connections via S3DestinationManager)
	s3Client, err := dt.s3DestinationManager.GetS3Client(tenantID, dataset.ID, dataset.S3Destination)
	if err != nil {
		reachable = false
		message = fmt.Sprintf("Failed to create S3 client: %v", err)
		log.Warnf("Destination test failed for %s/%s: %s", tenantID, dataset.ID, message)
	} else {
		// Test connectivity
		if err := s3Client.TestConnection(); err != nil {
			reachable = false
			message = fmt.Sprintf("S3 connectivity test failed: %v", err)
			log.Warnf("Destination test failed for %s/%s: %s", tenantID, dataset.ID, message)
		} else {
			message = fmt.Sprintf("S3 write test passed on bucket '%s'", dataset.S3Destination.BucketName)
			log.Debugf("Destination test passed for %s/%s: %s", tenantID, dataset.ID, message)
		}
	}

	// Report to control plane
	if err := dt.reportStatus(tenantID, dataset.ID, reachable, message); err != nil {
		log.Warnf("Failed to report destination status for %s/%s: %v", tenantID, dataset.ID, err)
	}
}

type destinationStatusReport struct {
	Reachable bool   `json:"reachable"`
	Message   string `json:"message"`
	CheckedAt string `json:"checked_at"`
}

// reportStatus POSTs the destination test result to control plane
func (dt *DestinationTester) reportStatus(tenantID, datasetID string, reachable bool, message string) error {
	report := destinationStatusReport{
		Reachable: reachable,
		Message:   message,
		CheckedAt: time.Now().UTC().Format(time.RFC3339),
	}

	body, err := sonic.Marshal(report)
	if err != nil {
		return fmt.Errorf("failed to marshal report: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/tenants/%s/datasets/%s/destination-status", dt.controlURL, tenantID, datasetID)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if dt.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+dt.apiKey)
	}

	resp, err := dt.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("control API returned HTTP %d", resp.StatusCode)
	}

	return nil
}
