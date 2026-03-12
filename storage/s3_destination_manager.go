// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/domain"
)

// S3DestinationManager manages lazy S3 destination connections per tenant:dataset
type S3DestinationManager struct {
	connections map[string]*S3Client
	failureData map[string]*DestinationFailureData
	mutex       sync.RWMutex
}

// DestinationFailureData tracks failure information for a destination
type DestinationFailureData struct {
	FailureCount int
	LastError    string
	LastFailure  time.Time
	Active       bool
}

// NewS3DestinationManager creates a new S3 destination manager
func NewS3DestinationManager() *S3DestinationManager {
	return &S3DestinationManager{
		connections: make(map[string]*S3Client),
		failureData: make(map[string]*DestinationFailureData),
	}
}

// GetS3Client gets or creates an S3 client for the given tenant:dataset destination
func (sdm *S3DestinationManager) GetS3Client(tenantID, datasetID string, destination *domain.S3Destination) (*S3Client, error) {
	if destination == nil {
		return nil, fmt.Errorf("destination configuration is nil for %s:%s", tenantID, datasetID)
	}

	destKey := fmt.Sprintf("%s:%s", tenantID, datasetID)

	// Check if destination is marked as inactive
	sdm.mutex.RLock()
	if failureData, exists := sdm.failureData[destKey]; exists && !failureData.Active {
		sdm.mutex.RUnlock()
		return nil, fmt.Errorf("destination %s is marked as inactive due to repeated failures", destKey)
	}
	sdm.mutex.RUnlock()

	// Skip if destination is not active in configuration
	if !destination.Active {
		return nil, fmt.Errorf("destination %s is disabled in configuration", destKey)
	}

	// Try to get existing connection
	sdm.mutex.RLock()
	if client, exists := sdm.connections[destKey]; exists {
		sdm.mutex.RUnlock()
		return client, nil
	}
	sdm.mutex.RUnlock()

	// Create new connection
	sdm.mutex.Lock()
	defer sdm.mutex.Unlock()

	// Double-check after acquiring write lock
	if client, exists := sdm.connections[destKey]; exists {
		return client, nil
	}

	log.Infof("Creating lazy S3 destination connection for %s (bucket: %s, endpoint: %s)",
		destKey, destination.BucketName, destination.Endpoint)

	// Create S3 client
	s3Client, err := NewS3Client(S3Config{
		BucketName: destination.BucketName,
		Region:     destination.Region,
		AccessKey:  destination.AccessKey,
		SecretKey:  destination.SecretKey,
		Endpoint:   destination.Endpoint,
		Ssl:        destination.Ssl,
		UseIamRole: destination.UseIamRole,
	})
	if err != nil {
		sdm.recordFailure(destKey, fmt.Sprintf("Failed to create S3 client: %v", err))
		return nil, fmt.Errorf("failed to create S3 client for %s: %w", destKey, err)
	}

	// Test connection
	if err := s3Client.TestConnection(); err != nil {
		sdm.recordFailure(destKey, fmt.Sprintf("Failed to connect to S3: %v", err))
		return nil, fmt.Errorf("failed to connect to S3 for %s: %w", destKey, err)
	}

	// Store successful connection
	sdm.connections[destKey] = s3Client

	// Reset failure data on successful connection
	if failureData, exists := sdm.failureData[destKey]; exists {
		failureData.FailureCount = 0
		failureData.LastError = ""
		failureData.Active = true
	}

	log.Infof("Successfully created S3 destination connection for %s", destKey)
	return s3Client, nil
}

// RecordFailure records a failure for a destination and marks it inactive if threshold exceeded
func (sdm *S3DestinationManager) RecordFailure(tenantID, datasetID, errorMsg string) {
	destKey := fmt.Sprintf("%s:%s", tenantID, datasetID)
	sdm.recordFailure(destKey, errorMsg)
}

// recordFailure is the internal implementation of failure recording
func (sdm *S3DestinationManager) recordFailure(destKey, errorMsg string) {
	sdm.mutex.Lock()
	defer sdm.mutex.Unlock()

	failureData, exists := sdm.failureData[destKey]
	if !exists {
		failureData = &DestinationFailureData{
			Active: true,
		}
		sdm.failureData[destKey] = failureData
	}

	failureData.FailureCount++
	failureData.LastError = errorMsg
	failureData.LastFailure = time.Now()

	log.Warnf("Destination %s failure #%d: %s", destKey, failureData.FailureCount, errorMsg)

	// Mark as inactive after 3 failures
	if failureData.FailureCount >= 3 {
		failureData.Active = false
		log.Errorf("Destination %s marked as INACTIVE after %d failures. Last error: %s",
			destKey, failureData.FailureCount, errorMsg)

		// Remove connection so it doesn't get reused
		delete(sdm.connections, destKey)
	}
}

// GetFailureData returns failure information for a destination
func (sdm *S3DestinationManager) GetFailureData(tenantID, datasetID string) *DestinationFailureData {
	destKey := fmt.Sprintf("%s:%s", tenantID, datasetID)

	sdm.mutex.RLock()
	defer sdm.mutex.RUnlock()

	if failureData, exists := sdm.failureData[destKey]; exists {
		// Return a copy to avoid race conditions
		return &DestinationFailureData{
			FailureCount: failureData.FailureCount,
			LastError:    failureData.LastError,
			LastFailure:  failureData.LastFailure,
			Active:       failureData.Active,
		}
	}

	return &DestinationFailureData{Active: true}
}

// RefreshConnections clears all cached S3 destination connections so they
// get re-created with fresh config from control on next use. Called during
// housekeeping to pick up credential/endpoint changes without restart.
func (sdm *S3DestinationManager) RefreshConnections() {
	sdm.mutex.Lock()
	defer sdm.mutex.Unlock()

	count := len(sdm.connections)
	if count > 0 {
		sdm.connections = make(map[string]*S3Client)
		log.Infof("Cleared %d cached S3 destination connections (will re-create on next use)", count)
	}

	// Reset failure data so previously-failed destinations get retried
	for key, fd := range sdm.failureData {
		if !fd.Active {
			fd.Active = true
			fd.FailureCount = 0
			log.Infof("Reset inactive destination %s for retry", key)
		}
	}
}

// CloseAll closes all S3 connections (for cleanup)
func (sdm *S3DestinationManager) CloseAll() {
	sdm.mutex.Lock()
	defer sdm.mutex.Unlock()

	log.Infof("Closing %d S3 destination connections", len(sdm.connections))
	sdm.connections = make(map[string]*S3Client)
	sdm.failureData = make(map[string]*DestinationFailureData)
}
