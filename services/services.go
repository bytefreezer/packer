// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package services

import (
	"context"
	"errors"
	"time"

	"github.com/bytefreezer/packer/config"
	"github.com/bytefreezer/packer/domain"
	"github.com/bytefreezer/packer/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const (
	METER = "otel-meter"
)

type Services struct {
	Config        *config.Config
	OtelMeter     metric.Meter
	TenantMetrics *TenantMetrics
}

type HealthService interface {
	GetHealth() bool
}

func NewServices(conf *config.Config) *Services {
	meter := otel.Meter(METER)

	// Initialize tenant metrics
	tenantMetrics, err := NewTenantMetrics(meter)
	if err != nil {
		// Fall back to nil metrics if initialization fails
		tenantMetrics = nil
	}

	// Create services struct
	services := &Services{
		Config:        conf,
		OtelMeter:     meter,
		TenantMetrics: tenantMetrics,
	}

	return services
}

// GetTenants returns all cached tenants from the local database
func (s *Services) GetTenants() []domain.Tenant {
	if s.Config.TenantDatabase == nil {
		return []domain.Tenant{}
	}
	return s.Config.TenantDatabase.GetAllTenants()
}

// GetTenantByID returns a specific tenant by ID from the local database
func (s *Services) GetTenantByID(tenantID string) *domain.Tenant {
	if s.Config.TenantDatabase == nil {
		return nil
	}
	return s.Config.TenantDatabase.GetTenantByID(tenantID)
}

// GetTenantCount returns the number of cached tenants
func (s *Services) GetTenantCount() int {
	if s.Config.TenantDatabase == nil {
		return 0
	}
	return s.Config.TenantDatabase.GetTenantCount()
}

// IsTenantDatabaseHealthy returns the health status of the tenant database
func (s *Services) IsTenantDatabaseHealthy() bool {
	if s.Config.TenantDatabase == nil {
		return false
	}
	return s.Config.TenantDatabase.IsHealthy()
}

// GetS3SourceClient returns the S3 source client
func (s *Services) GetS3SourceClient() *storage.S3SourceClient {
	return s.Config.S3SourceClient
}

// IsS3SourceHealthy returns whether the S3 source client is available
func (s *Services) IsS3SourceHealthy() bool {
	return s.Config.S3SourceClient != nil
}

// CheckS3SourceConnectivity performs an actual connectivity test to S3
func (s *Services) CheckS3SourceConnectivity() error {
	if s.Config.S3SourceClient == nil {
		return errors.New("S3 source client not initialized")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Try to list the bucket to verify connectivity
	_, err := s.Config.S3SourceClient.ListDataFiles(ctx, "health-check/")
	return err
}

// CheckLockingConnectivity performs an actual connectivity test to the locking backend (PostgreSQL)
func (s *Services) CheckLockingConnectivity() error {
	// Check lock client connectivity
	lockClient := s.Config.GetLockClient()
	if lockClient != nil {
		// Test the lock client connection
		return lockClient.TestConnection()
	}

	return errors.New("no lock client configured")
}
