// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package tenant

import (
	"sync"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/domain"
)

type TenantDatabase struct {
	tenants   map[string]domain.Tenant
	mutex     sync.RWMutex
	lastSync  time.Time
	isHealthy bool
}

func NewTenantDatabase() *TenantDatabase {
	return &TenantDatabase{
		tenants:   make(map[string]domain.Tenant),
		isHealthy: false,
	}
}

// PopulateDatabase initializes the database with tenants from the controller
// Uses exponential backoff retry logic to handle temporary control service unavailability
func (tdb *TenantDatabase) PopulateDatabase(client *TenantClient) error {
	log.Info("Populating tenant database from controller...")

	maxRetries := 5
	baseDelay := 2 * time.Second

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		tenants, err := client.FetchTenants()
		if err == nil {
			// Success - populate the database
			tdb.mutex.Lock()
			defer tdb.mutex.Unlock()

			// Clear existing tenants
			tdb.tenants = make(map[string]domain.Tenant)

			// Filter out disabled tenants if failure tracker is available
			enabledTenants := tdb.filterEnabledTenants(tenants)

			// Add enabled tenants only
			for _, tenant := range enabledTenants {
				tdb.tenants[tenant.ID] = tenant
			}

			tdb.lastSync = time.Now()
			tdb.isHealthy = true

			originalCount := len(tenants)
			enabledCount := len(enabledTenants)

			if originalCount != enabledCount {
				log.Infof("Successfully populated tenant database with %d enabled tenants (%d disabled)",
					enabledCount, originalCount-enabledCount)
			} else {
				log.Infof("Successfully populated tenant database with %d tenants", enabledCount)
			}

			return nil
		}

		lastErr = err

		// Calculate exponential backoff delay
		delay := baseDelay * time.Duration(1<<uint(attempt))
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}

		log.Warnf("Failed to fetch tenants (attempt %d/%d): %v - retrying in %v",
			attempt+1, maxRetries, err, delay)
		time.Sleep(delay)
	}

	// After max retries, start with empty tenant database and log warning
	// Service will continue to retry via UpdateDatabase in background
	tdb.setHealthy(false)
	log.Warnf("Failed to populate tenant database after %d attempts - starting with empty database. "+
		"Service will retry in background. Last error: %v", maxRetries, lastErr)

	// Return nil to allow service to start
	return nil
}

// UpdateDatabase refreshes the database with latest tenant data
func (tdb *TenantDatabase) UpdateDatabase(client *TenantClient) error {
	log.Debug("Updating tenant database from controller...")

	tenants, err := client.FetchTenants()
	if err != nil {
		log.Errorf("Failed to update tenant database: %v", err)
		// Don't mark as unhealthy on update failure, keep existing data
		return err
	}

	tdb.mutex.Lock()
	defer tdb.mutex.Unlock()

	// Filter out disabled tenants if failure tracker is available
	enabledTenants := tdb.filterEnabledTenants(tenants)

	// Update tenants map with enabled tenants only
	newTenants := make(map[string]domain.Tenant)
	for _, tenant := range enabledTenants {
		newTenants[tenant.ID] = tenant
	}

	tdb.tenants = newTenants
	tdb.lastSync = time.Now()
	tdb.isHealthy = true

	originalCount := len(tenants)
	enabledCount := len(enabledTenants)

	if originalCount != enabledCount {
		log.Debugf("Successfully updated tenant database with %d enabled tenants (%d disabled)",
			enabledCount, originalCount-enabledCount)
	} else {
		log.Debugf("Successfully updated tenant database with %d tenants", enabledCount)
	}

	return nil
}

// GetAllTenants returns all cached tenants
func (tdb *TenantDatabase) GetAllTenants() []domain.Tenant {
	tdb.mutex.RLock()
	defer tdb.mutex.RUnlock()

	tenants := make([]domain.Tenant, 0, len(tdb.tenants))
	for _, tenant := range tdb.tenants {
		tenants = append(tenants, tenant)
	}

	return tenants
}

// GetTenantByID returns a specific tenant by ID
func (tdb *TenantDatabase) GetTenantByID(tenantID string) *domain.Tenant {
	tdb.mutex.RLock()
	defer tdb.mutex.RUnlock()

	if tenant, exists := tdb.tenants[tenantID]; exists {
		return &tenant
	}

	return nil
}

// GetTenantCount returns the number of cached tenants
func (tdb *TenantDatabase) GetTenantCount() int {
	tdb.mutex.RLock()
	defer tdb.mutex.RUnlock()

	return len(tdb.tenants)
}

// IsHealthy returns the health status of the database
func (tdb *TenantDatabase) IsHealthy() bool {
	tdb.mutex.RLock()
	defer tdb.mutex.RUnlock()

	return tdb.isHealthy
}

// GetLastSyncTime returns the last successful sync time
func (tdb *TenantDatabase) GetLastSyncTime() time.Time {
	tdb.mutex.RLock()
	defer tdb.mutex.RUnlock()

	return tdb.lastSync
}

// setHealthy sets the health status (internal method)
func (tdb *TenantDatabase) setHealthy(healthy bool) {
	tdb.mutex.Lock()
	defer tdb.mutex.Unlock()

	tdb.isHealthy = healthy
}

// filterEnabledTenants filters out disabled tenants using the failure tracker
func (tdb *TenantDatabase) filterEnabledTenants(tenants []domain.Tenant) []domain.Tenant {
	// No failure tracking in packer, return all tenants
	return tenants
}
