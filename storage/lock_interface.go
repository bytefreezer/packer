package storage

import (
	"context"
	"time"
)

// TenantLock represents a lock on a tenant for processing
type TenantLock struct {
	TenantID      string    `json:"tenant_id"`
	LockedBy      string    `json:"locked_by"`
	LockTimestamp time.Time `json:"lock_timestamp"`
	TTL           int64     `json:"ttl"`
}

// LockClient defines the interface for distributed locking implementations
type LockClient interface {
	// TestConnection tests the connection to the locking backend
	TestConnection() error

	// AcquireLock attempts to acquire a lock for a tenant
	AcquireLock(ctx context.Context, tenantID, instanceID string, lockDuration time.Duration) (*TenantLock, error)

	// ReleaseLock releases a lock for a tenant
	ReleaseLock(ctx context.Context, tenantID, instanceID string) error

	// IsLocked checks if a tenant is currently locked
	IsLocked(ctx context.Context, tenantID string) (bool, *TenantLock, error)
}

// HeartbeatLockClient extends LockClient with heartbeat capabilities
type HeartbeatLockClient interface {
	LockClient

	// UpdateHeartbeat updates the last_heartbeat timestamp for an active lock
	UpdateHeartbeat(ctx context.Context, tenantID, instanceID string) error

	// CleanupStaleLocks removes locks that haven't been updated recently
	CleanupStaleLocks(ctx context.Context, staleThresholdMinutes int) error

	// CleanupExpiredLocks removes locks that have passed their TTL
	CleanupExpiredLocks(ctx context.Context) error

	// CleanupInstanceLocks removes all locks held by a specific instance
	// Used on startup to clean up abandoned locks from previous runs of this instance
	CleanupInstanceLocks(ctx context.Context, instanceID string) error
}

// GetLockClient returns the appropriate lock client based on configuration
func GetLockClient(cfg interface{}) LockClient {
	switch client := cfg.(type) {
	case *PostgreSQLLockClient:
		return client
	default:
		return nil
	}
}
