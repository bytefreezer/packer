package storage

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/bytefreezer/goodies/log"
	_ "github.com/lib/pq"
)

type PostgreSQLLockClient struct {
	config    *PostgreSQLLockConfig
	db        *sql.DB
	tableName string
}

// validateTableName validates that table name only contains safe characters
func validateTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	// Only allow alphanumeric characters, underscores, and dots (for schema.table)
	validName := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)
	if !validName.MatchString(tableName) {
		return fmt.Errorf("table name contains invalid characters: %s", tableName)
	}

	return nil
}

// safeTableName returns a safely quoted table name
func (pc *PostgreSQLLockClient) safeTableName() string {
	// PostgreSQL identifier quoting to prevent injection
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(pc.tableName, `"`, `""`))
}

type PostgreSQLLockConfig struct {
	Host      string `mapstructure:"host"`
	Port      int    `mapstructure:"port"`
	Database  string `mapstructure:"database"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
	SSLMode   string `mapstructure:"ssl_mode"`
	TableName string `mapstructure:"table_name"`
}

// GetDB returns the underlying database connection for compatibility with tracking modules
func (pc *PostgreSQLLockClient) GetDB() *sql.DB {
	return pc.db
}

func NewPostgreSQLLockClient(conf *PostgreSQLLockConfig) (*PostgreSQLLockClient, error) {
	if err := validateTableName(conf.TableName); err != nil {
		return nil, fmt.Errorf("invalid PostgreSQL table name: %w", err)
	}

	if conf.Host == "" {
		conf.Host = "localhost"
	}
	if conf.Port == 0 {
		conf.Port = 5432
	}
	if conf.Database == "" {
		return nil, fmt.Errorf("PostgreSQL database name is required")
	}
	if conf.Username == "" {
		return nil, fmt.Errorf("PostgreSQL username is required")
	}
	if conf.SSLMode == "" {
		conf.SSLMode = "disable"
	}

	// Build connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		conf.Host, conf.Port, conf.Username, conf.Password, conf.Database, conf.SSLMode)

	// Open database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open PostgreSQL connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	client := &PostgreSQLLockClient{
		config:    conf,
		db:        db,
		tableName: conf.TableName,
	}

	return client, nil
}

// TestConnection tests the PostgreSQL connection and creates table if needed
func (pc *PostgreSQLLockClient) TestConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	log.Debugf("Testing PostgreSQL lock connection to table: %s", pc.tableName)

	// Test database connection
	if err := pc.db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to ping PostgreSQL database: %w", err)
	}

	// Create table if it doesn't exist
	if err := pc.createLockTableIfNotExists(ctx); err != nil {
		return fmt.Errorf("failed to create lock table: %w", err)
	}

	log.Infof("PostgreSQL lock connection successful to table: %s", pc.tableName)
	return nil
}

// createLockTableIfNotExists creates the tenant lock table if it doesn't exist
func (pc *PostgreSQLLockClient) createLockTableIfNotExists(ctx context.Context) error {
	safeTable := pc.safeTableName()

	// Create table with heartbeat support
	createTableSQL := `CREATE TABLE IF NOT EXISTS ` + safeTable + ` (
		tenant_id VARCHAR(255) PRIMARY KEY,
		locked_by VARCHAR(255) NOT NULL,
		lock_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
		last_heartbeat TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
		ttl TIMESTAMP WITH TIME ZONE NOT NULL
	)` // #nosec G202 - table name is validated and safely quoted

	_, err := pc.db.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL table %s: %w", pc.tableName, err)
	}

	// Add last_heartbeat column to existing tables (migration)
	alterSQL := `ALTER TABLE ` + safeTable + ` ADD COLUMN IF NOT EXISTS last_heartbeat TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP` // #nosec G202 - table name is validated and safely quoted
	_, err = pc.db.ExecContext(ctx, alterSQL)
	if err != nil {
		log.Warnf("Failed to add last_heartbeat column (may already exist): %v", err)
	}

	// Create indexes separately to avoid complex string formatting
	ttlIndexName := fmt.Sprintf(`"idx_%s_ttl"`, strings.ReplaceAll(pc.tableName, `"`, `""`))
	createTTLIndexSQL := `CREATE INDEX IF NOT EXISTS ` + ttlIndexName + ` ON ` + safeTable + `(ttl)` // #nosec G202 - table name is validated and safely quoted

	_, err = pc.db.ExecContext(ctx, createTTLIndexSQL)
	if err != nil {
		return fmt.Errorf("failed to create TTL index for PostgreSQL table %s: %w", pc.tableName, err)
	}

	// Create heartbeat index for efficient stale lock cleanup
	heartbeatIndexName := fmt.Sprintf(`"idx_%s_heartbeat"`, strings.ReplaceAll(pc.tableName, `"`, `""`))
	createHeartbeatIndexSQL := `CREATE INDEX IF NOT EXISTS ` + heartbeatIndexName + ` ON ` + safeTable + `(last_heartbeat)` // #nosec G202 - table name is validated and safely quoted

	_, err = pc.db.ExecContext(ctx, createHeartbeatIndexSQL)
	if err != nil {
		return fmt.Errorf("failed to create heartbeat index for PostgreSQL table %s: %w", pc.tableName, err)
	}

	log.Infof("Successfully ensured PostgreSQL lock table exists: %s", pc.tableName)
	return nil
}

// AcquireLock attempts to acquire a lock for a tenant
func (pc *PostgreSQLLockClient) AcquireLock(ctx context.Context, tenantID, instanceID string, lockDuration time.Duration) (*TenantLock, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID cannot be empty")
	}
	if instanceID == "" {
		return nil, fmt.Errorf("instance ID cannot be empty")
	}

	// First check if we already own this lock
	isLocked, existingLock, err := pc.IsLocked(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing lock: %w", err)
	}

	if isLocked && existingLock.LockedBy == instanceID {
		// We already own this lock and it hasn't expired - return silently
		return existingLock, nil
	}

	now := time.Now()
	ttl := now.Add(lockDuration)

	lock := &TenantLock{
		TenantID:      tenantID,
		LockedBy:      instanceID,
		LockTimestamp: now,
		TTL:           ttl.Unix(),
	}

	// Use UPSERT with WHERE condition to ensure we only acquire the lock if it doesn't exist or has expired
	// PostgreSQL doesn't have conditional upsert like DynamoDB, so we use a transaction
	tx, err := pc.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	// First, clean up expired locks for this tenant
	safeTable := pc.safeTableName()
	cleanupSQL := `DELETE FROM ` + safeTable + ` WHERE tenant_id = $1 AND ttl < CURRENT_TIMESTAMP` // #nosec G202 - table name is validated and safely quoted
	_, err = tx.ExecContext(ctx, cleanupSQL, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to cleanup expired locks: %w", err)
	}

	// Try to insert the lock (will fail if lock already exists and hasn't expired)
	insertSQL := `INSERT INTO ` + safeTable + ` (tenant_id, locked_by, lock_timestamp, last_heartbeat, ttl) VALUES ($1, $2, $3, $4, $5)` // #nosec G202 - table name is validated and safely quoted

	_, err = tx.ExecContext(ctx, insertSQL, tenantID, instanceID, now, now, ttl)
	if err != nil {
		if strings.Contains(err.Error(), "duplicate key") || strings.Contains(err.Error(), "unique constraint") {
			// Check if it's our lock or someone else's - only log for other instances
			_, currentLock, checkErr := pc.IsLocked(ctx, tenantID)
			if checkErr == nil && currentLock != nil && currentLock.LockedBy != instanceID {
				log.Debugf("Failed to acquire lock for tenant %s: already locked by %s", tenantID, currentLock.LockedBy)
			}
			return nil, fmt.Errorf("tenant %s is already locked", tenantID)
		}
		return nil, fmt.Errorf("failed to acquire lock for tenant %s: %w", tenantID, err)
	}

	if err = tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit lock transaction: %w", err)
	}

	log.Infof("Successfully acquired lock for tenant %s by instance %s", tenantID, instanceID)
	return lock, nil
}

// ReleaseLock releases a lock for a tenant
func (pc *PostgreSQLLockClient) ReleaseLock(ctx context.Context, tenantID, instanceID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty")
	}
	if instanceID == "" {
		return fmt.Errorf("instance ID cannot be empty")
	}

	// Only delete if the lock is owned by this instance
	safeTable := pc.safeTableName()
	deleteSQL := `DELETE FROM ` + safeTable + ` WHERE tenant_id = $1 AND locked_by = $2` // #nosec G202 - table name is validated and safely quoted

	result, err := pc.db.ExecContext(ctx, deleteSQL, tenantID, instanceID)
	if err != nil {
		return fmt.Errorf("failed to release lock for tenant %s: %w", tenantID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("cannot release lock for tenant %s: not owned by instance %s", tenantID, instanceID)
	}

	log.Infof("Successfully released lock for tenant %s by instance %s", tenantID, instanceID)
	return nil
}

// UpdateHeartbeat updates the last_heartbeat timestamp for an active lock
func (pc *PostgreSQLLockClient) UpdateHeartbeat(ctx context.Context, tenantID, instanceID string) error {
	if tenantID == "" {
		return fmt.Errorf("tenant ID cannot be empty")
	}
	if instanceID == "" {
		return fmt.Errorf("instance ID cannot be empty")
	}

	safeTable := pc.safeTableName()
	updateSQL := `UPDATE ` + safeTable + ` SET last_heartbeat = CURRENT_TIMESTAMP WHERE tenant_id = $1 AND locked_by = $2` // #nosec G202 - table name is validated and safely quoted

	result, err := pc.db.ExecContext(ctx, updateSQL, tenantID, instanceID)
	if err != nil {
		return fmt.Errorf("failed to update heartbeat for tenant %s: %w", tenantID, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Warnf("Failed to get rows affected during heartbeat update: %v", err)
		return nil // Non-critical error
	}

	if rowsAffected == 0 {
		return fmt.Errorf("no lock found to update heartbeat for tenant %s by instance %s", tenantID, instanceID)
	}

	log.Debugf("Updated heartbeat for tenant %s by instance %s", tenantID, instanceID)
	return nil
}

// IsLocked checks if a tenant is currently locked
func (pc *PostgreSQLLockClient) IsLocked(ctx context.Context, tenantID string) (bool, *TenantLock, error) {
	if tenantID == "" {
		return false, nil, fmt.Errorf("tenant ID cannot be empty")
	}

	// First clean up any expired locks
	safeTable := pc.safeTableName()
	cleanupSQL := `DELETE FROM ` + safeTable + ` WHERE tenant_id = $1 AND ttl < CURRENT_TIMESTAMP` // #nosec G202 - table name is validated and safely quoted
	_, err := pc.db.ExecContext(ctx, cleanupSQL, tenantID)
	if err != nil {
		log.Warnf("Failed to cleanup expired locks for tenant %s: %v", tenantID, err)
	}

	// Check if lock exists and hasn't expired
	selectSQL := `SELECT tenant_id, locked_by, lock_timestamp, EXTRACT(EPOCH FROM ttl)::bigint as ttl_unix FROM ` + safeTable + ` WHERE tenant_id = $1 AND ttl > CURRENT_TIMESTAMP` // #nosec G202 - table name is validated and safely quoted

	var lock TenantLock
	var lockTimestamp time.Time
	err = pc.db.QueryRowContext(ctx, selectSQL, tenantID).Scan(
		&lock.TenantID,
		&lock.LockedBy,
		&lockTimestamp,
		&lock.TTL,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return false, nil, nil // No lock exists
		}
		return false, nil, fmt.Errorf("failed to check lock for tenant %s: %w", tenantID, err)
	}

	lock.LockTimestamp = lockTimestamp

	return true, &lock, nil
}

// CleanupExpiredLocks removes all expired locks from the database
func (pc *PostgreSQLLockClient) CleanupExpiredLocks(ctx context.Context) error {
	safeTable := pc.safeTableName()
	cleanupSQL := `DELETE FROM ` + safeTable + ` WHERE ttl < CURRENT_TIMESTAMP` // #nosec G202 - table name is validated and safely quoted

	result, err := pc.db.ExecContext(ctx, cleanupSQL)
	if err != nil {
		return fmt.Errorf("failed to cleanup expired locks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Warnf("Failed to get rows affected during cleanup: %v", err)
	} else if rowsAffected > 0 {
		log.Infof("Cleaned up %d expired locks", rowsAffected)
	}

	return nil
}

// ClearAllLocks removes all locks from the database (for clean restarts)
func (pc *PostgreSQLLockClient) ClearAllLocks(ctx context.Context) error {
	safeTable := pc.safeTableName()
	clearSQL := `DELETE FROM ` + safeTable // #nosec G202 - table name is validated and safely quoted

	result, err := pc.db.ExecContext(ctx, clearSQL)
	if err != nil {
		return fmt.Errorf("failed to clear all locks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Warnf("Failed to get rows affected during clear: %v", err)
	} else {
		log.Infof("Cleared all %d locks from table", rowsAffected)
	}

	return nil
}

// CleanupStaleLocks removes locks that haven't been updated recently (stale heartbeats)
func (pc *PostgreSQLLockClient) CleanupStaleLocks(ctx context.Context, staleThresholdMinutes int) error {
	safeTable := pc.safeTableName()
	cleanupSQL := `DELETE FROM ` + safeTable + ` WHERE last_heartbeat < CURRENT_TIMESTAMP - INTERVAL '` +
		fmt.Sprintf("%d", staleThresholdMinutes) + ` MINUTES'` // #nosec G202 - table name is validated and safely quoted

	result, err := pc.db.ExecContext(ctx, cleanupSQL)
	if err != nil {
		return fmt.Errorf("failed to cleanup stale locks: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		log.Warnf("Failed to get rows affected during stale lock cleanup: %v", err)
	} else if rowsAffected > 0 {
		log.Infof("Cleaned up %d stale locks (heartbeat older than %d minutes)", rowsAffected, staleThresholdMinutes)
	}

	return nil
}

// Close closes the database connection
func (pc *PostgreSQLLockClient) Close() error {
	if pc.db != nil {
		return pc.db.Close()
	}
	return nil
}
