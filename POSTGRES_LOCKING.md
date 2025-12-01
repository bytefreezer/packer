# PostgreSQL Locking Implementation

This document describes the new PostgreSQL-based distributed locking feature that replaces DynamoDB locking.

## Overview

The ByteFreezer Packer now supports PostgreSQL as a backend for distributed locking, providing an alternative to AWS DynamoDB. This allows for:

- **Cost Reduction**: No AWS service dependencies for locking
- **Simplified Infrastructure**: Use existing PostgreSQL databases
- **Better Control**: Full control over the locking database
- **Performance**: Potentially better performance in some environments

## Configuration

### PostgreSQL Lock Configuration

Add the following to your `config.yaml`:

```yaml
# PostgreSQL lock configuration
postgreslock:
  host: "localhost"               # PostgreSQL host
  port: 5432                     # PostgreSQL port
  database: "bytefreezer"        # Database name
  username: "bytefreezer"        # Database username
  password: "password"           # Database password
  ssl_mode: "disable"            # SSL mode: disable, require, verify-ca, verify-full
  table_name: "tenant_locks"     # Table name for locks
```

### Environment Variables

All PostgreSQL lock configuration options can be overridden with environment variables:

```bash
export BYTEFREEZER_POSTGRESLOCK_HOST=localhost
export BYTEFREEZER_POSTGRESLOCK_PORT=5432
export BYTEFREEZER_POSTGRESLOCK_DATABASE=bytefreezer
export BYTEFREEZER_POSTGRESLOCK_USERNAME=bytefreezer
export BYTEFREEZER_POSTGRESLOCK_PASSWORD=password
export BYTEFREEZER_POSTGRESLOCK_SSL_MODE=disable
export BYTEFREEZER_POSTGRESLOCK_TABLE_NAME=tenant_locks
```

## Database Setup

### Create Database and User

```sql
-- Create database
CREATE DATABASE bytefreezer;

-- Create user
CREATE USER bytefreezer WITH PASSWORD 'password';

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE bytefreezer TO bytefreezer;

-- Connect to the bytefreezer database
\c bytefreezer

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO bytefreezer;
```

### Table Creation

The service will automatically create the required table on first connection:

```sql
CREATE TABLE IF NOT EXISTS tenant_locks (
    tenant_id VARCHAR(255) PRIMARY KEY,
    locked_by VARCHAR(255) NOT NULL,
    lock_timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ttl TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Index for efficient cleanup
CREATE INDEX IF NOT EXISTS idx_tenant_locks_ttl ON tenant_locks(ttl);
```

## Migration from DynamoDB

### Configuration Migration

**Before (DynamoDB):**
```yaml
dynamodblock:
  table_name: "tenant-locks"
  region: "us-east-1"
  access_key: "your-access-key"
  secret_key: "your-secret-key"
```

**After (PostgreSQL):**
```yaml
postgreslock:
  host: "localhost"
  port: 5432
  database: "bytefreezer"
  username: "bytefreezer"
  password: "password"
  ssl_mode: "require"
  table_name: "tenant_locks"
```

### Priority Order

If both PostgreSQL and DynamoDB configurations are present, PostgreSQL takes priority:

1. **PostgreSQL** (if `postgreslock.table_name` is configured)
2. **DynamoDB** (if `dynamodblock.table_name` is configured)
3. **No locking** (if neither is configured)

### Data Migration

Since locks are ephemeral and have TTL, no data migration is required. Simply:

1. Update configuration to use PostgreSQL
2. Restart the service
3. Old DynamoDB locks will expire naturally

## Features

### Lock Operations

The PostgreSQL implementation provides the same interface as DynamoDB:

- **AcquireLock**: Acquire exclusive lock for tenant/dataset
- **ReleaseLock**: Release lock owned by specific instance
- **IsLocked**: Check if tenant/dataset is currently locked
- **TestConnection**: Verify database connectivity

### Lock Properties

- **Uniqueness**: One lock per tenant_id
- **TTL Support**: Automatic expiration of stale locks
- **Instance Ownership**: Only lock owner can release
- **Atomic Operations**: ACID transactions prevent race conditions

### Automatic Cleanup

- **Expired Lock Cleanup**: Automatic removal of expired locks during operations
- **Connection Pooling**: Efficient database connection management
- **Health Monitoring**: Connection status included in health checks

## Performance Considerations

### Connection Pool Settings

The PostgreSQL client uses optimized connection pool settings:

```go
db.SetMaxOpenConns(25)      // Maximum concurrent connections
db.SetMaxIdleConns(25)      // Maximum idle connections
db.SetConnMaxLifetime(5 * time.Minute) // Connection lifetime
```

### Indexing

The `ttl` column is indexed for efficient cleanup operations:

```sql
CREATE INDEX idx_tenant_locks_ttl ON tenant_locks(ttl);
```

### Lock Duration

Default lock duration is 30 minutes, but can be configured per operation.

## Monitoring

### Health Checks

The service health check includes PostgreSQL connectivity:

```bash
curl http://localhost:8080/health
```

Response includes locking backend status:
```json
{
  "status": "healthy",
  "services": {
    "locking": "healthy",
    "s3_source": "healthy",
    "tenant_database": "healthy"
  }
}
```

### Logs

PostgreSQL lock operations are logged with appropriate levels:

```
INFO: Successfully acquired lock for tenant tenant-001 by instance instance-001
INFO: Released lock for tenant tenant-001 by instance instance-001
DEBUG: Lock for tenant tenant-002 has expired, considering it unlocked
```

## High Availability

### Database HA

For production deployments, use PostgreSQL high availability solutions:

- **Streaming Replication**: Primary/replica setup
- **Connection Pooling**: PgBouncer for connection management
- **Load Balancing**: HAProxy for database load balancing

### Multi-Instance Support

Multiple packer instances can share the same PostgreSQL database:

```yaml
# Instance 1
postgreslock:
  host: "postgres-primary.internal"
  database: "bytefreezer"
  table_name: "tenant_locks"

# Instance 2 (same configuration)
postgreslock:
  host: "postgres-primary.internal"
  database: "bytefreezer"
  table_name: "tenant_locks"
```

## Security

### SSL Configuration

Enable SSL for production environments:

```yaml
postgreslock:
  host: "postgres.internal"
  ssl_mode: "require"        # or "verify-ca", "verify-full"
```

### Connection Security

- Use strong passwords
- Limit database user permissions
- Enable PostgreSQL authentication logging
- Use network firewalls to restrict access

### Credential Management

Store credentials securely:

```bash
# Use environment variables
export BYTEFREEZER_POSTGRESLOCK_PASSWORD="$(cat /secrets/postgres_password)"

# Or use external secret management
export BYTEFREEZER_POSTGRESLOCK_PASSWORD="$(vault kv get -field=password secret/postgres)"
```

## Troubleshooting

### Common Issues

**Connection Refused:**
```
failed to ping PostgreSQL database: dial tcp 127.0.0.1:5432: connect: connection refused
```
- Verify PostgreSQL is running
- Check host/port configuration
- Verify network connectivity

**Authentication Failed:**
```
failed to ping PostgreSQL database: pq: password authentication failed
```
- Verify username/password
- Check PostgreSQL pg_hba.conf settings
- Ensure user exists and has permissions

**Table Access Denied:**
```
failed to create PostgreSQL table tenant_locks: permission denied
```
- Grant CREATE permissions on database
- Grant ALL permissions on schema
- Verify user ownership

### Debug Mode

Enable debug logging for detailed information:

```yaml
logging:
  level: "debug"
```

### Testing Connection

Use the provided test program:

```bash
go run test_postgres_locks.go
```

## Benefits vs DynamoDB

| Feature | PostgreSQL | DynamoDB |
|---------|------------|----------|
| **Cost** | Database hosting only | Per-request pricing |
| **Latency** | Network latency to DB | AWS API latency |
| **Control** | Full database control | AWS managed service |
| **Backup** | Standard DB backups | AWS automated backups |
| **Monitoring** | Standard DB monitoring | CloudWatch integration |
| **Scaling** | Manual scaling | Automatic scaling |
| **Dependencies** | PostgreSQL server | AWS account + IAM |

## Future Enhancements

### Planned Features

- **Failure Tracking**: PostgreSQL-based failure tracking (currently DynamoDB-only)
- **Metrics Integration**: Enhanced PostgreSQL metrics collection
- **Connection Pooling**: External connection pooler integration
- **Partitioning**: Table partitioning for high-volume deployments

### Compatibility

The PostgreSQL implementation maintains full API compatibility with the DynamoDB implementation, allowing seamless migration without code changes.