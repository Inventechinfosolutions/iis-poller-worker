# Environment Variables Configuration

This document describes all environment variables used in the poller worker service and how to configure them.

## Overview

All configuration values are loaded from environment variables for better flexibility and deployment management. The service uses Pydantic Settings for type-safe configuration with validation.

## Environment Files

- `.env` - Production environment variables (not committed to git)
- `.env.example` - Template with default values (committed to git)

## Quick Start

```bash
# Copy example environment file
cp .env.example .env

# Edit with your values
nano .env  # or use your preferred editor

# Run the application
python -m src.main
```

## Configuration Categories

### 1. Application Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_NAME` | `iis-poller-worker` | Application name |
| `APP_VERSION` | `1.0.0` | Application version |
| `WORKER_TIMEOUT` | `300` | Worker timeout in seconds |
| `ENVIRONMENT` | `development` | Environment: development, staging, production |
| `DEBUG` | `false` | Enable debug mode |

### 2. Kafka Configuration

#### Core Kafka Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated list of Kafka broker addresses |
| `KAFKA_CLIENT_ID` | `poller-worker` | Client ID for Kafka connections |
| `KAFKA_GROUP_ID` | `poller-worker-group` | Consumer group ID |
| `KAFKA_REPLICATION_FACTOR` | `1` | Topic replication factor |

#### Kafka Topics

| Variable | Default | Description |
|----------|---------|-------------|
| `POLLING_QUEUE_TOPIC` | `polling-queue` | Topic for receiving polling jobs |
| `FILE_EVENT_QUEUE_TOPIC` | `file-event-queue` | Topic for publishing file events |

#### Kafka Producer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_PRODUCER_ACKS` | `all` | Acknowledgment mode (all, 0, 1) |
| `KAFKA_PRODUCER_RETRIES` | `3` | Number of retries |
| `KAFKA_PRODUCER_BATCH_SIZE` | `16384` | Batch size in bytes |
| `KAFKA_PRODUCER_LINGER_MS` | `10` | Linger time in milliseconds |
| `KAFKA_PRODUCER_COMPRESSION_TYPE` | `gzip` | Compression type |
| `KAFKA_PRODUCER_ENABLE_IDEMPOTENCE` | `true` | Enable idempotent producer |

#### Kafka Consumer Settings

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_CONSUMER_AUTO_OFFSET_RESET` | `earliest` | Offset reset policy |
| `KAFKA_CONSUMER_ENABLE_AUTO_COMMIT` | `false` | Enable auto commit |
| `KAFKA_CONSUMER_SESSION_TIMEOUT_MS` | `30000` | Session timeout in milliseconds |
| `KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS` | `10000` | Heartbeat interval in milliseconds |
| `KAFKA_CONSUMER_MAX_POLL_INTERVAL_MS` | `300000` | Max poll interval in milliseconds |

### 3. Poller Worker Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `POLL_INTERVAL` | `5` | Poll interval in seconds |
| `MAX_CONCURRENT_JOBS` | `10` | Maximum concurrent jobs |
| `BATCH_SIZE` | `100` | Batch size for processing |

### 4. Source Connection Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DEFAULT_SOURCE_TYPE` | `dummy` | Default source type (dummy, s3, ftp, sftp, local, http) |
| `CONNECTION_TIMEOUT` | `30` | Connection timeout in seconds |
| `READ_TIMEOUT` | `60` | Read timeout in seconds |

### 5. File Processing Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SUPPORTED_FILE_EXTENSIONS` | `.csv,.xlsx,.xls,.txt` | Comma-separated list of supported extensions |
| `MAX_FILE_SIZE` | `104857600` | Maximum file size in bytes (100MB) |

### 6. Dummy Data Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DUMMY_DATA_PATH` | `./dummy_data` | Path to dummy data files |

### 7. Logging Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `LOG_FORMAT` | `json` | Log format (json for production, console for development) |

### 8. Metrics and Monitoring

| Variable | Default | Description |
|----------|---------|-------------|
| `METRICS_PORT` | `9093` | Prometheus metrics port |
| `ENABLE_METRICS` | `true` | Enable metrics collection |
| `HEALTH_CHECK_INTERVAL` | `60` | Health check interval in seconds |
| `ENABLE_HEALTH_CHECK` | `true` | Enable health check endpoint |

### 9. Security Configuration (Production)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL) |
| `KAFKA_SASL_MECHANISM` | - | SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) |
| `KAFKA_SASL_USERNAME` | - | SASL username |
| `KAFKA_SASL_PASSWORD` | - | SASL password |
| `KAFKA_SSL_CA_LOCATION` | - | SSL CA certificate path |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | - | SSL certificate path |
| `KAFKA_SSL_KEY_LOCATION` | - | SSL key path |

### 10. Source-Specific Configuration

#### S3 Configuration
| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key ID |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key |
| `AWS_REGION` | AWS region |
| `S3_BUCKET_NAME` | S3 bucket name |
| `S3_ENDPOINT_URL` | S3 endpoint URL (for S3-compatible services) |

#### FTP Configuration
| Variable | Description |
|----------|-------------|
| `FTP_HOST` | FTP host address |
| `FTP_PORT` | FTP port (default: 21) |
| `FTP_USERNAME` | FTP username |
| `FTP_PASSWORD` | FTP password |
| `FTP_USE_TLS` | Use TLS (true/false) |

#### SFTP Configuration
| Variable | Description |
|----------|-------------|
| `SFTP_HOST` | SFTP host address |
| `SFTP_PORT` | SFTP port (default: 22) |
| `SFTP_USERNAME` | SFTP username |
| `SFTP_PASSWORD` | SFTP password |
| `SFTP_KEY_FILE_PATH` | Path to SSH private key file |

## Usage Examples

### Development Environment

```bash
# .env file for development
APP_NAME=iis-poller-worker
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
LOG_LEVEL=DEBUG
LOG_FORMAT=console
ENVIRONMENT=development
DEBUG=true
```

### Production Environment

```bash
# .env file for production
APP_NAME=iis-poller-worker
KAFKA_BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092,kafka3:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_SASL_USERNAME=your_username
KAFKA_SASL_PASSWORD=your_password
LOG_LEVEL=INFO
LOG_FORMAT=json
ENVIRONMENT=production
DEBUG=false
```

## Best Practices

1. **Never commit `.env`** - Add to `.gitignore`
2. **Use `.env.example`** - Document all required variables
3. **Set defaults** - Provide sensible defaults for all variables
4. **Use secret management** - For production, use AWS Secrets Manager, HashiCorp Vault, etc.
5. **Validate configuration** - The service validates all configuration on startup
6. **Document changes** - Update this file when adding new variables

## Troubleshooting

### Environment Variables Not Loading

```bash
# Check if .env file exists
ls -la .env

# Verify variable format (no spaces around =)
cat .env | grep KAFKA_BOOTSTRAP_SERVERS

# Check Python can read the file
python -c "from dotenv import load_dotenv; load_dotenv(); import os; print(os.getenv('KAFKA_BOOTSTRAP_SERVERS'))"
```

### Kafka Connection Issues

```bash
# Verify Kafka is accessible
telnet localhost 9092

# Check Kafka topics
kafka-topics.sh --list --bootstrap-server localhost:9092

# Test connection with environment variables
python -c "from src.config.kafka_settings import kafka_settings; print(kafka_settings.bootstrap_servers)"
```

### Configuration Validation Errors

The service validates all configuration on startup. Check logs for validation errors:

```bash
python -m src.main
# Look for Pydantic validation errors in the output
```

## Security Considerations

1. **Never commit secrets** - Use `.env` for local development, secret management for production
2. **Use TLS/SSL** - Enable SSL for Kafka in production
3. **Use SASL** - Enable SASL authentication for Kafka in production
4. **Rotate credentials** - Regularly rotate passwords and keys
5. **Limit access** - Use least privilege principle for service accounts

## Production Deployment

For production deployments:

1. Use environment-specific `.env` files or secret management
2. Enable Kafka security (SSL/SASL)
3. Set appropriate log levels (INFO or WARNING)
4. Enable metrics and monitoring
5. Configure health checks
6. Set appropriate timeouts and retries
7. Use connection pooling
8. Enable distributed tracing if needed

---

**Last Updated:** 2025-12-05

