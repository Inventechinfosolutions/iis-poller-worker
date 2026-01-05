# IIS Poller Worker Service

Production-ready poller worker service for processing polling queue jobs and publishing file events.

## Architecture

The service implements a 5-step process:

1. **Listen to Kafka Topic**: Consumes jobs from the `polling-queue` Kafka topic
2. **Pick Job from Kafka**: Parses and validates polling job messages
3. **Connect to Source**: Establishes connection to the configured data source
4. **Read Files from Source**: Discovers and lists files from the source
5. **Push to File Event Queue**: Publishes CSV files to the `file-event-queue` Kafka topic

## Folder Structure

```
iis-poller-worker/
├── src/
│   ├── __init__.py
│   ├── main.py                 # Application entry point
│   ├── config/
│   │   ├── __init__.py
│   │   ├── settings.py         # Application settings
│   │   └── kafka_settings.py   # Kafka configuration
│   ├── models/
│   │   ├── __init__.py
│   │   └── schemas.py          # Data models and schemas
│   ├── services/
│   │   ├── __init__.py
│   │   ├── kafka_consumer.py   # Kafka consumer for polling queue
│   │   ├── kafka_producer.py   # Kafka producer for file event queue
│   │   ├── source_connector.py # Source connection logic
│   │   └── poller_worker.py    # Main orchestration logic
│   └── utils/
│       ├── __init__.py
│       ├── logging.py           # Logging configuration
│       └── monitoring.py       # Metrics and health checks
├── dummy_data/                 # Dummy data files for testing
│   ├── sample_data_1.csv
│   ├── sample_data_2.csv
│   └── sample_data_3.csv
├── requirements.txt
├── .env.example
└── README.md
```

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment variables (see `.env.example`)

3. Create dummy data directory:
```bash
mkdir -p dummy_data
```

## Configuration

### Environment Variables

- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers (default: `localhost:9092`)
- `KAFKA_CLIENT_ID`: Kafka client ID (default: `poller-worker`)
- `KAFKA_GROUP_ID`: Kafka consumer group ID (default: `poller-worker-group`)
- `POLLING_QUEUE_TOPIC`: Polling queue topic name (default: `polling-queue`)
- `FILE_EVENT_QUEUE_TOPIC`: File event queue topic name (default: `file-event-queue`)
- `DUMMY_DATA_PATH`: Path to dummy data files (default: `./dummy_data`)
- `LOG_LEVEL`: Logging level (default: `INFO`)
- `LOG_FORMAT`: Log format - `json` or `console` (default: `json`)

## Usage

### Running the Service

```bash
python -m src.main
```

### Sending a Test Job

**Option 1: Using the JavaScript script (Recommended)**
```bash
cd scripts
npm install
node push_polling_job.js
```

**Option 2: Manual JSON**
Publish a job to the `polling-queue` topic:

```json
{
  "job_id": "job_123",
  "source_config": {
    "source_type": "dummy",
    "path": "./dummy_data"
  },
  "file_pattern": "*.csv",
  "priority": "normal",
  "metadata": {}
}
```

### Utility Scripts

The `scripts/` directory contains JavaScript/Node.js utilities for testing and debugging (matching the style of `iis-resume-parser-llama-extract`):

- **`push_polling_job.js`**: Push test jobs to the polling queue
- **`check_polling_queue.js`**: Check messages in the polling queue
- **`check_file_event_queue.js`**: Check file events published by the worker
- **`verify_kafka_setup.js`**: Verify Kafka configuration and connectivity

**Setup:**
```bash
cd scripts
npm install
```

**Usage:**
```bash
node scripts/push_polling_job.js
node scripts/check_polling_queue.js
node scripts/check_file_event_queue.js
node scripts/verify_kafka_setup.js
```

See `scripts/README.md` for detailed usage instructions.

## Source Types

Currently supported source types:

- **dummy**: Uses local dummy data files (fully implemented)
- **local**: Local file system (fully implemented)
- **s3**: AWS S3 (placeholder)
- **ftp**: FTP server (placeholder)
- **sftp**: SFTP server (placeholder)
- **http**: HTTP endpoint (placeholder)

## Monitoring

The service exposes Prometheus metrics:

- `poller_jobs_total`: Total number of polling jobs processed
- `poller_job_duration_seconds`: Time spent processing jobs
- `poller_files_processed_total`: Total number of files processed
- `poller_files_published_total`: Total number of files published
- `poller_active_jobs`: Number of active polling jobs
- `poller_kafka_messages_consumed_total`: Kafka messages consumed
- `poller_kafka_messages_published_total`: Kafka messages published
- `poller_connection_errors_total`: Connection errors by source type

## Error Handling

- Comprehensive error handling at each step
- Automatic retry logic for Kafka operations
- Connection error tracking and metrics
- Graceful shutdown handling
- Dead letter queue support (via Kafka error topics)

## Production Features

- Structured logging with JSON format
- Prometheus metrics integration
- Health check endpoints
- Graceful shutdown
- Connection pooling
- Error tracking and monitoring
- Configurable timeouts and retries

