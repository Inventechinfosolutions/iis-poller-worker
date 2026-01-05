# Poller Worker Scripts

Utility scripts for testing, debugging, and managing the poller worker service.

**Note:** These scripts are written in JavaScript/Node.js (using KafkaJS), matching the style of the `iis-resume-parser-llama-extract` project.

## Setup

1. **Install Node.js dependencies:**
   ```bash
   cd scripts
   npm install
   ```

2. **Configure environment variables (optional):**
   ```bash
   # Set Kafka connection details (or use defaults)
   export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   export POLLING_QUEUE_TOPIC=polling-queue
   export FILE_EVENT_QUEUE_TOPIC=file-event-queue
   ```

## Available Scripts

### 1. `push_polling_job.js`
Push a polling job to the Kafka `polling-queue` topic.

**Usage - Custom Format (Recommended):**
```bash
# Push a job from JSON file
node scripts/push_polling_job.js scripts/example_minio_job.json "*.csv" normal

# Push a job with inline JSON (escape quotes properly)
node scripts/push_polling_job.js '{"cvDatabaseType":"CV_SOURCE","configurationType":"MINIO_CONFIGURATION","configurationValue":{"endPoint":"http://localhost:9000","access":"minioadmin","secret":"minioadmin","bucketName":"iis-internal-dev-bucket"}}' "*.csv" normal
```

**Usage - Legacy Format:**
```bash
# Push a default dummy job
node scripts/push_polling_job.js

# Push a job with custom source type
node scripts/push_polling_job.js <source_type> <path> <file_pattern> <priority>

# Examples:
node scripts/push_polling_job.js dummy ./dummy_data "*.csv" normal
node scripts/push_polling_job.js minio ./dummy_data "*.csv" high

# Or using npm script:
npm run push
```

**Custom Format Parameters:**
- First argument: JSON file path OR JSON string with job configuration
- `file_pattern`: File pattern to match (optional, default: "*.csv")
- `priority`: Job priority (optional, default: "normal")

**Custom Format Example:**
```json
{
    "cvDatabaseType": "CV_SOURCE",
    "configurationType": "MINIO_CONFIGURATION",
    "configurationValue": {
        "endPoint": "http://localhost:9000",
        "access": "minioadmin",
        "secret": "minioadmin",
        "bucketName": "iis-internal-dev-bucket"
    }
}
```

**Legacy Format Parameters:**
- `source_type`: Source type (dummy, s3, ftp, etc.) - default: "dummy"
- `path`: Path to source (file path, bucket name, etc.) - default: "./dummy_data"
- `file_pattern`: File pattern to match - default: "*.csv"
- `priority`: Job priority (low, normal, high) - default: "normal"

### 2. `check_polling_queue.js`
Check messages in the `polling-queue` topic.

**Usage:**
```bash
# Check last 10 messages (default)
node scripts/check_polling_queue.js

# Check last N messages
node scripts/check_polling_queue.js 20

# Or using npm script:
npm run check-polling
```

**Output:**
- Shows job details, partition, offset, and timestamp for each message
- Useful for debugging job processing issues

### 3. `check_file_event_queue.js`
Check messages in the `file-event-queue` topic (where poller worker publishes file events).

**Usage:**
```bash
# Check last 10 file events (default)
node scripts/check_file_event_queue.js

# Check last N file events
node scripts/check_file_event_queue.js 20

# Or using npm script:
npm run check-events
```

**Output:**
- Shows file event details including event ID, job ID, file name, file type, source type, and file path
- Useful for verifying that files are being published correctly

### 4. `verify_kafka_setup.js`
Verify Kafka setup and configuration for the poller worker.

**Usage:**
```bash
node scripts/verify_kafka_setup.js

# Or using npm script:
npm run verify
```

**Checks:**
- ✅ Kafka broker connectivity
- ✅ Required topics exist (polling-queue, file-event-queue)
- ✅ Producer configuration
- ✅ Consumer configuration
- ✅ Kafka settings validation

**Output:**
- Detailed verification report
- Exit code 0 if all checks pass, 1 if any fail

## Quick Start

1. **Install dependencies:**
   ```bash
   cd scripts
   npm install
   ```

2. **Verify Kafka setup:**
   ```bash
   node scripts/verify_kafka_setup.js
   # or: npm run verify
   ```

3. **Start the poller worker:**
   ```bash
   python -m src.main
   ```

4. **Push a test job:**
   ```bash
   node scripts/push_polling_job.js
   # or: npm run push
   ```

5. **Check if file events were published:**
   ```bash
   node scripts/check_file_event_queue.js
   # or: npm run check-events
   ```

## Examples

### Test with Dummy Data
```bash
# 1. Verify setup
node scripts/verify_kafka_setup.js

# 2. Start poller worker (in another terminal)
python -m src.main

# 3. Push a job
node scripts/push_polling_job.js dummy ./dummy_data "*.csv" normal

# 4. Check file events
node scripts/check_file_event_queue.js
```

### Test with S3 Source
```bash
# Push a job for S3 source
node scripts/push_polling_job.js s3 my-bucket "*.csv" high

# Check the polling queue to see the job
node scripts/check_polling_queue.js
```

## Troubleshooting

### Scripts can't find modules
Make sure you've installed Node.js dependencies:
```bash
cd scripts
npm install
```

### Node.js not installed
Install Node.js from https://nodejs.org/ (version 14+ required)

### Kafka connection errors
1. Verify Kafka is running:
   ```bash
   # Check if Kafka is accessible
   node scripts/verify_kafka_setup.js
   ```

2. Check environment variables:
   ```bash
   # Make sure environment variables are set correctly
   echo $KAFKA_BOOTSTRAP_SERVERS
   ```

### No messages in queue
- Make sure the poller worker is running and consuming messages
- Check if jobs were actually published (use `check_polling_queue.js`)
- Verify topic names match in configuration

## Notes

- All scripts use environment variables or defaults (matching the Python service configuration)
- Scripts create temporary consumer groups (won't interfere with the main poller worker)
- Scripts are designed to be non-destructive (read-only operations, except `push_polling_job.js`)
- Scripts use KafkaJS library (same as the resume parser project)
- Environment variables can be set in `.env` file or exported in shell

