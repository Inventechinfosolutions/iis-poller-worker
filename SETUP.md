# Poller Worker - Local Setup Guide

This guide will help you set up and run the Poller Worker service locally in the Nx workspace.

## 📋 Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.8+** (Python 3.13 recommended)
- **Node.js 18+** and npm (for Nx workspace)
- **MySQL 8.0+** (for database)
- **Kafka** (for message queue)
- **Redis** (optional, for caching/persistence)
- **MinIO** (optional, for S3-compatible storage)

### Quick Prerequisites Check

```bash
# Check Python version
python3 --version

# Check Node.js version
node --version

# Check npm version
npm --version
```

## 🚀 Installation Steps

### 1. Navigate to Project Root

```bash
cd /path/to/nxtworkforceai-nx
```

### 2. Initialize Git Submodule (if needed)

The poller-worker is a git submodule. If you're setting up the workspace for the first time:

```bash
# Initialize and update all submodules
git submodule update --init --recursive

# Or update just poller-worker
git submodule update --init apps/poller-worker
```

### 3. Create Python Virtual Environment

Navigate to the poller-worker directory and create a Python virtual environment:

```bash
# Navigate to poller-worker directory
cd apps/poller-worker

# Create virtual environment named .venv
python3 -m venv .venv
```

**Verify the virtual environment was created:**

```bash
# Check if .venv directory exists
ls -la .venv  # macOS/Linux
# or
dir .venv     # Windows

# You should see directories like: bin/, lib/, include/, pyvenv.cfg
```

**Note:** 
- If the `.venv` directory already exists, you can skip this step or remove it first if you want to recreate it:
  ```bash
  # Remove existing virtual environment (optional)
  rm -rf .venv  # macOS/Linux
  # or
  rmdir /s .venv  # Windows
  
  # Then create a new one
  python3 -m venv .venv
  ```
- The `.venv` folder is git-ignored and should not be committed to the repository.

### 4. Activate Virtual Environment

```bash
# macOS/Linux
source .venv/bin/activate

# Windows (PowerShell)
.venv\Scripts\Activate.ps1

# Windows (Command Prompt)
.venv\Scripts\activate.bat
```

### 5. Install Python Dependencies

Using Nx (recommended):

```bash
# From workspace root
cd /path/to/nxtworkforceai-nx
nx run poller-worker:install
```

Or manually:

```bash
cd apps/poller-worker
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
```

### 6. Verify Installation

```bash
# Check if dependencies are installed
cd apps/poller-worker
.venv/bin/python -c "import fastapi, kafka, redis; print('Dependencies installed successfully')"
```

## ⚙️ Configuration

### 1. Environment Variables

The service uses environment files located in the `environments/` directory:

- **`environments/env.local`** - Local development configuration
- **`environments/env.dev`** - Development/Docker environment configuration

The environment file is automatically loaded based on the Nx configuration when running the service.

### 2. Database Setup (MySQL)

#### Option A: Using Docker Compose (Recommended)

If you have docker-compose set up in the workspace:

```bash
# From workspace root
docker-compose up -d mysql redis kafka
```

#### Option B: Local MySQL Installation

1. **Install MySQL** (if not already installed):

```bash
# macOS
brew install mysql
brew services start mysql

# Ubuntu/Debian
sudo apt-get install mysql-server
sudo systemctl start mysql
```

2. **Create Database and User**:

```bash
mysql -u root -p
```

```sql
CREATE DATABASE iis_poller_worker CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'root'@'localhost' IDENTIFIED BY 'rootpassword';
GRANT ALL PRIVILEGES ON iis_poller_worker.* TO 'root'@'localhost';
FLUSH PRIVILEGES;
EXIT;
```

3. **Update Environment File**:

The `environments/env.local` file is already configured with:
- Database: `iis_poller_worker`
- Host: `localhost`
- Port: `3310` (adjust if your MySQL runs on a different port)
- User: `root`
- Password: `rootpassword`

Update these values in `environments/env.local` if your setup differs.

### 3. Kafka Setup

#### Option A: Using Docker Compose

```bash
# From workspace root
docker-compose up -d kafka zookeeper
```

#### Option B: Local Kafka Installation

Follow the [Kafka Quick Start Guide](https://kafka.apache.org/quickstart) to install and start Kafka locally.

The default configuration expects Kafka on `localhost:9092`.

### 4. Redis Setup (Optional)

Redis is used for file tracking and connection statistics. It's optional but recommended.

```bash
# Using Docker
docker run -d -p 6379:6379 redis:7-alpine

# Or using Homebrew (macOS)
brew install redis
brew services start redis
```

### 5. MinIO Setup (Optional)

MinIO is used for S3-compatible storage. It's optional unless you're using S3 source types.

```bash
# Using Docker
docker run -d -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ACCESS_KEY=minioadmin" \
  -e "MINIO_SECRET_KEY=minioadmin123" \
  minio/minio server /data --console-address ":9001"
```

## 🏃 Running the Service

### Using Nx (Recommended)

From the workspace root:

```bash
# Run with local configuration
nx run poller-worker:serve:local

# Run with dev configuration (for Docker environments)
nx run poller-worker:serve:dev
```

### Direct Python Execution

```bash
cd apps/poller-worker

# Activate virtual environment
source .venv/bin/activate  # macOS/Linux
# or
.venv\Scripts\activate  # Windows

# Set environment file
export ENV_FILE=environments/env.local  # macOS/Linux
# or
set ENV_FILE=environments\env.local  # Windows

# Run the service
python -m src.main
```

## ✅ Verification

### 1. Check Service Status

Once the service is running, you should see logs indicating:

```
Starting IIS Poller Worker Service
Initializing poller worker application
Database client initialized
Poller worker initialized
Health check API started on port 3009
```

### 2. Test Health Endpoint

```bash
# Health check
curl http://localhost:3009/poller-worker/api/v1/health

# Readiness check
curl http://localhost:3009/poller-worker/api/v1/ready

# Metrics
curl http://localhost:3009/poller-worker/api/v1/metrics
```

### 3. Access API Documentation

Open your browser and navigate to:

- **Swagger UI**: http://localhost:3009/docs
- **ReDoc**: http://localhost:3009/redoc

### 4. Verify Database Connection

The service automatically creates required database tables on startup. Check the logs for:

```
Database initialized successfully, tables created/verified
```

### 5. Verify Kafka Connection

Check the logs for Kafka connection status:

```
Polling queue consumer initialized successfully
File event queue producer initialized successfully
```

If you see connection errors, ensure Kafka is running and accessible at the configured address.

## 🧪 Testing

### 1. Run Tests

```bash
# Using Nx
nx run poller-worker:test

# Or directly
cd apps/poller-worker
.venv/bin/python -m pytest
```

### 2. Send Test Job to Kafka

The service consumes jobs from the `polling-queue` Kafka topic. You can send test jobs using the scripts:

```bash
cd apps/poller-worker/scripts
npm install
node push_polling_job.js
```

### 3. Check File Events

After processing a job, file events are published to the `file-event-queue` topic:

```bash
cd apps/poller-worker/scripts
node check_file_event_queue.js
```

## 🔧 Common Issues and Troubleshooting

### Issue: Python Virtual Environment Not Found

**Solution:**
```bash
cd apps/poller-worker
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### Issue: Module Not Found Errors

**Solution:**
```bash
# Ensure virtual environment is activated
source .venv/bin/activate

# Reinstall dependencies
pip install -r requirements.txt
```

### Issue: Kafka Connection Refused

**Symptoms:** Logs show `Connection refused` errors for Kafka

**Solution:**
1. Verify Kafka is running:
   ```bash
   # Check if Kafka is listening on port 9092
   netstat -an | grep 9092  # macOS/Linux
   # or
   netstat -an | findstr 9092  # Windows
   ```

2. Check Kafka configuration in `environments/env.local`:
   ```bash
   KAFKA_BOOTSTRAP_SERVERS=localhost:9092
   ```

3. Start Kafka if not running:
   ```bash
   docker-compose up -d kafka zookeeper
   ```

### Issue: Database Connection Failed

**Symptoms:** Database initialization errors

**Solution:**
1. Verify MySQL is running:
   ```bash
   # Check MySQL status
   brew services list  # macOS
   # or
   sudo systemctl status mysql  # Linux
   ```

2. Verify database exists:
   ```bash
   mysql -u root -p -e "SHOW DATABASES LIKE 'iis_poller_worker';"
   ```

3. Check connection settings in `environments/env.local`:
   ```bash
   MYSQL_HOST=localhost
   MYSQL_PORT=3310
   MYSQL_USER=root
   MYSQL_PASSWORD=rootpassword
   MYSQL_DATABASE=iis_poller_worker
   ```

### Issue: Port Already in Use

**Symptoms:** `Address already in use` error on port 3009

**Solution:**
1. Find process using the port:
   ```bash
   # macOS/Linux
   lsof -i :3009
   
   # Windows
   netstat -ano | findstr :3009
   ```

2. Kill the process or change the port in `environments/env.local`:
   ```bash
   HEALTH_API_PORT=3009  # Change to another port if needed
   ```

### Issue: Environment File Not Loading

**Symptoms:** Service uses default values instead of environment file values

**Solution:**
1. Verify the environment file path is correct
2. Check the ENV_FILE environment variable is set when using Nx (it's automatically set by the project.json configuration)
3. When running directly, ensure ENV_FILE is set:
   ```bash
   export ENV_FILE=environments/env.local
   python -m src.main
   ```

## 📝 Nx Commands Reference

The following Nx commands are available for poller-worker:

```bash
# Install dependencies
nx run poller-worker:install

# Run service (local configuration)
nx run poller-worker:serve:local

# Run service (dev configuration)
nx run poller-worker:serve:dev

# Run tests
nx run poller-worker:test

# Run linting
nx run poller-worker:lint
```

## 🔗 Useful Links

- [Environment Variables Documentation](./ENVIRONMENT_VARIABLES.md)
- [Health Check Implementation](./HEALTH_CHECK_IMPLEMENTATION.md)
- [Project Summary](./PROJECT_SUMMARY.md)
- [Main README](./README.md)

## 📞 Support

For issues or questions:
1. Check the troubleshooting section above
2. Review the logs for detailed error messages
3. Check the [README.md](./README.md) for architecture and usage details
4. Review environment variable documentation in [ENVIRONMENT_VARIABLES.md](./ENVIRONMENT_VARIABLES.md)

