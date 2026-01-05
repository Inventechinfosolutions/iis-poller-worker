#!/usr/bin/env node

/**
 * Push Polling Job to Kafka
 * 
 * Pushes a polling job to the polling-queue topic for testing the poller worker.
 */

const { Kafka } = require('kafkajs');

// Kafka configuration
const kafka = new Kafka({
    clientId: 'poller-job-pusher',
    brokers: process.env.KAFKA_BOOTSTRAP_SERVERS ? process.env.KAFKA_BOOTSTRAP_SERVERS.split(',') : ['localhost:9092'],
});

const producer = kafka.producer();

/**
 * Transform custom job format to PollingJob format
 * Custom format:
 * {
 *   "cvDatabaseType": "CV_SOURCE",
 *   "configurationType": "MINIO_CONFIGURATION",
 *   "configurationValue": {
 *     "endPoint": "http://localhost:9000",
 *     "access": "minioadmin",
 *     "secret": "minioadmin",
 *     "bucketName": "iis-internal-dev-bucket"
 *   }
 * }
 */
function createPollingJobFromCustomFormat(customJob, filePattern = '*.csv', priority = 'normal') {
    const jobId = `job_${new Date().toISOString().replace(/[-:]/g, '').split('.')[0]}_${Math.random().toString(36).substr(2, 8)}`;
    
    // Map configurationType to source_type
    const configTypeMap = {
        'MINIO_CONFIGURATION': 'minio',
        'S3_CONFIGURATION': 's3',
        'FTP_CONFIGURATION': 'ftp',
        'SFTP_CONFIGURATION': 'sftp',
        'MYSQL_CONFIGURATION': 'mysql',
        'LOCAL_CONFIGURATION': 'local'
    };
    
    const sourceType = configTypeMap[customJob.configurationType] || 'dummy';
    const configValue = customJob.configurationValue || {};
    
    // Build source_config based on configuration type
    const sourceConfig = {
        source_type: sourceType
    };
    
    // Map MinIO/S3 configuration
    if (sourceType === 'minio' || sourceType === 's3') {
        if (configValue.endPoint) sourceConfig.endpoint = configValue.endPoint;
        if (configValue.access) sourceConfig.access_key = configValue.access;
        if (configValue.secret) sourceConfig.secret_key = configValue.secret;
        if (configValue.bucketName) sourceConfig.bucket_name = configValue.bucketName;
        if (configValue.path) sourceConfig.path = configValue.path;
    }
    
    return {
        job_id: jobId,
        connection_list: [sourceConfig],  // Use connection_list (new format)
        file_pattern: filePattern,
        priority: priority,
        metadata: {
            cvDatabaseType: customJob.cvDatabaseType,
            configurationType: customJob.configurationType,
            original_configuration: customJob.configurationValue,
            created_at: new Date().toISOString(),
            description: `Polling job for ${customJob.cvDatabaseType} with ${customJob.configurationType}`
        },
        created_at: new Date().toISOString()
    };
}

async function createPollingJob(sourceType = 'dummy', path = './dummy_data', filePattern = '*.csv', priority = 'normal') {
    const jobId = `job_${new Date().toISOString().replace(/[-:]/g, '').split('.')[0]}_${Math.random().toString(36).substr(2, 8)}`;
    
    return {
        job_id: jobId,
        connection_list: [{
            source_type: sourceType,
            path: path
        }],
        file_pattern: filePattern,
        priority: priority,
        metadata: {
            test: true,
            created_at: new Date().toISOString(),
            description: `Test polling job for ${sourceType} source`
        },
        created_at: new Date().toISOString()
    };
}

async function pushJobToKafka(job, topic = 'polling-queue') {
    try {
        await producer.connect();
        console.log('✅ Connected to Kafka producer');
        
        await producer.send({
            topic: topic,
            messages: [{
                key: job.job_id,
                value: JSON.stringify(job),
                headers: {
                    job_id: job.job_id,
                    priority: job.priority
                }
            }]
        });
        
        console.log('\n✅ Job published successfully!');
        console.log('📋 Job Details:');
        console.log(`   Job ID: ${job.job_id}`);
        console.log(`   Topic: ${topic}`);
        if (job.connection_list && job.connection_list.length > 0) {
            const conn = job.connection_list[0];
            console.log(`   Source Type: ${conn.source_type}`);
            if (conn.endpoint) console.log(`   Endpoint: ${conn.endpoint}`);
            if (conn.bucket_name) console.log(`   Bucket: ${conn.bucket_name}`);
            if (conn.path) console.log(`   Path: ${conn.path}`);
        } else if (job.source_config) {
            console.log(`   Source Type: ${job.source_config.source_type}`);
            if (job.source_config.path) console.log(`   Path: ${job.source_config.path}`);
        }
        console.log(`   File Pattern: ${job.file_pattern}`);
        console.log(`   Priority: ${job.priority}`);
        
        return true;
        
    } catch (error) {
        console.error('❌ Error publishing job:', error.message);
        return false;
    } finally {
        await producer.disconnect();
    }
}

async function main() {
    console.log('='.repeat(70));
    console.log('📤 Publishing Polling Job to Kafka');
    console.log('='.repeat(70));
    
    const brokers = process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';
    const topic = process.env.POLLING_QUEUE_TOPIC || 'polling-queue';
    
    let job;
    
    // Check if first argument is a JSON file or JSON string
    const firstArg = process.argv[2];
    
    if (firstArg && (firstArg.startsWith('{') || firstArg.endsWith('.json'))) {
        // Custom format: JSON string or file path
        let customJobData;
        
        if (firstArg.startsWith('{')) {
            // JSON string provided directly
            try {
                customJobData = JSON.parse(firstArg);
            } catch (e) {
                console.error('❌ Invalid JSON string:', e.message);
                process.exit(1);
            }
        } else {
            // JSON file path
            const fs = require('fs');
            try {
                const fileContent = fs.readFileSync(firstArg, 'utf8');
                customJobData = JSON.parse(fileContent);
            } catch (e) {
                console.error('❌ Error reading JSON file:', e.message);
                process.exit(1);
            }
        }
        
        // Validate custom format
        if (!customJobData.configurationType || !customJobData.configurationValue) {
            console.error('❌ Invalid custom job format. Expected:');
            console.error('   {');
            console.error('     "cvDatabaseType": "CV_SOURCE",');
            console.error('     "configurationType": "MINIO_CONFIGURATION",');
            console.error('     "configurationValue": { ... }');
            console.error('   }');
            process.exit(1);
        }
        
        const filePattern = process.argv[3] || '*.csv';
        const priority = process.argv[4] || 'normal';
        
        console.log(`\nKafka Server: ${brokers}`);
        console.log(`Topic: ${topic}`);
        console.log(`\n📋 Custom Job Format:`);
        console.log(`   CV Database Type: ${customJobData.cvDatabaseType || 'N/A'}`);
        console.log(`   Configuration Type: ${customJobData.configurationType}`);
        console.log(`   File Pattern: ${filePattern}`);
        console.log(`   Priority: ${priority}`);
        console.log('-'.repeat(70));
        
        job = createPollingJobFromCustomFormat(customJobData, filePattern, priority);
        
    } else {
        // Legacy format: command line arguments
        const sourceType = firstArg || 'dummy';
        const path = process.argv[3] || './dummy_data';
        const filePattern = process.argv[4] || '*.csv';
        const priority = process.argv[5] || 'normal';
        
        console.log(`\nKafka Server: ${brokers}`);
        console.log(`Topic: ${topic}`);
        console.log(`\n📋 Job Details:`);
        console.log(`   Source Type: ${sourceType}`);
        console.log(`   Path: ${path}`);
        console.log(`   File Pattern: ${filePattern}`);
        console.log(`   Priority: ${priority}`);
        console.log('-'.repeat(70));
        
        job = await createPollingJob(sourceType, path, filePattern, priority);
    }
    
    // Push to Kafka
    const success = await pushJobToKafka(job, topic);
    
    if (success) {
        console.log('\n' + '='.repeat(70));
        console.log('✅ Job Published Successfully!');
        console.log('='.repeat(70));
        console.log(`Job ID: ${job.job_id}`);
        console.log('\n💡 Now check your poller worker logs to see:');
        console.log('   Step 1: Message received from Kafka');
        console.log('   Step 2: Job parsed');
        console.log('   Step 3: Connected to source');
        console.log('   Step 4: Files discovered');
        console.log('   Step 5: Files published to file-event-queue');
        console.log('\n💡 To check file events:');
        console.log(`   node scripts/check_file_event_queue.js 50 ${job.job_id}`);
        process.exit(0);
    } else {
        console.log('\n❌ Failed to publish job');
        process.exit(1);
    }
}

// Handle errors
process.on('unhandledRejection', (error) => {
    console.error('💥 Unhandled error:', error.message);
    process.exit(1);
});

// Run main function
if (require.main === module) {
    main().catch(error => {
        console.error('💥 Fatal error:', error.message);
        process.exit(1);
    });
}

module.exports = { createPollingJob, createPollingJobFromCustomFormat, pushJobToKafka };

