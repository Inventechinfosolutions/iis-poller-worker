#!/usr/bin/env node

/**
 * Verify Kafka Setup
 * 
 * Verifies Kafka setup and configuration for the poller worker.
 */

const { Kafka } = require('kafkajs');

// Kafka configuration
const kafka = new Kafka({
    clientId: 'kafka-verifier',
    brokers: process.env.KAFKA_BOOTSTRAP_SERVERS ? process.env.KAFKA_BOOTSTRAP_SERVERS.split(',') : ['localhost:9092'],
});

const admin = kafka.admin();
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'kafka-verifier-group' });

async function verifyBrokerConnectivity() {
    console.log('🔍 Testing Kafka broker connectivity...');
    
    try {
        await admin.connect();
        const metadata = await admin.fetchMetadata();
        
        console.log(`✅ Connected to Kafka broker: ${process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'}`);
        console.log(`   Cluster ID: ${metadata.clusterId || 'N/A'}`);
        console.log(`   Available topics: ${metadata.topicMetadata.length}`);
        
        return true;
        
    } catch (error) {
        console.log(`❌ Failed to connect to Kafka broker: ${error.message}`);
        console.log(`   Make sure Kafka is running at: ${process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'}`);
        return false;
    }
}

async function verifyTopics() {
    console.log('\n🔍 Verifying required topics...');
    
    const requiredTopics = [
        process.env.POLLING_QUEUE_TOPIC || 'polling-queue',
        process.env.FILE_EVENT_QUEUE_TOPIC || 'file-event-queue'
    ];
    
    try {
        const metadata = await admin.fetchMetadata();
        const existingTopics = new Set(metadata.topicMetadata.map(t => t.name));
        
        console.log('\nRequired topics:');
        let allExist = true;
        
        for (const topic of requiredTopics) {
            if (existingTopics.has(topic)) {
                const topicMeta = metadata.topicMetadata.find(t => t.name === topic);
                const partitions = topicMeta ? topicMeta.partitions.length : 0;
                console.log(`  ✅ ${topic} - ${partitions} partition(s)`);
            } else {
                console.log(`  ❌ ${topic} - NOT FOUND`);
                allExist = false;
            }
        }
        
        if (!allExist) {
            console.log('\n⚠️  Some topics are missing. They will be created automatically when the service starts.');
        }
        
        return allExist;
        
    } catch (error) {
        console.log(`❌ Error verifying topics: ${error.message}`);
        return false;
    }
}

async function verifyProducerConfig() {
    console.log('\n🔍 Verifying producer configuration...');
    
    try {
        await producer.connect();
        
        console.log('✅ Producer configuration valid');
        console.log(`   Bootstrap Servers: ${process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'}`);
        console.log(`   Client ID: ${process.env.KAFKA_CLIENT_ID || 'poller-worker'}`);
        
        await producer.disconnect();
        return true;
        
    } catch (error) {
        console.log(`❌ Producer configuration error: ${error.message}`);
        return false;
    }
}

async function verifyConsumerConfig() {
    console.log('\n🔍 Verifying consumer configuration...');
    
    try {
        await consumer.connect();
        
        console.log('✅ Consumer configuration valid');
        console.log(`   Bootstrap Servers: ${process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'}`);
        console.log(`   Group ID: ${process.env.KAFKA_GROUP_ID || 'poller-worker-group'}`);
        console.log(`   Client ID: ${process.env.KAFKA_CLIENT_ID || 'poller-worker'}`);
        
        await consumer.disconnect();
        return true;
        
    } catch (error) {
        console.log(`❌ Consumer configuration error: ${error.message}`);
        return false;
    }
}

async function verifyKafkaSettings() {
    console.log('\n🔍 Verifying Kafka settings...');
    
    console.log('Configuration:');
    console.log(`   Bootstrap Servers: ${process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'}`);
    console.log(`   Client ID: ${process.env.KAFKA_CLIENT_ID || 'poller-worker'}`);
    console.log(`   Group ID: ${process.env.KAFKA_GROUP_ID || 'poller-worker-group'}`);
    console.log(`   Polling Queue Topic: ${process.env.POLLING_QUEUE_TOPIC || 'polling-queue'}`);
    console.log(`   File Event Queue Topic: ${process.env.FILE_EVENT_QUEUE_TOPIC || 'file-event-queue'}`);
    
    // Check critical fields
    console.log('\nCritical Field Verification:');
    
    const bootstrapServers = process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092';
    const groupId = process.env.KAFKA_GROUP_ID || 'poller-worker-group';
    const pollingTopic = process.env.POLLING_QUEUE_TOPIC || 'polling-queue';
    const fileEventTopic = process.env.FILE_EVENT_QUEUE_TOPIC || 'file-event-queue';
    
    if (bootstrapServers) {
        console.log(`   ✅ bootstrap_servers is set: '${bootstrapServers}'`);
    } else {
        console.log(`   ❌ bootstrap_servers is EMPTY!`);
        return false;
    }
    
    if (groupId) {
        console.log(`   ✅ group_id is set: '${groupId}'`);
    } else {
        console.log(`   ❌ group_id is EMPTY!`);
        return false;
    }
    
    if (pollingTopic) {
        console.log(`   ✅ polling_queue_topic is set: '${pollingTopic}'`);
    } else {
        console.log(`   ❌ polling_queue_topic is EMPTY!`);
        return false;
    }
    
    if (fileEventTopic) {
        console.log(`   ✅ file_event_queue_topic is set: '${fileEventTopic}'`);
    } else {
        console.log(`   ❌ file_event_queue_topic is EMPTY!`);
        return false;
    }
    
    return true;
}

async function main() {
    console.log('='.repeat(70));
    console.log('🔍 Kafka Setup Verification for Poller Worker');
    console.log('='.repeat(70));
    console.log();
    
    const results = [];
    
    // Verify settings
    const settingsResult = verifyKafkaSettings();
    results.push({ name: 'Kafka Settings', result: await settingsResult });
    
    // Verify broker connectivity
    const connectivityResult = await verifyBrokerConnectivity();
    results.push({ name: 'Broker Connectivity', result: connectivityResult });
    
    if (connectivityResult) {
        // Verify topics
        const topicsResult = await verifyTopics();
        results.push({ name: 'Required Topics', result: topicsResult });
    }
    
    // Verify producer config
    const producerResult = await verifyProducerConfig();
    results.push({ name: 'Producer Configuration', result: producerResult });
    
    // Verify consumer config
    const consumerResult = await verifyConsumerConfig();
    results.push({ name: 'Consumer Configuration', result: consumerResult });
    
    // Cleanup
    try {
        await admin.disconnect();
    } catch (e) {
        // Ignore
    }
    
    // Summary
    console.log('\n' + '='.repeat(70));
    console.log('📊 Verification Summary');
    console.log('='.repeat(70));
    
    let passed = 0;
    const total = results.length;
    
    for (const { name, result } of results) {
        const status = result ? '✅ PASS' : '❌ FAIL';
        console.log(`${name.padEnd(40)} ${status}`);
        if (result) {
            passed++;
        }
    }
    
    console.log('='.repeat(70));
    console.log(`Tests passed: ${passed}/${total}`);
    
    if (passed === total) {
        console.log('\n🎉 All verifications passed! Kafka setup is ready for poller worker.');
        process.exit(0);
    } else {
        console.log(`\n⚠️  ${total - passed} verification(s) failed. Please check the setup.`);
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

module.exports = {
    verifyBrokerConnectivity,
    verifyTopics,
    verifyProducerConfig,
    verifyConsumerConfig,
    verifyKafkaSettings
};

