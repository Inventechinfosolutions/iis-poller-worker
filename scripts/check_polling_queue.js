#!/usr/bin/env node

/**
 * Check Polling Queue
 * 
 * Checks messages in the polling-queue topic for debugging and monitoring.
 */

const { Kafka } = require('kafkajs');

// Kafka configuration
const kafka = new Kafka({
    clientId: 'polling-queue-checker',
    brokers: process.env.KAFKA_BOOTSTRAP_SERVERS ? process.env.KAFKA_BOOTSTRAP_SERVERS.split(',') : ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'polling-queue-checker-group' });

async function checkPollingQueue(limit = 10, timeout = 5000) {
    const topic = process.env.POLLING_QUEUE_TOPIC || 'polling-queue';
    
    console.log('🔍 Checking polling queue:', topic);
    console.log(`   Bootstrap Servers: ${process.env.KAFKA_BOOTSTRAP_SERVERS || 'localhost:9092'}`);
    console.log(`   Limit: ${limit} messages`);
    console.log('-'.repeat(70));
    
    try {
        await consumer.connect();
        await consumer.subscribe({ 
            topics: [topic],
            fromBeginning: false 
        });
        
        const messages = [];
        let messageCount = 0;
        
        await consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                if (messageCount < limit) {
                    const key = message.key?.toString() || 'no-key';
                    let value;
                    
                    try {
                        value = JSON.parse(message.value.toString());
                    } catch (e) {
                        value = message.value.toString();
                    }
                    
                    const timestamp = message.timestamp ? new Date(parseInt(message.timestamp)).toISOString() : 'N/A';
                    
                    messages.push({
                        key,
                        value,
                        partition,
                        offset: message.offset,
                        timestamp
                    });
                    
                    messageCount++;
                    
                    if (messageCount >= limit) {
                        await consumer.stop();
                    }
                }
            }
        });
        
        // Wait for messages or timeout
        await new Promise((resolve) => {
            setTimeout(async () => {
                await consumer.stop();
                await consumer.disconnect();
                resolve();
            }, timeout);
        });
        
        // Display messages
        if (messages.length > 0) {
            console.log(`\n📋 Found ${messages.length} message(s):\n`);
            messages.forEach((msg, i) => {
                console.log(`Message ${i + 1}:`);
                console.log(`   Key: ${msg.key}`);
                console.log(`   Partition: ${msg.partition}`);
                console.log(`   Offset: ${msg.offset}`);
                console.log(`   Timestamp: ${msg.timestamp}`);
                console.log(`   Value:`);
                console.log(JSON.stringify(msg.value, null, 6));
                console.log('-'.repeat(70));
            });
        } else {
            console.log('\n📭 Queue is empty or no new messages found');
            console.log('💡 Try pushing a job first: node push_polling_job.js');
        }
        
        return messages;
        
    } catch (error) {
        console.error('❌ Error checking queue:', error.message);
        return [];
    } finally {
        try {
            await consumer.disconnect();
        } catch (e) {
            // Ignore disconnect errors
        }
    }
}

async function main() {
    console.log('='.repeat(70));
    console.log('📊 Polling Queue Checker');
    console.log('='.repeat(70));
    
    const limit = parseInt(process.argv[2]) || 10;
    
    const messages = await checkPollingQueue(limit);
    
    console.log(`\n✅ Check complete. Found ${messages.length} message(s)`);
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

module.exports = { checkPollingQueue };

