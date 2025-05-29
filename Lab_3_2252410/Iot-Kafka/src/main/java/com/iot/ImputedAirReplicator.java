package com.iot;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ImputedAirReplicator {

    private static final String LOCAL_BOOTSTRAP_SERVERS = "192.168.182.128:9092";
    private static final String REMOTE_BOOTSTRAP_SERVERS = "192.168.182.129:9092";
    private static final String INPUT_TOPIC = "air-imputed";
    private static final String OUTPUT_TOPIC = "air";
    private static final String CONSUMER_GROUP_ID = "imputed-air-replicator-group";
    private static final String AIR_CLASS_NAME = "com.iot.Air";

    private static final AtomicBoolean running = new AtomicBoolean(true);

    public static void main(String[] args) {
        // Consumer Properties for reading from localhost
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, LOCAL_BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IoTDeserializer.class.getName());
        consumerProps.put("deserializer.target.type", AIR_CLASS_NAME); // Custom property for IoTDeserializer
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // Or manage commits manually

        // Producer Properties for writing to remote Kafka
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, REMOTE_BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, IoTSerializer.class.getName());
        // Optional: Add StationPartitioner if the remote 'air' topic expects it
        // producerProps.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, StationPartitioner.class.getName());
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all"); // For stronger delivery guarantees

        KafkaConsumer<String, Object> consumer = null;
        KafkaProducer<String, Object> producer = null;

        // Shutdown Hook to gracefully close consumer and producer
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook initiated. Closing replicator...");
            running.set(false);
            // Note: consumer.wakeup() should be called from another thread if consumer.poll() is blocking indefinitely.
            // However, with a timeout in poll(), setting 'running' to false will eventually stop the loop.
        }));

        try {
            consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new IoTDeserializer<>(AIR_CLASS_NAME));
            producer = new KafkaProducer<>(producerProps);

            consumer.subscribe(Collections.singletonList(INPUT_TOPIC));
            System.out.println("Subscribed to topic: " + INPUT_TOPIC + " on " + LOCAL_BOOTSTRAP_SERVERS);
            System.out.println("Producing to topic: " + OUTPUT_TOPIC + " on " + REMOTE_BOOTSTRAP_SERVERS);

            while (running.get()) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(1000)); // Poll with a timeout
                if (records.isEmpty() && !running.get()) {
                    // Exit if poll returns empty due to shutdown signal and loop condition
                    break;
                }

                for (ConsumerRecord<String, Object> record : records) {
                    if (!running.get()) break; // Check running flag before processing each record

                    String key = record.key();
                    Object value = record.value(); // This should be an Air object

                    if (value instanceof Air) {
                        System.out.println("Consumed from '" + INPUT_TOPIC + "': Key=" + key + ", Value=" + value.toString());
                        ProducerRecord<String, Object> producerRecord =
                                new ProducerRecord<>(OUTPUT_TOPIC, key, value);

                        producer.send(producerRecord, (metadata, exception) -> {
                            if (exception == null) {
                                System.out.println("Successfully produced to '" + OUTPUT_TOPIC + "' (partition " + metadata.partition() +
                                                   ", offset " + metadata.offset() + "): Key=" + key);
                            } else {
                                System.err.println("Failed to produce to '" + OUTPUT_TOPIC + "': Key=" + key + ". Error: " + exception.getMessage());
                                // Consider how to handle send failures (e.g., retry, log to dead-letter queue)
                            }
                        });
                    } else {
                        System.err.println("Warning: Consumed a record from '" + INPUT_TOPIC +
                                           "' that is not an instance of Air. Key=" + key +
                                           ", Type=" + (value != null ? value.getClass().getName() : "null"));
                    }
                }
                producer.flush(); // Flush records periodically or on shutdown
            }

        } catch (Exception e) {
            System.err.println("An error occurred in the replicator: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("Closing Kafka consumer and producer...");
            if (consumer != null) {
                consumer.close();
                System.out.println("Consumer closed.");
            }
            if (producer != null) {
                producer.close();
                System.out.println("Producer closed.");
            }
            System.out.println("Replicator shut down complete.");
        }
    }
}
