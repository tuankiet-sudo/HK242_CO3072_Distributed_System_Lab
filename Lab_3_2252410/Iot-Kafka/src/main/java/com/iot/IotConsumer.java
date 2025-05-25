package com.iot;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Properties;
import java.time.Duration;
import java.util.Arrays;

public class IotConsumer {
    public static void main(String[] args) {
        String[] topics = {"air"};
        
        int partition_num = 3;

        ExecutorService executor = Executors.newFixedThreadPool(topics.length * partition_num);

        for (String topic : topics) {
            for (int i=0; i<partition_num; i++) {
                executor.execute(new ConsumerTask(i, topic));
            }
        } 
    }
}

class ConsumerTask implements Runnable {
    private int partition;
    private String topic;
    
    public ConsumerTask(int partition, String topic) {
        this.partition = partition;
        this.topic = topic;
    }

    @Override
    public void run() {
        String group_name = this.topic + "-group";
        String objectClassName = "com.iot." + this.topic.substring(0, 1).toUpperCase() + this.topic.substring(1);

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.182.133:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group_name);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "com.iot.IoTDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("deserializer.target.type", objectClassName);

        KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props, new StringDeserializer(), new IoTDeserializer<>(objectClassName));
        try {
            System.out.println(Class.forName(objectClassName));
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e);
        }
        
        TopicPartition topicPartition = new TopicPartition(this.topic, this.partition);
        consumer.assign(Arrays.asList(topicPartition));

        System.out.println("------------------");
        System.out.println("Consumer consuming from topic " + this.topic + " partition " + String.valueOf(this.partition));
        System.out.println("------------------");
        
        try {
            while (true) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofSeconds(1));
                System.out.println(records.count());
                records.forEach(record -> {
                    System.out.println("Record");;
                    System.out.println(record.value().toString());
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
        }

    }
}