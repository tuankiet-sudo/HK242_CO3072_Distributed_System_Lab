package com.iot;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IotProducer{

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.182.129:9092,192.168.182.130:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.iot.IoTSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.iot.StationPartitioner");

        Producer<String, Object> producer = new KafkaProducer<>(props);
        // 1 producer for all threads (Producer is thread-safe)

        ExecutorService executor = Executors.newFixedThreadPool(3);

        String[][] files = {
            {"AIR2308.csv", "air"},
            {"EARTH2308.csv", "earth"},
            {"WATER2308.csv", "water"}
        };

        for (String[] file : files) {
            executor.execute(new ProducerTask(file[0], file[1], producer));
        }

        // producer.close();
    }
}

class ProducerTask implements Runnable {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
    private String file;
    private String topic;
    private Producer<String, Object> producer;

    public ProducerTask(String file, String topic, Producer<String, Object> producer) {
        this.file = file;
        this.topic = topic;
        this.producer = producer;
    }

    @Override
    public void run() {
        String csvFile = Paths.get("Dataset", this.file).toString(); // Adjust path

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile));) {
            System.out.println("Reading file: " + this.file);

            String line;
            line = br.readLine(); //Skip header line

            // int count = 0;

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                LocalDateTime time = LocalDateTime.parse(values[0], formatter);
                String station = values[1];

                Object data;
                switch (this.topic) {
                    case "air":
                        data = new Air(time, station, values[2], values[3], values[4], values[5], values[6], values[7],values[8], values[9], values[10], values[11], values[12]);
                        break;
                    case "earth":
                        data = new Earth(time, station, values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9]);
                        break;
                    case "water":
                        data = new Water(time, station, values[2], values[3], values[4], values[5]);
                        break;
                    default:
                        System.out.println("Unknown topic: " + this.topic);
                        continue; // Skip unknown topics
                }

                System.out.println("-------------------");
                
                System.out.println(data.toString());
                
                producer.send(new ProducerRecord<>(this.topic, station, data));
                System.out.println("Sent above record to topic: " + this.topic);
                // count++;
                // if (count == 10) {
                //     break;
                // }

                Thread.sleep(1000);
                // Simulate real-life scenario
            }

            System.out.println("-------------------");
            System.out.println("Done reading file: " + this.file);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e);
        }
    }
}