package com.iot;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class StationPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        int partitions = cluster.partitionCountForTopic(topic);

        // Ensure key is a String before proceeding
        if (!(key instanceof String)) {
            throw new IllegalArgumentException("Key must be a String, but got: " + key.getClass().getName());
        }

        String station = (String) key; // Safe cast
        switch (station) {
            case "SVDT1":
                return 0;
            case "SVDT2":
                return 0;
            case "SVDT3":
                return 0;
            default:
                return Math.abs(station.hashCode()) % partitions;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(java.util.Map<String, ?> configs) {
    }
}
