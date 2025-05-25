package com.iot;

import com.fasterxml.jackson.databind.ObjectMapper;
// import com.fasterxml.jackson.databind.SerializationFeature;
// import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

/**
 * A Kafka SerDe for RunningStats objects using JSON serialization.
 */
public class StatsSerde implements Serde<RunningStats> {

    private final ObjectMapper objectMapper;

    public StatsSerde() {
        this.objectMapper = new ObjectMapper();
        // Optional: Configure object mapper (e.g., for date/time formats if RunningStats had them)
        // objectMapper.registerModule(new JavaTimeModule());
        // objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public Serializer<RunningStats> serializer() {
        return new Serializer<RunningStats>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // No-op
            }

            @Override
            public byte[] serialize(String topic, RunningStats data) {
                if (data == null) {
                    return null;
                }
                try {
                    return objectMapper.writeValueAsBytes(data);
                } catch (IOException e) {
                    throw new SerializationException("Error serializing RunningStats to JSON", e);
                }
            }

            @Override
            public void close() {
                // No-op
            }
        };
    }

    @Override
    public Deserializer<RunningStats> deserializer() {
        return new Deserializer<RunningStats>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // No-op
            }

            @Override
            public RunningStats deserialize(String topic, byte[] data) {
                if (data == null || data.length == 0) {
                    return null;
                }
                try {
                    return objectMapper.readValue(data, RunningStats.class);
                } catch (IOException e) {
                    throw new SerializationException("Error deserializing JSON to RunningStats", e);
                }
            }

            @Override
            public void close() {
                // No-op
            }
        };
    }

    // Optional: configure and close for the SerDe itself if needed,
    // but typically handled by serializer/deserializer instances.
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer().configure(configs, isKey);
        deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer().close();
        deserializer().close();
    }
}
