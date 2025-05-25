package com.iot;

import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class IoTDeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> targetType;

    public IoTDeserializer(String targetTypeName) {
        try {
            this.targetType = (Class<T>) Class.forName(targetTypeName);
            objectMapper.findAndRegisterModules();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e);
        }
        
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, targetType);
        } catch (Exception e) {
            throw new RuntimeException("Error deserializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }
}
