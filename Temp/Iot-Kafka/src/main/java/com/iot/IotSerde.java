package com.iot;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.Deserializer;

public class IotSerde<T> implements Serde<T> {
    private final IoTSerializer<T> serializer;
    private final IoTDeserializer<T> deserializer;

    public IotSerde(String targetType) {
        this.serializer = new IoTSerializer<>();
        this.deserializer = new IoTDeserializer<>(targetType);
        System.out.println("IotSerde initialized with target type: " + targetType);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
