package com.iot;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

public class FieldStatsSerde implements Serde<FieldStats> {

    @Override
    public Serializer<FieldStats> serializer() {
        return new Serializer<FieldStats>() {
            @Override
            public byte[] serialize(String topic, FieldStats data) {
                if (data == null) return null;
                ByteBuffer buffer = ByteBuffer.allocate(8 + 8 + 8); // count (long), mean (double), m2 (double)
                buffer.putLong(data.getCount());
                buffer.putDouble(data.getMean());
                buffer.putDouble(data.getCount() > 1 ? Math.sqrt(data.getM2() / (data.getCount() - 1)) : 0.0); // for compatibility
                return buffer.array();
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) { }
            @Override
            public void close() { }
        };
    }

    @Override
    public Deserializer<FieldStats> deserializer() {
        return new Deserializer<FieldStats>() {
            @Override
            public FieldStats deserialize(String topic, byte[] data) {
                if (data == null) return null;
                ByteBuffer buffer = ByteBuffer.wrap(data);
                long count = buffer.getLong();
                double mean = buffer.getDouble();
                double stddev = buffer.getDouble(); // Unused, but could be recalculated
                FieldStats stats = new FieldStats();
                stats.set(count, mean); // you will need to add a set() method in FieldStats
                return stats;
            }

            @Override
            public void configure(Map<String, ?> configs, boolean isKey) { }
            @Override
            public void close() { }
        };
    }
}
