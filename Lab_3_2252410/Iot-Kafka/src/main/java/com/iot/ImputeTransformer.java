package com.iot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Iterator;

public class ImputeTransformer implements ValueTransformerWithKey<String, String, String> {
    private KeyValueStore<String, FieldStats> store;
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        System.out.println("ImputeTransformer init() called");
        store = (KeyValueStore<String, FieldStats>) context.getStateStore("stats-store");
    }

    @Override
    public String transform(String key, String value) {
        try {
            System.out.println("ImputeTransformer: " + value);

            JsonNode json = mapper.readTree(value);
            ObjectNode output = (ObjectNode) json.deepCopy();

            String sensorPrefix = "air"; // e.g., "air", "water"

            Iterator<String> fieldNames = json.fieldNames();
            while (fieldNames.hasNext()) {
                String field = fieldNames.next();
                JsonNode valNode = json.get(field);
                String statKey = sensorPrefix + "-" + field;

                FieldStats stats = store.get(statKey);
                if (stats == null) stats = new FieldStats();

                if (valNode != null && valNode.isNumber()) {
                    double val = valNode.asDouble();

                    if (val != -1.0) {
                        stats.update(val);
                        store.put(statKey, stats);
                    } else if (stats.getCount() > 1) {
                        double imputed = stats.getMean() + getRandom(stats.getStdDev());
                        output.put(field, imputed);
                    } else {
                        output.put(field, -1);
                    }
                } else {
                    output.put(field, -1);
                }
            }

            return output.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return value;
        }
    }

    // private String extractSensorPrefix(JsonNode json) {
    //     return json.has("sensor") ? json.get("sensor").asText() : "generic";
    // }

    private double getRandom(double std) {
        return (Math.random() * 2 - 1) * std;
    }

    @Override
    public void close() {}
}
