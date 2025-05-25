package com.iot;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

// Removed java.lang.reflect.Field as it's no longer directly used for the check
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

public class ImputerProcessor implements Processor<String, Object, String, Object> {

    private ProcessorContext<String, Object> context;
    private StatsStore statsStore;
    private final ObjectMapper objectMapper;
    private final Random random = new Random();
    private final String storeName;

    public ImputerProcessor(String storeName) {
        this.storeName = storeName;
        System.out.println("ImputerProcessor CONSTRUCTOR called with storeName: '" + this.storeName + "'");
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void init(ProcessorContext<String, Object> context) {
        this.context = context;
        System.out.println("ImputerProcessor INIT (Task: " + context.taskId() + "): Attempting to get store '" + storeName + "'");
        try {
            this.statsStore = context.getStateStore(storeName);
            if (this.statsStore == null) {
                System.err.println("ImputerProcessor INIT (Task: " + context.taskId() + "): context.getStateStore('" + storeName + "') returned null.");
                throw new IllegalStateException("State store '" + storeName + "' not found. Check topology.");
            }
            System.out.println("ImputerProcessor INIT (Task: " + context.taskId() + "): Successfully initialized with state store: " + storeName);
        } catch (Exception e) {
            System.err.println("Error initializing ImputerProcessor (Task: " + context.taskId() + ") with store '" + storeName + "': " + e.getMessage());
            if (!(e instanceof IllegalStateException && e.getMessage().startsWith("State store '" + storeName + "' not found"))) {
                e.printStackTrace();
            }
            throw new RuntimeException("Failed to initialize ImputerProcessor (Task: " + context.taskId() + ")", e);
        }
    }

    @Override
    public void process(Record<String, Object> record) {
        Object inputValue = record.value();
        String recordKey = record.key();

        if (inputValue == null) {
            System.err.println("ImputerProcessor PROCESS (Task: " + context.taskId() + ", Key: " + recordKey + "): Received null record value. Skipping.");
            return;
        }

        String sensorPrefix = inputValue.getClass().getSimpleName().toLowerCase();
        System.out.println("\nImputerProcessor PROCESS (Task: " + context.taskId() + ", Key: " + recordKey + ", Sensor: " + sensorPrefix + ")");
        System.out.println("Input Record Value: " + inputValue.toString());

        try {
            System.out.println("  Attempting objectMapper.valueToTree(inputValue)...");
            JsonNode inputJsonNode = objectMapper.valueToTree(inputValue);
            System.out.println("  objectMapper.valueToTree(inputValue) SUCCEEDED. Result: " + (inputJsonNode != null ? inputJsonNode.getNodeType() : "null"));

            if (!(inputJsonNode instanceof ObjectNode)) {
                System.err.println("  EARLY EXIT: inputJsonNode is NOT an instance of ObjectNode. Actual type: " + (inputJsonNode != null ? inputJsonNode.getClass().getName() : "null") + ". Forwarding original record.");
                context.forward(record); 
                return;
            }
            System.out.println("  inputJsonNode is an ObjectNode. Proceeding to deepCopy...");
            ObjectNode outputJsonNode = (ObjectNode) inputJsonNode.deepCopy();
            System.out.println("  deepCopy SUCCEEDED. Proceeding to iterate fields...");


            Iterator<Map.Entry<String, JsonNode>> fields = inputJsonNode.fields();
            boolean imputationOccurred = false;

            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> fieldEntry = fields.next();
                String fieldName = fieldEntry.getKey(); // This is the JSON key, e.g., "temperature", "moisture"
                JsonNode fieldValueNode = fieldEntry.getValue();
                System.out.println("    Iterating JSON field: " + fieldName + ", Value Node Type: " + fieldValueNode.getNodeType());


                if (fieldName.equalsIgnoreCase("time") || fieldName.equalsIgnoreCase("station")) {
                    System.out.println("      Skipping non-imputable field: " + fieldName);
                    continue;
                }
                
                if (fieldValueNode.isNumber()) {
                    System.out.println("      Field '" + fieldName + "' has a numeric JSON value. Processing for imputation.");
                    double originalValue = fieldValueNode.asDouble();
                    String statKey = sensorPrefix + "-" + fieldName; // e.g., "air-temperature", "air-moisture"
                    System.out.println("      Processing numeric field: " + fieldName + ", Original Value: " + originalValue + ", StatKey: " + statKey);

                    RunningStats stats = statsStore.get(statKey);
                    if (stats == null) {
                        stats = new RunningStats();
                        System.out.println("        No existing stats for " + statKey + ". Created new RunningStats.");
                    } else {
                        System.out.println("        Retrieved stats for " + statKey + ": " + stats.toString());
                    }

                    if (originalValue == -1.0) { 
                        System.out.println("    Field '" + fieldName + "' is -1.0 (missing). Attempting imputation.");
                        if (stats.getCount() > 1) { 
                            double mean = stats.getMean();
                            double stdDev = stats.getStdDev();
                            double imputedValue = mean + (random.nextGaussian() * stdDev);
                            outputJsonNode.put(fieldName, imputedValue);
                            imputationOccurred = true;
                            System.out.println("    IMPUTED: " + fieldName + " from -1.0 to " + String.format("%.2f", imputedValue) + 
                                               " (using mean=" + String.format("%.2f", mean) + ", stdDev=" + String.format("%.2f", stdDev) + ")");
                        } else {
                            outputJsonNode.put(fieldName, -1.0); 
                            System.out.println("    NOT IMPUTED (Stats Count <= 1): " + fieldName + ". Kept as -1.0. Stats count: " + stats.getCount());
                        }
                    } else {
                        System.out.println("        Field '" + fieldName + "' has valid value: " + originalValue + ". Updating stats.");
                        stats.update(originalValue);
                        statsStore.put(statKey, stats); 
                        System.out.println("        Updated stats for " + statKey + ": " + stats.toString());
                    }
                } else {
                     System.out.println("      Field '" + fieldName + "' JSON value is NOT a number ("+fieldValueNode.getNodeType()+"). Skipping imputation for it.");
                }
            }

            Object outputValue = objectMapper.treeToValue(outputJsonNode, inputValue.getClass());
            
            if(imputationOccurred) {
                System.out.println("  Output Record Value (after potential imputation): " + outputValue.toString());
            } else {
                System.out.println("No imputation occurred for this record. Output is same as input.");
            }
            context.forward(record.withValue(outputValue));

        } catch (Exception e) {
            System.err.println("!!! EXCEPTION CAUGHT IN PROCESS METHOD (Task: " + context.taskId() + ", Key: " + recordKey + ", Sensor: " + sensorPrefix + ") !!!");
            System.err.println("Exception message: " + e.getMessage());
            e.printStackTrace(); 
            context.forward(record); 
        }
    }

    @Override
    public void close() {
        System.out.println("ImputerProcessor CLOSE (Task: " + (context != null ? context.taskId() : "N/A") + ")");
    }
}
