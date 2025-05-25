package com.iot;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.StoreBuilder;


import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class SensorImputerApplication {

    private static final String APP_ID_PREFIX = "iot-sensor-imputer-app-";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "192.168.182.128:9092"; // Replace if needed
    private static final String STATS_STORE_NAME = "SensorFieldStatsStore"; // Ensure this matches in ImputerProcessor

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: SensorImputerApplication <sensorType>");
            System.err.println("Example: SensorImputerApplication air (for air topic and com.iot.Air objects)");
            System.exit(1);
        }

        String sensorType = args[0].toLowerCase(); 
        String inputTopic = sensorType; 
        String outputTopic = sensorType + "-imputed";
        String objectClassName = "com.iot." + sensorType.substring(0, 1).toUpperCase() + sensorType.substring(1);

        System.out.println("Starting SensorImputerApplication for sensor type: " + sensorType);
        System.out.println("Input topic: " + inputTopic + ", Output topic: " + outputTopic);
        System.out.println("Expected object class: " + objectClassName);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID_PREFIX + sensorType);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
        props.put(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, DefaultProductionExceptionHandler.class.getName());

        Topology topology = new Topology();

        Serde<String> stringSerde = Serdes.String();
        IotSerde<Object> valueSerde = new IotSerde<>(objectClassName); 
        StatsSerde runningStatsSerde = new StatsSerde(); 

        topology.addSource(
                "SensorSource",
                stringSerde.deserializer(),
                valueSerde.deserializer(), 
                inputTopic
        );

        ProcessorSupplier<String, Object, String, Object> imputerProcessorSupplier =
            new ProcessorSupplier<String, Object, String, Object>() {
                @Override
                public Processor<String, Object, String, Object> get() {
                    return new ImputerProcessor(STATS_STORE_NAME);
                }

                @Override
                public Set<StoreBuilder<?>> stores() {
                    final StatsStoreBuilder statsStoreBuilder = new StatsStoreBuilder(
                            STATS_STORE_NAME,
                            stringSerde,        
                            runningStatsSerde   
                    );
                    statsStoreBuilder.withLoggingEnabled(new HashMap<>()); 
                    return Collections.singleton(statsStoreBuilder);
                }
            };

        topology.addProcessor(
                "ImputerNode",
                imputerProcessorSupplier, 
                "SensorSource"  
        );

        topology.addSink(
                "ImputedDataSink",
                outputTopic,
                stringSerde.serializer(),
                valueSerde.serializer(), 
                "ImputerNode"   
        );

        System.out.println("Topology description:\n" + topology.describe());

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                // Try printing to System.err
                System.err.println("Closing Kafka Streams application (Shutdown Hook)..."); 
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.setStateListener((newState, oldState) -> {
                System.out.println("Kafka Streams state changed from " + oldState + " to " + newState);
            });
            streams.start();
            System.out.println("SensorImputerApplication started. Consuming from " + inputTopic +
                               ", producing to " + outputTopic + ".");
            latch.await();
        } catch (Throwable e) {
            System.err.println("Unhandled exception in SensorImputerApplication: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (streams.state().isRunningOrRebalancing()) {
                streams.close();
            }
        }
        System.exit(0);
    }
}
