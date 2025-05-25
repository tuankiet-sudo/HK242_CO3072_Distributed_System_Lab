package com.iot;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ImputerApp {
    public static void main(String[] args) {
        System.out.println("Starting ImputerApp...");

        String topic = args[0];
        String objectClassName = "com.iot." + topic.substring(0, 1).toUpperCase() + topic.substring(1);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fog-node-imputer-test2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.182.133:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, objectClassName);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Air> rawStream = builder.stream(topic, Consumed.with(Serdes.String(), new IotSerde<>(objectClassName)));

        rawStream.peek((key, value) -> System.out.println("Received: key=" + key + ", value=" + value));

        // StoreBuilder<KeyValueStore<String, Stats>> statsStoreBuilder =
        //         Stores.keyValueStoreBuilder(
        //                 Stores.persistentKeyValueStore("stats-store"),
        //                 Serdes.String(),
        //                 new StatsSerde());

        // builder.addStateStore(statsStoreBuilder);

        // KStream<String, String> imputedStream = rawStream.transformValues(
        //         ImputeTransformer::new, "stats-store");

        rawStream.to("test", Produced.with(Serdes.String(), new IotSerde<>(objectClassName)));

        final Topology topology = builder.build();
        System.out.println("Topology: " + topology.describe());

        KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        } finally {
            streams.close();
        }

        System.exit(0);
    }
}