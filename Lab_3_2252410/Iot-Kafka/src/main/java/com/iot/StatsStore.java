package com.iot;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator; // For potential future use
import org.apache.kafka.streams.state.internals.ThreadCache; // For potential future use

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A custom StateStore implementation to store RunningStats for sensor fields.
 * This is a key-value store where key is String (e.g., "air-Temperature")
 * and value is RunningStats.
 */
public class StatsStore implements StateStore {

    private final String name;
    private final boolean persistent; // True if logging to changelog is enabled
    private final Serde<String> keySerde; // Used for changelog, not directly by internal map
    private final Serde<RunningStats> valueSerde; // Used for changelog

    private Map<String, RunningStats> internalMap;
    private StateStoreContext context;
    private boolean open = false;
    private int partition; // The partition this store instance is responsible for

    public StatsStore(String name, Serde<String> keySerde, Serde<RunningStats> valueSerde, boolean persistent) {
        this.name = name;
        this.keySerde = keySerde; // Keep for potential direct changelog interaction if needed
        this.valueSerde = valueSerde;
        this.persistent = persistent;
        this.internalMap = new ConcurrentHashMap<>();
    }

    @Override
    public String name() {
        return name;
    }

    // /**
    //  * Initializes the state store. If the store is persistent (changelog enabled),
    //  * Kafka Streams will handle restoring its state from the changelog topic
    //  * before this method is called or as part of its registration.
    //  *
    //  * @param context The context provided by Kafka Streams.
    //  * @param root    The root store, not typically used for simple custom stores.
    //  */

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = context;
        this.partition = context.taskId().partition(); // Get partition for logging/debugging
        // this.internalMap = new ConcurrentHashMap<>(); // Ensure it's clean for this instance

        if (persistent) {
            System.out.println("StatsStore '" + name + "' (partition " + partition + ") is persistent. Registering restore callback.");
            
            StateRestoreCallback restoreCallback = (keyBytes, valueBytes) -> {
                if (keyBytes == null) { // Should ideally not happen with Kafka changelogs
                    System.err.println("StatsStore '" + name + "' (partition " + partition + ") received null key during restore. Skipping.");
                    return;
                }
                // Use the SerDes provided to the StatsStore for deserializing from changelog
                String key = keySerde.deserializer().deserialize(this.name(), keyBytes); // Use store name as topic hint

                if (valueBytes == null) {
                    // This is a tombstone message, indicating the key should be removed
                    internalMap.remove(key);
                    // System.out.println("StatsStore '" + name + "' (partition " + partition + ") restored tombstone for key: " + key);
                } else {
                    RunningStats value = valueSerde.deserializer().deserialize(this.name(), valueBytes);
                    internalMap.put(key, value);
                    // System.out.println("StatsStore '" + name + "' (partition " + partition + ") restored key: " + key + ", value: " + value);
                }
            };
            // Register the callback. Kafka Streams will invoke it for each record in the changelog.
            context.register(this, restoreCallback);
        }

        this.open = true;
        System.out.println("StatsStore '" + name + "' (partition " + partition + ") initialized. Persistent: " + persistent);
    }


    public void put(String key, RunningStats value) {
        if (!isOpen()) throw new IllegalStateException("Store " + name + " is not open.");
        internalMap.put(key, value);
        // If persistent, Kafka Streams handles writing to the changelog automatically
        // because the StoreBuilder indicated loggingEnabled and provided Serdes.
        // The actual write to changelog happens when Kafka Streams flushes the state.
    }

    public RunningStats get(String key) {
        if (!isOpen()) throw new IllegalStateException("Store " + name + " is not open.");
        return internalMap.get(key);
    }
    
    public Map<String, RunningStats> getInternalMap() {
        return internalMap;
    }


    @Override
    public void flush() {
        // For persistent stores, Kafka Streams will handle flushing to the changelog
        // and underlying persistent store (like RocksDB) if configured.
        // This method could be used for any custom flushing logic if needed.
        System.out.println("StatsStore '" + name + "' (partition " + partition + ") flush called.");
    }

    @Override
    public void close() {
        this.open = false;
        // internalMap.clear(); // Clearing might not be desired if state should persist across rebalances for some store types
        System.out.println("StatsStore '" + name + "' (partition " + partition + ") closed.");
    }

    @Override
    public boolean persistent() {
        return persistent;
    }

    @Override
    public boolean isOpen() {
        return open;
    }
}
