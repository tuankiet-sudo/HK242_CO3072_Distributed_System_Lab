package com.iot;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.StoreBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A StoreBuilder for creating instances of StatsStore.
 */
public class StatsStoreBuilder implements StoreBuilder<StatsStore> {

    private final String name;
    private final Serde<String> keySerde;
    private final Serde<RunningStats> valueSerde;
    private Map<String, String> logConfig = new HashMap<>();
    private boolean loggingEnabled = false; // Default to no changelog
    private boolean cachingEnabled = false; // Custom stores don't get automatic caching

    public StatsStoreBuilder(String name, Serde<String> keySerde, Serde<RunningStats> valueSerde) {
        this.name = Objects.requireNonNull(name, "name cannot be null");
        this.keySerde = Objects.requireNonNull(keySerde, "keySerde cannot be null");
        this.valueSerde = Objects.requireNonNull(valueSerde, "valueSerde cannot be null");
    }

    @Override
    public StoreBuilder<StatsStore> withCachingEnabled() {
        this.cachingEnabled = true;
        // Note: Kafka Streams does not automatically provide caching for custom stores.
        // Caching would need to be implemented within the StatsStore itself if desired.
        return this;
    }

    @Override
    public StoreBuilder<StatsStore> withCachingDisabled() {
        this.cachingEnabled = false;
        return this;
    }

    @Override
    public StoreBuilder<StatsStore> withLoggingEnabled(Map<String, String> config) {
        this.loggingEnabled = true;
        this.logConfig = config != null ? new HashMap<>(config) : new HashMap<>();
        return this;
    }

    @Override
    public StoreBuilder<StatsStore> withLoggingDisabled() {
        this.loggingEnabled = false;
        this.logConfig.clear();
        return this;
    }

    @Override
    public StatsStore build() {
        // The StatsStore needs to know if it's persistent (loggingEnabled)
        // to correctly report its persistent() status.
        // Kafka Streams will use the keySerde and valueSerde for the changelog topic if loggingEnabled.
        return new StatsStore(name, keySerde, valueSerde, loggingEnabled);
    }

    @Override
    public Map<String, String> logConfig() {
        // As per StoreBuilder contract, return null if logging is disabled.
        return loggingEnabled ? logConfig : null;
    }

    @Override
    public boolean loggingEnabled() {
        return loggingEnabled;
    }

    @Override
    public String name() {
        return name;
    }

    // These are not part of StoreBuilder interface but can be useful for inspection
    public Serde<String> keySerde() {
        return keySerde;
    }

    public Serde<RunningStats> valueSerde() {
        return valueSerde;
    }
}