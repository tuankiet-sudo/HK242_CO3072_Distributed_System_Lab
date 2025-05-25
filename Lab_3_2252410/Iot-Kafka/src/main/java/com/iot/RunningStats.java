package com.iot;

import java.io.Serializable;

/**
 * Holds running statistics (count, mean, M2 for variance calculation) for a data field.
 * Uses Welford's algorithm for stable online variance calculation.
 */
public class RunningStats implements Serializable {
    private static final long serialVersionUID = 1L; // Good practice for Serializable classes

    private long count = 0;
    private double mean = 0.0;
    private double m2 = 0.0; // Sum of squares of differences from the current mean

    public RunningStats() {
    }

    /**
     * Updates the statistics with a new value.
     * Implements Welford's algorithm for numerically stable online mean and variance.
     * @param value The new data value.
     */
    public void update(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean; // New delta based on the new mean
        m2 += delta * delta2;
    }

    public long getCount() {
        return count;
    }

    public double getMean() {
        return mean;
    }

    /**
     * Calculates the standard deviation.
     * @return The standard deviation, or 0.0 if count < 2.
     */
    public double getStdDev() {
        if (count < 2) {
            return 0.0;
        }
        double variance = m2 / (count - 1); // Sample variance
        return Math.sqrt(variance);
    }

    public double getM2() {
        return m2;
    }

    /**
     * Sets the state of this RunningStats object.
     * Useful for restoring state.
     * @param count The count of values.
     * @param mean The mean of values.
     * @param m2 The M2 value (sum of squares of differences).
     */
    public void setState(long count, double mean, double m2) {
        this.count = count;
        this.mean = mean;
        this.m2 = m2;
    }

    @Override
    public String toString() {
        return "RunningStats{" +
               "count=" + count +
               ", mean=" + String.format("%.2f", mean) +
               ", stdDev=" + String.format("%.2f", getStdDev()) +
               ", m2=" + String.format("%.2f", m2) +
               '}';
    }
}
