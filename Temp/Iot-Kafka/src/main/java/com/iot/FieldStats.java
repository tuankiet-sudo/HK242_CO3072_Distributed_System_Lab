package com.iot;

import java.io.Serializable;

public class FieldStats implements Serializable {
    private long count = 0;
    private double mean = 0.0;
    private double M2 = 0.0; // Sum of squares of differences from the current mean

    public void update(double value) {
        count++;
        double delta = value - mean;
        mean += delta / count;
        double delta2 = value - mean;
        M2 += delta * delta2;
    }

    public long getCount() {
        return count;
    }

    public double getMean() {
        return mean;
    }

    public double getStdDev() {
        return count > 1 ? Math.sqrt(M2 / (count - 1)) : 0.0;
    }

    public double getM2() {
        return M2;
    }

    @Override
    public String toString() {
        return "Stats{count=" + count + ", mean=" + mean + ", stdDev=" + getStdDev() + "}";
    }
}
