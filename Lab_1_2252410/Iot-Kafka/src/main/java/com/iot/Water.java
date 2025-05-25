package com.iot;

import java.time.LocalDateTime;

public class Water {
    private LocalDateTime time;
    private String station;
    private double pH;
    private double DO;
    private double Temperature;
    private double Salinity;

    public Water() {
    }

    public Water(LocalDateTime time, String station, String pH, String DO, String Temperature, String Salinity) {
        this.time = time;
        this.station = station;
        this.pH = parseDoubleOrDefault(pH, -1.0);
        this.DO = parseDoubleOrDefault(DO, -1.0);
        this.Temperature = parseDoubleOrDefault(Temperature, -1.0);
        this.Salinity = parseDoubleOrDefault(Salinity, -1.0);
    }

    public String toString() {
        return String.format("Water{time=%s, station=%s, pH=%s, DO=%s, Temperature=%s, Salinity=%s}",
                             time, station, pH, DO, Temperature, Salinity);
    }

    private double parseDoubleOrDefault(String value, double defaultValue) {
        if (value == null || value.trim().isEmpty() || value.equals("---")) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    public LocalDateTime getTime() {
        return time;
    }
    public void setTime(LocalDateTime time) {
        this.time = time;
    }

    public String getStation() {
        return station;
    }
    public void setStation(String station) {
        this.station = station;
    }

    public double getPH() {
        return pH;
    }
    public void setPH(double pH) {
        this.pH = pH;
    }

    public double getDO() {
        return DO;
    }
    public void setDO(double DO) {
        this.DO = DO;
    }

    public double getTemperature() {
        return Temperature;
    }
    public void setTemperature(double Temperature) {
        this.Temperature = Temperature;
    }

    public double getSalinity() {
        return Salinity;
    }
    public void setSalinity(double Salinity) {
        this.Salinity = Salinity;
    }
}
