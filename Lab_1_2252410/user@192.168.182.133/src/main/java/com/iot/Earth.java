package com.iot;

import java.time.LocalDateTime;

public class Earth {
    private LocalDateTime time;
    private String station;
    private double Moisture;
    private double Temperature;
    private double Salinity;
    private double PH;
    private double Water_Root;
    private double Water_Leaf;
    private double Water_Level;
    private double Voltage;
    
    public Earth() {
    }

    public Earth(LocalDateTime time, String station, String Moisture, String Temperature, String Salinity, String PH, String Water_Root, String Water_Leaf, String Water_Level, String Voltage) {
        this.time = time;
        this.station = station;
        this.Moisture = parseDoubleOrDefault(Moisture, -1.0);
        this.Temperature = parseDoubleOrDefault(Temperature, -1.0);
        this.Salinity = parseDoubleOrDefault(Salinity, -1.0);
        this.PH = parseDoubleOrDefault(PH, -1.0);
        this.Water_Root = parseDoubleOrDefault(Water_Root, -1.0);
        this.Water_Leaf = parseDoubleOrDefault(Water_Leaf, -1.0);
        this.Water_Level = parseDoubleOrDefault(Water_Level, -1.0);
        this.Voltage = parseDoubleOrDefault(Voltage, -1.0);
    }

    public String toString() {
        return String.format("Earth{time=%s, station=%s, Moisture=%s, Temperature=%s, Salinity=%s, PH=%s, Water_Root=%s, Water_Leaf=%s, Water_Level=%s, Voltage=%s}",
                             time, station, Moisture, Temperature, Salinity, PH, Water_Root, Water_Leaf, Water_Level, Voltage);
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

    public double getMoisture() {
        return Moisture;
    }
    public void setMoisture(double Moisture) {
        this.Moisture = Moisture;
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

    public double getPH() {
        return PH;
    }
    public void setPH(double PH) {
        this.PH = PH;
    }

    public double getWater_Root() {
        return Water_Root;
    }
    public void setWater_Root(double Water_Root) {
        this.Water_Root = Water_Root;
    }

    public double getWater_Leaf() {
        return Water_Leaf;
    }
    public void setWater_Leaf(double Water_Leaf) {
        this.Water_Leaf = Water_Leaf;
    }

    public double getWater_Level() {
        return Water_Level;
    }
    public void setWater_Level(double Water_Level) {
        this.Water_Level = Water_Level;
    }

    public double getVoltage() {
        return Voltage;
    }
    public void setVoltage(double Voltage) {
        this.Voltage = Voltage;
    }
}
