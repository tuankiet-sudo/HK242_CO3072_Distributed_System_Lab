package com.iot;

import java.time.LocalDateTime;

public class Air {
    private LocalDateTime time;
    private String station;
    private double Temperature;
    private double Moisture;
    private double Light;
    private double Total_Rainfall;
    private double Rainfall;
    private double Wind_Direction;
    private double PM2_5;
    private double PM10;
    private double CO;
    private double NOx;
    private double SO2;
    
    public Air() {
    }

    public Air(LocalDateTime time, String station, String Temperature, String Moisture, String Light, 
               String Total_Rainfall, String Rainfall, String Wind_Direction, String PM2_5, 
               String PM10, String CO, String NOx, String SO2) {
        this.time = time;
        this.station = station;
        this.Temperature = parseDoubleOrDefault(Temperature, -1.0);
        this.Moisture = parseDoubleOrDefault(Moisture, -1.0);
        this.Light = parseDoubleOrDefault(Light, -1.0);
        this.Total_Rainfall = parseDoubleOrDefault(Total_Rainfall, -1.0);
        this.Rainfall = parseDoubleOrDefault(Rainfall, -1.0);
        this.Wind_Direction = parseDoubleOrDefault(Wind_Direction, -1.0);
        this.PM2_5 = parseDoubleOrDefault(PM2_5, -1.0);
        this.PM10 = parseDoubleOrDefault(PM10, -1.0);
        this.CO = parseDoubleOrDefault(CO, -1.0);
        this.NOx = parseDoubleOrDefault(NOx, -1.0);
        this.SO2 = parseDoubleOrDefault(SO2, -1.0);
    }

    public String toString() {
        return String.format("Air{time=%s, station=%s, Temperature=%s, Moisture=%s, Light=%s, Total_Rainfall=%s, Rainfall=%s, Wind_Direction=%s, PM2_5=%s, PM10=%s, CO=%s, NOx=%s, SO2=%s}",
                             time, station, Temperature, Moisture, Light, Total_Rainfall, Rainfall, Wind_Direction, PM2_5, PM10, CO, NOx, SO2);
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

    public double getTemperature() {
        return Temperature;
    }
    public void setTemperature(double Temperature) {
        this.Temperature = Temperature;
    }

    public double getMoisture() {
        return Moisture;
    }
    public void setMoisture(double Moisture) {
        this.Moisture = Moisture;
    }

    public double getLight() {
        return Light;
    }
    public void setLight(double Light) {
        this.Light = Light;
    }

    public double getTotal_Rainfall() {
        return Total_Rainfall;
    }
    public void setTotal_Rainfall(double Total_Rainfall) {
        this.Total_Rainfall = Total_Rainfall;
    }

    public double getRainfall() {
        return Rainfall;
    }
    public void setRainfall(double Rainfall) {
        this.Rainfall = Rainfall;
    }

    public double getWind_Direction() {
        return Wind_Direction;
    }
    public void setWind_Direction(double Wind_Direction) {
        this.Wind_Direction = Wind_Direction;
    }

    public double getPM2_5() {
        return PM2_5;
    }
    public void setPM2_5(double PM2_5) {
        this.PM2_5 = PM2_5;
    }

    public double getPM10() {
        return PM10;
    }
    public void setPM10(double PM10) {
        this.PM10 = PM10;
    }

    public double getCO() {
        return CO;
    }
    public void setCO(double CO) {
        this.CO = CO;
    }

    public double getNOx() {
        return NOx;
    }
    public void setNOx(double NOx) {
        this.NOx = NOx;
    }

    public double getSO2() {
        return SO2;
    }
    public void setSO2(double SO2) {
        this.SO2 = SO2;
    }
}
