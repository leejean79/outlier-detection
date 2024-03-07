package com.leejean79.bean;

import java.util.ArrayList;

public class MicroCluster {

    private ArrayList<Double> center;
    private ArrayList<Data> points;

    public MicroCluster(ArrayList<Double> center, ArrayList<Data> points) {
        this.center = center;
        this.points = points;
    }

    public MicroCluster() {
    }

    public ArrayList<Double> getCenter() {
        return center;
    }

    public void setCenter(ArrayList<Double> center) {
        this.center = center;
    }

    public ArrayList<Data> getPoints() {
        return points;
    }

    public void setPoints(ArrayList<Data> points) {
        this.points = points;
    }
}
