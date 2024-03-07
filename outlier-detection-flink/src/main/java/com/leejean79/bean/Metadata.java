package com.leejean79.bean;


import java.util.Map;

public class Metadata {

    public Metadata(Map<Integer, Data> outLiers) {
        this.outLiers = outLiers;
    }

    private Map<Integer, Data> outLiers;

    public Map<Integer, Data> getAllFromMap(){
        return outLiers;
    }

    public void addToMap(Map<Integer, Data> data) {
        this.outLiers.putAll(data);
    }

    public void putToMap(Integer key, Data data){
        outLiers.put(key, data);
    }



}



//    var outliers: mutable.HashMap[Int, Data]

