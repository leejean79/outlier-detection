package com.leejean79.bean;

import java.util.HashMap;

/**
 *
 */

public class McodState {

    private HashMap<Integer, Data> pd;
    private HashMap<Integer, MicroCluster> mc;

    public McodState(HashMap<Integer, Data> pd, HashMap<Integer, MicroCluster> mc) {
        this.pd = pd;
        this.mc = mc;
    }

    public McodState() {
    }

    public HashMap<Integer, Data> getPd() {
        return pd;
    }

    public void setPd(HashMap<Integer, Data> pd) {
        this.pd = pd;
    }

    public HashMap<Integer, MicroCluster> getMc() {
        return mc;
    }

    public void setMc(HashMap<Integer, MicroCluster> mc) {
        this.mc = mc;
    }
}
