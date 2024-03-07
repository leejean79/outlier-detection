package com.leejean79.bean;

import com.leejean79.mtree.DistanceFunctions.EuclideanCoordinate;
import org.apache.flink.api.java.tuple.Tuple2;


import java.io.Serializable;
import java.util.*;

public class Data implements EuclideanCoordinate, Serializable, Comparable<Data> {

    private ArrayList<Double> value;
    private Long arrival;
    private int flag;
    private int id;
    private int count_after = 0;
    private Boolean safe_inlier = false;
    private List<Long> nn_before = new ArrayList<>();

    //MCOD Vars
    private int mc = -1;

    public int getMc() {
        return mc;
    }

    public void setMc(int mc) {
        this.mc = mc;
    }

    private HashSet<Integer> rmc;

    public HashSet<Integer> getRmc() {
        return rmc;
    }

    public void setRmc(Integer mc) {
        this.rmc.add(mc);
    }

    //pAMCOD Stuff
    private ArrayList<Tuple2<Long, Double>> nn_before_set;
    private ArrayList<Double> count_after_set;

    public List<Long> getNn_before() {
        return nn_before;
    }

    public int getCount_after() {
        return count_after;
    }

    public Boolean getSafe_inlier() {
        return safe_inlier;
    }

    public void setSafe_inlier(Boolean safe_inlier) {
        this.safe_inlier = safe_inlier;
    }

    public void setCount_after(int count_after) {
        this.count_after += count_after;
    }

    public ArrayList<Double> getValue() {
        return value;
    }

    public void setValue(ArrayList<Double> value) {
        this.value = value;
    }

    public Long getArrival() {
        return arrival;
    }

    public void setArrival(Long arrival) {
        this.arrival = arrival;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Data(ArrayList<Double> value, Long arrival, int flag, int id) {
        this.value = value;
        this.arrival = arrival;
        this.flag = flag;
        this.id = id;
    }

    /***
     * 实现更新 数据点 前 n 列表的操作
     * @param time 新插入数据的时间
     * @param k  近邻个数
     */
    public void insert_nn_before(Long time, int k){
//        如果点 p 的前 n 个节点个数已满
        if (nn_before.size() == k){
//            拿到列表中最老的数据
            Long tmp = Collections.min(nn_before);
//            如果新的数据时间比原有列表中最老的时间要新，则替换
            if (time > tmp){
                nn_before.remove(tmp);
                nn_before.add(time);
            }
        }
//        列表个数未满，直接加入
        nn_before.add(time);
    }

    // MCOD & AMCOD & KSKY Function
    public void clear(int newMc){
        nn_before_set.clear();//AMCOD
        count_after_set.clear();//AMCOD
        nn_before.clear();
        count_after = 0;
        mc = newMc;
    }

    @Override
    public int compareTo(Data o) {
        int dim = Math.min(this.dimensions(), o.dimensions());
        for (int i = 0; i < dim; i++) {
            if (this.value.get(i) > o.value.get(i)) return 1;
            else if (this.value.get(i) < o.value.get(i)) return -1;
            else return 0;
        }
        if (this.dimensions() > dim)
            return 1;
        else return -1;
    }

    @Override
    public int dimensions() {
        return 0;
    }

    @Override
    public double get(int index) {
        return 0;
    }
}
