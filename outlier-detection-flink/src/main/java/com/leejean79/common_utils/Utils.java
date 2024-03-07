package com.leejean79.common_utils;

import com.leejean79.bean.Data;
import com.leejean79.bean.MicroCluster;

public class Utils {

//    两点间的距离
    public static Double distance(Data o, Data p){
        int dim = Math.min(o.dimensions(), p.dimensions());
        Double value = 0.0;
        for (int i = 0; i < dim; i++) {
            Double difference = o.getValue().get(i) - p.getValue().get(i);
            value += difference * difference;
        }
        return Math.sqrt(value);
    }
//    点与微簇间的距离
    public static Double distance(Data xs, MicroCluster ys){
        int min = Math.min(xs.dimensions(), ys.getCenter().size());
        Double value = 0.0;
        for (int i = 0; i < min; i++) {
            value += Math.pow(xs.getValue().get(i) - ys.getCenter().get(i), 2);
        }
        return Math.sqrt(value);

    }
//    def distance(xs: Data, ys: MicroCluster): Double = {
//        val min = Math.min(xs.dimensions(), ys.center.size)
//        var value: Double = 0
//        for (i <- 0 until min) {
//            value += scala.math.pow(xs.value(i) - ys.center(i), 2)
//        }
//        val res = scala.math.sqrt(value)
//        res
//    }

//    更新new data 的nn_before：
//    new data 的nn_before是 自身 的 nn_before + old data
    public static Data combineElementsParallel(Data oldEl, Data newEl, int k){
        for (Long elTime : newEl.getNn_before()){
            oldEl.insert_nn_before(elTime,k);
        }
        return oldEl;
    }
}
