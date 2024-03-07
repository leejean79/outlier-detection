package com.leejean79.common_utils;

import com.leejean79.bean.Data;
import com.leejean79.jvptree.VPTree;
import com.leejean79.mtree.MTree;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class Partitioning {

    // 对每条数据根据数据点 id 计算其分区号
//  每个点的分区号如果等于当前的分区，则 flag=0；否则 flag=1
//  所以，每条数据都会发送给所有分区，存在冗余
    public static void replicationPartitioning(int parallelism, ArrayList<Double> value, Long time, int id, Collector collector){

        int flag = 0;
        Tuple2<Integer, Data> tmpEl = new Tuple2<>();
        ArrayList<Tuple2<Integer, Data>> list = new ArrayList<>();

        for (int i = 0; i < parallelism; i++) {
            if (id % parallelism == i) flag = 0;
            else flag = 1;
            collector.collect(new Tuple2<>(i, new Data(value, time, flag,id)));
        }

    }

    /**
     * 
     * @param value
     * @param time
     * @param id
     * @param range
     * @param parallelism
     * @param metricType
     * @param myMTree
     * @param myVPTree
     * @return 返回数据的分区号和数据本身
     */
    public static  void metricPartitioning(ArrayList<Double> value, Long time, int id, Double range, int parallelism, String metricType, MTree<Data> myMTree,  VPTree<Data,Data> myVPTree, Collector<Tuple2<Integer, Data>> collector){
        Data data = new Data(value, time, 0, id);
//        javaList: list["partition&false","partition&true",""partition&false",....]
//        给定一个查询点，可以通过在树中进行递归搜索来找到最近邻的分区
        List<String> vpTreePartitions = myVPTree.findPartitions(data, range, parallelism);

//        分区号列表
        List<Integer> partitions = new ArrayList<>();
        //        查询点的最近邻只能有一个
        if(vpTreePartitions.stream().filter(item -> item.contains("true")).count() !=1){
            System.exit(1);
        }

//        拿到查询点的分区号
        Optional<String> first = vpTreePartitions.stream().filter(item -> item.contains("true")).findFirst();
        if (first.isPresent()) {
            partitions.add(Integer.parseInt(first.get().split("&")[0]));
        } else {
            System.out.println("Stream is empty.");
        }

//        定义最终返回结果
        List<Tuple2<Integer, Data>> res = new ArrayList<>();
//        返回元祖列表（分区 id，数据点）
        collector.collect(new Tuple2<Integer, Data>(partitions.get(0),data));
//        将该点作为冗余数据发到临近分区
        if (partitions.size()>1) {
            Data redudantData = new Data(value, time, 1, id);
            for (int i = 1; i < partitions.size(); i++) {
                collector.collect(new Tuple2<>(partitions.get(i), redudantData));
            }
        }

    }

    /**
     *
     * @param metric_count
     * @param parallelism
     * @param treeInput 预训练数据地址
     * @return 构建好的 vptree
     */
    public static VPTree<Data,Data> createVPtree(int metric_count, int parallelism, String treeInput){
        BufferedReader reader = null;
        List<Data> sampleData = null;
        try{
            reader = new BufferedReader(new FileReader(treeInput));
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null & count < metric_count) {
                String[] splits = line.split(",");
//                转为 double 列表
                ArrayList<Double> mySampleList = Arrays.stream(splits)
                        .map(Double::parseDouble)
                        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
                Data data = new Data(mySampleList, 0l, 0, 0);
                sampleData.add(data);
                count ++;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
//        构造 vptree
        VPTree<Data, Data> myVPTree = new VPTree<>(new VPdistance(), sampleData);
        myVPTree.createPartitions(parallelism);
        return myVPTree;
    }

    private static class VPdistance implements com.leejean79.jvptree.DistanceFunction<Data>, Serializable {
        @Override
        public double getDistance(Data firstPoint, Data secondPoint) {
            int min = Math.min(firstPoint.dimensions(), secondPoint.dimensions());
            Double value = 0.0;
            for (int i = 0; i < min; i++) {
                value += Math.pow(firstPoint.getValue().get(i) - secondPoint.getValue().get(i), 2);
            }
            return Math.sqrt(value);
        }
    }
}
