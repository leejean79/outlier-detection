package com.leejean.pv;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class PvDemo3 {

    public static void main(String[] args) throws Exception {

//        read data
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");

//        直接将一行数据打散后，过滤、求和
        source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String userBehavior = value.split(",")[3];
                if ("pv".equals(userBehavior)){
                    out.collect(new Tuple2<String, Integer>("pv", 1));
                }
            }
        }).keyBy(element -> element.f0)
                .sum(1)
                .print();

        env.execute();

    }
}
