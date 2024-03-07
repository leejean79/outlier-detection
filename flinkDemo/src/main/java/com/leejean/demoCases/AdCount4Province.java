package com.leejean.demoCases;

import com.leejean.bean.AdClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AdCount4Province {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.readTextFile("input/AdClickLog.csv");

        SingleOutputStreamOperator<AdClickLog> addClickStream = source.map(new MapFunction<String, AdClickLog>() {
            @Override
            public AdClickLog map(String value) throws Exception {
                String[] splits = value.split(",");
                return new AdClickLog(
                        Long.valueOf(splits[0]),
                        Long.valueOf(splits[1]),
                        splits[2],
                        splits[3],
                        Long.valueOf(splits[4])
                );
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> mappedTuple = addClickStream.map(new MapFunction<AdClickLog, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(AdClickLog value) throws Exception {
                return Tuple2.of(value.getProvince() + "_" + value.getAdId(), 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> adClickResult = mappedTuple.keyBy(data -> data.f0).sum(1);

        adClickResult.print();

        env.execute();


    }

}
