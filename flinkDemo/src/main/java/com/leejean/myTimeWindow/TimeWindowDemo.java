package com.leejean.myTimeWindow;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class TimeWindowDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        env.setParallelism(1);

        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);

        KeyedStream<Tuple2<String, Integer>, String> dataKS = socketDS
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(r -> r.f0);
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> dataWS = dataKS
//                滚动窗口
//                .timeWindow(Time.seconds(5));
//                滑动窗口
        .timeWindow(Time.seconds(5), Time.seconds(2));

        dataWS.sum(1).print();
        env.execute();

    }
}
