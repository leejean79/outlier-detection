package com.leejean.myTimeWindow;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class WindowFuncWithAggr {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);

        KeyedStream<Tuple2<String, Integer>, String> dataKS = socketDS
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(r -> r.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> timeWindow = dataKS.timeWindow(Time.seconds(5));

        SingleOutputStreamOperator<Integer> aggregate = timeWindow.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {

            //            初始化累加器
            @Override
            public Integer createAccumulator() {
                System.out.println("init accumulator ...");
                return 0;
            }

            //            定义聚合操作
            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                System.out.println("doing add ...");
                return value.f1 + accumulator;
            }

            //            返回结果
            @Override
            public Integer getResult(Integer accumulator) {
                System.out.println("return the result: ");
                return accumulator;
            }

            //            会话窗口 才会调用：合并累加器的结果
//            此处可以不做任何操作
            @Override
            public Integer merge(Integer a, Integer b) {
                return null;
            }
        });

        aggregate.print();

        env.execute();
    }
}
