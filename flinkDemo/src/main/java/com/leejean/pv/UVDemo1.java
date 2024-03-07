package com.leejean.pv;

import com.leejean.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

public class UVDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//         读取数据，封装为pojo类
        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> mapDs = source.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] strings = value.split(",");
                return new UserBehavior(
                        Long.valueOf(strings[0]),
                        Long.valueOf(strings[1]),
                        Integer.valueOf(strings[2]),
                        strings[3],
                        Long.valueOf(strings[4])
                );
            }
        });

//        过滤，只留下pv行为数据行
        SingleOutputStreamOperator<UserBehavior> filterDs = mapDs.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        /***
         * 业务逻辑：
         * 1 将过滤后的数据封装为 tuple（"uv"，userId）
         * 2 以"uv"进行 keyBy
         * 3 process 中，利用 set 进行 userId 的去重，最后返回 set 大小
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> uvDs = filterDs.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return new Tuple2<String, Long>("UV", value.getUserId());
            }
        });

        KeyedStream<Tuple2<String, Long>, String> keyedStream = uvDs.keyBy(line -> line.f0);

        SingleOutputStreamOperator<Long> processDs = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Long>, Long>() {

            private Set<Long> uvSet = new HashSet<Long>();

            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Long> out) throws Exception {
                uvSet.add(value.f1);
                out.collect(Long.valueOf(uvSet.size()));
            }
        });

        processDs.print();

        env.execute();


    }
}
