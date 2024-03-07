package com.leejean.pv;

import com.leejean.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 利用 wordcount 的思路
 */
public class PvDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//         读取数据，封装为pojo类
        DataStreamSource<String> source = env.readTextFile("input/UserBehavior.csv");
        final SingleOutputStreamOperator<UserBehavior> mapDs = source.map(new MapFunction<String, UserBehavior>() {
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
        final SingleOutputStreamOperator<UserBehavior> filterDs = mapDs.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        /**
         * 进行pv的统计
         */

//        map操作：line--> ("pv", 1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDs = filterDs.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", 1);
            }
        });
//        keyBy分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDs = tupleDs.keyBy(0);
//        sum求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDs = keyByDs.sum(1);

//        输出
        sumDs.print();

        env.execute();

    }
}
