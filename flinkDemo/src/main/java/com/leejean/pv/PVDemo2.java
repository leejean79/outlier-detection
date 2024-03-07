package com.leejean.pv;

import com.leejean.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 直接进行计数统计
 * 利用 process 的方法
 */

public class PVDemo2 {

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

//        keyBy + process的方法直接进行计数
        final SingleOutputStreamOperator<Long> processDs = filterDs.keyBy(line -> line.getBehavior()).process(new KeyedProcessFunction<String, UserBehavior, Long>() {
            private long pvCount = 0;

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                pvCount++;
                out.collect(pvCount);
            }
        });

        //        输出
        processDs.print();

        env.execute();

    }
}
