package com.leejean.pv;

import com.leejean.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AppMarketWithChannel {

//    create  some data randomly
    public static class AppSourceFunction implements SourceFunction<MarketingUserBehavior> {

        private boolean flag = true;
        private List<String> userBehaviorList = Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
        private List<String> channelList = Arrays.asList("HUAWEI", "XIAOMI", "OPPO", "VIVO");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                ctx.collect(
                        new MarketingUserBehavior(
                                random.nextLong(),
                                userBehaviorList.get(random.nextInt(userBehaviorList.size())),
                                channelList.get(random.nextInt(channelList.size())),
                                System.currentTimeMillis()
                        )
                );
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<MarketingUserBehavior> source = env.addSource(new AppSourceFunction());

//        process data
//        change data struct to (app_channel, 1)
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapedStream = source.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of(value.getChannel() + "_" + value.getBehavior(), 1);
            }
        });

//        keyBy by "app_channel"
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = mapedStream.keyBy(data -> data.f0);

//        sum
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        env.execute();


    }
}
