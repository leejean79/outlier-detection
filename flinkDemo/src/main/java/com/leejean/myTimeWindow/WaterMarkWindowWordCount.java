package com.leejean.myTimeWindow;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * *  flink,1461756862000
 *  *  flink,1461756866000
 *  *  flink,1461756872000
 *  *  flink,1461756873000
 *  *  flink,1461756874000
 *  *  flink,1461756876000
 *  *  flink,1461756877000
 */

public class WaterMarkWindowWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//步骤一：设置时间类型
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//设置waterMark产生的周期为1s
//        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 9999);
        dataStream.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] fields = line.split(",");
                return new Tuple2<>(fields[0],Long.valueOf(fields[1]));
            }
//步骤二：获取数据里面的event Time
        }).assignTimestampsAndWatermarks(new EventTimeExtractor() )
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                .process(new SumProcessWindowFunction())
                .print().setParallelism(1);
        env.execute("TimeWindowWordCount");
    }
    /**
     * IN, OUT, KEY, W
     * IN：输入的数据类型
     * OUT：输出的数据类型
     * Key：key的数据类型（在Flink里面，String用Tuple表示）
     * W：Window的数据类型
     */
    public static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Long>,String, Tuple, TimeWindow> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        /**
         * 当一个window触发计算的时候会调用这个方法
         * @param tuple key
         * @param context operator的上下文
         * @param elements 指定window的所有元素
         * @param out 用户输出
         */
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) {
            System.out.println("处理时间：" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("window start time : " + dateFormat.format(context.window().getStart()));
            List<String> list = new ArrayList<>();
            for (Tuple2<String, Long> ele : elements) {
                list.add(ele.toString() + "|" + dateFormat.format(ele.f1));
            }
            out.collect(list.toString());
            System.out.println("window end time : " + dateFormat.format(context.window().getEnd()));
        }
    }
    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10000; // 最大允许的乱序时间 10 秒
        // 拿到每一个事件的 Event Time
        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long previousElementTimestamp) {
            long currentElementEventTime = element.f1;
            currentMaxEventTime = Math.max(currentMaxEventTime, currentElementEventTime);
            System.out.println("event = " + element
                            + "|" + dateFormat.format(element.f1) // Event Time
                            + "|" + dateFormat.format(currentMaxEventTime) // Max EventTime
                            + "|" + dateFormat.format(getCurrentWatermark().getTimestamp())); // Current Watermark
            return currentElementEventTime;
        }
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
/**
 * WasterMark会周期性的产生，默认就是每隔200毫秒产生一个
 *
 * 设置 watermark 产生的周期为 1000ms
 * env.getConfig().setAutoWatermarkInterval(1000);
 */
//window延迟5秒触发
            System.out.println("water mark...");
            return new Watermark(currentMaxEventTime - maxOutOfOrderness);
        }
    }
}
