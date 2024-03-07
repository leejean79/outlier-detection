package com.leejean.myProcessFunction;

import com.leejean.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class WaterSensorAlarm {

    public static void main(String[] args) throws Exception {
        // 0.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1.读取数据
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
//                .readTextFile("input/sensor-data.log")
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));
                    }
                })
                .assignTimestampsAndWatermarks(
//                        注册基于时间的 watermark 产生周期
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {
                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                return new Watermark(extractedTimestamp);
                            }

                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        sensorDS.keyBy(sensor -> sensor.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
//                    定义定时器的初始值
                    private Long timeTs = 0L;
//                    定义当前水位值
                    private Integer lastVc = -1;
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
//                        水位上升
                        if (value.getVc()>lastVc){
//                         判断是否已经注册了定时器
                            if (timeTs == 0){
                                timeTs = ctx.timestamp() + 5000L;
                                ctx.timerService().registerEventTimeTimer(timeTs);
                            }

                            lastVc = value.getVc();

                        }
//                        水位没有上升
                        else{
//                            清除定时器
                            ctx.timerService().deleteEventTimeTimer(timeTs);
//                            定时器清零
                            timeTs = 0L;
                        }

//                        水位没有上升，则更新为当前水位
                        lastVc = value.getVc();
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
//                        定时器触发后进行报警
                        out.collect("警告：水位已经连续 5 秒上涨！！");
//                        重置水位和定时器
                        lastVc = -1;
                        timeTs = 0L;
                    }
                }).print();

        env.execute();


    }
}
