package com.leejean79.outlierDetect;

import com.leejean79.Algorithms.*;
import com.leejean79.bean.Data;
import com.leejean79.jvptree.VPTree;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;
import scala.tools.nsc.transform.patmat.Lit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.leejean79.common_utils.Partitioning.*;

public class OutlierDetection {

    public static void main(String[] args) throws Exception {

//        减缓流速
        long cur_time = System.currentTimeMillis()+1000000L;
//        并行度
        int parallelism = 1;

//        if (args.length == 0) {
//            System.out.println("请提供命令行参数。");
//            return;
//        }
//
//        // 解析和处理命令行参数
//        for (int i = 0; i < args.length; i++) {
//            String arg = args[i];
//            System.out.println("参数 " + (i + 1) + ": " + arg);
//        }

//        一个滑窗的大小
        int count_slide = 1;
//        整个监测窗口的大小
        int count_window = 10;
//        滑窗与监测窗口的比例
        double count_slide_percent = 100 * ((double) count_slide / count_window);
//        一个窗口的时间：将整体的监测窗口划分为几个窗口
        int time_window = count_window / 10;
//        一个滑窗的时间：每个窗口根据滑窗比例计算出滑窗的时间
        int time_slide = (int)(time_window * (count_slide_percent / 100));
//        近邻个数
        int k = 5;
//        近邻半径
        double range = 0;
//        分区方法
        String partitioning_type = "metric";//采用 Tree-based partitioning对数据分区
//        采用何种算法
        String algorithm = "advanced_vp";
//      创建 vptree 的样本个数
        int metric_count = 10000;
//        已有树的文件地址
        String treeInput = "";

//      预处理，根据数据生成 vp-tree
        VPTree myVPTree = createVPtree(metric_count, parallelism, treeInput);



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(parallelism);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //设置waterMark产生的周期为1s
//        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<String> source = env.readTextFile("power_data.txt");//电力数据：一维


        SingleOutputStreamOperator<Tuple2<Integer, Data>> mappedData = source.flatMap(new FlatMapFunction<String, Tuple2<Integer, Data>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<Integer, Data>> collector) {

                String[] splitLine;
                int id;
//                每条数据的属性值列表
                ArrayList<Double> value = new ArrayList<>();
                double multiplication;
                long new_time;

                switch (algorithm) {
                    case "parallel":
                        splitLine = s.split("&");
                        id = Integer.parseInt(splitLine[0]);
//                各维度信息放入 list 列表
                        String[] splits = splitLine[1].split(",");
                        for (int i = 0; i < splits.length; i++) {
                            value.add(Double.parseDouble(splits[i]));
                        }
                        multiplication = id / count_slide;
                        new_time = cur_time + (long) (multiplication * time_slide);
//              数据预处理，根据 id 进行分区
//                返回Tuple(分区号i, Data(value, time, flag, id))
                        replicationPartitioning(parallelism, value, new_time, id, collector);
                        break;

                    case "advanced_vp":
                    case "pmcod":
                        splitLine = s.split("&");
                        id = Integer.parseInt(splitLine[0]);
                        splits = splitLine[1].split(",");
                        for (int i = 0; i < splits.length; i++) {
                            value.add(Double.parseDouble(splits[i]));
                        }
                        multiplication = id / count_slide;
                        new_time = cur_time + (long) (multiplication * time_slide);
                        /***list[(Int, Data)] 数据及其分区号，可能存在冗余，冗余数据 cflag=1
                         * 返回：将 查询点及其最近邻的分区号、非近邻的分区号封装为：
                         * (最近邻的分区号，查询点), (非最近邻分区号，查询点'),....
                         * 查询点发往其他分区是冗余的，表现在 flag=1
                         */
                        metricPartitioning(value, new_time, id, range, parallelism, "VPTree", null, myVPTree, collector);
                        break;

                }

            }
        });

//        添加水位
        SingleOutputStreamOperator<Tuple2<Integer, Data>> timestampData = mappedData.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator((ctx) -> new PeriodicWatermarkGenerator())
                .withTimestampAssigner((ctx) -> new TimeStampExtractor()));

        switch (algorithm){
            case "parallel":
                SingleOutputStreamOperator<Data> firstWindow = timestampData
                        .keyBy(line -> line.f0) //已分区 id 为 key
                        .window(SlidingEventTimeWindows.of(Time.milliseconds(time_window), Time.milliseconds(time_slide)))
                        .allowedLateness(Time.milliseconds(1000))
                        .evictor(new MyEvictor(time_slide)) //窗口处理开始前，剔除滑动窗口内历史冗余数据 （flag=1且到达时间为：窗口起始-最新滑窗之前 ）
                        /***
                         * * compute pairwise distances involving new objects
                         * * update o.count_after and o.nn_before metadata
                         * *
                         */
                        .process(new NaiveParellel(time_slide, range, k));
                        /***
                         * 开启第二个窗口：滚动窗口 用于对所有数据的元数据进行全局的合并
                         * 分区：将按照数据在上一窗口的分区号进行重新分区
                         * 窗口大小：按照每一个最基本的滑动窗口大小
                         */
                        SingleOutputStreamOperator<Tuple2<Long, Integer>> result1 = firstWindow
                                .keyBy(p -> p.getId() % parallelism)
                                .window(TumblingEventTimeWindows.of(Time.milliseconds(time_slide)))
                                .process(new GroupMetadataParallel(time_window, time_slide, range, k));

                        // 按窗口打印结果
                        SingleOutputStreamOperator<String> outlierInfo = result1.keyBy(t -> t.f0)
                                .window(TumblingEventTimeWindows.of(Time.milliseconds(time_slide)))
                                .process(new ShowResults());
                        outlierInfo.print();
                        break;
            case "advanced_vp":
                timestampData.keyBy(t -> t.f0)
                        .window(SlidingEventTimeWindows.of(Time.milliseconds(time_window), Time.milliseconds(time_slide)))
                        .allowedLateness(Time.milliseconds(1000))
                        .process(new AdvancedVP(time_slide, range, k));
                break;
            case "pmcod":
                SingleOutputStreamOperator<Tuple2<Long, Integer>> result2 = timestampData.keyBy(t -> t.f0)
                        .window(SlidingEventTimeWindows.of(Time.milliseconds(time_window), Time.milliseconds(time_slide)))
                        .allowedLateness(Time.milliseconds(1000))
                        .process(new Pmcod(time_slide, range, k));

                SingleOutputStreamOperator<String> outlierInfo2 = result2.keyBy(t -> t.f0)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(time_slide)))
                        .process(new ShowResults());

                outlierInfo2.print();


        }



        Long time1 = System.currentTimeMillis();
        env.execute("outlier-flink");
        Long time2 = System.currentTimeMillis();
        System.out.printf("Time Cusuming: %l", time1-time2);
        System.out.println("\n");

    }


    private static class PeriodicWatermarkGenerator implements WatermarkGenerator<Tuple2<Integer, Data>>, Serializable {
        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10000L; // 最大允许的乱序时间 10 秒

        @Override
        public void onEvent(Tuple2<Integer, Data> tuple, long eventTimestamp, WatermarkOutput output) {
            //指定时间字段
            Long arrival = tuple.f1.getArrival();
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark((currentMaxEventTime - maxOutOfOrderness)  ));
        }
    }

    private static class TimeStampExtractor implements TimestampAssigner<Tuple2<Integer, Data>> {
        @Override
        public long extractTimestamp(Tuple2<Integer, Data> tuple, long recordTimestamp) {

            try {
                return tuple.f1.getArrival();
            } catch (Exception e) {
                e.printStackTrace();
            }
            return 0;
        }

    }

    private static class MyEvictor implements Evictor<Tuple2<Integer, Data>, TimeWindow> {

        private int time_slide;
        public MyEvictor(int time_slide) {
            this.time_slide = time_slide;
        }
        /***
         *
         * @param elements
         * @param size
         * @param window
         * @param evictorContext
         */
        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<Integer, Data>>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            Iterator<TimestampedValue<Tuple2<Integer, Data>>> iterator = elements.iterator();
            while (iterator.hasNext()){
                Data tmpNode = iterator.next().getValue().f1;
                if (tmpNode.getFlag() == 1 && tmpNode.getArrival() >= window.getStart() && tmpNode.getArrival() < window.getEnd() - time_slide){
                    iterator.remove();
                }
            }
        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<Integer, Data>>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

        }
    }
}


