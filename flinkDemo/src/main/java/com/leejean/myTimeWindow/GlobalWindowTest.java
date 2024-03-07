package com.leejean.myTimeWindow;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

/**
 * 单词每出现三次统计一次
 执行结果：
 总结：效果跟CountWindow(3）很像，但又有点不像，因为如果是CountWindow(3)，单词每次出现的
 都是3次，不会包含之前的次数，而我们刚刚的这个每次都包含了之前的次数。
 4.3.2 Trigger
 需求：自定义一个CountWindow

 */
public class GlobalWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("10.148.15.10", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>>
                            collector) throws Exception {
                        String[] fields = line.split(",");
                        for (String word : fields) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                });
        stream.keyBy(0)
                .window(GlobalWindows.create())
//如果不加这个程序是启动不起来的
                .trigger(CountTrigger.of(3))
                .sum(1)
                .print();
        env.execute("SessionWindowTest");
    }
}
