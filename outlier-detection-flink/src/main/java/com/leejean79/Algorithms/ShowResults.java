package com.leejean79.Algorithms;

import com.leejean79.bean.Data;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class ShowResults extends ProcessWindowFunction<Tuple2<Long, Integer>, String, Long, TimeWindow> {
    @Override
    public void process(Long key, Context context, Iterable<Tuple2<Long, Integer>> elements, Collector<String> out) throws Exception {
        int sum = 0;
        Iterator<Tuple2<Long, Integer>> iterator = elements.iterator();
        while (iterator.hasNext()){
            sum += iterator.next().f1;
        }
        String result = String.valueOf(key) + ";" + String.valueOf(sum);
        out.collect(result);

    }
}
