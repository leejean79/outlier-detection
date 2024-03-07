package com.leejean79.Algorithms;

import com.leejean79.bean.Data;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static com.leejean79.common_utils.Utils.distance;

/***
 * 算法思路：
 *  1 获取最新滑窗数据
 *  2 更新最新数据状态
 *      2.1 找到最新数据在当前窗口的所有近邻；
 *      2.2 如果近邻为历史滑窗中的数据，则更新最新数据的 nn_before；
 *          否则，则更新最新数据的 count_after +1 ；
 *          count_after > k : 则为最新数据为正常值。
 *      2.3 更新最新数据的近邻状态；
 *          如果近邻的到达时间是历史滑窗，则这些历史近邻的 count_after +1；
 *          若果历史近邻 count_after > k，则这些历史近邻为正常值。
 *  3 输出潜在异常数据到下一个窗口
 */


public class NaiveParellel extends ProcessWindowFunction<Tuple2<Integer, Data>, Data, Integer, TimeWindow> {

    private int time_slide;
    private double range;
    private int k;

    public NaiveParellel(int time_slide, double range, int k) {
        this.time_slide = time_slide;
        this.range = range;
        this.k = k;
    }

    @Override
    public void process(Integer key, Context context, Iterable<Tuple2<Integer, Data>> elements, Collector<Data> out) throws Exception {

//        获取窗口
        TimeWindow window = context.window();
//        窗口所有数据转为 list
        Iterator<Tuple2<Integer, Data>> iterator = elements.iterator();
        List<Data> inputList = new ArrayList<>();
        while (iterator.hasNext()) {
            Data element = iterator.next().f1;
            inputList.add(element);
        }

//        获取最后一个滑窗的新到数据
        ArrayList<Data> newList = new ArrayList<>();
        for (Data p:inputList) {
            if (p.getArrival() >= window.getEnd() - time_slide){
                newList.add(p);
            }
        }
//      更新所有新到数据的 safe_inlier、count_after、nn_before
        for (Data el:newList) {
            refreshList(el, inputList, window);
        }

//        将潜在异常点输出到下一个窗口
        inputList.forEach(p -> {
            if (!p.getSafe_inlier()){
                out.collect(p);
            }
        });
    }

    private void refreshList(Data el, List<Data> inputList, TimeWindow window) {
        List<Data> neighbors = new ArrayList<>();
//        找到近邻
        if (!inputList.isEmpty()){
            for (Data p: inputList) {
                if (p.getId() != el.getId()){
                    if (distance(p, el) < range){
                        neighbors.add(p);
                    }
                }
        }
//        根据近邻，更新新数据点的状态
            neighbors.forEach(n -> {
//                近邻为以前滑动窗口数据，则更新 nn_before 列表
                if (n.getArrival() < window.getEnd() - time_slide){
                    el.insert_nn_before(n.getArrival(), k);
                }else {
//                    否则，更新新到数据的后续近邻个数 Count_after 和 该点正常值状态 Safe_inlier
                    el.setCount_after(1);
                    if (el.getCount_after() >= k){
                        el.setSafe_inlier(true);
                    }
                }
            });
            inputList.forEach(p -> {
                if (p.getArrival() < window.getEnd() - time_slide && neighbors.contains(p)){
                    p.setCount_after(1);
                    if (p.getCount_after() >= k){
                        p.setSafe_inlier(true);
                    }
                }
            });

        }

    }


}
