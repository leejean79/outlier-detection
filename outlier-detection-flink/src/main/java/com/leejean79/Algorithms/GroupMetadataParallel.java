package com.leejean79.Algorithms;

import com.leejean79.bean.Data;
import com.leejean79.bean.Metadata;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

import static com.leejean79.common_utils.Utils.combineElementsParallel;

/***
 * 算法：
 *  在每一个窗口中（窗口大小：滑动步长）
 *  1 合并当前分区流中数据的状态 nn_before （利用每一个 keyed 流的ValueState保存）；
 *    1.1 如果state 为空（第一次计算）：则把当前点的状态放入；
 *    1.2 如果 state 不为空，则更新 state 中的每一个点 Count_after 和 nn_before：
 *          如果 该点不在最新的窗口中，则删除；
 *          否则，如果该点在历史滑窗中，
 *              则更新该状态点的 Count_after；
 *              否则，更新该状态点的 nn_before；
 *  2 状态更新完后，遍历ValueState，计算 state 中每个点的 nn_before + count_after < k, 则为异常点，输出；
 *    2.1 计算每一个状态点的整个计算窗口所有前那个点 nn_before；
 *    2.2 计算 nn_before + count_after < k ，输出为异常值。
 */

public class GroupMetadataParallel extends ProcessWindowFunction<Data, Tuple2<Long,Integer>, Integer, TimeWindow> {
//    (time_window: Int, time_slide: Int, range: Double, k: Int)
    private int time_widow;
    private int time_slide;
    private double range;
    private int k;

    public GroupMetadataParallel(int time_widow, int time_slide, double range, int k) {
        this.time_widow = time_widow;
        this.time_slide = time_slide;
        this.range = range;
        this.k = k;
    }

    //    记录异常值状态
    private ValueState<Metadata> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<Metadata> descriptor = new ValueStateDescriptor<Metadata>(
                "countState",
                Metadata.class);
        state = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void process(Integer key, Context context, Iterable<Data> elements, Collector<Tuple2<Long, Integer>> out) throws Exception {

        TimeWindow window = context.window();
        Metadata current = state.value();
//        第一次处理该窗口，还没有异常点,
//          更新当前滑窗里所有点的 nn_before，
//          将当前点都进行缓存
        if (current == null){
            Map<Integer, Data> newMap = new HashMap<>();
            elements.forEach(el -> {
                Data oldEl = newMap.getOrDefault(el.getId(), null);
                if (oldEl == null) {
                    newMap.put(el.getId(), el);
                } else {
                    //   合并当前点的nn_before (prune o.nn_before )
                    Data newValue = combineElementsParallel(oldEl, el, k);
                    newMap.put(el.getId(), newValue);
                }
            });
            current = new Metadata(newMap);
//            state.update(current);
        }else { //如果窗口状态中已经有数据，则：1 清除状态中的旧数据；2 插入或合并新数据
            List<Integer> forRemoval = new ArrayList<>(); //创建待删除列表
            //  1 删除当前状态中的旧数据：如果 当前状态中的数据不在新的窗口中，则添加到删除列表
            current.getAllFromMap().values().forEach(el -> {
                 int count = 0;
                 while (elements.iterator().hasNext()){
                     if(elements.iterator().next().getId() == el.getId()){
                         count += 1;
                     }
                 }
                 if (count == 0){
                     forRemoval.add(el.getId());
                 }
            });
            for (int i = 0; i < forRemoval.size(); i++) {
                current.getAllFromMap().remove(forRemoval.get(i));
            }

//            2 插入或合并新数据
            Iterator<Data> dataIterator = elements.iterator();
            while (dataIterator.hasNext()) {
                Data data = dataIterator.next();
                Data oldEl = current.getAllFromMap().getOrDefault(data.getId(), null);
                if (oldEl == null){
                    current.putToMap(data.getId(), data);
                }else {
//                    如果状态不为空，且该点是历史数据，则更新状态数据的Count_after
                    if (data.getArrival() < window.getEnd() - time_slide){
                        oldEl.setCount_after(data.getCount_after());
                    }else {
//                        否则，状态不为空，且该点为当前窗口数据，则更新 nn_before
                        Data newValue = combineElementsParallel(oldEl,data,k);
                        current.putToMap(data.getId(), newValue);
                    }
                }


            }

        }
        state.update(current);

        List<Integer> outliers = new ArrayList<>();
        current.getAllFromMap().forEach((id, data) -> {
            // 整个计算窗口的状态值
            int tempSum = (int) data.getNn_before().stream().filter(time -> time >= window.getEnd() - time_widow).count();
            // 当潜在 PO 的前后近邻个数小于阈值，则为异常值
            if (data.getCount_after() + tempSum < k){
                outliers.add(id);
            }

        });

        out.collect(new Tuple2(window.getEnd(), outliers.size()));

    }
}
