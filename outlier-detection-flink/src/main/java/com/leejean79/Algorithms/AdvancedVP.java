package com.leejean79.Algorithms;

import com.leejean79.bean.AdvancedVPState;
import com.leejean79.bean.Data;
import com.leejean79.mtree.*;
import com.leejean79.mtree.utils.Pair;
import com.leejean79.mtree.utils.Utils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class AdvancedVP extends ProcessWindowFunction<Tuple2<Integer, Data>, Tuple2<Long, Integer>, Integer, TimeWindow> {

    private int time_slide;
    private double range;
    private int k;

    public AdvancedVP(int time_slide, double range, int k) {
        this.time_slide = time_slide;
        this.range = range;
        this.k = k;
    }

    //
    private ValueState<AdvancedVPState> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<AdvancedVPState> descriptor = new ValueStateDescriptor<AdvancedVPState>(
                "myTree",
                AdvancedVPState.class);
        state = getRuntimeContext().getState(descriptor);
    }


    @Override
    public void process(Integer key, Context context, Iterable<Tuple2<Integer, Data>> elements, Collector<Tuple2<Long, Integer>> out) throws Exception {

        TimeWindow window = context.window();

        AdvancedVPState current = state.value();

//        拿到窗口的新数据
        List<Data> newDataList = new ArrayList<>();
        while (elements.iterator().hasNext()) {
            if (elements.iterator().next().f1.getArrival() > window.getEnd() - time_slide) {
                newDataList.add(elements.iterator().next().f1);
            }
//      第一次保存状态
            if (current == null) {
                PromotionFunction<Data> promotionFunction = new PromotionFunction<Data>() {
                    /**
                     * Chooses (promotes) a pair of objects according to some criteria that is
                     * suitable for the application using the M-Tree.
                     *
                     * @param dataSet          The set of objects to choose a pair from.
                     * @param distanceFunction A function that can be used for choosing the
                     *                         promoted objects.
                     * @return A pair of chosen objects.
                     */
                    @Override
                    public Pair<Data> process(Set<Data> dataSet, DistanceFunction<? super Data> distanceFunction) {
                        return Utils.minMax(dataSet);
                    }
                };
                //      定义节点划分的函数或策略。它用于选择优势点和划分超平面，以将数据集划分为两个子集
                ComposedSplitFunction<Data> mySplit = new ComposedSplitFunction<>(promotionFunction, new PartitionFunctions.BalancedPartition<Data>());
//            创建 MTree
                MTree<Data> mTree = new MTree<Data>(k, DistanceFunctions.EUCLIDEAN, mySplit);

                HashMap<Integer, Data> hashMap = new HashMap<>();

                Iterator<Tuple2<Integer, Data>> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    mTree.add(iterator.next().f1);
                    hashMap.put(iterator.next().f1.getId(), iterator.next().f1);
                }
                current = new AdvancedVPState(mTree, hashMap);

            } else {
//            如果状态已存在，则从窗口中将新到数据放入状态
                for (int i = 0; i < newDataList.size(); i++) {
                    Data data = newDataList.get(i);
                    current.tree.add(data);
                    current.hashMap.put(data.getId(), data);
                }
            }
//        获得近邻
            for (int i = 0; i < newDataList.size(); i++) {
                Data p = newDataList.get(i);
                Data tempData = new Data(p.getValue(), p.getArrival(), p.getFlag(), p.getId());
//                拿到新数据点的近邻迭代器
                MTree<Data>.Query query = current.tree.getNearestByRange(tempData, range);
                Iterator<MTree<Data>.ResultItem> iterator = query.iterator();
//                遍历近邻
                while (iterator.hasNext()) {
                    Data neighbor = iterator.next().data;
                    if (neighbor.getId() != tempData.getId()) {
//                        该近邻是历史数据
                        if (neighbor.getArrival() < window.getEnd() - time_slide) {
//                            如果是不是冗余数据
                            if (tempData.getFlag() == 0) {
//                                把近邻加入给点的前 n 个
                                current.hashMap.get(tempData.getId()).insert_nn_before(neighbor.getArrival(), k);
                            }
//                            如果近邻还没有过期，则更新该近邻的后续节点个数，并判断是否为异常值
                            if (neighbor.getFlag() == 0) {
                                current.hashMap.get(neighbor.getId()).setCount_after(1);
                                if (current.hashMap.get(neighbor.getId()).getCount_after() >= k) {
                                    current.hashMap.get(neighbor.getId()).setSafe_inlier(true);
                                }
                            }
                            /**
                             * 如果近邻为最近窗口数据，并且新数据仍是有效的，则：
                             * 更新新数据的Count_after直；
                             * 并判断新数据是否为异常值
                             */
                        } else {
                            if (tempData.getFlag() == 0) {
                                current.hashMap.get(tempData.getId()).setCount_after(1);
                                if (current.hashMap.get(tempData.getId()).getCount_after() >= k) {
                                    current.hashMap.get(tempData.getId()).setSafe_inlier(true);
                                }
                            }
                        }
                    }
                }
            }
        }

//        记录异常值个数
        int outliers = 0;

        Iterator<Data> dataIterator = current.hashMap.values().iterator();

        while (dataIterator.hasNext()) {
            Data data = dataIterator.next();
            if (data.getFlag() == 0 && !data.getSafe_inlier()) {
                List<Long> nn_before = data.getNn_before();
                int nnBefore = (int) nn_before.stream().filter(arrive -> arrive >= window.getStart()).count();
                if (data.getCount_after() + nnBefore < k) {
                    outliers += 1;
                }
            }
        }
//        输出（窗口结束时间，异常值个数)
        out.collect(new Tuple2<Long, Integer>(window.getEnd(), outliers));

        //Remove expiring objects and flagged ones from state
        List<Data> expiringData = new ArrayList<>();
        while (elements.iterator().hasNext()) {
            if (elements.iterator().next().f1.getArrival() < window.getStart() + time_slide) {
                expiringData.add(elements.iterator().next().f1);
            }
            for (int i = 0; i < expiringData.size(); i++) {
                current.tree.remove(expiringData.get(i));
                current.hashMap.remove(expiringData.get(i).getId());
            }
        }
        state.update(current);
    }
}
