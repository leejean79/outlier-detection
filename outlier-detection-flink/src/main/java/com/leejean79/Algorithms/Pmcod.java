package com.leejean79.Algorithms;

import com.leejean79.bean.Data;
import com.leejean79.bean.McodState;
import com.leejean79.bean.MicroCluster;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.leejean79.common_utils.Utils.distance;

public class Pmcod extends ProcessWindowFunction<Tuple2<Integer, Data>, Tuple2<Long, Integer>, Integer, TimeWindow> {

    private int slide;
    private double range;
    private int k;

    public Pmcod(int slide, double range, int k_c) {
        this.slide = slide;
        this.range = range;
        this.k = k_c;
    }

    public Pmcod() {
    }

    int mc_counter = 1;

//     state consists of the micro-clusters, the potential outliers PO and the M-tree
    private ValueState<McodState> state;

    @Override
    public void open(Configuration parameters) throws Exception {

        ValueStateDescriptor<McodState> descriptor = new ValueStateDescriptor<McodState>(
                "myState",
                McodState.class);
        state = getRuntimeContext().getState(descriptor);
    }

    /**
     * 1 取出滑窗的最新数据；
     * 2 插入新数据：
     *      2.1 如果新数据属于某个微簇，则计算它与微簇间的距离，然后更新PO 的元数据；
     *      2.2 如果新数据不属于任何微簇，则将其插入 PO，然后通过距离查询它的近邻，根据近邻个数产生新的微簇
     * 3 寻找 outliers：
     * 4 移除过期数据
     * 5 删除微簇
     * @param integer
     * @param context
     * @param elements
     * @param out
     * @throws Exception
     */

    @Override
    public void process(Integer integer, Context context, Iterable<Tuple2<Integer, Data>> elements, Collector<Tuple2<Long, Integer>> out) throws Exception {

        TimeWindow window = context.window();

//        初始化状态
        if (state.value() == null){
            HashMap<Integer, Data> pd = new HashMap<>();
            HashMap<Integer, MicroCluster> mc = new HashMap<>();
            McodState current = new McodState(pd, mc);
            state.update(current);
        }

//        取出新数据，去除过期数据
        List<Data> newDataList = new ArrayList<>();
        while (elements.iterator().hasNext()) {
            if (elements.iterator().next().f1.getArrival() > window.getEnd() - slide) {
                newDataList.add(elements.iterator().next().f1);
            }
        }
        /***
         * 将新数据插入：
         *   1 如果新数据属于某个微簇，则计算它与微簇间的距离，然后更新PO 的元数据；
         *   2 如果新数据不属于任何微簇，则将其插入 PO，然后通过距离查询它的近邻，根据近邻个数产生新的微簇
         */
        newDataList.forEach(newData -> {
            try {
                insertPoint(newData, true, null);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        //find outliers
        int outliers = 0;
        Collection<Data> datas = state.value().getPd().values();
        while (datas.iterator().hasNext()){
            Data p = datas.iterator().next();
            if (!p.getSafe_inlier() && p.getFlag() == 0){
                List<Long> nn_before = p.getNn_before();
                int nnBefore = (int) nn_before.stream().filter(arrive -> arrive >= window.getStart()).count();
                if (p.getCount_after() + nnBefore < k){
                    outliers += 1;
                    }
                }
        }

        out.collect(new Tuple2<Long, Integer>(window.getEnd(), outliers));

        //Remove old points
        HashSet<Integer> deletedMCs = new HashSet<>();
        while (elements.iterator().hasNext()){
//            下一步滑动就过期数据
            if (elements.iterator().next().f1.getArrival() < window.getStart() + slide){
                int delete = deletedPoint(elements.iterator().next().f1);
                if (delete > 0){
                    deletedMCs.add(delete);
                }
            }
        }

        //Delete MCs
        if (!deletedMCs.isEmpty()){
            ArrayList<Data> reinsert = new ArrayList<>();
            deletedMCs.forEach(mc -> {
                try {
                    reinsert.addAll(state.value().getMc().get(mc).getPoints());
                    state.value().getMc().remove(mc);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            ArrayList<Integer> reinsertIndexes = new ArrayList<>();
            reinsert.forEach(p -> {
                reinsertIndexes.add(p.getId());
            });

            //Reinsert points from deleted MCs
            reinsert.forEach(p -> {
                try {
                    insertPoint(p, false, reinsertIndexes);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }


    }

    private int deletedPoint(Data data) throws IOException {
        int res = 0;
        if (data.getMc() < 0){
            //Delete it from PD
            state.value().getPd().remove(data.getId());
        }else {
            //Deleted it from mc
            state.value().getMc().get(data.getMc()).getPoints().remove(data);
            //如果微簇点个数小于阈值，则解散
            if (state.value().getMc().get(data.getMc()).getPoints().size() <= k){
                res = data.getMc();
            }
        }
        return res;
    }

    /** 1 插入数据有可能是新的，也有可能是旧的。（插入旧数据，则删除原有微簇信息）
     *  2 计算插入数据与窗口状态中所有微簇的距离，返回距离小于 3/2 range 的微簇；
     *  3 从返回的微簇中找到距离最近的那个微簇：
     *      3.1 如果距离小于 1/2 range，将该点插入微簇；并且，如果是新数据，则找出该簇中的 pd，计算 pd 与插入点的距离；如果小于 range，则更新 pd 的状态。
     *                                              3.1.2 如果是解散的旧数据，则从 state 中找到即是该簇、又是解散旧数据的点 pd，更新 pd 的状态。
     *      3.2 否则，计算插入点与所有 pd 的距离；3.2.1 距离小于 range，更新插入点状态；如果插入点是新数据，更新 pd 的状态，否则，如果 pd 是解散数据，则更新 pd 状态。
     *                                      3.2.2 距离小于 1/2 range，则 pd加入待建微簇 NC； 其他距离则加入 NNC。
     *  4 产生新簇：4.1如果 NC 点的个数＞阈值，则创建以插入点为中心，NC 为组成的微簇；
     *            4.4 否则，将插入点放入 pd 中。
     * @param el 数据点
     * @param newPoint 是否是新数据
     * @param reinsert 原有微簇解散后的数据点的 id
     * @throws IOException
     */
    private void insertPoint(Data el, boolean newPoint, ArrayList<Integer> reinsert) throws IOException {

        McodState state = this.state.value();

//        判断是否是新数据（旧数据指的是旧簇解散后的点）
        if (!newPoint){
            el.clear(-1);//旧数据则清除原有微簇信息
        }

//        计算数据点与所有微簇的距离，返回与数据点距离小于 2/3 range 的微簇
        //HashMap<微簇 id,  点到微簇的距离>
        HashMap<Integer, Double> closeMCs = findCloseMCs(el);

//        从所有微簇closeMCs中找到 距离最近 的微簇closerMC；
        Tuple2<Integer, Double> closerMC = new Tuple2<>();
        if (!closeMCs.isEmpty()){
            ArrayList<Map.Entry<Integer, Double>> mcList = new ArrayList<>(closeMCs.entrySet());
            Collections.sort(mcList, new Comparator<Map.Entry<Integer, Double>>() {
                @Override
                public int compare(Map.Entry<Integer, Double> mc1, Map.Entry<Integer, Double> mc2) {
                    return mc1.getValue().compareTo(mc2.getValue());
                }
            });
            closerMC.setFields(mcList.get(0).getKey(), mcList.get(0).getValue());
        }else {
            closerMC.setFields(0, Double.MAX_VALUE);
        }

        //Insert new element to MC
        if (closerMC.f1 < range / 2){
            if (newPoint){
                insertToMC(el, closerMC.f0, true, null);
            }else {
                insertToMC(el, closerMC.f0, false, reinsert);
            }
        }else{//Check against PD 检查pd 中的点
            ArrayList<Data> NC = new ArrayList<>();
            ArrayList<Data> NNC = new ArrayList<>();
            state.getPd().values().forEach(p -> {
                Double thisDistance = distance(el, p);
                if (thisDistance <= 3*range / 2) {
                    if (thisDistance <= range) {//Update el's metadata
                        addNeighbor(el, p);
                        if (newPoint) {
                            addNeighbor(p, el);//如果该点是新数据，则更新状态中的 pd点的 状态
                        } else {
                            if (reinsert.contains(p.getId())) {
                                //如果不是新数据到来
                                //如果pd是解散后微簇中已有的点，则 更新该 pd 的状态
                                addNeighbor(p, el);
                            }
                        }
                    }
                    if (thisDistance <= range / 2) {
                        NC.add(p);
                    } else {
                        NNC.add(p);
                    }
                }
            });
            // 产生新的微簇
            if (NC.size() >= k){
                createMC(el, NC, NNC);
            }else {
                //Insert in PD
                Iterator<Map.Entry<Integer, Double>> entryIterator = closeMCs.entrySet().iterator();
                while (entryIterator.hasNext()){
                    el.setRmc(entryIterator.next().getKey());
                }

                HashMap<Integer, MicroCluster> closeHashMap = new HashMap<>();
                //对 state 中的微簇进行过滤，拿到其中 MCs on 3/2R
                HashMap<Integer, MicroCluster> microClusterHashMap = state.getMc();
                for (Map.Entry<Integer, MicroCluster> entry : microClusterHashMap.entrySet()){
                    if (closeMCs.containsKey(entry.getKey())){
                        closeHashMap.put(entry.getKey(), entry.getValue());
                    }
                }

                Iterator<Integer> keyIter = closeHashMap.keySet().iterator();
                // 对过滤后的微簇进行遍历，计算簇中每一点与插入点的距离，当距离＜range，则更新插入点，最后将插入点放入 pd
                while (keyIter.hasNext()){
                    Integer key = keyIter.next();
                    MicroCluster cluster = closeHashMap.get(key);
                    cluster.getPoints().forEach(p -> {
                        Double thisDistance = distance(el, p);
                        if (thisDistance < range){
                            addNeighbor(el, p);
                        }
                    });
                }
                state.getPd().put(el.getId(), el);

            }
        }

    }

    private void createMC(Data el, ArrayList<Data> NC, ArrayList<Data> NNC) throws IOException {
        NC.forEach(p -> {
            p.clear(mc_counter);
            try {
                state.value().getPd().remove(p.getId());
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        el.clear(mc_counter);
        NC.add(el);
        MicroCluster newMC = new MicroCluster(el.getValue(), NC);//以 el 为中心创建新的微簇
        state.value().getMc().put(mc_counter, newMC);
        NNC.forEach(p -> {
            p.getRmc().add(mc_counter);
        });
        mc_counter += 1;
    }

    private void insertToMC(Data newData, Integer mc, boolean update, ArrayList<Integer> reinsert) throws IOException {
        newData.clear(mc);
        MicroCluster cluster = state.value().getMc().get(mc);
//        加入到了当前的微簇
        cluster.getPoints().add(newData);
        //            拿出当前状态的潜在异常值
        HashMap<Integer, Data> pds = state.value().getPd();
        if (update){
//            过滤出潜在异常值包含在微簇中的点
            List<Map.Entry<Integer, Data>> collect = pds.entrySet().stream().filter(pd -> pd.getValue().getRmc().contains(mc)).collect(Collectors.toList());
//            如果新数据点与潜在异常点距离小于阈值，则更新潜在异常值的元数据
            collect.forEach(
                    map -> {
                        if (distance(map.getValue(), newData) <= range ){
                            addNeighbor(map.getValue(), newData);
                        }
                    }
            );

        }else{
            List<Map.Entry<Integer, Data>> collect = pds.entrySet().stream()
                    .filter(pd -> pd.getValue().getRmc().contains(mc) && reinsert.contains(pd.getValue().getId()))
                    .collect(Collectors.toList());
            collect.forEach(
                    map -> {
                        if (distance(map.getValue(), newData) <= range ){
                            addNeighbor(map.getValue(), newData);
                        }
                    }
            );
        }
    }

    private void addNeighbor(Data p, Data newData) {

        if ( p.getArrival() > newData.getArrival()){
            p.insert_nn_before(newData.getArrival(), k);
        }else{
            p.setCount_after(1);
            if (p.getCount_after() >= k){
                p.setSafe_inlier(true);
            }
        }
    }

    /**
     *
     * @param newData
     * @return 返回数据点与每个微簇的距离 hashMap（微簇号，距离）
     * @throws IOException
     */
    private HashMap<Integer, Double> findCloseMCs(Data newData) throws IOException {
        HashMap<Integer, Double> res = new HashMap<>();
//        拿到当前状态中的微簇HashMap<微簇号, MicroCluster>
        HashMap<Integer, MicroCluster> mcMap = state.value().getMc();
//        计算数据与每一个微簇的距离
        for (Map.Entry<Integer, MicroCluster> entry : mcMap.entrySet()){
            MicroCluster microCluster = entry.getValue();
            Double distance = distance(newData, microCluster);
            if (distance <= (3*range)/2){
                res.put(entry.getKey(), distance);
            }
        }
        return res;
    }

}
