package com.leejean.demoCases;

import com.leejean.bean.OrderEvent;
import com.leejean.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class OrderPayRealSurviliant {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        SingleOutputStreamOperator<OrderEvent> orderDS = env.readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new OrderEvent(
                                Long.valueOf(datas[0]),
                                datas[1],
                                datas[2],
                                Long.valueOf(datas[3])
                        );
                    }
                });

        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new TxEvent(
                                datas[0],
                                datas[1],
                                Long.valueOf(datas[2])
                        );
                    }
                });

        SingleOutputStreamOperator<String> processedResult = orderDS.connect(txDS)

//      todo          多并行度需要将相同的 key 放在一个
                .keyBy(order -> order.getTxId(), tx -> tx.getTxId())
                
                .process(new OrderTxDetectioin<OrderEvent, TxEvent, String>());

        processedResult.print();

        env.execute();
    }

    public static class OrderTxDetectioin<O, T, S> extends CoProcessFunction<OrderEvent, TxEvent, String>{

        Map<String, OrderEvent> orderMap = new HashMap<>();
        Map<String, TxEvent> txMap = new HashMap<>();

        @Override
        public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
//            从 order 中获取交易 ID
//            根据交易 ID，查找交易事件是否存在
            TxEvent txEvent = txMap.get(value.getTxId());
//            如果交易事件为空，则该条交易 ID 所对应的order，放入 order map 中
            if (txEvent == null){
                orderMap.put(value.getTxId(), value);
            }else{
//                如果交易数据已经有了，则进行业务处理
                out.collect("订单["+value.getOrderId()+"]对账成功");
//                处理完毕将删除交易 ID 对应的交易数据
                txMap.remove(value.getTxId());
            }
        }

        @Override
        public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {

            OrderEvent orderEvent = orderMap.get(value.getTxId());
            if (orderEvent == null){
                txMap.put(value.getTxId(), value);
            }else{
                out.collect("订单["+orderEvent.getOrderId()+"]对账成功");
                orderMap.remove(value.getTxId());
            }
        }
    }



}
