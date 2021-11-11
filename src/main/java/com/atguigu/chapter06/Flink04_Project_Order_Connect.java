package com.atguigu.chapter06;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter06
 * @since 2021/11/9 14:14
 */
public class    Flink04_Project_Order_Connect {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10004);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        SingleOutputStreamOperator<OrderEvent> order = env.readTextFile("input/OrderLog.csv").map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.valueOf(split[0]),
                        split[1],
                        split[2],
                        Long.valueOf(split[3]));
            }
        });
        SingleOutputStreamOperator<TxEvent> tx = env.readTextFile("input/ReceiptLog.csv").map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.valueOf(split[2])
                );
            }
        });

        ConnectedStreams<OrderEvent, TxEvent> orderAndTxConnect = order.connect(tx);

        orderAndTxConnect
                .keyBy("txId", "txId")
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    HashMap<String, OrderEvent> orderMap = new HashMap<>();
                    HashMap<String, TxEvent> txMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent orderEvent,
                                                Context ctx,
                                                Collector<String> out) throws Exception {
                        if (txMap.containsKey(orderEvent.getTxId())) {
                            out.collect("对账成功！,txid为"+orderEvent.getTxId());
                            txMap.remove(orderEvent.getTxId());
                        } else {
                            orderMap.put(orderEvent.getTxId(), orderEvent);
                        }
                    }

                    @Override
                    public void processElement2(TxEvent txEvent,
                                                Context ctx,
                                                Collector<String> out) throws Exception {

                        if (orderMap.containsKey(txEvent.getTxId())) {
                            out.collect("对账成功！,txid为"+txEvent.getTxId());
                            orderMap.remove(txEvent.getTxId());
                        } else {
                            txMap.put(txEvent.getTxId(), txEvent);
                        }
                    }
                }).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}