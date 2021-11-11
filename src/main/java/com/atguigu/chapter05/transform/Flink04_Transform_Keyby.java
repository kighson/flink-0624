package com.atguigu.chapter05.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05.transform
 * @since 2021/11/8 15:16
 */
public class Flink04_Transform_Keyby {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> DS = env.fromElements(1, 2, 3, 4);

        // KeyedStream<Integer, Integer> integerIntegerKeyedStream = DS.keyBy(new KeySelector<Integer, Integer>() {
        //     @Override
        //     public Integer getKey(Integer value) throws Exception {
        //         if (value % 2 == 0) {
        //             return 0;
        //         } else {
        //             return 1;
        //         }
        //     }
        // });

        KeyedStream<Integer, String> integerStringKeyedStream = DS.keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer value) throws Exception {
                if (value % 2 == 0) {
                    return "偶数";
                } else {
                    return "奇数";
                }
            }
        });

        // integerIntegerKeyedStream.print();
        integerStringKeyedStream.print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}