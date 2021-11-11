package com.atguigu.chapter05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05.transform
 * @since 2021/11/9 9:57
 */
public class Flink09_Transform_Union {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);
        DataStreamSource<Integer> DS1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Integer> DS2 = env.fromElements(55, 66, 77, 88);
        DataStreamSource<String> DS3 = env.fromElements("a", "b", "c");
        DataStreamSource<Integer> DS4 = env.fromElements(111, 222, 333, 444);

        DS1
                .union(DS2)
                .union(DS4)
                // 这里ds3的数据类型和钱三个不同，不能进行union
                // .union(DS3)
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}