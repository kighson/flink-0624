package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import com.atguigu.chapter05.source.MyRandomWatersensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05.transform
 * @since 2021/11/8 14:49
 */
public class Flink02_Transform_Filter {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> DS = env.fromElements(1, 2, 3, 4);

        DS.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {

                return value % 2 != 0;

            }
        }).print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}