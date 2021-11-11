package com.atguigu.chapter05.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05
 * @since 2021/11/8 11:02
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        // DataStreamSource<Integer> DS1 = env.fromElements(1, 2, 3, 4);

        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("sensor_1", 1633792000L, 10),
                new WaterSensor("sensor_2", 1633794000L, 50),
                new WaterSensor("sensor_3", 1633796000L, 40)
        );

        env.fromCollection(waterSensors).print();

        // DS1.print();





        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}