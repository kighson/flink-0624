package com.atguigu.chapter05.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05.transform
 * @since 2021/11/9 9:01
 */
public class Flink07_Transform_Process {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 注意当我们设置多个并行度时候，求和发生在每个并行度中
        env.setParallelism(2);
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        waterSensors.add(new WaterSensor("sensor_3", 1607527995000L, 30));
        DataStreamSource<WaterSensor> waterSensorDS = env.fromCollection(waterSensors);

        waterSensorDS.process(new ProcessFunction<WaterSensor, Integer>() {
                    Integer sum = 0;

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<Integer> out) throws Exception {
                        sum += value.getVc();
                        out.collect(sum);
                    }
                })
                .print();

        env.execute("process");

    }
}