package com.atguigu.chapter05.sink;

import com.atguigu.chapter05.source.MyRandomWatersensor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05.sink
 * @since 2021/11/9 10:28
 */
public class Flink02_Sink_Kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.addSource(new MyRandomWatersensor())
                .map(value -> value.toString())
                .addSink(new FlinkKafkaProducer<String>(
                        "hadoop102:9092,hadoop103:9092,hadoop104:9092,",
                        "sensor",
                        new SimpleStringSchema()));

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}