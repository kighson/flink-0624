package com.atguigu.chapter05;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05
 * @since 2021/11/8 11:17
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        // 创建kafka的配置类
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty("group.id", "Flink03_Source_Kafka");
        properties.setProperty("auto.offset.reset", "latest");

        env.addSource(new FlinkKafkaConsumer<String>(
                "sensor",
                new SimpleStringSchema(StandardCharsets.UTF_8),
                properties)).print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}