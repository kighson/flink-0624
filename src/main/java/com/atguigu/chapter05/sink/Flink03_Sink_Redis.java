package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import com.atguigu.chapter05.source.MyRandomWatersensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05.sink
 * @since 2021/11/9 10:38
 */
public class Flink03_Sink_Redis {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.addSource(new MyRandomWatersensor())
                .addSink(new RedisSink<WaterSensor>(
                        new FlinkJedisPoolConfig.Builder()
                                .setHost("hfaliredis.redis.rds.aliyuncs.com")
                                .setPassword("atguigu:Atguigu123")
                                .setDatabase(0)
                                .setMaxTotal(10)
                                .setMaxIdle(10)
                                .setMinIdle(5)
                                .build(),
                        new RedisMapper<WaterSensor>() {
                            @Override
                            public RedisCommandDescription getCommandDescription() {
                                return new RedisCommandDescription(RedisCommand.HSET, "sensor");
                            }
                            @Override
                            public String getKeyFromData(WaterSensor data) {
                                return data.getId();
                            }
                            @Override
                            public String getValueFromData(WaterSensor data) {
                                return data.getVc()+"";
                            }
                        }));
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}