package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import com.atguigu.chapter05.source.MyRandomWatersensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05.sink
 * @since 2021/11/9 11:20
 */
public class Flink04_Sink_MySQL {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10002);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.addSource(new MyRandomWatersensor())

                .addSink(JdbcSink.sink(
                        "replace into sensor (`id`,`ts`,`vc`) values (?,?,?)",
                        new JdbcStatementBuilder<WaterSensor>() {
                            @Override
                            public void accept(PreparedStatement ps, WaterSensor ws) throws SQLException {
                                ps.setString(1, ws.getId());
                                ps.setLong(2, ws.getTs());
                                ps.setInt(3, ws.getVc());
                            }
                        },
                        JdbcExecutionOptions
                                .builder()
                                .withBatchIntervalMs(1000)
                                .withBatchSize(1024 * 1024)
                                .withMaxRetries(5).build()
                        , new JdbcConnectionOptions
                                .JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://118.195.133.78:3306/test_database?useSSL=false")
                                .withUsername("test_database")
                                .withPassword("123456")
                                .withDriverName("com.mysql.jdbc.Driver")
                                .build()
                ));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}