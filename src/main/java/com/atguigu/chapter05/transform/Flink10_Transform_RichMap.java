package com.atguigu.chapter05.transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05.transform
 * @since 2021/11/9 10:02
 */
public class Flink10_Transform_RichMap {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        DataStreamSource<Integer> DS = env.fromElements(1, 2, 3, 4);
        DS.map(new RichMapFunction<Integer, Integer>() {
                    // open方法在每次代码执行的时候只会调用一次
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 创建数据库的连接
                    }

                    // close方法在每次代码关闭的时候才会调用 并且只调用一次
                    @Override
                    public void close() throws Exception {
                        // 归还数据库连接 或者说 关闭各种资源
                        super.close();
                    }

                    @Override
                    public Integer map(Integer value) throws Exception {
                        // 使用数据库连接
                        return value * value;
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}