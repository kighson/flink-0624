package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter02
 * @since 2021/11/6 10:37
 */
public class Flink03_UnBounded_Stream_Wordcount {
    // 用流处理无界数据
    // 本次无界数据来自于socket

    public static void main(String[] args) throws Exception {
        // 1. 创建一个流式处理执行环境
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(5);


        System.out.println("*************************Application mode*****************************");
        //2. 获取有界数据
        DataStreamSource<String> wordDSS = env.socketTextStream("localhost", 9999);
        //3. 做转换
        wordDSS
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String line, Collector<String> out) throws Exception {
                        String[] words = line.split(" ");

                        for (String word : words) {
                            out.collect(word);
                        }
                    }
                })
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                }).setParallelism(8)
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1).setParallelism(9)
                .print().setParallelism(5);


        env.execute();
    }
}