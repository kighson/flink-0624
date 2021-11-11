package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter02
 * @since 2021/11/6 10:22
 */
public class Flink02_Bounded_Stream_Wordcount {
    public static void main(String[] args) throws Exception {
        // 1. 创建一个流式处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //2. 获取有界数据
        DataStreamSource<String> wordDSS = env.readTextFile("input/words.txt");
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
                }).setParallelism(2)
                .map(new MapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String word) throws Exception {
                        return Tuple2.of(word, 1);
                    }
                }).setParallelism(3)
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                })
                .sum(1)
                .print();

        System.out.println(env.getExecutionPlan());
        // 4. 启动我们的执行环境
        env.execute();

    }
}