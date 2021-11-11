package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter02
 * @since 2021/11/6 10:09
 */
public class Flink01_Bounded_Batch_Wordcount {
    public static void main(String[] args) throws Exception {

        //1 .创建批处理执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        System.out.println(env.getExecutionPlan());
        //2. 使用执行环境来获取数据源
        DataSource<String> wordDS = env.readTextFile("input/words.txt");

        wordDS
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
                })
                .groupBy(0)
                .sum(1)
                .print();


    }
}