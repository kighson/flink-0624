package com.atguigu.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
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
 * @since 2021/11/6 10:37
 */
public class Flink03_UnBounded_Stream_Wordcount_Lambda {
    // 用流处理无界数据
    // 本次无界数据来自于socket

    public static void main(String[] args) throws Exception {
        // 1. 创建一个流式处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2. 获取有界数据
        DataStreamSource<String> wordDSS = env.socketTextStream("hadoop102", 9999);
        //3. 做转换
        wordDSS
                // lambda表达式的写法：  (参数列表) -> {代码逻辑}
                // 泛型擦除 当我们返回的数据类型涉及到 泛型中套泛型的情况
                // 那么内部的泛型信息会被编译器抹掉 编译器无法知道我们具体的返回值类型 这时候需要我们显式地指定返回值的泛型信息
                /**
                 * 对于泛型擦除，可以有以下解决办法
                 * 1. 使用return来显式指定返回的泛型信息
                 * 2. 使用匿名内部类
                 * 3. 使用外部类
                 */
                .flatMap((FlatMapFunction<String, String>) (line, out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(word);
                    }
                })
                .returns(Types.STRING)

                .map((MapFunction<String, Tuple2<String, Integer>>) word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING,Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) value -> value.f0)


                .sum(1)
                .print();


        env.execute();
    }
}