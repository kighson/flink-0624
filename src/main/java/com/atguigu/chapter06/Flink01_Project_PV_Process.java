package com.atguigu.chapter06;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter06
 * @since 2021/11/9 14:14
 */
public class Flink01_Project_PV_Process {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10001);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new UserBehavior(

                                Long.valueOf(data[0]),
                                Long.valueOf(data[1]),
                                Integer.valueOf(data[2]),
                                data[3],
                                Long.valueOf(data[4])
                        );
                    }
                })
                .keyBy(UserBehavior::getBehavior)
                .process(new KeyedProcessFunction<String, UserBehavior, Long>() {
                    Long sum = 0L;

                    @Override
                    public void processElement(UserBehavior value,
                                               Context ctx,
                                               Collector<Long> out) throws Exception {

                        if ("pv".equals(value.getBehavior())) {
                            sum++;
                            out.collect(sum);
                        }
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