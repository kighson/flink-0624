package com.atguigu.chapter02;

import com.atguigu.chapter05.MyRandomWatersensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05
 * @since 2021/11/8 11:33
 */
public class Flink04_Source_Custom {
    public static void main(String[] args) {
         Configuration conf = new Configuration();
                 conf.setInteger("rest.port", 10000);
                 StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
                 env.setParallelism(1);


        env.addSource(new MyRandomWatersensor()).print();


                 try {
                     env.execute();
                 } catch (Exception e) {
                     e.printStackTrace();
                 }
    }
}