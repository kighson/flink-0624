package com.atguigu.chapter05.sink;

import com.atguigu.bean.WaterSensor;
import com.atguigu.chapter05.source.MyRandomWatersensor;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05.sink
 * @since 2021/11/9 10:19
 */
public class Flink01_Sink_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<WaterSensor> input = env.addSource(new MyRandomWatersensor());
        SingleOutputStreamOperator<String> map = input.map(WaterSensor::toString);

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(
                        new Path("./output"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                // 文件在必须滚动之前可以保持打开的最长时间 也就是多久滚动一次文件
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                // 允许的不活动间隔，在此间隔后文件必须滚动
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                // 设置最大的文件大小
                                .withMaxPartSize(1024 * 2)
                                .build())
                .build();

        map.addSink(sink);

        env.execute();
    }
}