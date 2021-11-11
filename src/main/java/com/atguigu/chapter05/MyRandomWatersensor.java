package com.atguigu.chapter05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter05
 * @since 2021/11/8 11:35
 */
public class MyRandomWatersensor implements SourceFunction<WaterSensor> {

    boolean running =true;
    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        Random random = new Random();
        while (running) {
            ctx.collect(new WaterSensor(
                    "sensor" + random.nextInt(50),
                    Calendar.getInstance().getTimeInMillis(),
                    random.nextInt(100)
            ));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

        running = false;
    }
}