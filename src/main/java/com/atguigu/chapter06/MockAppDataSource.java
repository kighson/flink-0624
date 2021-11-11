package com.atguigu.chapter06;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author Hefei
 * @description
 * @project_name flink-0624
 * @package_name com.atguigu.chapter06
 * @since 2021/11/9 15:33
 */
public class MockAppDataSource implements SourceFunction<MarketingUserBehavior> {
    boolean running = true;
    Random random = new Random();
    List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "tencent", "oppo", "vivo");
    List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");
    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        while (running) {
            MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                    (long) random.nextInt(500000),
                    behaviors.get(random.nextInt(behaviors.size())),
                    channels.get(random.nextInt(channels.size())),
                    System.currentTimeMillis());
            ctx.collect(marketingUserBehavior);
            Thread.sleep(200);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}