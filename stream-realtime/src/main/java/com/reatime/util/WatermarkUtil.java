package com.reatime.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/**
 * @Package utils.WatermarkUtil
 * @Author guo.jia.hui
 * @Date 2025/5/15 21:10
 * @description:
 */
public class WatermarkUtil {
    public static WatermarkStrategy<String> WatermarkStrategy() {
        return WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event, timestamp) -> {
                            JSONObject jsonObject = JSONObject.parseObject(event);
                            if (event != null && jsonObject.containsKey("ts_ms")) {
                                try {
                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                    return 0L;
                                }
                            }
                            return 0L;
                        }
                );
    }
}