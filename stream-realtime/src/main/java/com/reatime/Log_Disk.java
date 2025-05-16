package com.reatime;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.reatime.util.Config;
import com.reatime.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * @Package PACKAGE_NAME.Log_Disk
 * @Author zhoumingkai
 * @Date 2025/5/13 16:18
 * @description: 处理日志数据
 */

/**
 这段代码是一个使用 Apache Flink 处理
 Kafka 日志数据的应用程序，主要功能是从 Kafka
 读取日志数据，进行解析、转换和时间戳处理，最终输出处理后的结果
 */
public class Log_Disk {
    private static final Logger LOG = LoggerFactory.getLogger(Log_Disk.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 获取 Kafka 数据源
        //通过 KafkaUtil 工具类连接 Kafka，分别获取两个主题（topic_log 和 disk_data）的数据流
        DataStreamSource<String> topic_log = KafkaUtil.getKafkaConsumer(env, Config.KAFKA_BOOT_SERVER, "topic_log");
        DataStreamSource<String> disk = KafkaUtil.getKafkaConsumer(env, Config.KAFKA_BOOT_SERVER, "disk_data");

        // 解析 JSON 并处理空值
        //将 Kafka 中的字符串消息解析为 JSON 对象。
        //捕获解析异常并记录错误日志，确保无效数据不会继续处理。
        SingleOutputStreamOperator<JSONObject> Topic_log_json = topic_log.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                try {
                    return JSONObject.parseObject(s);
                } catch (Exception e) {
                    LOG.error("解析 topic_log 数据失败: {}", s, e);
                    throw new RuntimeException("Invalid JSON in topic_log", e);
                }
            }
        });

        SingleOutputStreamOperator<JSONObject> disk_json = disk.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                try {
                    return JSON.parseObject(s);
                } catch (Exception e) {
                    LOG.error("解析 disk_data 数据失败: {}", s, e);
                    throw new RuntimeException("Invalid JSON in disk_data", e);
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> {
                    // 安全获取 ts 字段，处理 null 和缺失情况
                    Long ts = event.getLong("ts");
                    if (ts == null) {
                        LOG.warn("'ts' field is null in event: {}", event);
                        return System.currentTimeMillis(); // 使用当前时间作为默认值
                    }
                    return ts;
                }));
        disk_json.print("disk_data 解析后");

        SingleOutputStreamOperator<JSONObject> process = Topic_log_json.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject common = jsonObject.getJSONObject("common");
                    if (common == null) {
                        LOG.warn("Missing 'common' field in JSON: {}", jsonObject);
                        return;
                    }

                    String mid = common.getString("mid");
                    String uid = common.getString("uid");
                    Long ts = jsonObject.getLong("ts"); // 可能为 null

                    JSONObject log_disk = new JSONObject();
                    log_disk.put("mid", mid);
                    if (uid != null) {
                        log_disk.put("uid", uid);
                    }
                    log_disk.put("ts", ts != null ? ts : System.currentTimeMillis()); // 处理 ts 为 null 的情况
                    collector.collect(log_disk);
                } catch (Exception e) {
                    LOG.error("Processing failed for event: {}", jsonObject, e);
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                .withTimestampAssigner((event, timestamp) -> {
                    Long ts = event.getLong("ts");
                    return ts != null ? ts : System.currentTimeMillis(); // 再次确保 ts 非 null
                }));
        process.print("数据处理结果");

        env.execute("Log Disk Processing");
    }
}