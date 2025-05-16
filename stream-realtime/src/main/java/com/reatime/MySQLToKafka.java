package com.reatime;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Package PACKAGE_NAME.Damo_Disk
 * @Author zhoumingkai
 * @Date 2025/5/12 22:20
 * @description:
 */
public class MySQLToKafka {
    public static void main(String[] args) throws Exception {
        // 1. 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置 KafkaSource
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
                .setTopics("disk_data")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. 将 KafkaSource 添加到作业
        DataStreamSource<String> disk = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        SingleOutputStreamOperator<JSONObject> disk_json = disk.map((MapFunction<String, JSONObject>) JSON::parseObject);
        disk_json.print("数据源数据");

        OutputTag<String> user_info_sup_msg = new OutputTag<String>("user_info_sup_msg") {
        };
        SingleOutputStreamOperator<String> process = disk_json.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) {
                String table = jsonObject.getJSONObject("source").getString("table");
                if (table != null && table.equals("user_info")) {
                    collector.collect(jsonObject.toJSONString());
                } else {
                    context.output(user_info_sup_msg, jsonObject.toString());
                }
            }
        });
        SideOutputDataStream<String> userinfo_sup_msg = process.getSideOutput(user_info_sup_msg);
        process.print("主流数据");
        userinfo_sup_msg.print("测流数据");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("user_info")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        process.sinkTo(sink);

        KafkaSink<String> sink2 = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092,cdh02:9092,cdh03:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("user_info_sup_msg")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
        userinfo_sup_msg.sinkTo(sink2);

        // 4. 执行作业
        env.execute("Flink Kafka Consumer");
    }
}