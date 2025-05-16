package com.reatime.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package utils.KafkaUtil
 * @Author zhoumingkai
 * @Date 2025/5/13 16:20
 * @description: kafka工具类
 */
public class KafkaUtil {
    public static KafkaSink<String> getKafkaProduct(String servers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(servers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();
    }

    public static DataStreamSource<String> getKafkaConsumer(StreamExecutionEnvironment env, String servers, String topic) {
        // 配置 KafkaSource
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(servers)
                .setTopics(topic)
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 将 KafkaSource 添加到作业
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
    }
}
