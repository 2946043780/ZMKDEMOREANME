package com.reatime;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @Package PACKAGE_NAME.FlinkCDC
 * @Author zhoumingkai
 * @Date 2025/5/12 15:35
 * @description: 获取mysql表数据
 */
public class FlinkCDCMySQL {
    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        prop.put("decimal.handling.mode", "string");
        prop.put("connect.decimal.precision", "16");
        prop.put("connect.decimal.scale", "2");
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 2.设置并行度
        env.setParallelism(1);
        // enable checkpoint
        env.enableCheckpointing(3000);
        //TODO 3.使用FlinkCDC读取MySQL表中的数据
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("cdh03")
                .port(3306)
                .databaseList("realtime_v1") // set captured database
                .tableList("realtime_v1.*") // set captured table
                .username("root")
                .password("root")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.earliest())
                .includeSchemaChanges(true)
                .debeziumProperties(prop)
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("cdh01:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("disk_data")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .build();

        DataStreamSource<String> mySQL_source = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        mySQL_source.sinkTo(sink);
        mySQL_source.print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
