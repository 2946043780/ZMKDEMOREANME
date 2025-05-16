package com.reatime.learn_zmk_flink;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Package PACKAGE_NAME.learn_flink.BroadCast
 * @Author guo.jia.hui
 * @Date 2025/5/15 13:45
 * @description: 广播流
 */
public class BroadCast {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> orderStream = env.fromElements("P123,iPhone 13,5999.00", "P456,小米电视,3299.00");
        DataStreamSource<String> discountRuleStream = env.fromElements("P123:0.9", "P456:0.8");

        MapStateDescriptor<String, Double> discountRuleDescriptor = new MapStateDescriptor<>(
                "ProductState",
                Types.STRING,
                Types.DOUBLE);
        BroadcastStream<String> broadcastRules = discountRuleStream.broadcast(discountRuleDescriptor);

        SingleOutputStreamOperator<String> adjustedOrders = orderStream.connect(broadcastRules).process(new PriceAdjuster());
        adjustedOrders.print();

        env.execute();
    }

    private static class PriceAdjuster extends BroadcastProcessFunction<String, String, String> {
        MapStateDescriptor<String, Double> discountRuleDescriptor = new MapStateDescriptor<>(
                "ProductState",
                Types.STRING,
                Types.DOUBLE);

        @Override
        public void processElement(String order, BroadcastProcessFunction<String, String, String>.ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {
            String[] parts = order.split(",");
            String productId = parts[0];
            String productName = parts[1];
            double originalPrice = Double.parseDouble(parts[2]);

            Double discountRate = readOnlyContext.getBroadcastState(discountRuleDescriptor).get(productId);
            discountRate = discountRate == null ? 1.0 : discountRate;

            double adjustedPrice = originalPrice * discountRate;
            collector.collect(String.format("%s,%s,%.2f,%.2f,%.2f",
                    productId, productName, originalPrice, discountRate, adjustedPrice));
        }

        @Override
        public void processBroadcastElement(String rule, BroadcastProcessFunction<String, String, String>.Context context, Collector<String> collector) throws Exception {
            System.out.println("更新价格规则: " + rule);
            String[] parts = rule.split(":");
            String productId = parts[0];
            double discount = Double.parseDouble(parts[1]);

            context.getBroadcastState(discountRuleDescriptor).put(productId, discount);
        }
    }
}