package cn.jaychang.flink.learn.example06;

import cn.jaychang.flink.learn.common.model.SubOrderDetail;
import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.ToString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.base.MoreObjects;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

/**
 * 电商大屏实战案例
 */
public class ECommenceBigScreenExample {

    public static final String ORDER_EXT_TOPIC_NAME = "test-example06";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(0L);
        env.enableCheckpointing(60 * 1000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(30 * 1000);

        Properties props = new Properties();
        props.put("bootstrap.servers", "10.1.80.213:9092");
        props.put("zookeeper.connect", "10.1.80.213:2181");
        props.put("group.id", "example06-group");
        props.put("key.deserializer", "org.apache.flink.api.common.serialization.SimpleStringSchema");
        props.put("value.deserializer", "org.apache.flink.api.common.serialization.SimpleStringSchema");
        props.put("auto.offset.reset", "latest");
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(ORDER_EXT_TOPIC_NAME, new SimpleStringSchema(), props);

        SingleOutputStreamOperator<String> dataStream1 = env.addSource(flinkKafkaConsumer).setParallelism(1).name("source_kafka_" + ORDER_EXT_TOPIC_NAME)
                .uid("source_kafka_" + ORDER_EXT_TOPIC_NAME);

        SingleOutputStreamOperator<SubOrderDetail> dataStream2 = dataStream1.map(x -> JSON.parseObject(x, SubOrderDetail.class));

        // 按照自然日来统计以下指标，并以10秒的刷新频率呈现在大屏上
        WindowedStream<SubOrderDetail, Tuple1<Long>, TimeWindow> site30SecondsWindowStream = dataStream2.keyBy(value -> Tuple1.of(value.getSiteId()), Types.TUPLE(Types.LONG))
                .window(TumblingProcessingTimeWindows.of(Time.days(1L), Time.hours(-8))).trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10L)));

//        WindowedStream<SubOrderDetail, Tuple, TimeWindow> site30SecondsWindowStream = dataStream2.keyBy("siteId")
//                .window(TumblingProcessingTimeWindows.of(Time.days(1L), Time.hours(-8)))
//                //.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1L)));
//                // 10秒更新一次
//                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(10L)));


        SingleOutputStreamOperator<OrderAggregationResult> siteAggStream = site30SecondsWindowStream.aggregate(new SubOrderDetailAggregateFunction())
                .name("aggregate_site_order_gmv").uid("aggregate_site_order_gmv");

        siteAggStream.print();

        env.execute("ECommenceBigScreenExample");
    }


    static class SubOrderDetailAggregateFunction implements AggregateFunction<SubOrderDetail, OrderAggregation, OrderAggregationResult> {
        @Override
        public OrderAggregation createAccumulator() {
            OrderAggregation orderAggregation = new OrderAggregation();
            orderAggregation.setOrderIds(Sets.newHashSet());
            return orderAggregation;
        }

        @Override
        public OrderAggregation add(SubOrderDetail subOrderDetail, OrderAggregation orderAggregation) {
            if (Objects.isNull(orderAggregation.getSiteId())) {
                orderAggregation.setSiteId(subOrderDetail.getSiteId());
                orderAggregation.setSiteName(subOrderDetail.getSiteName());
            }
            orderAggregation.addOrderId(subOrderDetail.getOrderId());
            orderAggregation.addQuantity(subOrderDetail.getQuantity());
            orderAggregation.addSubOrderCount(1);
            double gmv = BigDecimal.valueOf(subOrderDetail.getPrice()).multiply(BigDecimal.valueOf(subOrderDetail.getQuantity())).doubleValue();
            orderAggregation.addGmv(gmv);
            return orderAggregation;
        }

        @Override
        public OrderAggregationResult getResult(OrderAggregation orderAggregation) {
            // 这里仅仅只是强调下，ACC和OUT可以相同类型，也可以不同类型
            OrderAggregationResult result = new OrderAggregationResult();
            result.setOrderIds(orderAggregation.getOrderIds());
            result.setSiteId(orderAggregation.getSiteId());
            result.setSiteName(orderAggregation.getSiteName());
            result.setTotalOrderCount(orderAggregation.getOrderIds().size());
            result.setSubOrderCount(orderAggregation.getSubOrderCount());
            result.setTotalQuantity(orderAggregation.totalQuantity);
            result.setGmv(orderAggregation.getGmv());
            return result;
        }

        @Override
        public OrderAggregation merge(OrderAggregation acc1, OrderAggregation acc2) {
            if (Objects.isNull(acc1.getSiteId())) {
                acc1.setSiteId(acc2.getSiteId());
                acc1.setSiteName(acc2.getSiteName());
            }
            acc1.addSubOrderCount(acc2.getSubOrderCount());
            acc1.addQuantity(acc2.getTotalQuantity());
            acc1.addGmv(acc2.getGmv());

            return acc1;
        }
    }


    @Data
    static class OrderAggregation implements Serializable {
        private Long siteId;

        private String siteName;

        /**
         * 订单集合
         */
        private Set<Long> orderIds;

        private Integer totalQuantity;

        /**
         * 子订单数量
         */
        private Integer subOrderCount;

        private Double gmv;

        public OrderAggregation addOrderId(Long orderId) {
            if (CollectionUtils.isEmpty(orderIds)) {
                this.orderIds = Sets.newHashSet();
            }
            this.orderIds.add(orderId);
            return this;
        }

        public OrderAggregation addQuantity(Integer quantity) {
            if (Objects.isNull(totalQuantity)) {
                totalQuantity = 0;
            }
            totalQuantity += quantity;
            return this;
        }

        public OrderAggregation addSubOrderCount(Integer subOrderCount) {
            this.subOrderCount = MoreObjects.firstNonNull(this.subOrderCount, 0);
            this.subOrderCount += subOrderCount;
            return this;
        }

        public OrderAggregation addGmv(Double gmv) {
            if (Objects.isNull(this.gmv)) {
                this.gmv = 0.00D;
            }
            this.gmv = BigDecimal.valueOf(this.gmv).add(BigDecimal.valueOf(gmv)).doubleValue();
            return this;
        }
    }

    @Data
    @ToString(callSuper = true)
    static class OrderAggregationResult extends OrderAggregation implements Serializable {
        private Integer totalOrderCount;

        @Override
        public String toString() {
            return JSON.toJSONString(this);
        }
    }
}
