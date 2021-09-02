package cn.jaychang.flink.learn.example06;

import cn.jaychang.flink.learn.common.model.SubOrderDetail;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.time.Duration;
import java.util.Date;
import java.util.Properties;

import static cn.jaychang.flink.learn.example06.KafkaSubOrderDetailUtil.ORDER_EXT_TOPIC_NAME;

/**
 * 电商大屏实战案例
 */
public class CustomAggregationExample02 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        SingleOutputStreamOperator<SubOrderDetail> dataStream2 = dataStream1.map(x -> JSON.parseObject(x, SubOrderDetail.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SubOrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<SubOrderDetail>() {
                                    //提取SensorRecord中时间戳
                                    @Override
                                    public long extractTimestamp(SubOrderDetail element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                );


        // 按照自然日来统计以下指标，并以5秒的刷新频率呈现在大屏上
        WindowedStream<SubOrderDetail, Tuple1<Long>, TimeWindow> site30SecondsWindowStream = dataStream2.keyBy(value -> Tuple1.of(value.getSiteId()), Types.TUPLE(Types.LONG))
               // .window(TumblingEventTimeWindows.of(Time.seconds(15L)));
            .window(TumblingEventTimeWindows.of(Time.days(1L))).trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5L)));

        SingleOutputStreamOperator<OrderAggregationResult> siteAggStream = site30SecondsWindowStream.aggregate(new SubOrderDetailAggregateFunction())
                .name("aggregate_site_order_gmv").uid("aggregate_site_order_gmv");

        //siteAggStream.process(new OutputOrderGmvProcessFunc()).name("process_site_gmv_changed").uid("process_site_gmv_changed");

        // 该Sink用于打印
        siteAggStream.addSink(new SinkFunction<OrderAggregationResult>() {
            @Override
            public void invoke(OrderAggregationResult value, Context context) throws Exception {
                System.out.println(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss") + " " + JSON.toJSONString(value));
            }
        });

        // 该Sink用于将结果存redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("10.1.80.70").setDatabase(4).build();
        siteAggStream.addSink(new RedisSink<>(conf, new RedisExampleMapper()));


        env.execute("CustomAggregationExample02");
    }


    static class RedisExampleMapper implements RedisMapper<OrderAggregationResult> {
        // REDIS KEY名前缀
        private static final String  ORDER_AGGREGATION_RESULT_KEY_PREFIX = "ORDER_AGGREGATION_RESULT:";

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(OrderAggregationResult orderAggregationResult) {
            return ORDER_AGGREGATION_RESULT_KEY_PREFIX+orderAggregationResult.getSiteId();
        }

        @Override
        public String getValueFromData(OrderAggregationResult orderAggregationResult) {
            return JSON.toJSONString(orderAggregationResult);
        }
    }


}
