package cn.jaychang.flink.learn.example06;

import cn.jaychang.flink.learn.common.model.SubOrderDetail;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

import static cn.jaychang.flink.learn.example06.KafkaSubOrderDetailUtil.ORDER_EXT_TOPIC_NAME;

/**
 * 电商大屏实战案例
 */
public class CustomAggregationExample01 {



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

        // 按照自然日来统计以下指标，并以1秒的刷新频率呈现在大屏上
        WindowedStream<SubOrderDetail, Tuple1<Long>, TimeWindow> site30SecondsWindowStream = dataStream2.keyBy(value -> Tuple1.of(value.getSiteId()), Types.TUPLE(Types.LONG))
                .window(TumblingProcessingTimeWindows.of(Time.days(1L), Time.hours(-8))).trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(1L)));


        SingleOutputStreamOperator<OrderAggregationResult> siteAggStream = site30SecondsWindowStream.aggregate(new SubOrderDetailAggregateFunction())
                .name("aggregate_site_order_gmv").uid("aggregate_site_order_gmv");

        siteAggStream.print();

        env.execute("CustomAggregationExample01");
    }








}
