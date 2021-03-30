package cn.jaychang.flink.learn.example06;

import cn.jaychang.flink.learn.common.model.Student;
import cn.jaychang.flink.learn.common.model.SubOrderDetail;
import com.alibaba.fastjson.JSON;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaSubOrderDetailUtil {
    private static final String brokerList = "10.1.80.213:9092";
    private static final String topic = "test";


    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Map<Long,String> cities = new HashMap<Long,String>(){
            {
                put(1L,"北京市");
                put(2L,"上海市");
                put(3L,"广州市");
                put(4L,"深圳市");
            }
        };

        Map<Long,Double> merchandises = new HashMap<Long,Double>(){
            {
                put(1L,1.00D);
                put(2L,2.00D);
                put(3L,3.00D);
                put(4L,4.00D);
            }
        };

        while (true) {
            for (int i = 1; i <= 2; i++) {
                Long siteId = RandomUtils.nextLong(97L, 100L);
                Long cityId = RandomUtils.nextLong(1L,5L);
                Long merchandiseId = RandomUtils.nextLong(1L,5L);
                SubOrderDetail subOrderDetail = new SubOrderDetail()
                        .setOrderId(RandomUtils.nextLong(1000,1026))
                        .setSubOrderId(System.currentTimeMillis())
                        .setIsNewOrder(0 == RandomUtils.nextInt(0,1))
                        .setSiteId(siteId)
                        .setSiteName("站点-"+siteId)
                        .setOrderStatus(RandomUtils.nextInt(0,2))
                        .setCityId(cityId).setCityName(cities.get(cityId))
                        .setTimestamp(System.currentTimeMillis())
                        .setUserId(RandomUtils.nextLong(1001L,1010L))
                        .setMerchandiseId(merchandiseId)
                        .setPrice(merchandises.get(merchandiseId))
                        .setQuantity(RandomUtils.nextInt(1,10));

                ProducerRecord record = new ProducerRecord<String, String>(ECommenceBigScreenExample.ORDER_EXT_TOPIC_NAME, null, null, JSON.toJSONString(subOrderDetail));
                producer.send(record);
                System.out.println("发送数据: " + JSON.toJSONString(subOrderDetail));
            }
            producer.flush();
            try {
                Thread.sleep(15000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
