package cn.jaychang.flink.learn.example06;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class OutputOrderGmvProcessFunc extends KeyedProcessFunction<Tuple1, OrderAggregationResult, Tuple2<Long, String>> implements Serializable {
    private static final long serialVersionUID = 1L;

    private MapState<Long, OrderAggregationResult> state;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        state = this.getRuntimeContext().getMapState(new MapStateDescriptor<>("state_site_order_gmv", Long.class, OrderAggregationResult.class));
    }

    @Override
    public void processElement(OrderAggregationResult value, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
        long siteId = value.getSiteId();
        OrderAggregationResult cachedValue = state.get(siteId);

        if (cachedValue == null || value.getSubOrderCount() != cachedValue.getSubOrderCount()) {
            JSONObject result = new JSONObject();
            result.put("siteId", value.getSiteId());
            result.put("siteName", value.getSiteName());
            result.put("totalQuantity", value.getTotalQuantity());
            result.put("totalOrderCount", value.getTotalOrderCount());
            result.put("subOrderCount", value.getSubOrderCount());
            result.put("gmv", value.getGmv());
            out.collect(new Tuple2<>(siteId, result.toJSONString()));
            state.put(siteId, value);
        }
    }

    @Override
    public void close() throws Exception {
        state.clear();
        super.close();
    }
}