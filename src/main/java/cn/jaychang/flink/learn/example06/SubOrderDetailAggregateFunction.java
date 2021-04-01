package cn.jaychang.flink.learn.example06;

import cn.jaychang.flink.learn.common.model.SubOrderDetail;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import java.math.BigDecimal;
import java.util.Objects;

public class SubOrderDetailAggregateFunction implements AggregateFunction<SubOrderDetail, OrderAggregation, OrderAggregationResult> {
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

        // 用于检验
        orderAggregation.getDateTimes().add(DateFormatUtils.format(subOrderDetail.getTimestamp(),"yyyy-MM-dd HH:mm:ss"));
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
        result.setTotalQuantity(orderAggregation.getTotalQuantity());
        result.setGmv(orderAggregation.getGmv());
        result.setDateTimes(orderAggregation.getDateTimes());
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
        acc1.getDateTimes().addAll(acc2.getDateTimes());
        return acc1;
    }
}
