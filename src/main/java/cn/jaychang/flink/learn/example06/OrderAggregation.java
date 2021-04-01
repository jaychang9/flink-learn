package cn.jaychang.flink.learn.example06;

import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.shaded.guava18.com.google.common.base.MoreObjects;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Set;
@Data
public class OrderAggregation implements Serializable {
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

    // 用于检验
    private List<String> dateTimes = Lists.newArrayList();

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