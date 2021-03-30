package cn.jaychang.flink.learn.common.model;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class SubOrderDetail {
        private Long userId;
        private Long orderId;
        private Long subOrderId;
        private Long siteId;
        private String siteName;
        private Long cityId;
        private String cityName;
        private Long warehouseId;
        private Long merchandiseId;
        private Double price;
        private Integer quantity;
        private Integer orderStatus;
        private Boolean isNewOrder;
        private long timestamp;
}
