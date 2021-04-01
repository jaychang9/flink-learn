package cn.jaychang.flink.learn.example06;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@Data
@ToString(callSuper = true)
public class OrderAggregationResult extends OrderAggregation implements Serializable {

    private Integer totalOrderCount;

    // 用于检验结果
    private List<String> dateTimes;

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}