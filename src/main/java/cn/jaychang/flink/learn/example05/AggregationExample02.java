package cn.jaychang.flink.learn.example05;

import cn.jaychang.flink.learn.common.model.Student;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class AggregationExample02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Student> studentList = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            Student student = new Student(Long.valueOf(String.valueOf(i)), "jaychang" + i, RandomUtils.nextInt(10, 30));
            studentList.add(student);
        }

        System.out.println(JSON.toJSONString(studentList));

        DataStreamSource<Student> dataStream = env.setParallelism(1).fromElements(studentList.toArray(new Student[studentList.size()]));

        dataStream.union(dataStream).print().setParallelism(1);


        env.execute("ReduceExample01");
    }
}
