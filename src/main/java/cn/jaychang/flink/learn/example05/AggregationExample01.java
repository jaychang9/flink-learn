package cn.jaychang.flink.learn.example05;

import cn.jaychang.flink.learn.common.model.Student;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class AggregationExample01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Student> studentList = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            Student student = new Student(Long.valueOf(String.valueOf(i)), "jaychang" + i, RandomUtils.nextInt(10, 30));
            studentList.add(student);
        }

        System.out.println(JSON.toJSONString(studentList));

        DataStreamSource<Student> dataStream = env.setParallelism(1).fromElements(studentList.toArray(new Student[studentList.size()]));

        KeyedStream<Student, String> keyedStream = dataStream.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student student) throws Exception {
                if (student.getAge() >= 10 && student.getAge() <= 15) {
                    //student.setName("age from 10 to 15");
                    return "age from 10 to 15";
                } else if (student.getAge() >= 16 && student.getAge() <= 25) {
                    //student.setName("age from 16 to 25");
                    return "age from 16 to 25";
                } else {
                    //student.setName("age from 26 to 30");
                    return "age from 26 to 30";
                }
            }
        });

        keyedStream.sum("age").print();


        env.execute("ReduceExample01");
    }
}
