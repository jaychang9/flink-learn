package cn.jaychang.flink.learn.example04;

import cn.jaychang.flink.learn.common.model.Student;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class ReduceExample01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Student> studentList = new ArrayList<>();
        for (int i = 1; i <= 5; i++) {
            Student student = new Student(Long.valueOf(String.valueOf(i)), "jaychang" + i, RandomUtils.nextInt(10, 30));
            studentList.add(student);
        }

        System.out.println(JSON.toJSONString(studentList));

        DataStreamSource<Student> dataStream = env.setParallelism(1).fromElements(studentList.toArray(new Student[studentList.size()]));

        dataStream.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student student) throws Exception {
                if (student.getAge() >= 10 && student.getAge() <= 15) {
                    return "age from 10 to 15";
                } else if (student.getAge() >= 16 && student.getAge() <= 25) {
                    return "age from 16 to 25";
                } else {
                    return "age from 26 to 30";
                }
            }
        }).reduce(new ReduceFunction<Student>() {
            @Override
            public Student reduce(Student s1, Student s2) throws Exception {
                Student student = new Student();
                student.setId((s1.getId()+s2.getId())/2);
                student.setAge(Math.max(s1.getAge(),s2.getAge()));
                student.setName(s1.getName()+","+s2.getName());
                return student;
            }
        }).print().setParallelism(1);


        env.execute("ReduceExample01");
    }
}
