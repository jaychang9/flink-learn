package cn.jaychang.flink.learn.example03;

import cn.jaychang.flink.learn.common.model.Student;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class KeyByExample01 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Student> studentList = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            Student student = new Student(Long.valueOf(String.valueOf(i)), "jaychang" + i, RandomUtils.nextInt(10,30));
            studentList.add(student);
        }

        DataStreamSource<Student> dataStream = env.fromElements(studentList.toArray(new Student[studentList.size()]));

        dataStream.flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student student, Collector<Student> collector) throws Exception {
                if (student.getAge() >= 15 && student.getAge() <= 20) {
                    collector.collect(student);
                }
            }
        }).print();

        env.execute("KeyByExample01");
    }
}
