package cn.jaychang.flink.learn.example03;

import cn.jaychang.flink.learn.common.model.Student;
import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class KeyByExample02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Student> studentList = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            Student student = new Student(Long.valueOf(String.valueOf(i)), "jaychang" + i, RandomUtils.nextInt(10, 20));
            studentList.add(student);
        }

        DataStreamSource<Student> dataStream = env.fromElements(studentList.toArray(new Student[studentList.size()]));

        System.out.println(JSON.toJSONString(studentList));

        dataStream.flatMap(new FlatMapFunction<Student, Tuple2<Integer,Integer>>() {
            @Override
            public void flatMap(Student student, Collector<Tuple2<Integer,Integer>> collector) throws Exception {
                    collector.collect(Tuple2.of(student.getAge(),1));
            }
        }).keyBy(new KeySelector<Tuple2<Integer, Integer>, Integer>() {

            @Override
            public Integer getKey(Tuple2<Integer, Integer> tuple2) throws Exception {
                // f0为age
                return tuple2.f0;
            }
        }).sum(1).print().setParallelism(1);

        env.execute("KeyByExample02");
    }
}
