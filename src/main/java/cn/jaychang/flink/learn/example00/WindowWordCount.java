package cn.jaychang.flink.learn.example00;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;


public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = env.socketTextStream("10.1.80.213", 9999)
                .flatMap(new Splitter())
                .keyBy(tuple2 -> tuple2.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5L)))
                .sum(1);

        streamOperator.print();
        env.execute("WindowWordCount");
    }

    static class Splitter implements FlatMapFunction<String,Tuple2<String,Integer>>{
            @Override
            public void flatMap(String sentence, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                for (String word: sentence.split(" ")) {
//                    collector.collect(new Tuple2<String, Integer>(word, 1));
//                }
                Arrays.stream(sentence.split(" ")).forEach(word->collector.collect(Tuple2.of(word,1)));
            }
    }
}
