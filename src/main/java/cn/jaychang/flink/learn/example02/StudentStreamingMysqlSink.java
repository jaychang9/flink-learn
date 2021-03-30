/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.jaychang.flink.learn.example02;

import cn.jaychang.flink.learn.common.model.Student;
import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StudentStreamingMysqlSink {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		props.put("bootstrap.servers", "10.1.80.213:9092");
		props.put("zookeeper.connect", "10.1.80.213:2181");
		props.put("group.id", "example01-group");
		props.put("key.deserializer", "org.apache.flink.api.common.serialization.SimpleStringSchema");
		props.put("value.deserializer", "org.apache.flink.api.common.serialization.SimpleStringSchema");
		props.put("auto.offset.reset", "latest");

		FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props);

		SingleOutputStreamOperator<Student> streamOperator = env.addSource(flinkKafkaConsumer).setParallelism(1).map((MapFunction<String, Student>) s -> JSON.parseObject(s, Student.class))
				.returns(TypeInformation.of(Student.class));
		streamOperator.addSink(new StudentMysqlSink("jdbc:mysql://10.1.80.214:3306/test?characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&connectTimeout=60000&socketTimeout=60000&autoReconnect=true&failOverReadOnly=false&useSSL=false&useUnicode=true","root","123456"));
		env.execute("Flink Custom Sink Example");
	}
}
