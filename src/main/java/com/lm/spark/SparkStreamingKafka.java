package com.lm.spark;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SparkStreamingKafka implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static Logger LOGGER = LoggerFactory.getLogger(SparkStreamingKafka.class);

	@Value("${spark.appname}")
	private String appName;
	@Value("${spark.master}")
	private String master;
	@Value("${spark.seconds}")
	private long second;
	@Value("${kafka.metadata.broker.list}")
	private String metadataBrokerList;
	@Value("${kafka.auto.offset.reset}")
	private String autoOffsetReset;
	@Value("${kafka.topics}")
	private String kafkaTopics;
	@Value("${kafka.group.id}")
	private String kafkaGroupId;

	public void processSparkStreaming() throws InterruptedException {
		// 1.配置sparkconf,必须要配置master
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "com.lm.kryo.MyRegistrator");

		// 2.根据sparkconf 创建JavaStreamingContext
		JavaStreamingContext jsc = new JavaStreamingContext(conf, Durations.seconds(second));

		// 3.配置kafka
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", metadataBrokerList);
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", kafkaGroupId);
		kafkaParams.put("auto.offset.reset", autoOffsetReset);
		kafkaParams.put("enable.auto.commit", false);

		// 4.kafka主题
		Collection<String> topics = Arrays.asList(kafkaTopics.split(","));

		// 5.创建SparkStreaming输入数据来源input Stream
		final JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

		// 6.spark rdd转化和行动处理
		stream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<ConsumerRecord<String, String>> v1, Time v2) throws Exception {

				List<ConsumerRecord<String, String>> consumerRecords = v1.collect();

				System.out.println("获取消息:" + consumerRecords.size());

			}
		});

		// 6. 启动执行
		jsc.start();
		// 7. 等待执行停止，如有异常直接抛出并关闭
		jsc.awaitTermination();
	}
}
