package com.lm.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.lm.exception.MessageException;
import com.lm.kafkahbase.HBaseUtils;
import com.lm.kafkahbase.OffsetHBaseUtils;
import com.lm.kafkahbase.OggKafkaJsonUtils;
import com.lm.kafkahbase.OggKafkaUtils;
import com.lm.utils.BeanUtil;

@Component
public class SparkStreamingKafka2 implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static Logger LOGGER = LoggerFactory.getLogger(SparkStreamingKafka2.class);

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

	String datatable = "dfs.offset";
	String offsetFamily = "topic_partition_offset";

	public void processSparkStreaming() throws InterruptedException {
		// 1.配置sparkconf,必须要配置master
		SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
		conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		conf.set("spark.kryo.registrator", "com.lm.kryo.MyRegistrator");
		conf.set("spark.kryoserializer.buffer.mb", "256");
		conf.set("spark.kryoserializer.buffer.max", "512");
		
		conf.set("spark.executor.memory", "4g");
		
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
		JavaInputDStream<ConsumerRecord<String, String>> stream = null;
		// KafkaUtils.createDirectStream(jsc,
		// LocationStrategies.PreferConsistent(),
		// ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

		// 判断数据表是否存在，如果不存在则从topic首位置消费，并新建该表；如果表存在，则从表中恢复话题对应分区的消息的offset
		boolean isExists = HBaseUtils.isExistTable(datatable);
		if (isExists) {

			ResultScanner rs = HBaseUtils.getResultScanner(datatable, kafkaTopics);

			if (rs == null || !rs.iterator().hasNext()) {
				stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

			} else {

				Map<TopicPartition, Long> fromOffsets = OffsetHBaseUtils.getOffset(rs);
				stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String> Assign(fromOffsets.keySet(), kafkaParams, fromOffsets));
			}
		} else {
			// 如果不存在TopicOffset表，则从topic首位置开始消费
			stream = KafkaUtils.createDirectStream(jsc, LocationStrategies.PreferConsistent(),
					ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

			// 并创建TopicOffset表
			HBaseUtils.createTable(datatable, offsetFamily);

			System.out.println(datatable + "表已经成功创建!----------------");
		}

		// 6.spark rdd转化和行动处理
		stream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(JavaRDD<ConsumerRecord<String, String>> v1, Time v2) {
				
				OffsetRange[] offsetRanges = ((HasOffsetRanges) v1.rdd()).offsetRanges();
				for (OffsetRange offsetRange : offsetRanges) {
					// begin your transaction
					// 为了保证业务的事务性，最好把业务计算结果和offset同时进行hbase的存储，这样可以保证要么都成功，要么都失败，最终从端到端体现消费精确一次消费的意境
					// 存储
					long startDate = new Date().getTime();
					List<ConsumerRecord<String, String>> consumerRecords = v1.collect();

					List<String> oggValues = new ArrayList<>();
					for (ConsumerRecord<String, String> record : consumerRecords) {
						oggValues.add(record.value());
					}
					OggKafkaJsonUtils.processBatchPut(oggValues);
					long endDate = new Date().getTime();
					System.out.println("插入完成:" + (endDate - startDate));

					// update results
					// update offsets where the end of existing offsets
					// matches
					// the beginning of this batch of offsets
					// assert that offsets were updated correctly

					try {
						HBaseUtils.putData(datatable, offsetFamily, offsetFamily, BeanUtil.objectToMap(offsetRange));
					} catch (Exception e) {
						throw new MessageException("object与map转化", e);
					}
					System.out.println("add data Success!");
					// end your transaction
				}
				System.out.println("the RDD records counts is " + v1.count());

			}
		});

		// 6. 启动执行
		jsc.start();
		// 7. 等待执行停止，如有异常直接抛出并关闭
		jsc.awaitTermination();
	}
}
