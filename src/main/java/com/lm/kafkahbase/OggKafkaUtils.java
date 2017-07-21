package com.lm.kafkahbase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lm.utils.ConstUtil;

public class OggKafkaUtils {
	private static Logger logger = LoggerFactory.getLogger(OggKafkaUtils.class);

	/**
	 * 执行接收到的kafka数据到hbase
	 * 
	 * @param oggKafkaValues
	 */
	public static void processBatchPut(List<String> oggKafkaValues) {
		//

		List<Put> insertPuts = getOggKafkaPuts(oggKafkaValues);

		HBaseUtils.batchInsert("dfs.order", insertPuts);

		List<Delete> deletes = getOggKafkaDelete(oggKafkaValues);

		HBaseUtils.batchDelete("dfs.order", deletes);

	}

	/**
	 * 根据sparkStreaming获取源ogg经kafka发送的消息
	 * 
	 * @param oggKafkaValues
	 * @return
	 */
	public static List<HashMap<String, HashMap<String, String>>> getOggKafkas(List<String> oggKafkaValues) {
		// 一次加载多有数据,第一个map为header和body,第二个map对应key,value值
		List<HashMap<String, HashMap<String, String>>> oggKafkaMapList = new ArrayList<>();
		for (String value : oggKafkaValues) {
			// 获取value对应分隔信息
			List<String> values = Arrays.asList(value.split(ConstUtil.KAFKA_SPLIT));

			oggKafkaMapList.add(getKafkaMap(values));
		}

		return oggKafkaMapList;
	}

	/**
	 * 根据sparkStreaming获取源ogg经kafka发送的消息
	 * 
	 * @param oggKafkaValues
	 * @return
	 */
	public static List<Put> getOggKafkaPuts(List<String> oggKafkaValues) {
		// 带插入或更新的put
		List<Put> insertPuts = new ArrayList<>();
		// 一次加载多有数据,第一个map为header和body,第二个map对应key,value值
		List<HashMap<String, HashMap<String, String>>> oggKafkaMapList = getOggKafkas(oggKafkaValues);

		for (HashMap<String, HashMap<String, String>> hashMap : oggKafkaMapList) {
			if (!"D".equals(hashMap.get("header").get("ogg_status"))) {
				insertPuts.addAll(handlerMsgToHBasePut(hashMap));
			}
		}
		return insertPuts;
	}

	/**
	 * 根据sparkStreaming获取源ogg经kafka发送的消息
	 * 
	 * @param oggKafkaValues
	 * @return
	 */
	public static List<Delete> getOggKafkaDelete(List<String> oggKafkaValues) {
		// 删除的put
		List<Delete> deletes = new ArrayList<>();
		// 一次加载多有数据,第一个map为header和body,第二个map对应key,value值
		List<HashMap<String, HashMap<String, String>>> oggKafkaMapList = getOggKafkas(oggKafkaValues);
		for (HashMap<String, HashMap<String, String>> hashMap : oggKafkaMapList) {
			if ("D".equals(hashMap.get("header").get("ogg_status"))) {
				deletes.addAll(handlerMsgToHBaseDelete(hashMap));
			}
		}
		return deletes;
	}

	/**
	 * 每一个kafka消息对应的map header:消息头 body:消息体
	 * 
	 * @param kafkaMsgList
	 * @return
	 */
	public static HashMap<String, HashMap<String, String>> getKafkaMap(List<String> kafkaMsgList) {
		HashMap<String, HashMap<String, String>> mapList = new HashMap<String, HashMap<String, String>>();

		if (kafkaMsgList == null || kafkaMsgList.isEmpty()) {
			return mapList;
		}

		HashMap<String, String> headerMap = new HashMap<>();
		if (kafkaMsgList == null || kafkaMsgList.size() < 5) {
			logger.error("getKafkaMap 异常消息头");
			return mapList;
		}
		// 1.数据头
		List<String> kafkaHeader = kafkaMsgList.subList(0, 5);
		List<String> kafkaBody = kafkaMsgList.subList(5, kafkaMsgList.size());
		headerMap.put("ogg_status", kafkaHeader.get(0));
		headerMap.put("ogg_table", kafkaHeader.get(1));
		headerMap.put("ogg_created", kafkaHeader.get(2));
		headerMap.put("ogg_updated", kafkaHeader.get(3));
		headerMap.put("ogg_id", kafkaHeader.get(4));
		mapList.put("header", headerMap);

		HashMap<String, String> bodyMap = new HashMap<>();
		for (int i = 0; i < kafkaBody.size(); i += 2) {
			// 偶数时key,奇数是value
			bodyMap.put(kafkaBody.get(i), kafkaBody.get(i + 1));
		}
		mapList.put("body", bodyMap);
		return mapList;
	}

	/**
	 * 每一个加入加入到put
	 * 
	 * @param kafkaMapList
	 */
	public static List<Put> handlerMsgToHBasePut(HashMap<String, HashMap<String, String>> kafkaMapList) {
		List<Put> puts = new ArrayList<>();
		if (kafkaMapList == null || kafkaMapList.size() == 0) {
			return puts;
		}

		HashMap<String, String> headerMap = kafkaMapList.get("header");
		String ogg_table = headerMap.get("ogg_table").toLowerCase();
		ogg_table = "dfs.order";
		HashMap<String, String> bodyMap = kafkaMapList.get("body");

		Iterator<String> keys = bodyMap.keySet().iterator();
		String rowKey = bodyMap.get(ConstUtil.TABLE_ROWKEY.get(ogg_table));

		while (keys.hasNext()) {
			String key = keys.next();
			String value = bodyMap.get(key);
			if (null != value && ConstUtil.STRING_NULL.equals(value.toUpperCase())) {
				continue;
			}

			puts.add(HBaseUtils.getPut(rowKey, ConstUtil.HBASE_FAMILY, key, value));
		}

		return puts;

	}

	/**
	 * 每一个加入加入到put
	 * 
	 * @param kafkaMapList
	 */
	public static List<Delete> handlerMsgToHBaseDelete(HashMap<String, HashMap<String, String>> kafkaMapList) {
		List<Delete> deletes = new ArrayList<>();
		if (kafkaMapList == null || kafkaMapList.size() == 0) {
			return deletes;
		}

		HashMap<String, String> headerMap = kafkaMapList.get("header");
		String ogg_table = headerMap.get("ogg_table").toLowerCase();
		ogg_table = "dfs.order";
		HashMap<String, String> bodyMap = kafkaMapList.get("body");

		Iterator<String> keys = bodyMap.keySet().iterator();
		String rowKey = bodyMap.get(ConstUtil.TABLE_ROWKEY.get(ogg_table));

		while (keys.hasNext()) {
			String key = keys.next();
			String value = bodyMap.get(key);
			if (null != value && ConstUtil.STRING_NULL.equals(value.toUpperCase())) {
				continue;
			}

			deletes.add(HBaseUtils.getDelete(rowKey, ConstUtil.HBASE_FAMILY, key));
		}

		return deletes;

	}

	/**
	 * 插入操作
	 * 
	 * @param tableName
	 * @param columnMaps
	 */
	public static void processInsertHbase(String tableName, HashMap<String, String> columnMaps) {

		if (columnMaps == null) {
			return;
		}

		Iterator<String> keys = columnMaps.keySet().iterator();
		String rowKey = columnMaps.get("order_id");

		while (keys.hasNext()) {
			String key = keys.next();
			String value = columnMaps.get(key);
			if (null != value && "NULL".equals(value.toUpperCase())) {
				value = "";
			}

			HBaseUtils.getPut(rowKey, "cf1", key, value);

		}

	}

	/**
	 * 插入操作
	 * 
	 * @param tableName
	 * @param columnMaps
	 */
	public static void processDeleteHbase(String tableName, HashMap<String, String> columnMaps) {

		if (columnMaps == null) {
			return;
		}

		Iterator<String> keys = columnMaps.keySet().iterator();
		String rowKey = columnMaps.get("order_id");

		while (keys.hasNext()) {
			String key = keys.next();
			HBaseUtils.deleteColumn(tableName, rowKey, "cf1", key);
		}

	}

	public static void main(String[] args) {
		String[] arr = new String[] { "0", "1", "2", "3", "4", "5" };
		for (int i = 0; i < arr.length; i += 2) {
			// 偶数时key,奇数是value
			System.out.println(arr[i] + ":" + arr[i + 1]);
		}

		List<String> list = Arrays.asList(arr);
		List<String> list1 = list.subList(0, 2);
		List<String> list2 = list.subList(2, list.size());

		System.out.println(list1.toString());
		System.out.println(list2.toString());
	}
}
