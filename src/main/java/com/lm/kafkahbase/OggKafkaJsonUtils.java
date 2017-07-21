package com.lm.kafkahbase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;

import com.alibaba.fastjson.JSON;
import com.lm.utils.ConstUtil;


public class OggKafkaJsonUtils {
	public static void processBatchPut(List<String> oggKafkaJsonValues) {
		
		List<Put> puts = new ArrayList<>();
		
		System.out.println(oggKafkaJsonValues.size());
		for (String json : oggKafkaJsonValues) {
			//1.json转map
			Map jsonMap = JSON.parseObject(json);  
			//2.获取状态和表
			String tableName = jsonMap.get("table").toString();
			List<String> primaryKey =(List<String>) jsonMap.get("primary_keys");
			
			Map<String, Object> values = (Map<String, Object>)jsonMap.get("after");
			
			if(values == null || values.isEmpty()){
				continue;
			}
			
			String rowKey = values.get("order_id").toString();
			puts.add(HBaseUtils.getPut(rowKey, ConstUtil.HBASE_FAMILY, values));
		}
		
		HBaseUtils.batchInsert("dfs.order", puts);
		
		
	}
}
