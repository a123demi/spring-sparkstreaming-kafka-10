package com.lm.utils;

import java.util.HashMap;

/**
 * 常量工具
 */
public class ConstUtil {
	public final static String KAFKA_SPLIT = "\\â«";
	
	public final static String HBASE_FAMILY = "cf1";
	
	public final static String STRING_NULL = "NULL";
	
	public final static HashMap<String, String> TABLE_ROWKEY = new HashMap<>();
	
	static{
		TABLE_ROWKEY.put("dfs.order", "order_id");
	}
}
