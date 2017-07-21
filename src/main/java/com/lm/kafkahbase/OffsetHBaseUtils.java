package com.lm.kafkahbase;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OffsetHBaseUtils {
	private static Logger LOGGER = LoggerFactory.getLogger(OffsetHBaseUtils.class);
	public static Map<TopicPartition, Long> getOffset(ResultScanner rs){
		// begin from the the offsets committed to the database
		Map<TopicPartition, Long> fromOffsets = new HashMap<>();
		String s1 = null;
		int s2 = 0;
		long s3 = 0;
		for (Result r : rs) {
			System.out.println("rowkey:" + new String(r.getRow()));
			for (Cell cell : r.rawCells()) {
				String qualifier = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell));
				String family = new String(CellUtil.cloneFamily(cell));

				if (qualifier.equals("topic")) {
					s1 = value;
					LOGGER.info("列族:" + family + " 列:" + qualifier + ":" + s1);
				}

				if (qualifier.equals("partition")) {
					s2 = Integer.valueOf(value);
					LOGGER.info("列族:" + family + " 列:" + qualifier + ":" + s2);
				}

				if (qualifier.equals("offset")) {
					s3 = Long.valueOf(value);
					LOGGER.info("列族:" + family + " 列:" + qualifier + ":" + s3);
				}

			}

			fromOffsets.put(new TopicPartition(s1, s2), s3);
		}
		return fromOffsets;
	}
}
