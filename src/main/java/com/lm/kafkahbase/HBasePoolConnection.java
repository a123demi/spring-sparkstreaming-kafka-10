package com.lm.kafkahbase;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.lm.exception.MessageException;

/**
 * Hbase连接池
 * 
 * @author liangming.deng
 * @date 2017年7月7日
 *
 */
public class HBasePoolConnection {
	private HBasePoolConnection() {
	}

	// 连接池
	private static Connection connection = null;
	// 配置文件
	static Configuration hbaseConfiguration = HBaseConfiguration.create();

	public static Connection getConnection() {
		if (connection == null) {
			ExecutorService pool = Executors.newFixedThreadPool(10);// 建立一个固定大小的线程池
			hbaseConfiguration.addResource("hbase-site.xml");
			try {
				connection = ConnectionFactory.createConnection(hbaseConfiguration, pool);// 创建连接时，拿到配置文件和线程池
			} catch (IOException e) {
				throw new MessageException("Hbase连接池初始化错误", e);
			}
		}
		return connection;
	}

}