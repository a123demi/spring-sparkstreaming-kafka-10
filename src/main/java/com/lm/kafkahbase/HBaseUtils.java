package com.lm.kafkahbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lm.exception.MessageException;

/**
 * HBase使用例子
 */
public class HBaseUtils {
	private final static Logger logger = LoggerFactory.getLogger(HBaseUtils.class);

	/**
	 * 关闭连接
	 * 
	 * @param admin
	 * @param table
	 * @param bufferedMutator
	 */
	public static void release(Admin admin, Table table, BufferedMutator bufferedMutator) {
		try {
			if (admin != null)
				admin.close();
			if (table != null)
				table.close();
			if (bufferedMutator != null)
				bufferedMutator.close();
		} catch (IOException e) {
			throw new MessageException("Hbase判断表是否存在", e);
		}
	}

	/**
	 * 判断表是否存在
	 * 
	 * @param tableName
	 * @return
	 */
	public static boolean isExistTable(String tableName) {
		boolean isExist = false;
		Admin admin = null;

		try {
			admin = HBasePoolConnection.getConnection().getAdmin();
			TableName table = TableName.valueOf(tableName);
			if (admin.tableExists(table)) {// 如果表已经存在
				isExist = true;
			}

		} catch (IOException e) {
			throw new MessageException("Hbase判断表是否存在", e);

		} finally {
			release(admin, null, null);
		}

		return isExist;
	}

	/**
	 * 创建表
	 *
	 * @param tablename
	 *            表名
	 * @param columnFamily
	 *            列族
	 * @ @throws
	 *       ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 */
	public static void createTable(String tablename, String columnFamily) {

		Admin admin = null;

		try {
			admin = HBasePoolConnection.getConnection().getAdmin();
			TableName tableName = TableName.valueOf(tablename);
			if (!admin.tableExists(tableName)) {// 如果表已经存在
				HTableDescriptor tableDesc = new HTableDescriptor(tableName);
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
				admin.createTable(tableDesc);
				logger.info(tablename + "表已经成功创建!----------------");
			}

		} catch (IOException e) {
			throw new MessageException("Hbase创建表", e);
		} finally {
			release(admin, null, null);
		}

	}

	/**
	 * 向表中插入一条新数据
	 *
	 * @param tableName
	 *            表名
	 * @param row
	 *            行键key
	 * @param columnFamily
	 *            列族
	 * @param column
	 *            列名
	 * @param data
	 *            要插入的数据 @
	 */
	public static void putData(String tableName, String row, String columnFamily, String column, String data) {
		TableName tableNameObj = null;
		Table table = null;
		try {
			tableNameObj = TableName.valueOf(tableName);
			table = HBasePoolConnection.getConnection().getTable(tableNameObj);
			Put put = new Put(Bytes.toBytes(row));
			put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
			table.put(put);
			logger.info("-------put '" + row + "','" + columnFamily + ":" + column + "','" + data + "'");
		} catch (IOException e) {
			throw new MessageException("Hbase插入数据", e);
		} finally {
			release(null, table, null);
		}

	}

	/**
	 * map数据插入
	 * 
	 * @param tableName
	 * @param row
	 * @param columnFamily
	 * @param datas
	 * @
	 */
	public static void putData(String tableName, String row, String columnFamily, Map<String, Object> datas) {
		if (null == datas || datas.isEmpty()) {
			return;
		}
		TableName tableNameObj = null;
		Table table = null;
		try {
			tableNameObj = TableName.valueOf(tableName);
			table = HBasePoolConnection.getConnection().getTable(tableNameObj);
			Put put = new Put(Bytes.toBytes(row));
			Iterator<String> columns = datas.keySet().iterator();
			while (columns.hasNext()) {
				String column = columns.next();
				put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
						Bytes.toBytes(datas.get(column).toString()));
			}
			table.put(put);
		} catch (IOException e) {
			throw new MessageException("Hbase插入对象数据", e);
		} finally {
			release(null, table, null);
		}
	}

	/**
	 * 获取指定行的所有数据
	 * 
	 * @param tableName
	 * @param row
	 * @param columnFamily
	 * @param column
	 * @return @
	 */
	public static String getData(String tableName, String row, String columnFamily, String column) {

		TableName tableNameObj = null;
		Table table = null;
		String value = "";
		try {
			tableNameObj = TableName.valueOf(tableName);
			table = HBasePoolConnection.getConnection().getTable(tableNameObj);
			Get get = new Get(Bytes.toBytes(row));
			Result result = table.get(get);
			byte[] rb = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
			value = new String(rb, "UTF-8");
			logger.info("------" + value);
		} catch (IOException e) {
			throw new MessageException("Hbase获取指定行", e);
		} finally {
			release(null, table, null);
		}

		return value;
	}

	/**
	 * 获取ResultScanner
	 * 
	 * @param tableName
	 * @param topics
	 * @return @
	 */
	public static ResultScanner getResultScanner(String tableName, String value) {

		TableName tableNameObj = null;
		Table table = null;
		ResultScanner rs = null;
		try {
			tableNameObj = TableName.valueOf(tableName);
			table = HBasePoolConnection.getConnection().getTable(tableNameObj);
			Filter filter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(Bytes.toBytes(value + "_")));
			Scan s = new Scan();
			s.setFilter(filter);
			rs = table.getScanner(s);
		} catch (IOException e) {
			throw new MessageException("Hbase获取指定行", e);
		} finally {
			release(null, table, null);
		}

		return rs;
	}

	/**
	 * 获取指定表的所有数据
	 *
	 * @param tableName
	 *            表名 @
	 */
	public static void scanAll(String tableName) {

		TableName tableNameObj = null;
		Table table = null;
		ResultScanner resultScanner = null;
		try {
			tableNameObj = TableName.valueOf(tableName);
			table = HBasePoolConnection.getConnection().getTable(tableNameObj);
			Scan scan = new Scan();
			resultScanner = table.getScanner(scan);
			for (Result result : resultScanner) {
				List<Cell> cells = result.listCells();
				for (Cell cell : cells) {
					String row = new String(result.getRow(), "UTF-8");
					String family = new String(CellUtil.cloneFamily(cell), "UTF-8");
					String qualifier = new String(CellUtil.cloneQualifier(cell), "UTF-8");
					String value = new String(CellUtil.cloneValue(cell), "UTF-8");
					logger.info(":::::[row:" + row + "],[family:" + family + "],[qualifier:" + qualifier + "],[value:"
							+ value + "]");
				}
			}
		} catch (IOException e) {
			throw new MessageException("Hbase获取指定行", e);
		} finally {
			release(null, table, null);
		}

	}

	/*
	 * 删除指定的列
	 * 
	 * @tableName 表名
	 * 
	 * @rowKey rowKey
	 * 
	 * @familyName 列族名
	 * 
	 * @columnName 列名
	 */
	public static void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) {
		TableName tableNameObj = null;
		Table table = null;
		try {
			tableNameObj = TableName.valueOf(tableName);
			table = HBasePoolConnection.getConnection().getTable(tableNameObj);
			Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
			deleteColumn.addColumn(Bytes.toBytes(falilyName), Bytes.toBytes(columnName));
			table.delete(deleteColumn);
			logger.info(falilyName + ":" + columnName + "is deleted!");
		} catch (IOException e) {
			throw new MessageException("Hbase删除指定行", e);
		} finally {
			release(null, table, null);
		}

	}

	public static void bathWriteData(List<Put> puts, String tableName, String columnName) {
		TableName tableNameObj = null;
		Table table = null;
		try {
			tableNameObj = TableName.valueOf(tableName);
			table = HBasePoolConnection.getConnection().getTable(tableNameObj);

			table.put(puts);
			((HTable) table).flushCommits();
		} catch (IOException e) {
			throw new MessageException("Hbase批量put插入", e);

		} finally {
			release(null, table, null);
		}

	}

	public static Put getPut(String row, String columnFamily, String column, String data) {

		Put put = new Put(Bytes.toBytes(row));
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(data));
		return put;
	}
	
	
	public static Put getPut(String row, String columnFamily, Map<String,Object> map) {

		if(map == null || map.isEmpty()){
			return null;
		}
		
		Put put = new Put(Bytes.toBytes(row));

		Iterator<String> keys = map.keySet().iterator();
		while(keys.hasNext()){
			String key = keys.next();
			
			put.addColumn(Bytes.toBytes(columnFamily), 
							Bytes.toBytes(key), 
							Bytes.toBytes(map.get(key) == null ? "null":map.get(key).toString()));
		}
		
		return put;
	}
	
	
	public static Delete getDelete(String row, String columnFamily, String column) {

		Delete delete = new Delete(Bytes.toBytes(row));
		delete.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		return delete;
	}

	/**
	 * 批量插入
	 * 
	 * @param tableName
	 * @param puts
	 */
	public static void batchInsert(String tableName, List<Put> puts) {
		BufferedMutator table = null;
		try {
			// 连接表 获取表对象
			table = HBasePoolConnection.getConnection().getBufferedMutator(TableName.valueOf(tableName));
			List<Mutation> mutations = new ArrayList<Mutation>();
			for (Put put : puts) {
				mutations.add(put);
			}

			table.mutate(mutations);
			// 如果不flush 在后面get可能是看不见的
			table.flush();
		} catch (IOException e) {
			throw new MessageException("Hbase批量插入异常", e);
		} finally {
			release(null, null, table);
		}

	}

	/*
	 * 删除指定的列
	 * 
	 * @tableName 表名
	 * 
	 * @rowKey rowKey
	 * 
	 * @familyName 列族名
	 * 
	 * @columnName 列名
	 */
	public static void batchDelete(String tableName, List<Delete> deletes) {
		TableName tableNameObj = null;
		Table table = null;
		try {
			tableNameObj = TableName.valueOf(tableName);
			table = HBasePoolConnection.getConnection().getTable(tableNameObj);
			table.delete(deletes);

		} catch (IOException e) {
			throw new MessageException("Hbase删除指定行", e);
		} finally {
			release(null, table, null);
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			/*
			 * HbaseDemo.CreateTable("userinfo", "baseinfo");
			 * HbaseDemo.PutData("userinfo", "row2", "baseinfo", "vio2",
			 * "驾驶车辆违法信息2："); HbaseDemo.PutData("userinfo", "row5", "baseinfo",
			 * "vio2", "驾驶车辆违法信息2："); HbaseDemo.GetData("userinfo", "row2",
			 * "baseinfo", "vio2"); HbaseDemo.ScanAll("userinfo");
			 */

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}