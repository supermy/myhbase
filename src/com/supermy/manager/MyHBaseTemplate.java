package com.supermy.manager;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scanner;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.PageRowFilter;
import org.apache.hadoop.hbase.filter.RegExpRowFilter;
import org.apache.hadoop.hbase.filter.RowFilterInterface;
import org.apache.hadoop.hbase.filter.RowFilterSet;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;

import com.supermy.annotation.Table;
import com.supermy.annotation.test.TableUtil;
import com.supermy.domain.Action;
import com.supermy.utils.ConvertBean;
import com.supermy.utils.MyHBaseException;
import com.supermy.utils.MyHbaseUtil;

/**
 * 包装了数据库的常用操作;<br>
 * 
 * 数据的维护<br>
 * 不直接和数据库打交道；<br>
 * cache要能够自动完成<br>
 * 单个对对象的crud;<br>
 * 集合的crud;<br>
 * 所有的delete
 */
public class MyHBaseTemplate<T> {
	private static final Log log = LogFactory.getLog(MyHBaseTemplate.class);
	//private T obj;
	
	public MyHBaseTemplate() {
		super();
		
		log.debug("myhbse create !!!");
	}

	private static MyHBaseTemplate instance = new MyHBaseTemplate();;

	public static MyHBaseTemplate getInstance() {
		return instance;
	}

	// private HBaseAdmin admin = MyHbaseUtil.getAdmin();
	// private HBaseConfiguration hbc = MyHbaseUtil.getConfig();

	public void delete(Action obj) {
		obj.delete();
	}

	public void delete(List<Action> obj) {
		if (obj.size() <= 0) {
			return;
		}
		Action.delete(obj);
	}

	public void save(Action obj) {
		obj.saveOrUpdate();
	}

	public void save(List<Action> obj) {
		if (obj.size() <= 0) {
			return;
		}
		Action.saveOrUpdate(obj);
	}

	public Action get(Action obj) {
		return obj.get();
	}

	public void get(List<Action> obj) {
		for (Action action : obj) {
			action=action.get();
		}

	}

	/**
	 * 会自动忽略value为空的列
	 * 
	 * @param rowKey
	 * @param columns
	 * @param values
	 * @return
	 * @throws MyHBaseException
	 */
	public BatchUpdate putColumnByRow(String rowKey, String[] columns,
			String[] values) throws MyHBaseException {
		if (columns.length != values.length) {
			throw new MyHBaseException("列和列值的个数不一致!");
		}
		if (StringUtils.isBlank(rowKey)) {
			throw new MyHBaseException("rowKey为空!");
		}
		BatchUpdate rowBatchUpdate = new BatchUpdate(rowKey);
		try {
			for (int i = 0; i < columns.length; i++) {
				String value = values[i];
				if (StringUtils.isNotBlank(value)) {
					String col = columns[i].trim();
					col = col.contains(":") ? col : col.trim() + ":";
					rowBatchUpdate.put(col, value
							.getBytes(HConstants.UTF8_ENCODING));
				}
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new MyHBaseException("编码转换错误:" + e.getLocalizedMessage());
		}
		return rowBatchUpdate;
	}

	/**
	 * 操作更加方便，可以多次put col 和 value
	 * 
	 * @param bu
	 * @param rowKey
	 * @param columns
	 * @param values
	 * @return
	 * @throws MyHBaseException
	 */
	public BatchUpdate putColumnByRow(BatchUpdate bu, String column,
			String value) throws MyHBaseException {
		try {
			if (StringUtils.isNotBlank(value)) {

				// filterWords(value);// 自动翻译抓取网页，用户提交内容。

				String col = column.trim();
				col = col.contains(":") ? col : col.trim() + ":";
				bu.put(col, value.getBytes(HConstants.UTF8_ENCODING));
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new MyHBaseException("编码转换错误:" + e.getLocalizedMessage());
		}
		return bu;
	}

	/**
	 * @param table
	 * @param rowKey
	 * @param columns
	 * @return
	 */
	public BatchUpdate delColumnByRow(String rowKey, String[] columns) {
		BatchUpdate result = new BatchUpdate(rowKey);
		for (int i = 0; i < columns.length; i++) {
			String col = columns[i];
			if (StringUtils.isNotBlank(col)) {
				col = col.contains(":") ? col : col.trim() + ":";
			}
			result.delete(col.getBytes());
		}
		return result;
	}

	/**
	 * @param tableName
	 * @param rowKey
	 * @param columns
	 * @return
	 * @throws MyHBaseException
	 */
	public Map<String, String> find(String tableName, String rowKey,
			String... columns) throws MyHBaseException {
		HTable table = MyHbaseUtil.getTable(tableName);
		return find(table, rowKey, columns);
	}

	/**
	 * @param table
	 * @param rowKey
	 * @param columns
	 * @return
	 * @throws MyHBaseException
	 */
	public Map<String, String> find(HTable table, String rowKey,
			String... columns) throws MyHBaseException {
		Map<String, String> m = new HashMap<String, String>();

		for (int i = 0; i < columns.length; i++) {
			String col = columns[i];
			columns[i] = col.contains(":") ? col : col.trim() + ":";
		}
		try {
			// FIXME RowResult是无状态，还是会保留游标?
			RowResult row;

			if (columns.length > 0) {
				row = table.getRow(rowKey, columns);
			} else {
				row = table.getRow(rowKey);
			}

			Set<Entry<byte[], Cell>> entrySet = row.entrySet();
			for (Entry<byte[], Cell> entry : entrySet) {
				String v = new String(entry.getValue().getValue(),
						HConstants.UTF8_ENCODING);
				String key = new String(entry.getKey());
				key = key.replace(":", "_");
				m.put(key, v);
			}
			m.put("linekey", rowKey);
		} catch (IOException e) {
			e.printStackTrace();
			throw new MyHBaseException(e.getMessage());
		}
		return m;
	}

	/**
	 * 主键正则表达式查询
	 * 
	 * @param tableName
	 * @param key
	 * @param columns
	 * @param pageSize
	 * @return
	 * @throws MyHBaseException
	 */
	public List<Map<String, String>> findByRegexKey(String tableName,
			String key, String[] columns, int pageSize) throws MyHBaseException {
		HTable table = MyHbaseUtil.getTable(tableName);
		return findByRegexKey(table, key, columns, pageSize);

	}

	/**
	 * 按关键字正则表达式查找
	 * 
	 * @param table
	 * @param key
	 * @param columns
	 * @param pageSize
	 * @return
	 * @throws MyHBaseException
	 * @throws IOException
	 */
	public List<Map<String, String>> findByRegexKey(HTable table, String key,
			String[] columns, int pageSize) throws MyHBaseException {
		byte[][] cols = new byte[columns.length][];
		for (int i = 0; i < columns.length; i++) {
			String col = columns[i];
			col = col.contains(":") ? col : col.trim() + ":";
			cols[i] = col.getBytes();
		}
		// 常用字符串 .*9-28-3-.*
		// .*\\w{32}.* 主键
		RowFilterInterface reg = new RegExpRowFilter(key);
		PageRowFilter page = new PageRowFilter(pageSize);

		final Set<RowFilterInterface> rowFiltersSet = new HashSet<RowFilterInterface>();
		rowFiltersSet.add(page);
		rowFiltersSet.add(reg);
		RowFilterSet rowFilterSet = new RowFilterSet(
				RowFilterSet.Operator.MUST_PASS_ALL, rowFiltersSet);
		Scanner scanner = null;
		List<Map<String, String>> result = null;
		try {
			scanner = table.getScanner(cols, HConstants.EMPTY_START_ROW,
					rowFilterSet);
			result = getResult(scanner);
		} catch (IOException e) {
			e.printStackTrace();
			throw new MyHBaseException(e.getLocalizedMessage());
		}
		scanner.close();
		return result;
	}

	/**
	 * 返回结果 columnname=a:b <br>
	 * linekey;key:value<br>
	 * 
	 * @param scanner
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public List<Map<String, String>> getResult(Scanner scanner)
			throws UnsupportedEncodingException {
		// Set<Map<String, String>> result = new HashSet<Map<String, String>>();
		List<Map<String, String>> result = new LinkedList<Map<String, String>>();
		for (RowResult rowResult : scanner) {
			// log.debug(rowResult);
			Map<String, String> m = new HashMap<String, String>();
			m.put("linekey", new String(rowResult.getRow()));
			Set<Entry<byte[], Cell>> entrySet = rowResult.entrySet();
			for (Entry<byte[], Cell> entry : entrySet) {
				StringBuilder key1 = new StringBuilder();
				key1.append(new String(entry.getKey()));
				int indexOf = key1.indexOf(":");
				if (indexOf > 0) {
					key1 = key1.replace(indexOf, indexOf + 1, "_");
				} else
					log.info(key1);
				String value1 = new String(entry.getValue().getValue(),
						HConstants.UTF8_ENCODING);
				// 语种和email获取时间列，主要是用来排序使用，人工调整，但是会对分页有影响，在排序之前先get_last_key
				if (key1.indexOf("email") > 0 || key1.indexOf("sourcelang") > 0) {
					m.put(key1 + "time", new Long(entry.getValue()
							.getTimestamp()).toString());
				}
				m.put(key1.toString(), value1);
			}
			// log.debug(m);
			result.add(m);
		}
		scanner.close();
		return result;
	}

	public void tableSubmit(HTable table, BatchUpdate tablePutRow)
			throws MyHBaseException {
		try {
			table.commit(tablePutRow);
		} catch (IOException e) {
			e.printStackTrace();
			throw new MyHBaseException(e.getMessage());
		}
	}

	public void tableSubmit(HTable table, List<BatchUpdate> tablePutRow)
			throws MyHBaseException {
		try {
			table.commit(tablePutRow);

			// byte[][] startKeys = table.getStartKeys();
			// for (byte[] bs : startKeys) {
			// String startKey = new String(bs, HConstants.UTF8_ENCODING);
			// System.out.println("startKey:" + startKey);
			// }

		} catch (IOException e) {
			e.printStackTrace();
			throw new MyHBaseException(e);
		}

	}

	public String findByRegexKey(String tableName, String key)
			throws MyHBaseException {
		HTable table = MyHbaseUtil.getTable(tableName);
		log.debug(key);
		RowFilterInterface reg = new RegExpRowFilter(".*" + key + ".*");
		String result = "";
		try {
			String colName = table.getTableDescriptor().getFamilies()
					.iterator().next().getNameAsString();

			Scanner scanner = table.getScanner(new byte[][] { (colName + ":")
					.getBytes() }, HConstants.EMPTY_START_ROW, reg);
			for (RowResult rowResult : scanner) {
				log.debug(rowResult);
				byte[] row = rowResult.getRow();
				if (row != null) {
					result = new String(row);
					scanner.close();
					break;
				}
			}
			scanner.close();
		} catch (IOException e) {
			e.printStackTrace();
			throw new MyHBaseException(e.getLocalizedMessage());
		}
		return result;
	}

	/**
	 * 列值正则表达式查询
	 * 
	 * @param tableName
	 * @param columns
	 * @param values
	 * @param startRow
	 * @param pageSize
	 * @param returnCols
	 * @return
	 * @throws MyHBaseException
	 */
	public List<Map<String, String>> findByColumnValue(String tableName,
			String[] columns, String[] values, String startRow,
			String pageSize, String... returnCols) throws MyHBaseException {
		HTable table = MyHbaseUtil.getTable(tableName);
		return findByColumnValue(table, columns, values, startRow, pageSize,
				returnCols);

	}

	/**
	 * 在hbase中，按列值查询。<br>
	 * 返回值中要包含查询的列<br>
	 * * 列值正则表达式查询
	 * 
	 * @param table
	 * @param columns
	 * @param values
	 * @param startRow
	 * @param pageSize
	 * @param returnCols
	 * @param printValues
	 * @return
	 * @throws MyHBaseException
	 */
	public List<Map<String, String>> findByColumnValue(HTable table,
			String[] columns, String[] values, String startRow,
			String pageSize, String... returnCols) throws MyHBaseException {

		// String pageSize="20";
		// String startRow="";

		byte[] start = (startRow == null || startRow.isEmpty() || startRow
				.equals("")) ? HConstants.EMPTY_START_ROW : startRow.getBytes();

		if (returnCols.length <= 0) {
			throw new MyHBaseException("返回的列是一个必要的参数：returnCols，不能为空!");
		}

		if (columns.length != values.length) {
			throw new MyHBaseException("列和列值的个数不一致!");
		}

		Set<RowFilterInterface> filters = new HashSet<RowFilterInterface>();

		if (!"all".equalsIgnoreCase(pageSize)) {
			PageRowFilter page = new PageRowFilter(Long.parseLong(pageSize));
			filters.add(page);
		}

		for (int i = 0; i < columns.length; i++) {
			String column = columns[i];
			String value = values[i];
			if (value.isEmpty()) {
				continue;
			}
			column = column.contains(":") ? column : column.trim() + ":";
			ColumnValueFilter cvf = new ColumnValueFilter(column.getBytes(),
					ColumnValueFilter.CompareOp.EQUAL, value.trim().getBytes());
			filters.add(cvf);
		}
		RowFilterSet rowFilterSet = new RowFilterSet(
				RowFilterSet.Operator.MUST_PASS_ALL, filters);

		byte[][] colsResult = new byte[returnCols.length][];
		for (int i = 0; i < returnCols.length; i++) {
			returnCols[i] = returnCols[i].contains(":") ? returnCols[i]
					: returnCols[i].trim() + ":";
			colsResult[i] = returnCols[i].getBytes();
		}

		Scanner scanner = null;
		List<Map<String, String>> result = null;
		try {
			// if (columns.length==0 && values.length==0) {
			// scanner = table.getScanner(colsResult);
			// } else {
			scanner = table.getScanner(colsResult, start, rowFilterSet);
			// }
			result = getResult(scanner);
		} catch (IOException e) {
			e.printStackTrace();
			throw new MyHBaseException(e.getLocalizedMessage());
		}
		scanner.close();

		return result;
	}

	/**
	 * 查询某个数据表的某些列，无条件。
	 * 
	 * @param tableName
	 * @param columns
	 * @return
	 * @throws MyHBaseException
	 */
	public List<Map<String, String>> get(String tableName, String... columns)
			throws MyHBaseException {
		for (String col : columns) {
			if (StringUtils.isNotBlank(col)) {
				col = col.contains(":") ? col : col.trim() + ":";
			}
		}
		HTable table = MyHbaseUtil.getTable(tableName);
		try {
			return getResult(table.getScanner(columns));
		} catch (IOException e) {
			throw new MyHBaseException(e.getMessage());
		}

	}

	/**
	 * 防止嵌套调用
	 * 
	 * @param words
	 * @throws MyHBaseException
	 */
	// public void putFilterWord(String words) throws MyHBaseException {
	// log.info("create db filter words ...");
	// HTable f = MyHbaseUtil.getTable(MokiDBStruct.FILTER_WORD);
	// BatchUpdate line = new BatchUpdate("1");
	// try {
	// line.put(MokiDBStruct.KEYWORD, words
	// .getBytes(HConstants.UTF8_ENCODING));
	// } catch (UnsupportedEncodingException e) {
	// e.printStackTrace();
	// throw new MyHBaseException(e.getMessage());
	// }
	// tableSubmit(f, line);
	// }
	// public String[] getFilterWord() throws MyHBaseException {
	// HTable f = MyHbaseUtil.getTable(MokiDBStruct.FILTER_WORD);
	// try {
	// RowResult row = f.getRow("1");
	// log.debug(row);
	// String key = new String(row.get(MokiDBStruct.KEYWORD).getValue(),
	// HConstants.UTF8_ENCODING);
	// return key.split(";|,");
	// } catch (IOException e) {
	// e.printStackTrace();
	// throw new MyHBaseException(e.getMessage());
	// }
	// }
	// public String getFilterWordX() throws MyHBaseException {
	// HTable f = MyHbaseUtil.getTable(MokiDBStruct.FILTER_WORD);
	// try {
	// RowResult row = f.getRow("1");
	// log.debug(row);
	// String key = new String(row.get(MokiDBStruct.KEYWORD).getValue(),
	// HConstants.UTF8_ENCODING);
	// return key;
	// } catch (IOException e) {
	// e.printStackTrace();
	// throw new MyHBaseException(e.getMessage());
	// }
	// }
	/**
	 * true 表示没有敏感词； false表示有敏感词；
	 * 
	 * @param content
	 * @return
	 * @throws MyHBaseException
	 */
	// public boolean filterWords(String content) throws MyHBaseException {
	// return StringUtils.filterWords(content, getFilterWord());
	// }
	/**
	 * 利用列存储的属性
	 * 
	 * @param key
	 * @param value
	 * @throws MyHBaseException
	 */
//	@Deprecated
	// public void add(String key, Object value) throws MyHBaseException {
	// BatchUpdate line = new BatchUpdate(key);
	// BaseSerializing bs = new BaseSerializing();
	// line.put("value", bs.serialize(value));
	// // tableSubmit(caches, line);TODO
	// }
	/*
	 * 利用key查询 //TODO
	 * 
	 * @param key
	 * 
	 * @param value
	 * 
	 * @throws MyHBaseException
	 */
	// @Deprecated
	// public void delete(String key, Object value) throws MyHBaseException {
	// BatchUpdate line = new BatchUpdate(key);
	// BaseSerializing bs = new BaseSerializing();
	// line.put("value", bs.serialize(value));
	// // tableSubmit(caches, line); TODO
	// }
	/*
	 * 返回列可以返回指定属性的值，可以减轻对象的重量
	 * 
	 * @param clazz
	 * 
	 * @param columns
	 * 
	 * @param values
	 * 
	 * @param startRow
	 * 
	 * @param pageSize
	 * 
	 * @param returnCols
	 * 
	 * @return
	 * 
	 * @throws MyHBaseException
	 */
	public List<Action> find(Class<? extends Action> clazz, String[] columns,
			String[] values, String startRow, long pageSize,
			String... returnCols) throws MyHBaseException {
		if (columns.length != values.length) {
			throw new MyHBaseException("列和列值的个数不一致!");
		}

		String tableName=TableUtil.getTable(clazz);
		HTable table = MyHbaseUtil.getTable(tableName);

		// String pageSize="20";
		// String startRow="";

		byte[] start = (startRow == null || startRow.isEmpty() || startRow
				.equals("")) ? HConstants.EMPTY_START_ROW : startRow.getBytes();

		if (returnCols.length > 0) {
			// 但是返回列必须包含条件列否则得不到查询结果 同时防止出现重复列
			// String[] s3 = new String[columns.length + returnCols.length];
			// System.arraycopy(columns, 0, s3, 0, columns.length);
			// System.arraycopy(returnCols, 0, s3, columns.length,
			// returnCols.length);
			// returnCols=s3;
			Set<String> hb = new HashSet<String>();
			for (int i = 0; i < columns.length; i++) {
				hb.add(columns[i]);
			}
			for (int i = 0; i < returnCols.length; i++) {
				hb.add(returnCols[i]);
			}
			hb.toArray(returnCols);

		} else {
			// 返回全部的属性列
			// String[] returnCols;
			Map<String, Field> fileds = MyHbaseUtil.getFileds(clazz);
			Set<String> keySet = new HashSet<String>(fileds.keySet());
			keySet.remove("id");// 删除不存在的列
			// keySet.remove("contact"); 特殊类型列的自动处理还是用户处理 TODO
			log.debug("返回的列:" + keySet);
			// Map的列
			returnCols = keySet.toArray(new String[0]);
			log.debug(returnCols.length);
		}

		Set<RowFilterInterface> filters = new HashSet<RowFilterInterface>();

		if (pageSize != 0) {
			PageRowFilter page = new PageRowFilter(pageSize);
			filters.add(page);
		}

		for (int i = 0; i < columns.length; i++) {
			String column = columns[i];
			String value = values[i];
			if (value.isEmpty()) {
				continue;
			}
			column = column.contains(":") ? column : column.trim() + ":";
			ColumnValueFilter cvf = new ColumnValueFilter(column.getBytes(),
					ColumnValueFilter.CompareOp.EQUAL, value.trim().getBytes());
			filters.add(cvf);
		}
		RowFilterSet rowFilterSet = new RowFilterSet(
				RowFilterSet.Operator.MUST_PASS_ALL, filters);

		byte[][] colsResult = new byte[returnCols.length][];
		for (int i = 0; i < returnCols.length; i++) {
			returnCols[i] = returnCols[i].contains(":") ? returnCols[i]
					: returnCols[i].trim() + ":";
			log.debug("返回列名称：" + returnCols[i]);
			colsResult[i] = returnCols[i].getBytes();
		}

		Scanner scanner = null;
		List<Action> hbaserows2objects = null;
		try {
			// log.debug(colsResult);

			scanner = table.getScanner(colsResult, start, rowFilterSet);
			List<RowResult> result = getResults(scanner);
			log.debug(result);
			// Assert.assertTrue(result.size()>0);

			ConvertBean cb = new ConvertBean();
			hbaserows2objects = cb.hbaserows2objects(result, clazz);
		} catch (IOException e) {
			e.printStackTrace();
			throw new MyHBaseException(e.getLocalizedMessage());
		} finally {
			scanner.close();
		}
		return hbaserows2objects;
	}

	private List<RowResult> getResults(Scanner scanner)
			throws UnsupportedEncodingException {
		List<RowResult> result = new LinkedList<RowResult>();
		for (RowResult line : scanner) {
			log.debug(line);
			result.add(line);
		}
		return result;
	}

	/**
	 * 根据主键查询，不指定返回属性时，返回所有的属性
	 * 
	 * @param clazz
	 * @param rowKey
	 * @param columns
	 * @return
	 * @throws MyHBaseException
	 */
	public Action get(Class<? extends Action> clazz, String rowKey,
			String... columns) throws MyHBaseException {

		columns = checkAndTurnProperty2Column(clazz, columns);
//		Assert.assertNotNull(obj);
		String tableName=TableUtil.getTable(clazz);
		HTable table = MyHbaseUtil.getTable(tableName);

		try {
			RowResult row;
			if (columns.length > 0) {
				row = table.getRow(rowKey, columns);
			} else {
				row = table.getRow(rowKey);
			}
			if (row.size() <= 0) {
				return null;
			}
			ConvertBean cb = new ConvertBean();
			Action newInstance = clazz.newInstance();
			return cb.hbaserow2object(row, newInstance);

		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 验证列存在，并且去重 属性转hbase字段名称
	 * 
	 * @param clazz
	 * @param columns
	 * @return
	 */
	private String[] checkAndTurnProperty2Column(Class<? extends Action> clazz,
			String... columns) {

		if (columns.length > 0) {

			Map<String, Field> fileds = MyHbaseUtil.getFileds(clazz);
			Map<String, String> properties = MyHbaseUtil.getProperties(clazz);
			Set<String> set = new HashSet<String>();
			for (int i = 0; i < columns.length; i++) {
				// obj属性
				if (properties.containsKey(columns[i])) {
					String colname = properties.get(columns[i]);
					colname = colname.contains(":") ? colname : colname + ":";
					set.add(colname);
				}
				// hbase字段
				if (fileds.containsKey(columns[i])) {
					columns[i] = columns[i].contains(":") ? columns[i]
							: columns[i].trim() + ":";
					set.add(columns[i]);
				}
			}
			columns = set.toArray(new String[0]);
		}
		return columns;
	}


}