package com.supermy.utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;

import com.supermy.annotation.Column;
import com.supermy.annotation.ID;
import com.supermy.annotation.Many2One;
import com.supermy.annotation.One2Many;
import com.supermy.annotation.Transient;

/**
 * @author my 本地数据缓存
 * 
 */
public class MyHbaseUtil {
	private final static Log log = LogFactory.getLog(MyHbaseUtil.class);
	// private static MyHbaseUtil instance = new MyHbaseUtil();
	//
	// public static MyHbaseUtil getInstance() {
	// return instance;
	// }

	private static HBaseAdmin admin;
	private static HBaseConfiguration hbc;
	private static ConcurrentMap<String, HTable> tables = new ConcurrentHashMap<String, HTable>();
	private static ConcurrentMap<String, HTableDescriptor> tableDescs = new ConcurrentHashMap<String, HTableDescriptor>();
	private static ConcurrentMap<String, Map<String, Field>> fileds = new ConcurrentHashMap<String, Map<String, Field>>();
	private static ConcurrentMap<String, Map<String, String>> properties = new ConcurrentHashMap<String, Map<String, String>>();

	static {
		ResourceBundle bundle = ResourceBundle.getBundle("myhbase");
		String value = bundle.getObject("hbase.master").toString();
		hbc = new HBaseConfiguration();
		hbc.set("hbase.master", value);
		try {
			admin = new HBaseAdmin(hbc);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			log.error(e);
			throw new RuntimeException(e);
		}
	}

	public static HBaseAdmin getAdmin() {
		return admin;
	}

	public static HBaseConfiguration getConfig() {
		return hbc;
	}

	/**
	 * 缓存数据表
	 * 
	 * @param tableName
	 * @return
	 */
	public static HTable getTable(String tableName) {
		try {
			if (admin.tableExists(tableName)) {
				HTable table = tables.get(tableName);
				// synchronized (table) {
				if (table == null) {
					table = new HTable(getConfig(), tableName);
					tables.putIfAbsent(tableName, table);// 不存在插入，存在返回
				}
				// }
				return table;
			} else
				return null;
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 缓存数据结构描述
	 * 
	 * @param tableName
	 * @return
	 */
	public static HTableDescriptor getTableDesc(String tableName) {
		try {
			HTableDescriptor tableDesc = tableDescs.get(tableName);
			if (tableDesc == null) {
				if (admin.tableExists(tableName)) {
					HTable table = getTable(tableName);
					// synchronized (table) {
					tableDesc = table.getTableDescriptor();
					tableDescs.put(tableName, tableDesc);
					return tableDesc;
				}
			}
			return tableDesc;

		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 
	 * 递归读取所有字段（含父类，可以重复）
	 * 
	 * @param class1
	 * @param list
	 * @return
	 */
	private static List<Field> walk4field(Class<?> class1, List<Field> list) {
		Field[] fields = class1.getDeclaredFields();
		for (Field field : fields) {
			list.add(field);
		}
		if (class1.getSuperclass() != null) {
			list = walk4field(class1.getSuperclass(), list);
		}
		return list;
	}

	/**
	 * @param class1
	 * @return
	 */
	private static Map<String, Field> genFields(Class<?> class1) {
		Map<String, Field> result = new HashMap<String, Field>();
		List<Field> list = new ArrayList<Field>();
		list = walk4field(class1, list);
		for (Field field : list) {

			if (field.isAnnotationPresent(ID.class)) {
				result.put("id", field);
			}

			if (field.isAnnotationPresent(Transient.class)) {
				continue;
			}

			if (field.isAnnotationPresent(Column.class)) {
				result.put(field.getName(), field);
			}

			if (field.isAnnotationPresent(One2Many.class)) {
				One2Many o2m = field.getAnnotation(One2Many.class);
				log.debug(o2m);
			}

			if (field.isAnnotationPresent(Many2One.class)) {
				Many2One m2o = field.getAnnotation(Many2One.class);
				log.debug(m2o);
			}

			// 没有注释的使用默认Column的值
			if (!(field.isAnnotationPresent(ID.class)
					|| field.isAnnotationPresent(Transient.class)
					|| field.isAnnotationPresent(Column.class)
					|| field.isAnnotationPresent(One2Many.class) 
					|| field.isAnnotationPresent(Many2One.class))) {

				String fieldname = field.getName();
				if (!(field.getType().equals(Log.class) || fieldname
						.equalsIgnoreCase("serialVersionUID"))) {
					result.put(field.getName(), field);
				}

			}


			/*
			 * else { // TODO One2Many Many2One
			 * 
			 * Column annotation = field.getAnnotation(Column.class);
			 * log.debug(annotation); log.debug(field); if (annotation != null)
			 * result.put(field.getName(), field);
			 * 
			 * }
			 */}
		return result;
	}

	private static Map<String, String> genProperties(Class<?> class1) {
		Map<String, String> result = new HashMap<String, String>();
		List<Field> list = new ArrayList<Field>();
		list = walk4field(class1, list);
		for (Field field : list) {
			Column annotation = field.getAnnotation(Column.class);
			log.debug(annotation);
			log.debug(field);
			if (annotation != null)
				result.put(field.getName(), field.getName());
		}
		return result;
	}

	/**
	 * 缓存domain注释描述
	 * 
	 * @param class1
	 * @return
	 */
	public static Map<String, Field> getFileds(Class<?> class1) {
		Map<String, Field> map = fileds.get(class1.getName());
		if (map == null) {
			map = genFields(class1);
			fileds.put(class1.getName(), map);
		}
		return map;
	}

	public static Map<String, String> getProperties(Class<?> class1) {
		Map<String, String> map = properties.get(class1.getName());
		if (map == null) {
			map = genProperties(class1);
			properties.put(class1.getName(), map);
		}
		return map;
	}

	// /**
	// * 获取某个类的字段 hbase的名称与annon名称存在差异。
	// *
	// * @param class1
	// * @param name
	// * @return
	// */
	// @Deprecated
	// public static Field getField(Class<?> class1, String name) {
	// Field field = null;
	// try {
	// field = class1.getDeclaredField(name);
	// } catch (SecurityException e) {
	// e.printStackTrace();
	// throw new RuntimeException(e);
	// } catch (NoSuchFieldException e) {
	// e.printStackTrace();
	// log.info(e.getMessage());
	// throw new RuntimeException(e);
	// }
	// if (field == null) {
	// if (class1.getSuperclass() != null) {
	// field = getField(class1.getSuperclass(), name);
	// }
	// return field;
	// } else
	// return field;
	// }

}
