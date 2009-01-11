package com.supermy.domain;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang.SerializationUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.BatchUpdate;
import org.apache.hadoop.hbase.io.Cell;
import org.apache.hadoop.hbase.io.RowResult;
import org.apache.hadoop.hbase.util.Bytes;

import tv.movo.exception.MokiException;

import com.supermy.annotation.Column;
import com.supermy.annotation.ID;
import com.supermy.annotation.Table;
import com.supermy.annotation.ID.IdType;
import com.supermy.annotation.factory.GenKey;
import com.supermy.utils.MyHbaseUtil;

/**
 * @author my
 * 
 *         单个对象的生命周期
 */
public class Action {
	private static final Log log = LogFactory.getLog(Action.class);

	public void saveOrUpdate() {
		Class<? extends Action> clazz = getClass();
		Table t = clazz.getAnnotation(Table.class);
		HTable htable = MyHbaseUtil.getTable(t.name());
		BatchUpdate line = genSaveUpdateLine(clazz, t);
		try {
			htable.commit(line);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	/**
	 * 构造更新或者创建的行；方便多记录更新和创建
	 * 
	 * @param clazz
	 * @param t
	 * @return
	 */
	public BatchUpdate genSaveUpdateLine(Class<? extends Action> clazz, Table t) {
		BatchUpdate line;
		try {
			HTableDescriptor htd = MyHbaseUtil.getTableDesc(t.name());
			Collection<HColumnDescriptor> familiesDb = htd.getFamilies();
			Map<String, Field> fieldsObj = MyHbaseUtil.getFileds(clazz);

			String idValue = getId(t, fieldsObj);
			line = new BatchUpdate(idValue);
			// 设置rowvalue
			for (HColumnDescriptor columnDescriptor : familiesDb) {
				String columnName = columnDescriptor.getNameAsString();
				log.debug(columnName);
				typeConvert2Db(this, line, fieldsObj, columnName);
			}

		} catch (IllegalArgumentException e) {
			log.error(e.getMessage());
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return line;
	}

	/**
	 * 数据类型转换
	 * 
	 * @param line
	 * @param fieldsObj
	 * @param columnName
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws UnsupportedEncodingException
	 */
	private void typeConvert2Db(Object obj, BatchUpdate line,
			Map<String, Field> fieldsObj, String columnName)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException, UnsupportedEncodingException {
		Field field = fieldsObj.get(columnName);
		// TODO 其他类型支持
		// convertMap2Db(obj, line, columnName, field);
		convertMap2DbSerializable(obj, line, columnName, field);

		convertDate2Db(obj, line, columnName, field);
		convertInt2Db(obj, line, columnName, field);
		convertLong2Db(obj, line, columnName, field);
		convertString2Db(obj, line, columnName, field);

	}

	private void convertMap2DbSerializable(Object obj, BatchUpdate line,
			String columnName, Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException,
			UnsupportedEncodingException {
		if (!(field.getType().isAssignableFrom(String.class)
				|| field.getType().isAssignableFrom(int.class)
				|| field.getType().isAssignableFrom(Integer.class)
				|| field.getType().isAssignableFrom(long.class)
				|| field.getType().isAssignableFrom(Long.class) || field
				.getType().isAssignableFrom(Date.class))) {

			Object property = PropertyUtils.getProperty(obj, field.getName());
			log.debug("map:" + property);
			if (property == null)
				return;
			line.put(columnName + ":serializable", SerializationUtils
					.serialize((Serializable) property));
		}
	}

	private void convertMap2Db(Object obj, BatchUpdate line, String columnName,
			Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException,
			UnsupportedEncodingException {
		if (field.getType().isAssignableFrom(Map.class)) {

			Object property = PropertyUtils.getProperty(obj, field.getName());
			Map<String, Object> f = (Map<String, Object>) property;
			log.debug("map:" + f);
			if (f == null || f.size() <= 0) {
				return;
			}

			for (Entry<String, Object> entry : f.entrySet()) {
				StringBuffer key = new StringBuffer(columnName).append(":")
						.append(entry.getKey());
				Object value = entry.getValue();
				if (value.getClass().isAssignableFrom(int.class)
						|| value.getClass().isAssignableFrom(Integer.class)) {
					line.put(key.toString(), Bytes.toBytes((Integer) value));
					line.put(key.toString() + "_type", Integer.class.getName()
							.getBytes());
				}
				if (value.getClass().isAssignableFrom(long.class)
						|| value.getClass().isAssignableFrom(Long.class)) {
					line.put(key.toString(), Bytes.toBytes((Long) value));
					line.put(key.toString() + "_type", Long.class.getName()
							.getBytes());
				}
				if (value.getClass().isAssignableFrom(Date.class)) {
					line.put(key.toString(), Bytes.toBytes(((Date) value)
							.getTime()));
					line.put(key.toString() + "_type", Date.class.getName()
							.getBytes());
				}
				if (value.getClass().isAssignableFrom(String.class)) {
					line.put(key.toString(), value.toString().getBytes(
							HConstants.UTF8_ENCODING));
					line.put(key.toString() + "_type", String.class.getName()
							.getBytes());
				}
			}

		}
	}

	private void convertString2Db(Object obj, BatchUpdate line,
			String columnName, Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException,
			UnsupportedEncodingException {
		if (field.getType().isAssignableFrom(String.class)) {
			String value = BeanUtils.getProperty(obj, field.getName());
			if (StringUtils.isEmpty(value)) {
				return;
			}
			line
					.put(columnName + ":", value
							.getBytes(HConstants.UTF8_ENCODING));
		}
	}

	private void convertLong2Db(Object obj, BatchUpdate line,
			String columnName, Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		if (field.getType().isAssignableFrom(Long.class)
				|| field.getType().isAssignableFrom(long.class)) {
			log.debug("long:" + columnName);
			Long f = (Long) PropertyUtils.getProperty(obj, field.getName());
			line.put(columnName + ":", Bytes.toBytes(f));
		}
	}

	private void convertInt2Db(Object obj, BatchUpdate line, String columnName,
			Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		if (field.getType().isAssignableFrom(int.class)
				|| field.getType().isAssignableFrom(Integer.class)) {
			log.debug("int:" + columnName);
			int f = (Integer) PropertyUtils.getProperty(obj, field.getName());
			line.put(columnName + ":", Bytes.toBytes(f));
		}
	}

	private void convertDate2Db(Object obj, BatchUpdate line,
			String columnName, Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		if (field.getType().isAssignableFrom(Date.class)) {
			log.debug("Date:" + columnName);
			Date f = (Date) PropertyUtils.getProperty(obj, field.getName());
			line.put(columnName + ":", Bytes.toBytes(f.getTime()));
		}
	}

	/**
	 * 创建或者更新，根据ID策略，自动生成ID
	 * 
	 * @param t
	 * @param fieldsObj
	 * @return
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws MokiException
	 */
	private String getId(Table t, Map<String, Field> fieldsObj) {
		String idValue = null;
		try {
			idValue = BeanUtils.getProperty(this, "id");
			// 构造ID value;创建和更新
			if (StringUtils.isBlank(idValue)) {
				Field idField = fieldsObj.get("id");
				// Field idField=MyHbaseUtil.getField(clazz, "id");
				ID annotation = idField.getAnnotation(ID.class);
				IdType value = annotation.value();
				if (value.equals(IdType.INC)) {
					GenKey gk = new GenKey();
					idValue = gk.genAutoIncreasePrimaryKey(t.name());
				} else {
					idValue = UUID.randomUUID().toString();
				}
				BeanUtils.setProperty(this, "id", idValue);
			}
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (MokiException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return idValue;
	}

	public void delete() {
		try {
			Class<? extends Action> clazz = getClass();
			Table t = clazz.getAnnotation(Table.class);
			HTable htable = MyHbaseUtil.getTable(t.name());
			String idValue = BeanUtils.getProperty(this, "id");
			if (StringUtils.isEmpty(idValue)) {
				throw new RuntimeException("id don't is null!");
			}
			htable.deleteAll(idValue);

		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

	}

	public Object get() {
		Object action = this;
		try {
			Class<? extends Action> clazz = getClass();
			Table t = clazz.getAnnotation(Table.class);
			HTable htable = MyHbaseUtil.getTable(t.name());

			String idValue = BeanUtils.getProperty(action, "id");
			if (idValue == null) {
				throw new RuntimeException("id don't is null");
			}

			RowResult row = htable.getRow(idValue);
			log.debug(row);
			if (row.size() <= 0) {
				return null;
			}

			for (Entry<String, Field> entry : MyHbaseUtil.getFileds(clazz)
					.entrySet()) {
				Field field = entry.getValue();
				Column annotation = field.getAnnotation(Column.class);
				if (annotation == null)
					continue;

				String name = annotation.name();
				Cell value = row.get(name + ":");
				log.debug("name:" + name + " value:" + value);

				if ((value == null || value.getValue() == null)
						&& !field.getType().isAssignableFrom(Map.class)) {
					continue;
				}
				// String attrValue = new String(value.getValue(),
				// HConstants.UTF8_ENCODING);

				// convertMap2Obj(action, row, field, name, value);
				convertMap2ObjSerializable(action, row, field, name);

				convertDate2Obj(action, field, value);

				convertInt2Obj(action, field, value);

				convertLong2Obj(action, field, value);

				convertString2Obj(action, field, value);
			}

			// PropertyDescriptor[] propertyDescriptors = PropertyUtils
			// .getPropertyDescriptors(clazz);
			// for (PropertyDescriptor propertyDescriptor : propertyDescriptors)
			// {
			// log.debug(propertyDescriptor.getName());
			// log.debug(row.get(propertyDescriptor.getName()));
			// //
			// log.debug(propertyDescriptor.getValue(propertyDescriptor.getName()));
			// }
			//

		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return action;
	}

	private void convertString2Obj(Object action, Field field, Cell value)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException, UnsupportedEncodingException {
		if (field.getType().isAssignableFrom(String.class)) {
			String attrValue = new String(value.getValue(),
					HConstants.UTF8_ENCODING);

			PropertyUtils.setProperty(action, field.getName(), attrValue);
			log.debug("common type name:" + field.getName() + " value:"
					+ attrValue);
		}
	}

	private void convertLong2Obj(Object action, Field field, Cell value)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		if (field.getType().isAssignableFrom(Long.class)) {
			PropertyUtils.setProperty(action, field.getName(), Bytes
					.toLong(value.getValue()));

		}
	}

	private void convertInt2Obj(Object action, Field field, Cell value)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		if (field.getType().isAssignableFrom(Integer.class)) {
			PropertyUtils.setProperty(action, field.getName(), Bytes
					.toInt(value.getValue()));

		}
	}

	private void convertDate2Obj(Object action, Field field, Cell value)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {
		if (field.getType().isAssignableFrom(Date.class)) {
			PropertyUtils.setProperty(action, field.getName(), new Date(Bytes
					.toLong(value.getValue())));

		}
	}

	private void convertMap2ObjSerializable(Object action, RowResult row,
			Field field, String name) throws UnsupportedEncodingException,
			IllegalAccessException, InvocationTargetException,
			NoSuchMethodException, ClassNotFoundException {

		if (!(field.getType().isAssignableFrom(String.class)
				|| field.getType().isAssignableFrom(int.class)
				|| field.getType().isAssignableFrom(Integer.class)
				|| field.getType().isAssignableFrom(long.class)
				|| field.getType().isAssignableFrom(Long.class) || field
				.getType().isAssignableFrom(Date.class))) {

			log.debug("map type name:" + name);
			Cell value = row.get(name + ":serializable");

			Object deserialize = SerializationUtils.deserialize(value
					.getValue());
			// Map<String, Object> obj = (Map<String, Object>) deserialize;
			log.debug(deserialize);
			PropertyUtils.setProperty(action, field.getName(), deserialize);
		}
	}

	private void convertMap2Obj(Object action, RowResult row, Field field,
			String name, Cell attrValue) throws UnsupportedEncodingException,
			IllegalAccessException, InvocationTargetException,
			NoSuchMethodException, ClassNotFoundException {

		log.debug("================:" + field.getType().getName());
		log.debug(field.getType().isAssignableFrom(Map.class));

		if (field.getType().isAssignableFrom(Map.class)) {
			log.debug("map type name:" + name);
			Map<String, Object> obj = new HashMap<String, Object>();
			for (byte[] key1 : row.keySet()) {
				String key = new String(key1);
				if (key.startsWith(name) && !key.contains("_type")
						&& !key.contains("serializable")) {
					Cell valueline = row.get(key);
					byte[] value = valueline.getValue();
					if (valueline == null || value == null) {
						continue;
					}
					log.debug(key);
					String type = new String(row.get(key + "_type").getValue());
					log.debug(type);
					key = key.substring((name + ":").length());
					if (type.equalsIgnoreCase(Integer.class.getName())) {
						obj.put(key, Bytes.toInt(value));
					}
					if (type.equalsIgnoreCase(Long.class.getName())) {
						obj.put(key, Bytes.toLong(value));
					}
					if (type.equalsIgnoreCase(Date.class.getName())) {
						obj.put(key, new Date(Bytes.toLong(value)));
					}
					if (type.equalsIgnoreCase(String.class.getName())) {
						String vlaue1 = new String(value,
								HConstants.UTF8_ENCODING);
						obj.put(key, vlaue1);
					}
				}
			}
			log.debug(obj);
			PropertyUtils.setProperty(action, field.getName(), obj);

		}
	}
}
