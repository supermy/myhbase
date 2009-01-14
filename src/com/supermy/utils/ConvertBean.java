package com.supermy.utils;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

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
import tv.movo.utils.MD5;

import com.supermy.annotation.Column;
import com.supermy.annotation.ID;
import com.supermy.annotation.Table;
import com.supermy.annotation.ID.IdType;
import com.supermy.annotation.factory.GenKey;
import com.supermy.domain.Action;

/**
 * @author my hbase数据和对象之间的转换
 */
public class ConvertBean {
	private static final Log log = LogFactory.getLog(ConvertBean.class);

	public ConvertBean() {
		super();
		log.debug("ConvertBean create ... ... ");
	}

	// Class<T> obj;

	/**
	 * @param list
	 * @param clazz
	 * @return
	 */
	public List<Action> hbaserows2objects(List<RowResult> list,
			Class<? extends Action> clazz) {
		// 一次获取多个对象
		List<Action> results = new ArrayList<Action>();
		try {
			for (RowResult line : list) {
				Action newInstance;
				newInstance = clazz.newInstance();

				results.add(hbaserow2object(line, newInstance));
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return results;
	}

	/**
	 * 同步多个对象到数据库
	 * 
	 * @param list
	 * @return
	 */
	public List<BatchUpdate> objects2hbaswrows(List<Action> list) {
		// 多个对象同步到数据库
		List<BatchUpdate> results = new ArrayList<BatchUpdate>();
		for (Action action : list) {
			BatchUpdate line = object2hbaswrow(action);
			results.add(line);
		}
		return results;
	}

	@Deprecated
	public Action hbaserow2object(Class<? extends Action> clazz, String key) {
		log.debug("hbaserow2object start ... ...");
		log.debug(clazz.getName());
		Action newInstance;
		try {
			newInstance = clazz.newInstance();
			// newInstance =new User();

		} catch (InstantiationException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		newInstance.setId(key);
		return hbaserow2object(newInstance);
	}

	public Action hbaserow2object(Action obj) {
		log.debug("hbaserow2object(Action obj)");
		String tableName = obj.getClass().getAnnotation(Table.class).name();
		HTable htable = MyHbaseUtil.getTable(tableName);
		String key = obj.getId();
		try {

			if (key == null) {
				throw new RuntimeException("id don't is null");
			}

			RowResult row = htable.getRow(key);
			log.debug(row);
			if (row.size() <= 0) {
				return null;
			}

			obj = hbaserow2object(row, obj);

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
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		return obj;
	}

	/**
	 * 转换Object对象到hbase row
	 * 
	 * @param obj
	 * @return
	 */
	public BatchUpdate object2hbaswrow(Action obj) {
		String tableName = obj.getClass().getAnnotation(Table.class).name();
		BatchUpdate line;
		try {
			HTableDescriptor htd = MyHbaseUtil.getTableDesc(tableName);
			Collection<HColumnDescriptor> familiesDb = htd.getFamilies();
			Map<String, Field> fieldsObj = MyHbaseUtil
					.getFileds(obj.getClass());
			// 创建 选择主键创建机制
			String idValue = getId(tableName, fieldsObj, obj);
			line = new BatchUpdate(idValue);
			// 设置rowvalue
			for (HColumnDescriptor columnDescriptor : familiesDb) {
				String columnName = columnDescriptor.getNameAsString();
				log.debug(columnName);
				Field field = fieldsObj.get(columnName);

				// TODO 其他类型支持
				map2db(obj, line, columnName, field);
				other2db4serializable(this, line, columnName, field);

				Object propertyValue = PropertyUtils.getProperty(obj, field
						.getName());
				if (propertyValue == null) {
					continue;
				}

				date2db(line, columnName, propertyValue, field);
				int2db(line, columnName, propertyValue, field);
				long2db(line, columnName, propertyValue, field);
				string2db(line, columnName, propertyValue, field);

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
	 * 创建或者更新，根据ID策略，自动生成ID
	 * 
	 * @param t
	 * @param fieldsObj
	 * @param obj
	 * @return
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws MokiException
	 */
	private String getId(String tableName, Map<String, Field> fieldsObj,
			Action obj) {
		String idValue = null;
		try {
			// idValue = BeanUtils.getProperty(this, "id");
			idValue = obj.getId();
			// 构造ID value;创建和更新
			if (StringUtils.isBlank(idValue)) {
				
				//md5的配置，完全可以直接给ID赋值
				ID childId = obj.getClass().getAnnotation(ID.class);
				if (childId != null) {
					if (childId.value().equals(IdType.MD5)) {
						Object property = PropertyUtils.getProperty(obj,
								childId.md5FieldName());
						if (property != null) {
							return MD5.getMD5(property.toString().getBytes());
						}
						throw new RuntimeException(
								"ID is md5 ,property value is not null !");
					}
				}
				//md5 end

				Field idField = fieldsObj.get("id");
				ID annotation = idField.getAnnotation(ID.class);
				IdType value = annotation.value();
				if (value.equals(IdType.INC)) {
					GenKey gk = new GenKey();
					idValue = gk.genAutoIncreasePrimaryKey(tableName);
				} else {
					idValue = UUID.randomUUID().toString();
				}
			}
		} catch (MokiException e) {
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
		return idValue;
	}

	private void other2db4serializable(Object obj, BatchUpdate line,
			String columnName, Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException,
			UnsupportedEncodingException {
		if (!(field.getType().isAssignableFrom(String.class)
				|| field.getType().isAssignableFrom(int.class)
				|| field.getType().isAssignableFrom(Integer.class)
				|| field.getType().isAssignableFrom(Map.class)
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

	/**
	 * 
	 * @param obj
	 * @param line
	 * @param columnName
	 * @param field
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws UnsupportedEncodingException
	 */
	private void map2db(Object obj, BatchUpdate line, String columnName,
			Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException,
			UnsupportedEncodingException {

		if (field.getType().isAssignableFrom(Map.class)) {
			// 属性值
			Object property = PropertyUtils.getProperty(obj, field.getName());
			if (property == null) {
				return;
			}
			// 类型转换
			Map<String, Object> propertyValue = (Map<String, Object>) property;
			log.debug("map:" + propertyValue);
			if (propertyValue.size() <= 0) {
				return;
			}

			// map值中逐个处理
			for (Entry<String, Object> entry : propertyValue.entrySet()) {
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

	private void string2db(BatchUpdate line, String columnName, Object value,
			Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException,
			UnsupportedEncodingException {
		if (field.getType().isAssignableFrom(String.class)) {
			line.put(columnName + ":", ((String) value)
					.getBytes(HConstants.UTF8_ENCODING));
		}
	}

	private void long2db(BatchUpdate line, String columnName, Object value,
			Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		if (field.getType().isAssignableFrom(Long.class)
				|| field.getType().isAssignableFrom(long.class)) {
			log.debug("long:" + columnName);
			line.put(columnName + ":", Bytes.toBytes((Long) value));
		}
	}

	private void int2db(BatchUpdate line, String columnName, Object value,
			Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		if (field.getType().isAssignableFrom(int.class)
				|| field.getType().isAssignableFrom(Integer.class)) {
			log.debug("int:" + columnName);
			line.put(columnName + ":", Bytes.toBytes((Integer) value));
		}
	}

	private void date2db(BatchUpdate line, String columnName, Object value,
			Field field) throws IllegalAccessException,
			InvocationTargetException, NoSuchMethodException {
		if (field.getType().isAssignableFrom(Date.class)) {
			line.put(columnName + ":", Bytes.toBytes(((Date) value).getTime()));
		}
	}

	/**
	 * @param row
	 * @param obj
	 * @return
	 */
	public Action hbaserow2object(RowResult row, Action obj) {
		log.debug("hbaserow2object(RowResult row, Action obj)");
		try {
			
			String key = new String(row.getRow());
			obj.setId(key);

			// 对象的所有字段
			for (Entry<String, Field> entry : MyHbaseUtil.getFileds(
					obj.getClass()).entrySet()) {
				// 某个字段
				Field field = entry.getValue();
				Column annotation = field.getAnnotation(Column.class);
				// 没有Column注解的字段不予以处理
				if (annotation == null)
					continue;

				// 特殊类型转换
				map2obj(obj, row, field, annotation.name());
				other2obj4serializable(obj, row, field, annotation.name());

				Cell value = row.get(annotation.name() + ":");
				log.debug("name:" + annotation.name() + " value:" + value);
				// map 和其他类型可能处理不到FIXME
				if ((value == null || value.getValue() == null)) {
					continue;
				}

				// 常用类型转换
				date2obj(obj, field, value);
				int2obj(obj, field, value);
				long2obj(obj, field, value);
				string2obj(obj, field, value);
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return obj;
	}

	private void string2obj(Object action, Field field, Cell value)
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

	private void long2obj(Object action, Field field, Cell value)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {

		if (field.getType().isAssignableFrom(Long.class)
				|| field.getType().isAssignableFrom(long.class)) {
			PropertyUtils.setProperty(action, field.getName(), Bytes
					.toLong(value.getValue()));

		}
	}

	private void int2obj(Object action, Field field, Cell value)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {

		if (field.getType().isAssignableFrom(Integer.class)
				|| field.getType().isAssignableFrom(int.class)) {
			PropertyUtils.setProperty(action, field.getName(), Bytes
					.toInt(value.getValue()));

		}

	}

	private void date2obj(Object action, Field field, Cell value)
			throws IllegalAccessException, InvocationTargetException,
			NoSuchMethodException {

		if (field.getType().isAssignableFrom(Date.class)) {
			PropertyUtils.setProperty(action, field.getName(), new Date(Bytes
					.toLong(value.getValue())));

		}

	}

	private void other2obj4serializable(Object action, RowResult row,
			Field field, String name) throws UnsupportedEncodingException,
			IllegalAccessException, InvocationTargetException,
			NoSuchMethodException, ClassNotFoundException {

		if (!(field.getType().isAssignableFrom(String.class)
				|| field.getType().isAssignableFrom(int.class)
				|| field.getType().isAssignableFrom(Integer.class)
				|| field.getType().isAssignableFrom(Map.class)
				|| field.getType().isAssignableFrom(long.class)
				|| field.getType().isAssignableFrom(Long.class) || field
				.getType().isAssignableFrom(Date.class))) {

			log.debug("map type name:" + name);
			Cell value = row.get(name + ":serializable");

			Object deserialize = SerializationUtils.deserialize(value
					.getValue());
			log.debug(deserialize);
			PropertyUtils.setProperty(action, field.getName(), deserialize);
		}
	}

	private void map2obj(Object action, RowResult row, Field field, String name)
			throws UnsupportedEncodingException, IllegalAccessException,
			InvocationTargetException, NoSuchMethodException,
			ClassNotFoundException {

		log.debug("================:" + field.getType().getName());

		if (field.getType().isAssignableFrom(Map.class)) {
			log.debug("map type name:" + name);
			Map<String, Object> obj = new HashMap<String, Object>();

			for (byte[] key1 : row.keySet()) {
				String key = new String(key1);
				if (key.startsWith(name) && !key.contains("_type")
						&& !key.contains("serializable")) {
					Cell valueline = row.get(key);
					byte[] value = valueline.getValue();
					if (value == null) {
						continue;
					}
					log.debug(key);
					String type = new String(row.get(key + "_type").getValue());
					log.debug(type);
					// 得到属性map中的key
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
						obj.put(key,
								new String(value, HConstants.UTF8_ENCODING));
					}
				}
			}
			log.debug(obj);
			PropertyUtils.setProperty(action, field.getName(), obj);

		}
	}

}
