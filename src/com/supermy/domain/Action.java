package com.supermy.domain;

import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
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

				Field field = fieldsObj.get(columnName);
				// TODO 其他类型支持
				if (field.getType().isAssignableFrom(Map.class)) {
					Map<String, String> f = new HashMap<String, String>();
					f = (Map<String, String>) PropertyUtils.getProperty(this,
							field.getName());
					log.debug("map:" + f);
					if (f == null || f.size() <= 0) {
						continue;
					}
					for (Entry<String, String> entry : f.entrySet()) {
						StringBuffer key = new StringBuffer(columnName).append(
								":").append(entry.getKey());

						line.put(key.toString(), entry.getValue().getBytes(
								HConstants.UTF8_ENCODING));
					}

				}

				if (field.getType().isAssignableFrom(Date.class)) {
					Date f = (Date) PropertyUtils.getProperty(this, field
							.getName());
					log.debug("date:" + f);
					line.put(columnName + ":", Bytes.toBytes(f.getTime()));
				}
				if (field.getType().isAssignableFrom(int.class)) {
					int f = (Integer) PropertyUtils.getProperty(this, field
							.getName());
					log.debug("date:" + f);
					line.put(columnName + ":", Bytes.toBytes(f));
				}
				if (field.getType().isAssignableFrom(Long.class)) {
					Long f = (Long) PropertyUtils.getProperty(this, field
							.getName());
					log.debug("date:" + f);
					line.put(columnName + ":", Bytes.toBytes(f));
				}

				if (field.getType().isAssignableFrom(String.class)) {
					String value = BeanUtils.getProperty(this, field.getName());
					if (StringUtils.isEmpty(value)) {
						continue;
					}
					line.put(columnName + ":", value
							.getBytes(HConstants.UTF8_ENCODING));
				}
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
		try {
			Class<? extends Action> clazz = getClass();
			Table t = clazz.getAnnotation(Table.class);
			HTable htable = MyHbaseUtil.getTable(t.name());

			String idValue = BeanUtils.getProperty(this, "id");
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

				if (value == null || value.getValue() == null) {
					continue;
				}
				String attrValue = new String(value.getValue(),
						HConstants.UTF8_ENCODING);

				if (field.getType().isAssignableFrom(Map.class)) {
					log.debug("map type name:" + name + " value:" + attrValue);
					Map<String, String> obj = new HashMap<String, String>();
					for (byte[] key1 : row.keySet()) {
						String key = new String(key1);
						if (key.startsWith(name)) {
							Cell valueline = row.get(key);
							if (valueline == null
									|| valueline.getValue() == null) {
								continue;
							}
							key = key.substring((name + ":").length());
							String vlaue1 = new String(valueline.getValue(),
									HConstants.UTF8_ENCODING);
							obj.put(key, vlaue1);
						}
					}
					PropertyUtils.setProperty(this, field.getName(), obj);

				}

				if (field.getType().isAssignableFrom(Date.class)) {
					PropertyUtils.setProperty(this, field.getName(), new Date(
							Bytes.toLong(value.getValue())));

				}

				if (field.getType().isAssignableFrom(Integer.class)) {
					PropertyUtils.setProperty(this, field.getName(), Bytes
							.toInt(value.getValue()));

				}

				if (field.getType().isAssignableFrom(Long.class)) {
					PropertyUtils.setProperty(this, field.getName(), Bytes
							.toLong(value.getValue()));

				}

				if (field.getType().isAssignableFrom(String.class)) {
					PropertyUtils.setProperty(this, field.getName(), attrValue);
					log.debug("common type name:" + field.getName() + " value:"
							+ value);
				}
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
		}
		return this;
	}
}
