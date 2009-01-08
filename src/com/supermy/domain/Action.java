package com.supermy.domain;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;

import org.apache.commons.beanutils.BeanUtils;
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

import tv.movo.exception.MokiException;

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
//			Map<String, Field> fieldsObj = MyHbaseUtil.getFileds(clazz);

			String idValue = getId(t, clazz);
			line = new BatchUpdate(idValue);
			// 设置rowvalue
			for (HColumnDescriptor columnDescriptor : familiesDb) {
				log.debug(columnDescriptor.getNameAsString());

				//Field field = fieldsObj.get(columnDescriptor.getNameAsString());
				Field field = MyHbaseUtil.getField(clazz,columnDescriptor.getNameAsString());
				String value = BeanUtils.getProperty(this, field.getName());
				if (StringUtils.isEmpty(value)) {
					continue;
				}
				line.put(columnDescriptor.getNameAsString() + ":", value
						.getBytes(HConstants.UTF8_ENCODING));
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
	 * @param clazz
	 * @return
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalAccessException
	 * @throws IllegalAccessException
	 * @throws InvocationTargetException
	 * @throws NoSuchMethodException
	 * @throws MokiException
	 */
	private String getId(Table t, Class<? extends Action> clazz) {
		String idValue = null;
		try {
			idValue = BeanUtils.getProperty(this, "id");
			// 构造ID value;创建和更新
			if (StringUtils.isBlank(idValue)) {
				//Field idField = clazz.get("id");
				Field idField=MyHbaseUtil.getField(clazz, "id");
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

//			Map<String, Field> fieldsObj = MyHbaseUtil.getFileds(clazz);
			
			RowResult row = htable.getRow(idValue);
			log.debug(row);
			if (row.size() <= 0) {
				return null;
			}
			for (Entry<byte[], Cell> entry : row.entrySet()) {
				String key = new String(entry.getKey()).replace(":", "");
				String value = new String(entry.getValue().getValue(),
						HConstants.UTF8_ENCODING);
				if (value == null) {
					continue;
				}
				log.debug(key + "=>" + value);
				//Field field = fieldsObj.get(key);
				Field field = MyHbaseUtil.getField(clazz, key);
				BeanUtils.setProperty(this, field.getName(), value);
			}

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
