package com.segmeno.kodo.database;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.segmeno.kodo.annotation.Column;
import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.annotation.PrimaryKey;

/**
 * @author tdr, chu
 */
public abstract class DatabaseEntity {

	private static final Logger LOGGER = LogManager.getLogger(DatabaseEntity.class);

	// to be used within kodo framework only
	@SuppressWarnings("unused")
	private String tableAlias;
	private final Field primaryKey;

	private final transient HashMap<Class, MetaInfo> class2meta = new HashMap<>(64);

	private final static class MetaInfo {
		protected Field pk;
		protected ArrayList<Field> fields = new ArrayList<>(64);
		protected HashSet<String> withPrimaryKey = new HashSet<String>(64);
		protected HashSet<String> withoutPrimaryKey = new HashSet<String>(64);
	}

	private transient MetaInfo metaInfo;

	public DatabaseEntity() {
		final Class clazz = this.getClass();
		metaInfo = class2meta.get(clazz);
		if (metaInfo == null) {
			synchronized (class2meta) {
				metaInfo = class2meta.get(clazz);
				if (metaInfo == null) {
					metaInfo = new MetaInfo();
					metaInfo.pk = getFields(clazz, metaInfo);
					class2meta.put(clazz, metaInfo);
				}
			}
		}

		primaryKey = metaInfo.pk;
		if (primaryKey == null) {
			throw new RuntimeException(this.getClass().getName() + " has not @PrimaryKey defined");
		}
	}

	public ArrayList<Field> getCachedDbFields() {
		return metaInfo.fields;
	}

	private static Field getFields(Class startClass, MetaInfo metaInfo) {
		Field pk = null;
		Class clazz = startClass;
		while (clazz != null && !DatabaseEntity.class.equals(clazz)) {
			for (final Field field : clazz.getDeclaredFields()) {
				field.setAccessible(true);
				if (field.getAnnotation(DbIgnore.class) != null) {
					continue;
				}
				metaInfo.fields.add(field);

				// do not overwrite once found pk, with that from a base class
				if (field.getAnnotation(PrimaryKey.class) != null) {
					if (pk != null) {
						LOGGER.error("For " + startClass + " we found primary key " + pk + " and now also " + field + ", we will use the first one");
					} else {
						pk = field;
						metaInfo.withPrimaryKey.add(field.getName());
					}
				} else if (Collection.class.isAssignableFrom(field.getType())) {
					continue;
				} else if (field.getAnnotation(Column.class) != null && !field.getAnnotation(Column.class).columnName().isEmpty()) {
					metaInfo.withoutPrimaryKey.add(field.getAnnotation(Column.class).columnName());
					metaInfo.withPrimaryKey.add(field.getAnnotation(Column.class).columnName());
				} else if (field.getAnnotation(MappingRelation.class) != null && field.getAnnotation(MappingRelation.class).mappingTableName().isEmpty()) {
					metaInfo.withoutPrimaryKey.add(field.getAnnotation(MappingRelation.class).masterColumnName());
					metaInfo.withPrimaryKey.add(field.getAnnotation(MappingRelation.class).masterColumnName());
				} else {
					metaInfo.withoutPrimaryKey.add(field.getName());
					metaInfo.withPrimaryKey.add(field.getName());
				}
			}
			clazz = clazz.getSuperclass();
		}

		metaInfo.fields.trimToSize();

		return pk;
	}

	/**
	 *
	 * @return the tableName of this entity
	 */
	public abstract String getTableName();

	/**
	 *
	 * @return the column names of this entity
	 */
	public Set<String> getColumnNames(final boolean includePrimaryKeyColumn) throws Exception {
		return includePrimaryKeyColumn ? metaInfo.withPrimaryKey : metaInfo.withoutPrimaryKey;
	}

	/**
	 * * retrieves all fields which should be persisted in the db when saving the
	 * inheriting object
	 * 
	 * @return a map presentation of the object
	 * @throws Exception
	 */
	public Map<String, Object> toMap() throws Exception {
		final Map<String, Object> map = new HashMap<String, Object>();
		for (final Field f : metaInfo.fields) {
			if (Collection.class.isAssignableFrom(f.getType())) {
				continue;
			}
			final String colName;
			if (f.getAnnotation(Column.class) != null && !f.getAnnotation(Column.class).columnName().isEmpty()) {
				colName = f.getAnnotation(Column.class).columnName().toLowerCase();
				map.put(colName, f.get(this));
			} else if (f.getAnnotation(MappingRelation.class) != null && f.getAnnotation(MappingRelation.class).mappingTableName().isEmpty()) {
				colName = f.getAnnotation(MappingRelation.class).masterColumnName().toLowerCase();
				if (DatabaseEntity.class.isAssignableFrom(f.getType())) {
					final DatabaseEntity elem = (DatabaseEntity) f.get(this);
					if (elem == null) {
						map.put(colName, null);
					} else {
						final Object epk = elem.getPrimaryKeyValue();
						if (epk == null) {
							throw new RuntimeException("With One to One Relations the linked object has to exist (PK has to be set)!");
						}
						map.put(colName, epk);
					}
				} else {
					map.put(colName, f.get(this));
				}
			} else {
				colName = f.getName().toLowerCase();
				map.put(colName, f.get(this));
			}
		}
		return map;
	}

	/**
	 *
	 * @return the primary key column name
	 * @throws Exception
	 */
	public String getPrimaryKeyColumn() throws Exception {
		if (primaryKey != null) {
			return primaryKey.getName();
		}
		throw new Exception("Could not find primary key for entity '" + this.getClass().getName() + "'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!");
	}

	/**
	 * fills the inheriting object from the values from the map
	 * 
	 * @param map all values to the corresponding field names
	 */
	public void fromMap(final Map<String, Object> map) throws Exception {

		for (final Field f : metaInfo.fields) {
			f.setAccessible(true);
			if (map.get(f.getName()) != null) {
				f.set(this, map.get(f.getName()));
			}
		}
	}

	/**
	 * sets the primary key field
	 *
	 * @param id
	 * @throws Exception
	 */
	public void setPrimaryKeyValue(final Object id) throws Exception {
		if (primaryKey == null) {
			throw new Exception("Could not find primary key for entity '" + this.getClass().getName() + "'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!");
		}
		primaryKey.set(this, DataAccessManager.convertTo(primaryKey.getType(), id));
	}

	/**
	 * returns the value of the primary key field
	 *
	 * @return
	 */
	public Object getPrimaryKeyValue() {

		if (primaryKey == null) {
			final String msg = "Could not find primary key for entity '" + this.getClass().getName() + "'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!";
			LOGGER.error(msg);
			throw new RuntimeException(msg);
		}
		try {
			return primaryKey.get(this);
		} catch (final Exception e) {
			final String msg = "error during search for primary key field";
			LOGGER.error(msg);
			throw new RuntimeException(msg);
		}
	}

	/**
	 * convenience method to access map values which are integers
	 *
	 * @param map
	 * @param key
	 * @return
	 */
	protected Integer getIntOrNull(final Map<String, Object> map, final String key) {
		if (map == null || map.get(key) == null) {
			return null;
		}
		return Integer.valueOf(String.valueOf(map.get(key)));
	}

	/**
	 * convenience method to access map values which are booleans
	 *
	 * @param map
	 * @param key
	 * @return
	 */
	protected Boolean getBoolOrNull(final Map<String, Object> map, final String key) {
		if (map == null || map.get(key) == null) {
			return null;
		}
		return Boolean.valueOf(String.valueOf(map.get(key)));
	}

	/**
	 * convenience method to access map values which are strings
	 *
	 * @param map
	 * @param key
	 * @return
	 */
	protected String getStringOrNull(final Map<String, Object> map, final String key) {
		if (map == null || map.get(key) == null) {
			return null;
		}
		if (map.get(key) instanceof Date) {
			return String.valueOf(map.get(key));
		}
		return null;
	}

	/**
	 * convenience method to access map values which are dates
	 *
	 * @param map
	 * @param key
	 * @return
	 */
	protected Date getDateOrNull(final Map<String, Object> map, final String key) {
		if (map == null || map.get(key) == null) {
			return null;
		}
		if (map.get(key) instanceof Date) {
			return (Date) (map.get(key));
		}
		return null;
	}

}
