package com.segmeno.kodo.database;

import com.segmeno.kodo.annotation.Column;
import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.annotation.PrimaryKey;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class DatabaseEntity {

	private static final Logger LOGGER = LogManager.getLogger(DatabaseEntity.class);

	// to be used within kodo framework only
	@SuppressWarnings("unused")
	private String tableAlias;
	private final Field primaryKey;

	private final transient HashMap<Class, ArrayList<Field>> class2fields = new HashMap<>(64);
	private final transient HashMap<Class, Field> class2pk = new HashMap<>(64);
	private transient ArrayList<Field> fields;

	public DatabaseEntity() {
		final Class clazz = this.getClass();
		fields = class2fields.get(clazz);
		if(fields == null) {
			synchronized(class2fields) {
				fields = class2fields.get(clazz);
				if(fields == null) {
					fields = new ArrayList<>(128);
					final Field pk = getFields(clazz, fields);
					class2pk.put(clazz, pk);
					fields.trimToSize();
					class2fields.put(clazz, fields);
				}
			}
		}

		primaryKey = class2pk.get(clazz);
		if(primaryKey == null) {
		  throw new RuntimeException(this.getClass().getName() + " has not @PrimaryKey defined");
		}
	}

	public ArrayList<Field> getCachedDbFields() {
		return fields;
	}
	
	private Field getFields(Class startClass, ArrayList<Field> fields) {
		Field pk = null;
		Class clazz = startClass;
		while(clazz != null && !DatabaseEntity.class.equals(clazz)) {
			for (final Field field : clazz.getDeclaredFields()) {
				field.setAccessible(true);
				if (field.getAnnotation(DbIgnore.class) != null) {
					continue;
				}
				// do not overwrite once found pk, with that from a base class
				if (field.getAnnotation(PrimaryKey.class) != null) {
					if(pk != null) {
						LOGGER.warn("For " + startClass + " we found primary key " + pk + " and now also " + field + ", we will use the first one");
					} else {
						pk = field;
					}
				}
				fields.add(field);
			}
			clazz = clazz.getSuperclass();
		}
		
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
	public List<String> getColumnNames(final boolean includePrimaryKeyColumn) throws Exception {
		final List<String> cols = new ArrayList<>();
		for (final Field f : fields) {
			if ((!includePrimaryKeyColumn && f.getAnnotation(PrimaryKey.class) != null) || Collection.class.isAssignableFrom(f.getType())) {
				continue;
			}
			if (f.getAnnotation(Column.class) != null && !f.getAnnotation(Column.class).columnName().isEmpty()) {
				cols.add(f.getAnnotation(Column.class).columnName());
			}
			else if (f.getAnnotation(MappingRelation.class) != null && f.getAnnotation(MappingRelation.class).mappingTableName().isEmpty()) {
				cols.add(f.getAnnotation(MappingRelation.class).masterColumnName());
			}
			else {
				cols.add(f.getName());
			}
		}
		return cols;
	};

	/**
	 * * retrieves all fields which should be persisted in the db when saving the inheriting object
	 * @return a map presentation of the object
	 * @throws Exception
	 */
	public Map<String, Object> toMap() throws Exception {
		final Map<String,Object> map = new HashMap<String,Object>();
		for (final Field f : fields) {
			if (List.class.isAssignableFrom(f.getType())) {
				continue;
			}
			final String colName;
			if (f.getAnnotation(Column.class) != null && !f.getAnnotation(Column.class).columnName().isEmpty()) {
				colName = f.getAnnotation(Column.class).columnName().toLowerCase();
				map.put(colName, f.get(this));
			} else if (f.getAnnotation(MappingRelation.class) != null && f.getAnnotation(MappingRelation.class).mappingTableName().isEmpty()) {
				colName = f.getAnnotation(MappingRelation.class).masterColumnName().toLowerCase();
				if (DatabaseEntity.class.isAssignableFrom(f.getType())) {
					final DatabaseEntity elem = (DatabaseEntity)f.get(this);
					if(elem == null) {
						map.put(colName, null);
					} else {
						final Object epk = elem.getPrimaryKeyValue();
						if(epk == null) {
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
		throw new Exception("Could not find primary key for entity '" + this.getClass().getName() +"'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!");
	}

	/**
	 * fills the inheriting object from the values from the map
	 * @param map all values to the corresponding field names
	 */
	public void fromMap(final Map<String, Object> map) throws Exception {

		for (final Field f: fields) {
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
			throw new Exception("Could not find primary key for entity '" + this.getClass().getName() +"'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!");
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
			final String msg = "Could not find primary key for entity '" + this.getClass().getName() +"'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!";
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
	protected Integer getIntOrNull(final Map<String,Object> map, final String key) {
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
	protected Boolean getBoolOrNull(final Map<String,Object> map, final String key) {
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
	protected String getStringOrNull(final Map<String,Object> map, final String key) {
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
	protected Date getDateOrNull(final Map<String,Object> map, final String key) {
		if (map == null || map.get(key) == null) {
			return null;
		}
		if (map.get(key) instanceof Date) {
			return (Date)(map.get(key));
		}
		return null;
	}

}
