package com.segmeno.kodo.database;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.annotation.PrimaryKey;

public abstract class DatabaseEntity {
	
	private static final Logger LOGGER = Logger.getLogger(DatabaseEntity.class);
	// to be used within kodo framework only
	@SuppressWarnings("unused")
	private String tableAlias;
	private Field primaryKey;
	
	final transient List<Field> fields = new ArrayList<Field>();
	
	public DatabaseEntity() {
		
		for (Field field : this.getClass().getDeclaredFields()) {
			field.setAccessible(true);
			if (field.getAnnotation(DbIgnore.class) != null) {
				continue;
			}
			if (field.getAnnotation(PrimaryKey.class) != null) {
				primaryKey = field;
			}
			fields.add(field);
		}
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
	public List<String> getColumnNames(boolean includePrimaryKeyColumn) throws Exception {
		final List<String> cols = new ArrayList<>();
		for (Field f : fields) {
			if ((!includePrimaryKeyColumn && f.getAnnotation(PrimaryKey.class) != null) || 
					Collection.class.isAssignableFrom(f.getType()) || f.getAnnotation(MappingRelation.class) != null) {
				continue;
			}
			cols.add(f.getName());
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
		for (Field f : fields) {
			if (List.class.isAssignableFrom(f.getType())) {
				continue;
			}
			map.put(f.getName().toLowerCase(), f.get(this));
		}
		return map;		
	}
	
	@JsonIgnore
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
	public void fromMap(Map<String, Object> map) throws Exception {
		
		for (Field f: fields) {
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
	public void setPrimaryKeyValue(Object id) throws Exception {
		if (primaryKey == null) {
			throw new Exception("Could not find primary key for entity '" + this.getClass().getName() +"'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!");			
		}
		primaryKey.set(this, id);
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
		} catch (Exception e) {
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
	protected Integer getIntOrNull(Map<String,Object> map, String key) {
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
	protected Boolean getBoolOrNull(Map<String,Object> map, String key) {
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
	protected String getStringOrNull(Map<String,Object> map, String key) {
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
	protected Date getDateOrNull(Map<String,Object> map, String key) {
		if (map == null || map.get(key) == null) {
			return null;
		}
		if (map.get(key) instanceof Date) {
			return (Date)(map.get(key));
		}
		return null;
	}

}
