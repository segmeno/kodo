package com.segmeno.kodo.database;

import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.ParentKey;
import com.segmeno.kodo.annotation.PrimaryKey;

public abstract class DatabaseEntity {
	
	
	private static final Logger LOGGER = Logger.getLogger(DatabaseEntity.class);
	public static final String MYSQL_DATETIME_FORMAT = "yyyy-MM-dd hHH:mm:ss";
	
	private static final SimpleDateFormat FORMATTER = new SimpleDateFormat(MYSQL_DATETIME_FORMAT);
	
	final transient List<Field> fields = new ArrayList<Field>();
	
	public DatabaseEntity() {
		fields.addAll(Arrays.asList(this.getClass().getDeclaredFields()));
	}
	
	/**
	 * to stay generic inside the dbManager class, this method allows objects to populate their respective children (like assigned roles inside a user)	
	 * @param manager the SqlManager implementation
	 * @throws Exception
	 */
	public abstract void fillChildObjects(SqlManager manager) throws Exception;
	
	/**
	 * 
	 * @return the tableName of this entity
	 */
	public abstract String getTableName();
	
	/**
	 * 
	 * @return the column names of this entity, relevant for inserts
	 */
	public String[] getColumnNames() {
		final List<String> result = new ArrayList<String>();
		
		for (Field f : fields) {
			f.setAccessible(true);
			if (f.getAnnotation(DbIgnore.class) != null || f.getAnnotation(PrimaryKey.class) != null) {
    			continue;
    		}
			result.add(f.getName());
		}
		return result.toArray(new String[result.size()]);
	};
	
	/**
	 * * retrieves all fields which should be persisted in the db when saving the inheriting object
	 * @param isKeyCaseSensitive controls if the map key should be looked up case sensitive
	 * @return a map presentation of the object
	 * @throws Exception 
	 */
	public Map<String, Object> toMap(boolean isKeyCaseSensitive) throws Exception {
		
		final Map<String,Object> map = new HashMap<String,Object>();
		
		for (Field f : fields) {
			f.setAccessible(true);
			if (f.getAnnotation(DbIgnore.class) != null) {
    			continue;
    		}
			map.put(isKeyCaseSensitive ? f.getName() : f.getName().toLowerCase(), f.get(this));
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
		for (Field f : fields) {
			f.setAccessible(true);
			if (f.getAnnotation(PrimaryKey.class) != null) {
    			return f.getName();
    		}
		}
		throw new Exception("Could not find primary key for entity '" + this.getClass().getName() +"'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!");
	}
	
	@JsonIgnore
	/**
	 * 
	 * @return the parent key column name, in case this is a child object
	 * @throws Exception
	 */
	public String getParentKeyColunm() throws Exception {
		for (Field f : fields) {
			f.setAccessible(true);
			if (f.getAnnotation(ParentKey.class) != null) {
    			return f.getName();
    		}
		}
		throw new Exception("Could not find parent key for entity '" + this.getClass().getName() +"'. Please use the '@ParentKey' annotation to mark a field as ForeignKey to a parent table!");
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
	 * casts a date object into the correct datatype
	 * 
	 * @param o
	 * @return the cast date
	 */
	public Date getDate(Object o) {
		if (o instanceof Timestamp) {
			return new Date(((Timestamp)o).getTime());
		}
		if (o instanceof String) {
			try {
				return FORMATTER.parse((String)o);
			} catch (ParseException e) {
				LOGGER.error("can not parse date. Expected format: " + MYSQL_DATETIME_FORMAT + ", value is " + o, e);
			}
		}
		return null;
	}
	
	/**
	 * sets the primary key field
	 * 
	 * @param id
	 * @throws Exception
	 */
	public void setId(Object id) throws Exception {
		
		for (Field f : fields) {
			f.setAccessible(true);
			if (f.getAnnotation(PrimaryKey.class) != null) {
				f.set(this, id);
				return;
			}
		}
		throw new Exception("Could not find primary key for entity '" + this.getClass().getName() +"'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!");
	}
	
	/**
	 * returns the value of the primary key field
	 * 
	 * @return
	 */
	public Object getId() {
		
		for (Field f : fields) {
			f.setAccessible(true);
			if (f.getAnnotation(PrimaryKey.class) != null) {
    			try {
					return f.get(f.getName());
				} catch (Exception e) {
					final String msg = "error during search for primary key field";
					LOGGER.error(msg);
					throw new RuntimeException(msg);
				}
    		}
		}
		final String msg = "Could not find primary key for entity '" + this.getClass().getName() +"'. Please use the '@PrimaryKey' annotation to mark a field as PrimaryKey!";
		LOGGER.error(msg);
		throw new RuntimeException(msg);
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
