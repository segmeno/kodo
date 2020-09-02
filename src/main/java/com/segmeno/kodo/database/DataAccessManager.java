package com.segmeno.kodo.database;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.MappingTable;
import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.transport.AdvancedCriteria;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.OperatorId;

public abstract class DataAccessManager {

protected final static Logger log = Logger.getLogger(DataAccessManager.class);
	
	protected String tableColDelimiter = "_000_";
	protected DataSource dataSource;
	protected JdbcTemplate jdbcTemplate;
	protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	private enum MappingTableBehaviour {
		IGNORE,
		REGARD
	}
	
	protected abstract String getSelectQuery(DatabaseEntity mainEntity, List<DatabaseEntity> childEntitiesToJoin) throws Exception;
	
	protected abstract String getCountQuery(String sql) throws Exception;
	
	protected abstract String getUpdateQuery(String tableName, String params, String primaryKeyColumn) throws Exception;
	
	protected abstract String getDeleteQuery(DatabaseEntity mainEntity) throws Exception;
	
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
	
	public DataAccessManager(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
		this.dataSource = jdbcTemplate.getDataSource();
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}
	
	public DataAccessManager(DataSource dataSource) {
		this.dataSource = dataSource;
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}
	
	/**
	 * creates the select query by analyzing the main entity and joining child elements if present. is also considering passed in criterias 
	 * @param filterByType a map which allows filtering for each entity type related to the main entity type. If the main entity is 'user' with a list of 'roles', the roles
	 * entity will be joined on the user entity 
	 * 
	 * @param entityType the main entity type
	 * @param colAliasToType the column alias mapped to the entity type. Used for joins
	 * @return
	 * @throws Exception
	 */
	private String buildSelect(Map<Class<? extends DatabaseEntity>, AdvancedCriteria> filterByType, Class<? extends DatabaseEntity> entityType, Map<String,Class<? extends DatabaseEntity>> colAliasToType) throws Exception {
		final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();
    	// check if there is a filter set for the main entity. If so, enrich the private 'Advanced Criteria' field
    	if (filterByType.containsKey(entityType)) {
    		mainEntity.advancedCriteria = filterByType.get(entityType);
    	}
    	final List<DatabaseEntity> childEntitiesToJoin = new ArrayList<>();
    	
    	for (Field f : mainEntity.fields) {
    		// check if one of the main entities fields is a represented by another table (typically a list)
    		if (List.class.isAssignableFrom(f.getType())) {
    			final Type genericType = ((ParameterizedType) f.getGenericType()).getActualTypeArguments()[0];
    			final Class<?> genericClass = Class.forName(genericType.getTypeName());
    			// check if the list generic is of type DatabaseEntity
    			if (DatabaseEntity.class.isAssignableFrom(genericClass)) {
    				final DatabaseEntity childEntity = (DatabaseEntity)genericClass.getConstructor().newInstance();
    				// store the column names of the child entity
    				childEntity.fields.stream().forEach(childField -> colAliasToType.put(childEntity.getTableName() + tableColDelimiter + childField.getName(), childEntity.getClass()));
    				// check if there is an n:m mapping table specified via the annotation
    				if (f.getAnnotation(MappingTable.class) != null) {
    					childEntity.mappingTable = f.getAnnotation(MappingTable.class);
    				}
    				// check if there are filter settings for the child entity
    				if (filterByType.containsKey(genericType)) {
    					childEntity.advancedCriteria = filterByType.get(genericType);
    				}
    				childEntitiesToJoin.add(childEntity);
    			}
    		}
    		else {
    			// store the column names of the main entity
    			colAliasToType.put(mainEntity.getTableName() + tableColDelimiter + f.getName(), entityType);
    		}
    	}
    	return getSelectQuery(mainEntity, childEntitiesToJoin);
	}
	
	/**
	 * iterates the query result and stuffs all child entities into the appropriate parent entities. Then returns them as a list
	 * @param queryResult the result of the database query
	 * @param entityType the main entity type which will be enriched with the child entities if there are any
	 * @param colAliasToType a map which holds information about which column alias belongs to which entity type
	 * @return a list of all entities, enriched with their child entities
	 * @throws Exception
	 */
	protected <T> List<T> collectResultEntities(List<Map<String,Object>> queryResult, Class<? extends DatabaseEntity> entityType, Map<String,Class<? extends DatabaseEntity>> colAliasToType) throws Exception {
		final Map<String,T> resultMap = new HashMap<String,T>();
		for (Map<String,Object> rowMap : queryResult) {
			try {
				// keep track of the already created objects
				final Map<String,Object> tableToEntity = new HashMap<>();
				// separate the columns per entity
				DatabaseEntity entity;
				for (Map.Entry<String,Object> entry : rowMap.entrySet()) {
					final String colAlias = entry.getKey();
					final String tableName = colAlias.substring(0, colAlias.indexOf(tableColDelimiter));
					final String colName = colAlias.substring(colAlias.indexOf(tableColDelimiter)+tableColDelimiter.length());
					if (!tableToEntity.containsKey(tableName)) {
						entity = colAliasToType.get(colAlias).getConstructor().newInstance();
						final Field f = entity.fields.stream().filter(field -> field.getName().equals(colName)).findAny().orElse(null);
						f.set(entity, entry.getValue());
						tableToEntity.put(tableName, entity);
					}
					else {
						entity = (DatabaseEntity)tableToEntity.get(tableName);
						final Field f = entity.fields.stream().filter(field -> field.getName().equals(colName)).findAny().orElse(null);
						f.set(entity, entry.getValue());
					}
				}
				// now stuff the child entities into the main entity and add it all to the overall result
				DatabaseEntity mainObject = (DatabaseEntity)tableToEntity.values().stream().filter(obj -> obj.getClass().getSimpleName().equals(entityType.getSimpleName())).findFirst().orElse(null);
				final String pk = String.valueOf(mainObject.getId());
				// due to joins, the main object can repeat itself several times. Therefore we need to check if it is already member of the result
				if (resultMap.containsKey(pk)) {
					mainObject = (DatabaseEntity)resultMap.get(pk);
				}
				
				for (Field f : mainObject.fields) {
					// this is a list of sub entities
					if (List.class.isAssignableFrom(f.getType())) {
						final List list = (List)f.get(mainObject);
						final Type genericType = ((ParameterizedType) f.getGenericType()).getActualTypeArguments()[0];
						// fill list
						for (Map.Entry<String,Object> entry : tableToEntity.entrySet()) {
							if (entry.getValue().getClass().getName().equals(genericType.getTypeName())) {
								DatabaseEntity childObject = (DatabaseEntity)entry.getValue();
								list.add(childObject);
							}
						}
						f.set(mainObject, list);
					}
				}
				if (!resultMap.containsKey(pk)) {
					resultMap.put(pk, (T)mainObject);
				}
			} catch (Exception e) {
				log.error("could not instantiate object of type " + entityType.getName(), e);
			}
    	}
		return resultMap.values().stream().collect(Collectors.toList());
	}
	
	/**
	 * returns all entities of entityType by performing a simple select without any filters
	 * @param entityType
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElems(new HashMap<Class<? extends DatabaseEntity>, AdvancedCriteria>(), entityType);
	}
	
	/**
	 * returns a list of the queried entity type, considering a criteria for filtering
	 * @param criteria the criteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(Criteria criteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	final Map<Class<? extends DatabaseEntity>, AdvancedCriteria> filterByType = new HashMap<>();
    	filterByType.put(entityType, new AdvancedCriteria(OperatorId.AND, criteria));
    	return getElems(filterByType, entityType);
    }
	
	/**
	 * returns a list of the queried entity type, considering a criteria for filtering
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(AdvancedCriteria advancedCriteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	final Map<Class<? extends DatabaseEntity>, AdvancedCriteria> filterByType = new HashMap<>();
    	filterByType.put(entityType, advancedCriteria);
    	return getElems(filterByType, entityType);
    }
    
	/**
	 * returns a list of the queried entity type, considering criterias for filtering
	 * @param filterByType a map which allows filtering for each entity type related to the main entity type. If the main entity is 'user' with a list of 'roles', the roles
	 * entity will be joined on the user entity 
	 * @param entityType the main entity type to query
	 * 
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(Map<Class<? extends DatabaseEntity>, AdvancedCriteria> filterByType, Class<? extends DatabaseEntity> entityType) throws Exception {
    	// to be able to fill the result into the main entity and the child entities, we first need to know which field belongs to which entity
		final Map<String,Class<? extends DatabaseEntity>> colAliasToType = new HashMap<>();
		final String selectQuery = buildSelect(filterByType, entityType, colAliasToType);
    	log.debug("Query: " + selectQuery);
    	final List<Map<String,Object>> queryResult = jdbcTemplate.queryForList(selectQuery);
    	return collectResultEntities(queryResult, entityType, colAliasToType);
    }

    public Integer getElemCount(Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElemCount((Map<Class<? extends DatabaseEntity>, AdvancedCriteria>)null, entityType);
    }
    
    public Integer getElemCount(Criteria criteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	final Map<Class<? extends DatabaseEntity>, AdvancedCriteria> filterMap = new HashMap<>();
    	filterMap.put(entityType, new AdvancedCriteria(OperatorId.AND, criteria));
    	return getElemCount(filterMap, entityType);
    }
	
	public Integer getElemCount(Map<Class<? extends DatabaseEntity>, AdvancedCriteria> filterByType, Class<? extends DatabaseEntity> entityType) throws Exception {
		final Map<String,Class<? extends DatabaseEntity>> colAliasToType = new HashMap<>();
		final String selectQuery = buildSelect(filterByType, entityType, colAliasToType);
		final String countQuery = getCountQuery(selectQuery);
		log.debug("Query: " + countQuery);
    	return jdbcTemplate.queryForObject(countQuery, Integer.class);
    }
   
    @SuppressWarnings("unchecked")
	@Transactional(propagation = Propagation.REQUIRED)
    public <T> T addElem(DatabaseEntity obj) throws Exception {
    	final SimpleJdbcInsert insert = new SimpleJdbcInsert(dataSource)
	            .withTableName(obj.getTableName())
	            .usingGeneratedKeyColumns(obj.getPrimaryKeyColumn())
	            .usingColumns(obj.getColumnNames(false).toArray(new String[0]));
    	
    	final Number key = insert.executeAndReturnKey(obj.toMap());
    	obj.setId(key.intValue());
    	
    	return (T)obj;
    }
    
    @Transactional(propagation = Propagation.REQUIRED)
	public void updateElems(List<DatabaseEntity> entities) throws Exception {
    	for (DatabaseEntity entity : entities) {
    		updateElem(entity);
    	}
    }
    
    @Transactional(propagation = Propagation.REQUIRED)
	public void updateElem(DatabaseEntity obj) throws Exception {
    	if (obj.getId()== null || Integer.valueOf(String.valueOf(obj.getId())) == -1) {
			addElem(obj);
			return;
		}
    	
    	// first, update the main entity
    	final StringBuilder sb = new StringBuilder();
    	for (Field f : obj.getClass().getDeclaredFields()) {
    		if (f.getAnnotation(DbIgnore.class) != null || f.getAnnotation(PrimaryKey.class) != null || List.class.isAssignableFrom(f.getType())) {
    			continue;
    		}
    		sb.append(f.getName().toLowerCase()).append(" = :").append(f.getName().toLowerCase()).append(", ");
    	}
    	if (sb.length() > 1) {
    		sb.setLength(sb.length()-2);	// crop last comma
    	}
    	final String updateQuery = getUpdateQuery(obj.getTableName(), sb.toString(), obj.getPrimaryKeyColumn().toLowerCase());
    	log.debug("Query: " + updateQuery);
    	namedParameterJdbcTemplate.update(updateQuery, obj.toMap());
    	
    	// check if there is a list of sub entities which also need to be added or updated
		for (Map.Entry<Class<? extends DatabaseEntity>,List<DatabaseEntity>> entry : getAllChildEntities(obj, MappingTableBehaviour.IGNORE).entrySet()) {
			final List<DatabaseEntity> elements = entry.getValue();
			for (DatabaseEntity element : elements) {
				updateElem(element);
			}
		}
    }
    
    /**
     * deletes all elements which expect the specified type and match the provided filter
     * @param filterByType
     * @param entityType
     * @throws Exception
     */
    @Transactional(propagation = Propagation.REQUIRED)
	public void deleteElems(AdvancedCriteria advancedCriteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	List<DatabaseEntity> elemsToDelete = getElems(advancedCriteria, entityType);
    	for (DatabaseEntity entity : elemsToDelete) {
    		deleteElem(entity);
    	}
    }
	
    /**
     * deletes the main entity and all of its children by their primary key (this is the only field which needs to be provided)
     * @param obj
     * @throws Exception
     */
	@Transactional(propagation = Propagation.REQUIRED)
	public void deleteElem(DatabaseEntity obj) throws Exception {
		if (obj.getId()== null || Integer.valueOf(String.valueOf(obj.getId())) == -1) {
			log.warn("could not delete " + obj.getClass().getSimpleName() + ": no primary key set");
			return;
		}
		// check if there is a list of sub entities which need to be deleted first
		for (Map.Entry<Class<? extends DatabaseEntity>,List<DatabaseEntity>> entry : getAllChildEntities(obj, MappingTableBehaviour.IGNORE).entrySet()) {
			final List<DatabaseEntity> elements = entry.getValue();
			for (DatabaseEntity element : elements) {
				deleteElem(element);
			}
		}
    	final String deleteQuery = getDeleteQuery(obj);
    	log.debug("Query: " + deleteQuery);
    	jdbcTemplate.update(deleteQuery, obj.getId());
	}
    
//    /**
//     * this method can be used to retrieve data for grids, supports paging and filtering
//     */
//    public List<Object[]> getRows(Class<? extends DatabaseEntity> model) {
//    	final String tableName = model.getConstructor().newInstance().getTableName();
//    	jdbcTemplate.query("SELECT * FROM " + tableName)
//    }
    
    protected <T> String toCsv(final T[] list) {
		final StringBuilder sb = new StringBuilder();
		for (final T t : list) {
			sb.append(t).append(",");
		}
		if(sb.length() > 0) {
			sb.setLength(sb.length()-1); // crop last comma
		}
		return sb.toString();
	}
    
    protected String getColumnsCsvWithAlias(String tableAlias, List<String> cols) {
    	final StringBuilder sb = new StringBuilder();
    	for (String col : cols) {
    		final String s = tableAlias + "." + col + " AS " + tableAlias + tableColDelimiter + col;
    		sb.append(s).append(", ");
    	}
    	if (sb.length()>2) {
    		sb.setLength(sb.length()-2);	// crop last comma
    	}
    	return sb.toString();
    }
    
    /**
     * returns all generic types of lists which are part of the main entity. So if a user has roles and accounts, the result would
     * be a list with the role element and the account element
     * @param mainEntity
     * @param ignoreNmMappings
     * @return
     * @throws Exception
     */
    protected List<Class<? extends DatabaseEntity>> getChildTypes(DatabaseEntity mainEntity, MappingTableBehaviour mappingTableBehaviour) throws Exception {
    	final List<Class<? extends DatabaseEntity>> result = new ArrayList<>();
    	for (Field f : mainEntity.fields) {
			// check if this is a list
			if (List.class.isAssignableFrom(f.getType())) {
    			final Type genericType = ((ParameterizedType) f.getGenericType()).getActualTypeArguments()[0];
    			final Class<?> genericClass = Class.forName(genericType.getTypeName());
    			// check if the list generic is of type DatabaseEntity
    			if (DatabaseEntity.class.isAssignableFrom(genericClass)) {
    				// if the child element is related via a many-to-many table, we need to check if it should be part of the result
    				if (f.getAnnotation(MappingTable.class) != null && mappingTableBehaviour == MappingTableBehaviour.IGNORE) {
    					continue;
    				}
    				result.add((Class<? extends DatabaseEntity>)genericClass);
    			}
			}
    	}
    	return result;
    }
    
    /**
     * returns a map which holds lists of all elements of the main entity. So if a user has roles and accounts, the result would
     * be a map with all role elements and a list with all account elements, wrapped inside a list
     * @param mainEntity
     * @param ignoreNmMappings
     * @return
     * @throws Exception
     */
    protected Map<Class<? extends DatabaseEntity>,List<DatabaseEntity>> getAllChildEntities(DatabaseEntity mainEntity, MappingTableBehaviour mappingTableBehaviour) throws Exception {
    	final Map<Class<? extends DatabaseEntity>,List<DatabaseEntity>> typeToEntries = new HashMap<>();
    	
    	for (Field f : mainEntity.fields) {
			// check if this is a list
			if (List.class.isAssignableFrom(f.getType())) {
    			final Type genericType = ((ParameterizedType) f.getGenericType()).getActualTypeArguments()[0];
    			final Class<?> genericClass = Class.forName(genericType.getTypeName());
    			// check if the list generic is of type DatabaseEntity
    			if (DatabaseEntity.class.isAssignableFrom(genericClass)) {
    				// if the child element is related via a many-to-many table, we need to check if it should be part of the result
    				if (f.getAnnotation(MappingTable.class) != null && mappingTableBehaviour == MappingTableBehaviour.IGNORE) {
    					continue;
    				}
    				final List<DatabaseEntity> list = (List<DatabaseEntity>)f.get(mainEntity);
    				typeToEntries.put((Class<? extends DatabaseEntity>)genericClass, list);
    			}
			}
    	}
    	return typeToEntries;
    }
    
}
