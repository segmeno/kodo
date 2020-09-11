package com.segmeno.kodo.database;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
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

import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.transport.AdvancedCriteria;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.OperatorId;

public class DataAccessManager {

protected final static Logger log = Logger.getLogger(DataAccessManager.class);
	
	protected String tableColDelimiter = ".";
	protected DataSource dataSource;
	protected JdbcTemplate jdbcTemplate;
	protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	protected int fetchDepth = -1;
	
	private enum MappingTableBehaviour {
		IGNORE,
		REGARD
	}
	
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
	 * returns all entities of entityType by performing a simple select without any filters
	 * @param entityType
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElems((AdvancedCriteria)null, entityType);
	}
	
	/**
	 * returns a list of the queried entity type, considering a criteria for filtering
	 * @param criteria the criteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(Criteria criteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElems(new AdvancedCriteria(OperatorId.AND, criteria), entityType);
    }
	
	/**
	 * returns a list of the queried entity type, considering a criteria for filtering
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(AdvancedCriteria advancedCriteria, Class<? extends DatabaseEntity> entityType) throws Exception {

		final List<Object> params = new ArrayList<Object>();
		final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();
    	final String query = buildQuery(mainEntity, advancedCriteria, params);
    	
    	log.debug("Query: " + sqlPrettyPrint(query) + "\t\t" + params);
		final List<Map<String,Object>> rows = jdbcTemplate.queryForList(query, params.toArray());
		return rowsToObjects(mainEntity, rows);
    }
	
	private <T> List<T> rowsToObjects(DatabaseEntity baseEntity, List<Map<String, Object>> rows) throws Exception {
		final Map<String,T> resultMap = new HashMap<String,T>();
		
		final Map<String,Object> alreadyFilledObjects = new HashMap<String, Object>();
		String pk;
		
		for (Map<String,Object> row : rows) {
			
			pk = String.valueOf(getValueFromRow(baseEntity.getTableName(), baseEntity.getPrimaryKeyColumn(), row));
			if (!resultMap.containsKey(pk)) {
				baseEntity = baseEntity.getClass().getConstructor().newInstance();
				alreadyFilledObjects.clear();
			}
			rowToEntity(baseEntity, baseEntity.getTableName(), row, alreadyFilledObjects);
			resultMap.put(pk, (T)baseEntity);
		}
		return resultMap.values().stream().collect(Collectors.toList());
	}
	
	private void rowToEntity(DatabaseEntity entity, String childAlias, Map<String,Object> row, Map<String,Object> alreadyFilledObjects) throws Exception {
		
		// first thing to do: retrieve pk value and build unique key
		final String pk = String.valueOf(getValueFromRow(childAlias, entity.getPrimaryKeyColumn(), row));
		final String uniqueKey = childAlias + "#" + pk;
		
		for (Field field : entity.fields) {
			// search for child entities
			if (field.getAnnotation(MappingRelation.class) != null) {
				final DatabaseEntity childEntity;
				
				if (List.class.isAssignableFrom(field.getType())) {
					final Type genericType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
	    			final Class<?> genericClass = Class.forName(genericType.getTypeName());
	    			childEntity = (DatabaseEntity)genericClass.getConstructor().newInstance();
	    			childAlias = entity.getTableName() + "_" + field.getName();
	    			String childPk = String.valueOf(getValueFromRow(childAlias, childEntity.getPrimaryKeyColumn(), row));
	    			String childUniqueKey = childAlias + "#" + childPk;
	    			
					if (!alreadyFilledObjects.containsKey(childUniqueKey)) {
						final List<DatabaseEntity> list = (List)field.get(entity);
						list.add(childEntity);
						field.set(entity, list);
						rowToEntity(childEntity, childAlias, row, alreadyFilledObjects);
					}
				}
				else if (DatabaseEntity.class.isAssignableFrom(field.getType())) {
					childEntity = (DatabaseEntity)field.getType().getConstructor().newInstance();
					field.set(entity, childEntity);
					childAlias = entity.getTableName() + "_" + field.getName();
					if (!alreadyFilledObjects.containsKey(uniqueKey)) {
						rowToEntity(childEntity, childAlias, row, alreadyFilledObjects);
					}
				}
			}
			// search for the field and fill it
			else {
				for (Map.Entry<String, Object> cell : row.entrySet()) {
					final String fullName = cell.getKey();
					final String entityField = childAlias + tableColDelimiter + field.getName();
					
					if (fullName.equals(entityField)) {
						field.set(entity, cell.getValue());
						break;
					}
				}
			}
		}
		alreadyFilledObjects.put(uniqueKey, entity);
	}
	
	private Object getValueFromRow(String alias, String fieldName, Map<String,Object> row) {
		fieldName = alias + tableColDelimiter + fieldName;
		for (Map.Entry<String, Object> cell : row.entrySet()) {
			if (cell.getKey().equals(fieldName)) {
				return cell.getValue();
			}
		}
		return null;
	}

    public Long getElemCount(Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElemCount((AdvancedCriteria)null, entityType);
    }
    
    public Long getElemCount(Criteria criteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElemCount(new AdvancedCriteria(OperatorId.AND, criteria), entityType);
    }
    
    public Long getElemCount(AdvancedCriteria criteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();

		final List<Object> params = new ArrayList<Object>();
		final StringBuilder select = new StringBuilder();
		final StringBuilder from = new StringBuilder();
		final StringBuilder join = new StringBuilder();
		final StringBuilder where = new StringBuilder();
		buildQueryRecursively(mainEntity, criteria, select, from, join, where, params, 0);
		
		final String sql = "SELECT COUNT (DISTINCT " + mainEntity.getTableName() + "." + mainEntity.getPrimaryKeyColumn() + ")" + from.toString() + join.toString() + where.toString();
		
		log.debug("Query: " + sqlPrettyPrint(sql) + "\t[" + toCsv(params.toArray()) + "]");
		return jdbcTemplate.queryForObject(sql, params.toArray(), Long.class);
    }
	
    @SuppressWarnings("unchecked")
	@Transactional(propagation = Propagation.REQUIRED)
    public <T> T addElem(DatabaseEntity obj) throws Exception {
    	final SimpleJdbcInsert insert = new SimpleJdbcInsert(dataSource)
	            .withTableName(obj.getTableName())
	            .usingGeneratedKeyColumns(obj.getPrimaryKeyColumn())
	            .usingColumns(obj.getColumnNames(false).toArray(new String[0]));
    	
    	try {
			final Number key = insert.executeAndReturnKey(obj.toMap());
			obj.setPrimaryKeyValue(key.intValue());
		} catch (Exception e) {
			log.warn("could not insert " + obj.getClass().getSimpleName()+ ": " + e.getMessage());
			log.warn("trying alternative method");
			obj.setPrimaryKeyValue(jdbcTemplate.queryForObject("SELECT MAX( " + obj.getPrimaryKeyColumn() + ") FROM " + obj.getTableName(), Long.class));
		}
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
    	if (obj.getPrimaryKeyValue()== null || Integer.valueOf(String.valueOf(obj.getPrimaryKeyValue())) == -1) {
			addElem(obj);
		}
    	else {
    		// first, update the main entity
    		final StringBuilder sb = new StringBuilder();
    		for (String col : obj.getColumnNames(false)) {
    			sb.append(col.toLowerCase()).append(" = :").append(col.toLowerCase()).append(", ");
        	}
        	if (sb.length() > 1) {
        		sb.setLength(sb.length()-2);	// crop last comma
        	}
        	final String stmt = "UPDATE " + obj.getTableName() + " SET " + sb.toString() + " WHERE " + obj.getPrimaryKeyColumn() + " = :" + obj.getPrimaryKeyColumn();
        	log.debug("Query: " + sqlPrettyPrint(stmt) + "\t\t[" + toCsv(obj.toMap().values()) + "]");
        	namedParameterJdbcTemplate.update(stmt, obj.toMap());
        	
        	// check if there is a list of sub entities which also need to be added or updated
    		for (Map.Entry<Class<? extends DatabaseEntity>,List<DatabaseEntity>> entry : getAllChildEntities(obj, MappingTableBehaviour.IGNORE).entrySet()) {
    			final List<DatabaseEntity> elements = entry.getValue();
    			for (DatabaseEntity element : elements) {
    				updateElem(element);
    			}
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
		if (obj.getPrimaryKeyValue()== null || Integer.valueOf(String.valueOf(obj.getPrimaryKeyValue())) == -1) {
			log.warn("could not delete " + obj.getClass().getSimpleName() + ": no primary key set");
		}
		else {
			// check if there is a list of sub entities which need to be deleted first
			for (Map.Entry<Class<? extends DatabaseEntity>,List<DatabaseEntity>> entry : getAllChildEntities(obj, MappingTableBehaviour.IGNORE).entrySet()) {
				final List<DatabaseEntity> elements = entry.getValue();
				for (DatabaseEntity element : elements) {
					deleteElem(element);
				}
			}
			final String stmt = "DELETE FROM " + obj.getTableName() + " WHERE " + obj.getPrimaryKeyColumn() + " = ?";
			log.debug("Query: " + sqlPrettyPrint(stmt) + "\t\t[" + obj.getPrimaryKeyValue() + "]");
			jdbcTemplate.update(stmt, obj.getPrimaryKeyValue());
		}
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
    
    protected String toCsv(Collection<Object> values) {
    	final StringBuilder sb = new StringBuilder();
		for (final Object t : values) {
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
    		final String s;
			s = tableAlias + "." + col + " AS \"" + tableAlias + tableColDelimiter + col + "\"";
    		sb.append(s).append(", ");
    	}
    	if (sb.length()>2) {
    		sb.setLength(sb.length()-2);	// crop last comma
    	}
    	return sb.toString();
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
    				if (f.getAnnotation(MappingRelation.class) != null && mappingTableBehaviour == MappingTableBehaviour.IGNORE) {
    					continue;
    				}
    				final List<DatabaseEntity> list = (List<DatabaseEntity>)f.get(mainEntity);
    				typeToEntries.put((Class<? extends DatabaseEntity>)genericClass, list);
    			}
			}
    	}
    	return typeToEntries;
    }

	/**
	 * builds a query from the entity and given filters
	 * @param entity - the main entity
	 * @param filter - criteria(s) to filter on the main entity
	 * @param params - this list will be filled by the algorithm
	 * @return
	 * @throws Exception
	 */
	protected String buildQuery(DatabaseEntity entity, AdvancedCriteria filter, List<Object> params) throws Exception {
		final StringBuilder select = new StringBuilder();
		final StringBuilder from = new StringBuilder();
		final StringBuilder join = new StringBuilder();
		final StringBuilder where = new StringBuilder();
		buildQueryRecursively(entity, filter, select, from, join, where, params, 0);
		return select.toString() + from.toString() + join.toString() + where.toString();
	}
	
	private void buildQueryRecursively(DatabaseEntity entity, AdvancedCriteria filter, StringBuilder select, StringBuilder from, StringBuilder join, StringBuilder where, List<Object> params, int currentDepth) throws Exception {
		if (select.length() == 0) {
			select.append("SELECT " + getColumnsCsvWithAlias(entity.getTableName(), entity.getColumnNames(true)));
			from.append(" FROM " + entity.getTableName());
			if (filter != null && !filter.getCriterias().isEmpty()) {
				final WherePart wp = new WherePart(entity.getTableName(), filter);
				params.addAll(wp.getValues());
				where.append(" WHERE " + wp.toString());
			}
		}
		currentDepth++;
		
		final Field aliasField = findField(entity.getClass(), "tableAlias");
		final String entityTableAlias;
		// always true for the main entity
		if (aliasField.get(entity) == null) {
			entityTableAlias = entity.getTableName();
		}
		else {
			entityTableAlias = String.valueOf(aliasField.get(entity));
		}
		 
		for (Field field : entity.fields) {
			// only join children to the select if they are annotated with the MappingRelation annotation
			if (field.getAnnotation(MappingRelation.class) != null) {
				// if this child element position exceeds the maximum depth of the joins, we do not fetch it 
				if (fetchDepth != -1 && currentDepth > fetchDepth) {
					continue;
				}
				final MappingRelation relation = field.getAnnotation(MappingRelation.class);
				final DatabaseEntity childEntity;
				
				if (List.class.isAssignableFrom(field.getType())) {
					final Type genericType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
	    			final Class<?> genericClass = Class.forName(genericType.getTypeName());
	    			childEntity = (DatabaseEntity)genericClass.getConstructor().newInstance();
				}
				else if (DatabaseEntity.class.isAssignableFrom(field.getType())) {
					childEntity = (DatabaseEntity)field.getType().getConstructor().newInstance();
				}
				else {
					return;
				}
    			String childAlias = entity.getTableName() + "_" + field.getName();
    			aliasField.set(childEntity, childAlias);
    			select.append(", ").append(getColumnsCsvWithAlias(childAlias, childEntity.getColumnNames(true)));
    			
    			// this is an m:n mapping
    			if (!relation.mappingTableName().isEmpty()) {
    				join.append(" LEFT JOIN " + relation.mappingTableName() + " ON " + relation.mappingTableName() + "." + relation.masterColumnName() + " = " + entity.getTableName() + "." + entity.getPrimaryKeyColumn());
    				join.append(" LEFT JOIN " + childEntity.getTableName() + " " + childAlias + " ON " + relation.mappingTableName() + "." + relation.joinedColumnName() + " = " + childAlias + "." + childEntity.getPrimaryKeyColumn());
    			}
    			else {
    				join.append(" LEFT JOIN " + childEntity.getTableName() + " " + childAlias + " ON " + childAlias + "." + relation.joinedColumnName() + " = " + entityTableAlias + "." + relation.masterColumnName());
    			}
    			
    			buildQueryRecursively(childEntity, filter, select, from, join, where, params, currentDepth);
			}
			// this is a misconfiguration
			else if (DatabaseEntity.class.isAssignableFrom(field.getType())) {
				throw new Exception("DatabaseEntity " + field.getType().getName() + " in " + entity.getClass().getName() + " found, but the MappingRelation annotation is missing");
			}
		}
	}
    
    private Field findField(Class<?> clazz, String fieldName) {
    	for (Field f : clazz.getDeclaredFields()) {
    		f.setAccessible(true);
    		if (f.getName().equals(fieldName)) {
    			return f;
    		}
    	}
    	if (clazz.getSuperclass() != null) {
    		return findField(clazz.getSuperclass(), fieldName);
    	}
		return null;
	}

	public static String sqlPrettyPrint(String sql) {
		if (sql == null) {
			return null;
		}
		return sql.replaceAll("SELECT", "\n\tSELECT").replaceAll("FROM", "\n\tFROM").replaceAll("LEFT JOIN", "\n\tLEFT JOIN").replaceAll("WHERE", "\n\tWHERE");
	}
	
	/**
	 * sets the maximum level in the hierarchy tree. A flat list for e.g. users can be fetched by setting the depth to 0.
	 * If the user object has roles which should also be fetched, the depth must be set to 1. 
	 * If the roles have relations to other tables which should also be considered, the depth must be 2. To ignore depth and fetch all entities, pass in -1
	 * 
	 * default is -1 (fetches everything)
	 * @param fetchDepth
	 */
	public void setFetchDepth(Integer fetchDepth) {
		if (fetchDepth == null) {
			this.fetchDepth = -1;
		}
		else {
			this.fetchDepth = fetchDepth;
		}
	}
    
}
