package com.segmeno.kodo.database;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.segmeno.kodo.annotation.CustomSql;
import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.CriteriaGroup;
import com.segmeno.kodo.transport.Operator;
import com.segmeno.kodo.transport.Sort;
import com.segmeno.kodo.transport.Sort.SortDirection;

public class DataAccessManager {

	private static final Logger log = LogManager.getLogger(DataAccessManager.class);
	
	protected String tableColDelimiter = ".";
	protected DataSource dataSource;
	protected JdbcTemplate jdbcTemplate;
	protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	// H2, MySQL, Microsoft SQL Server, Oracle, PostgreSQL, Apache Derby, HSQL Database Engine
	private final String DB_PRODUCT;
	
	private enum MappingTableBehaviour {
		IGNORE,
		REGARD
	}
		
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
	
	public DataAccessManager(JdbcTemplate jdbcTemplate) throws SQLException {
		this.jdbcTemplate = jdbcTemplate;
		this.dataSource = jdbcTemplate.getDataSource();
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
		this.DB_PRODUCT = getProduct();
	}
	
	public DataAccessManager(DataSource dataSource) throws SQLException {
		this.dataSource = dataSource;
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
		this.DB_PRODUCT = getProduct();
	}
	
	/**
	 * returns all entities of entityType by performing a simple select without any filters
	 * @param entityType
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElems((CriteriaGroup)null, entityType);
	}
	
	/**
	 * returns a list of the queried entity type, considering a criteria for filtering
	 * @param criteria the criteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(Criteria criteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElems(new CriteriaGroup(Operator.AND, criteria), entityType);
    }
	
	/**
	 * returns a list of the queried entity type, considering a criteria for filtering. Fills all sub elements and their children
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(CriteriaGroup advancedCriteria, Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElems(advancedCriteria, entityType, null, -1);
	}
	
	/**
	 * returns a list of the queried entity type, considering a criteria for filtering
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @param fetchDepth - how deep to dig down in the hierarchy level. Pass in -1 to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(CriteriaGroup advancedCriteria, Class<? extends DatabaseEntity> entityType, Sort sort, int fetchDepth) throws Exception {

		try {
			final List<Object> params = new ArrayList<Object>();
			final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();
			final String query = buildQuery(mainEntity, advancedCriteria, sort, params, fetchDepth);
			
			log.debug("Query: " + sqlPrettyPrint(query) + "\t" + params);
			final List<Map<String,Object>> rows = jdbcTemplate.queryForList(query, params.toArray());
			return rowsToObjects(mainEntity, rows);
			
		} catch (Exception e) {
			log.error("could not get elements of type " + entityType.getName(), e);
			throw e;
		}
    }
	
	public List<Map<String,Object>> getRecords(String tableName, int pageSize, int currentPage) throws Exception {
		return getRecords(tableName, (Criteria)null, pageSize, currentPage);
	}
	
	public List<Map<String,Object>> getRecords(String tableName, Criteria criteria, int pageSize, int currentPage) throws Exception {
		return getRecords(tableName, new CriteriaGroup(Operator.AND, criteria), pageSize, currentPage, null);
	}
	
	public List<Map<String,Object>> getRecords(String tableName, CriteriaGroup criteriaGroup, int pageSize, int currentPage, Sort sort) throws Exception {

		if (sort == null) {
			sort = new Sort();
		}
		final WherePart where = new WherePart(tableName, criteriaGroup);
		String stmt = "SELECT * FROM " + tableName + " WHERE " + where.toString() + sort.toString();
		String count = "SELECT COUNT(*) FROM (" + stmt + ")";
		
		final Integer totalRows = jdbcTemplate.queryForObject(count, Integer.class);
		stmt = addPaging(stmt, currentPage, pageSize, totalRows);
		log.debug("Query: " + sqlPrettyPrint(stmt) + "\t" + where.getValues().toArray());

		return jdbcTemplate.queryForList(stmt, where.getValues().toArray());
	}
	
	private <T> List<T> rowsToObjects(DatabaseEntity baseEntity, List<Map<String, Object>> rows) throws Exception {
		final Map<String,T> resultMap = new LinkedHashMap<String,T>();
		
		final Map<String,Object> alreadyFilledObjects = new HashMap<String, Object>();
		String pk;
		
		for (Map<String,Object> row : rows) {
			
			pk = String.valueOf(getValueFromRow(baseEntity.getTableName(), baseEntity.getPrimaryKeyColumn(), row));
			if (!resultMap.containsKey(pk)) {
				baseEntity = baseEntity.getClass().getConstructor().newInstance();
				alreadyFilledObjects.clear();
			}
			else {
				baseEntity = (DatabaseEntity)resultMap.get(pk);
			}
			final String alias = baseEntity.getTableName() != null ? baseEntity.getTableName() : baseEntity.getClass().getSimpleName();
			rowToEntity(baseEntity, baseEntity.getTableName(), "/", row, alreadyFilledObjects);
			resultMap.put(pk, (T)baseEntity);
		}
		return resultMap.values().stream().collect(Collectors.toList());
	}
	
	private void rowToEntity(DatabaseEntity entity, String alias, String path, Map<String,Object> row, Map<String,Object> alreadyFilledObjects) throws Exception {
		// first thing to do: retrieve pk value and build unique key
		final String pk = String.valueOf(getValueFromRow(alias, entity.getPrimaryKeyColumn(), row));
		final String uniqueKey = alias + "#" + pk;
		
		for (Field field : entity.fields) {
			
			if (List.class.isAssignableFrom(field.getType())) {
				
				final Type genericType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
    			final Class<?> genericClass = Class.forName(genericType.getTypeName());
    			
				if (DatabaseEntity.class.isAssignableFrom(genericClass)) {
					
	    			final DatabaseEntity childEntity = (DatabaseEntity)genericClass.getConstructor().newInstance();
	    			// this is allowed to happen if the database entity has a custom sql annotation
	    			if (entity.getTableName() == null) {
	    				if (entity.getClass().isAnnotationPresent(CustomSql.class)) {
	    					alias = field.getName();
						}
	    				else {
	    					throw new Exception("no table name defined! Either change 'getTableName' method of " + entity.getClass().getName() + " to return a value or use the @CustomSql annotation");
	    				}
	    			}
	    			else {
	    				alias = entity.getTableName() + "_" + field.getName();
	    			}
	    			
	    			Object childPk = getValueFromRow(alias, childEntity.getPrimaryKeyColumn(), row);
	    			String childUniqueKey = alias + "#" + childPk;
						
					if (childPk != null && !alreadyFilledObjects.containsKey(childUniqueKey) && !path.contains(childEntity.getTableName())) {
						
						List<DatabaseEntity> list = (List)field.get(entity);
						if (list == null) {
							list = new ArrayList<>();
						}
						list.add(childEntity);
						field.set(entity, list);
						
						// keep track of the current level in the tree 
						path += "/" + entity.getTableName();
						rowToEntity(childEntity, alias, path, row, alreadyFilledObjects);
						path = path.substring(0, path.lastIndexOf("/"));
					}
				}
			}
			else if (DatabaseEntity.class.isAssignableFrom(field.getType())) {
				
				final DatabaseEntity childEntity = (DatabaseEntity)field.getType().getConstructor().newInstance();	
				alias = entity.getTableName() + "_" + field.getName();
				Object childPk = getValueFromRow(alias, childEntity.getPrimaryKeyColumn(), row);
				String childUniqueKey = alias + "#" + childPk;
				
				if (childPk != null && !alreadyFilledObjects.containsKey(childUniqueKey) && !path.contains(childEntity.getTableName())) {
					field.set(entity, childEntity);
					
					// keep track of the current level in the tree 
					path += "/" + entity.getTableName();
					rowToEntity(childEntity, alias, path, row, alreadyFilledObjects);
					path = path.substring(0, path.lastIndexOf("/"));
				}
			}
			else {
				for (Map.Entry<String, Object> cell : row.entrySet()) {
					final String fullName = cell.getKey();
					final String entityField = alias != null ? alias + tableColDelimiter + field.getName() : field.getName();
					
					if (fullName.equalsIgnoreCase(entityField)) {
						field.set(entity, convertTo(field.getType(), cell.getValue()));
						break;
					}
				}
			}
		}
		alreadyFilledObjects.put(uniqueKey, entity);
	}
	
	private Object getValueFromRow(String alias, String fieldName, Map<String,Object> row) {
		if (alias != null) {
			fieldName = alias + tableColDelimiter + fieldName;
		}
		for (Map.Entry<String, Object> cell : row.entrySet()) {
			if (cell.getKey().equalsIgnoreCase(fieldName)) {
				return cell.getValue();
			}
		}
		return null;
	}

    public Long getElemCount(Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElemCount((CriteriaGroup)null, entityType);
    }
    
    public Long getElemCount(Criteria criteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElemCount(new CriteriaGroup(Operator.AND, criteria), entityType);
    }
    
    public Long getElemCount(CriteriaGroup criteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	try {
			final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();

			final List<Object> params = new ArrayList<Object>();
			final StringBuilder select = new StringBuilder();
			final StringBuilder from = new StringBuilder();
			final StringBuilder join = new StringBuilder();
			final StringBuilder where = new StringBuilder();
			buildQueryRecursively(mainEntity, criteria, select, from, join, where, new Sort(), params, 0);
			
			final String sql = "SELECT COUNT (DISTINCT " + mainEntity.getTableName() + "." + mainEntity.getPrimaryKeyColumn() + ")" + from.toString() + join.toString() + where.toString();
			
			log.debug("Query: " + sqlPrettyPrint(sql) + "\t[" + toCsv(params.toArray()) + "]");
			return jdbcTemplate.queryForObject(sql, params.toArray(), Long.class);
			
		} catch (Exception e) {
			log.error("could not count elements of type " + entityType.getName(), e);
			throw e;
		}
    }
	
    /**
     * adds the given element to the DB. If there are sub elements set without an ID, these will be inserted too. Many-To-Many mappings will be ignored
     * @param obj
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
	@Transactional(propagation = Propagation.REQUIRED)
    public <T> T addElem(DatabaseEntity obj) throws Exception {
    	try {
			addElemRecursively(obj);
		} catch (Exception e) {
			log.error("could not add element of type " + obj.getClass().getName(), e);
			throw e;
		}
    	return (T)obj;
    }
    
    private void addElemRecursively(DatabaseEntity baseEntity) throws Exception {
    	
    	for (Field field : baseEntity.fields) {
			if (field.getAnnotation(MappingRelation.class) != null && field.getAnnotation(MappingRelation.class).mappingTableName().isEmpty()) {
				// these are required parent elements which will first be created
				if (DatabaseEntity.class.isAssignableFrom(field.getType())) {
					final DatabaseEntity parent = (DatabaseEntity)field.get(baseEntity);
					if (parent != null && parent.getPrimaryKeyValue() == null) {
						addElemRecursively(parent);
					}
				}
			}
    	}
    	final SimpleJdbcInsert insert = new SimpleJdbcInsert(dataSource)
	            .withTableName(baseEntity.getTableName())
	            .usingGeneratedKeyColumns(baseEntity.getPrimaryKeyColumn())
	            .usingColumns(baseEntity.getColumnNames(false).toArray(new String[0]));
    	
		final Number key = insert.executeAndReturnKey(baseEntity.toMap());
		baseEntity.setPrimaryKeyValue(key);
    	
		for (Field field : baseEntity.fields) {
			if (field.getAnnotation(MappingRelation.class) != null && field.getAnnotation(MappingRelation.class).mappingTableName().isEmpty()) {
				// these are dependent child elements which will be created after creating the parent element
				if (List.class.isAssignableFrom(field.getType())) {
	    			final List<DatabaseEntity> list = (List)field.get(baseEntity);
	    			for (DatabaseEntity child : list) {
	    				final Field fkField = child.fields.stream().filter(f -> f.getName().equalsIgnoreCase(field.getAnnotation(MappingRelation.class).joinedColumnName())).findFirst().orElse(null);
    					fkField.set(child, convertTo(fkField.getType(), baseEntity.getPrimaryKeyValue()));
    					
	    				if (child.getPrimaryKeyValue() == null) {
	    					addElemRecursively(child);
	    				}
	    			}
				}
			}
		}
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
    		try {
				// first, update the main entity
				final StringBuilder sb = new StringBuilder();
				for (String col : obj.getColumnNames(false)) {
					sb.append(col.toLowerCase()).append(" = :").append(col.toLowerCase()).append(", ");
				}
				if (sb.length() > 1) {
					sb.setLength(sb.length()-2);	// crop last comma
				}
				final String stmt = "UPDATE " + obj.getTableName() + " SET " + sb.toString() + " WHERE " + obj.getPrimaryKeyColumn().toLowerCase() + " = :" + obj.getPrimaryKeyColumn().toLowerCase();
				log.debug("Query: " + sqlPrettyPrint(stmt) + "\t[" + obj.toMap() + "]");
				namedParameterJdbcTemplate.update(stmt, obj.toMap());
				
				// check if there is a list of sub entities which also need to be added or updated
				for (Map.Entry<Class<? extends DatabaseEntity>,List<DatabaseEntity>> entry : getAllChildEntities(obj, MappingTableBehaviour.IGNORE).entrySet()) {
					final List<DatabaseEntity> elements = entry.getValue();
					for (DatabaseEntity element : elements) {
						updateElem(element);
					}
				}
			} catch (Exception e) {
				log.error("could not update element of type " + obj.getClass().getName(), e);
				throw e;
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
	public void deleteElems(Criteria criteria, Class<? extends DatabaseEntity> entityType) throws Exception {
    	deleteElems(new CriteriaGroup(Operator.AND, criteria), entityType);
    }
	
    /**
     * deletes the main entity and all of its children by their primary key (this is the only field which needs to be provided)
     * @param obj
     * @throws Exception
     */
	@Transactional(propagation = Propagation.REQUIRED)
	public void deleteElems(CriteriaGroup advancedCriteria, Class<? extends DatabaseEntity> entityType) throws Exception {
		try {
			final DatabaseEntity obj = entityType.getConstructor().newInstance();
			final WherePart whereClause = new WherePart(obj.getTableName(), advancedCriteria);
			final String stmt = "SELECT " + obj.getPrimaryKeyColumn() + " FROM " + obj.getTableName() + " WHERE " + whereClause.toString();
			
			deleteElemsRecursively(obj, stmt, whereClause.getValues());
		} catch (Exception e) {
			log.error("could not delete element of type " + entityType.getName(), e);
			throw e;
		}
	}
	
	private void deleteElemsRecursively(DatabaseEntity entity, String stmt, List<Object> params) throws Exception {
		
		for (Field field : entity.fields) {
			// discover all sub elements which are coming from sub tables
			if (field.getAnnotation(MappingRelation.class) != null) {
				final MappingRelation mapping = field.getAnnotation(MappingRelation.class);
				// if there is an m:n mapping table, remove the entry first
				if (!mapping.mappingTableName().isEmpty()) {
					final String nmDel = "DELETE FROM " + mapping.mappingTableName() + " WHERE " + mapping.masterColumnName() + " = (" + stmt + ")";
					log.debug("Query: " + sqlPrettyPrint(nmDel) + "\t[" + toCsv(params.toArray()) + "]");
					jdbcTemplate.update(nmDel, params.toArray());
				}
				else if (List.class.isAssignableFrom(field.getType())) {
					final Type genericType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
	    			final Class<?> genericClass = Class.forName(genericType.getTypeName());
	    			final DatabaseEntity childEntity = (DatabaseEntity)genericClass.getConstructor().newInstance();
	    			
	    			final String s = "SELECT " + childEntity.getPrimaryKeyColumn() + " FROM " + childEntity.getTableName() + " WHERE " + 
							field.getAnnotation(MappingRelation.class).joinedColumnName() + " IN (" + stmt + ")";
			
	    			deleteElemsRecursively(childEntity, s, params);
				}
			}
		}
		
		final String query = "DELETE FROM " + entity.getTableName() + " WHERE " + entity.getPrimaryKeyColumn() + " IN (" + stmt + ")";
		log.debug("Query: " + sqlPrettyPrint(query) + "\t[" + toCsv(params.toArray()) + "]");
		jdbcTemplate.update(query, params.toArray());
	}
    
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
    				if (f.getAnnotation(MappingRelation.class) != null && !f.getAnnotation(MappingRelation.class).mappingTableName().isEmpty() && mappingTableBehaviour == MappingTableBehaviour.IGNORE) {
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
    protected String buildQuery(DatabaseEntity entity, CriteriaGroup filter, List<Object> params) throws Exception {
    	return buildQuery(entity, filter, null, params, -1);
    }

	/**
	 * builds a query from the entity and given filters
	 * @param entity - the main entity
	 * @param filter - criteria(s) to filter on the main entity
	 * @param params - this list will be filled by the algorithm
	 * @param fetchDepth - how deep to dig down in the hierarchy level. Pass in -1 to fetch all (sub)elements 
	 * @return
	 * @throws Exception
	 */
	protected String buildQuery(DatabaseEntity entity, CriteriaGroup filter, Sort sort, List<Object> params, int fetchDepth) throws Exception {
		final StringBuilder select = new StringBuilder();
		final StringBuilder from = new StringBuilder();
		final StringBuilder join = new StringBuilder();
		final StringBuilder where = new StringBuilder();
		buildQueryRecursively(entity, "/", filter, select, from, join, where, sort, params, 0, fetchDepth);
		return select.toString() + from.toString() + join.toString() + where.toString() + (sort != null ? sort.toString() : "");
	}
	
	private void buildQueryRecursively(DatabaseEntity entity, CriteriaGroup filter, StringBuilder select, StringBuilder from, StringBuilder join, StringBuilder where, Sort orderBy, List<Object> params, int fetchDepth) throws Exception {
		buildQueryRecursively(entity, "/", filter, select, from, join, where, orderBy, params, 0, fetchDepth);
	}
	
	private void buildQueryRecursively(DatabaseEntity entity, String path, CriteriaGroup filter, StringBuilder select, StringBuilder from, StringBuilder join, StringBuilder where, Sort orderBy, List<Object> params, int currentDepth, int fetchDepth) throws Exception {
		
		currentDepth++;
		
		// search for custom sql
		if (entity.getClass().isAnnotationPresent(CustomSql.class)) {
			final CustomSql customSql = entity.getClass().getAnnotation(CustomSql.class);
			select.setLength(0);
			select.append(customSql.selectQuery());
			if (filter != null && !filter.getCriterias().isEmpty()) {
				final WherePart wp = new WherePart(null, filter);
				params.addAll(wp.getValues());
				where.append(" WHERE " + wp.toString());
			}
			return;
		}
		
		if (select.length() == 0) {
			select.append("SELECT " + getColumnsCsvWithAlias(entity.getTableName(), entity.getColumnNames(true)));
			from.append(" FROM " + entity.getTableName());
			if (filter != null && !filter.getCriterias().isEmpty()) {
				final WherePart wp = new WherePart(entity.getTableName(), filter);
				params.addAll(wp.getValues());
				where.append(" WHERE " + wp.toString());
			}
			if (orderBy == null) {
				orderBy = new Sort();
			}
			if (orderBy.getSortFields().isEmpty()) {
				orderBy.addSortField(entity.getTableName() + "." + entity.getPrimaryKeyColumn(), SortDirection.DESC);
			}
		}
		
		final Field aliasField = findField(entity.getClass(), "tableAlias");
		final String entityTableAlias;
		// always true for the main entity. We use this later for constructing the JOIN part
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
				// cycle detected. skip this entity to prevent an infinite loop
				if (path.contains(childEntity.getTableName())) {
					continue;
				}
    			String childAlias = entity.getTableName() + "_" + field.getName();
    			aliasField.set(childEntity, childAlias);
    			select.append(", ").append(getColumnsCsvWithAlias(childAlias, childEntity.getColumnNames(true)));
    			
    			// this is an m:n mapping
    			if (!relation.mappingTableName().isEmpty()) {
    				join.append(" LEFT JOIN " + relation.mappingTableName() + " ON " + relation.mappingTableName() + "." + relation.masterColumnName() + " = " + entityTableAlias + "." + entity.getPrimaryKeyColumn());
    				join.append(" LEFT JOIN " + childEntity.getTableName() + " " + childAlias + " ON " + relation.mappingTableName() + "." + relation.joinedColumnName() + " = " + childAlias + "." + childEntity.getPrimaryKeyColumn());
    			}
    			else {
    				join.append(" LEFT JOIN " + childEntity.getTableName() + " " + childAlias + " ON " + childAlias + "." + relation.joinedColumnName() + " = " + entityTableAlias + "." + relation.masterColumnName());
    			}
    			// keep track of the current level in the tree 
				path += "/" + entity.getTableName();
				buildQueryRecursively(childEntity, path, filter, select, from, join, where, orderBy, params, currentDepth, fetchDepth);
				path = path.substring(0, path.lastIndexOf("/"));
    				
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
    
    /**
     * checks if the type and object. Then converts into the correct type
     * @param type
     * @param pk
     * @return
     */
    public static Object convertTo(Class<?> type, Object obj) {
    	if (obj == null) {
    		return obj;
    	}
    	if (Number.class.isAssignableFrom(type) && obj instanceof Number) {
    		if (Long.class.isAssignableFrom(type)) {
    			return ((Number)obj).longValue();
    		}
    		if (Integer.class.isAssignableFrom(type)) {
    			return ((Number)obj).intValue();
    		}
    		if (Double.class.isAssignableFrom(type)) {
    			return ((Number)obj).doubleValue();
    		}
    		if (Float.class.isAssignableFrom(type)) {
    			return ((Number)obj).floatValue();
    		}
    	}
    	if (String.class.isAssignableFrom(type)) {
    		obj.toString();
    	}
		return obj;
	}

	public static String sqlPrettyPrint(String sql) {
		if (sql == null) {
			return null;
		}
		return sql.replaceAll("SELECT", "\n\tSELECT").replaceAll("FROM", "\n\tFROM").replaceAll("LEFT JOIN", "\n\tLEFT JOIN").replaceAll("WHERE", "\n\tWHERE");
	}
	
	
	private String addPaging(String query, int currentPage, int pageSize, int totalRows) throws Exception {
		
		int startRow = (currentPage - 1) * pageSize;
		
		if (DB_PRODUCT.equals("Microsoft SQL Server")) {
			return query + " OFFSET " + startRow + " ROWS FETCH NEXT " + pageSize + " ROWS ONLY";
		}
		else {
			int endRow = Math.min(startRow + pageSize, totalRows);
			return query + " LIMIT " + (endRow - startRow) + " OFFSET " + startRow;	
		}
	}
	
	private String getProduct() {
	    return this.jdbcTemplate.execute(new ConnectionCallback<String>() {
	        @Override
	        public String doInConnection(Connection connection) throws SQLException, DataAccessException {
	            return connection.getMetaData().getDatabaseProductName();
	        }
	    });
	}
    
}
