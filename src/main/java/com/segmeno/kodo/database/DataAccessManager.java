package com.segmeno.kodo.database;

import com.segmeno.kodo.annotation.Column;
import com.segmeno.kodo.annotation.CustomSql;
import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.CriteriaGroup;
import com.segmeno.kodo.transport.IKodoEnum;
import com.segmeno.kodo.transport.Operator;
import com.segmeno.kodo.transport.Sort;
import com.segmeno.kodo.transport.Sort.SortDirection;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

public class DataAccessManager {

	private static final Logger log = LogManager.getLogger(DataAccessManager.class);

	protected static final Pattern VALID_COLNAME_PATTERN = Pattern.compile("\\A[a-zA-Z_]{1}[0-9a-zA-Z_]*\\Z");
	protected static final String SUB_FIELD_DELIMITER = "_";
	protected static final String TABLE_COL_DELIMITER = ".";
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

	public DataAccessManager(final JdbcTemplate jdbcTemplate) throws SQLException {
		this.jdbcTemplate = jdbcTemplate;
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
		this.DB_PRODUCT = getProduct();
	}

	/**
	 * returns all entities of entityType by performing a simple select without any filters
	 * @param entityType
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElems((CriteriaGroup)null, entityType);
	}

	/**
	 * returns a list of the queried entity type, considering a criteria for filtering
	 * @param criteria the criteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final Criteria criteria, final Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElems(criteria, entityType, -1);
    }

	/**
	 * returns a list of the queried entity type, considering a criteria for filtering
	 * @param criteria the criteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @param fetchDepth - how deep to dig down in the hierarchy level. Pass in -1 to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final Criteria criteria, final Class<? extends DatabaseEntity> entityType, final Integer fetchDepth) throws Exception {
    	return getElems(new CriteriaGroup(Operator.AND, criteria), entityType, fetchDepth);
    }

	/**
	 * returns a list of the queried entity type, considering a criteria for filtering. Fills all sub elements and their children
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final CriteriaGroup advancedCriteria, final Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElems(advancedCriteria, entityType, null, -1);
	}

	/**
	 * returns a list of the queried entity type, considering a criteria for filtering. Fills all sub elements and their children
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @param fetchDepth - how deep to dig down in the hierarchy level. Pass in -1 to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final CriteriaGroup advancedCriteria, final Class<? extends DatabaseEntity> entityType, final Integer fetchDepth) throws Exception {
		return getElems(advancedCriteria, entityType, null, fetchDepth);
	}

	/**
	 * returns a list of the queried entity type, considering a criteria for filtering
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @param sort sort options
	 * @param fetchDepth - how deep to dig down in the hierarchy level. Pass in -1 to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final CriteriaGroup advancedCriteria, final Class<? extends DatabaseEntity> entityType, final Sort sort, final Integer fetchDepth) throws Exception {

		try {
			final ArrayList<Object> params = new ArrayList<Object>();
			final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();
			final String query = buildQuery(mainEntity, advancedCriteria, sort, params, fetchDepth);

			log.debug("Query: " + sqlPrettyPrint(query) + "\t" + params);
			final List<Map<String,Object>> rows = jdbcTemplate.queryForList(query, params.toArray());
			return rowsToObjects(mainEntity, rows);
		} catch (final Exception e) {
			log.error("could not get elements of type " + entityType.getName(), e);
			throw e;
		}
    }

	/**
     * returns a list of the queried entity type, which PrimaryKey is contained in the query given
     * @param advancedCriteria the advancedCriteria for filtering the main entity
     * @param entityType the main entity type to query
     * @return
     * @throws Exception
     */
    public <T> List<T> getElemsByPkQuery(final String queryByPK, final ArrayList<Object> queryByPKparams, final Class<? extends DatabaseEntity> entityType) throws Exception {
        return getElemsByPkQuery(queryByPK, queryByPKparams, entityType, null, -1);
    }

    /**
     * returns a list of the queried entity type, which PrimaryKey is contained in the query given
     * @param advancedCriteria the advancedCriteria for filtering the main entity
     * @param entityType the main entity type to query
     * @param fetchDepth - how deep to dig down in the hierarchy level. Pass in -1 to fetch all (sub)elements
     * @return
     * @throws Exception
     */
    public <T> List<T> getElemsByPkQuery(final String queryByPK, final ArrayList<Object> queryByPKparams, final Class<? extends DatabaseEntity> entityType, final Integer fetchDepth) throws Exception {
        return getElemsByPkQuery(queryByPK, queryByPKparams, entityType, null, fetchDepth);
    }

	/**
     * returns a list of the queried entity type, which PrimaryKey is contained in the query given
     * @param advancedCriteria the advancedCriteria for filtering the main entity
     * @param entityType the main entity type to query
     * @param sort sort options
     * @param fetchDepth - how deep to dig down in the hierarchy level. Pass in -1 to fetch all (sub)elements
     * @return
     * @throws Exception
     */
    public <T> List<T> getElemsByPkQuery(final String queryByPK, final ArrayList<Object> queryByPKparams, final Class<? extends DatabaseEntity> entityType, final Sort sort, final Integer fetchDepth) throws Exception {

        try {
            final ArrayList<Object> params = new ArrayList<Object>();
            final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();
            final ArrayList<Integer> fakeParams = new ArrayList<>();
            fakeParams.add(666);
            String query = buildQuery(mainEntity, new CriteriaGroup(Operator.AND, new Criteria(mainEntity.getPrimaryKeyColumn(), Operator.IN_SET, fakeParams)), sort, params, fetchDepth);

            query = query.replace("?", queryByPK);

            log.debug("Query: " + sqlPrettyPrint(query) + "\t" + queryByPKparams);
            final List<Map<String,Object>> rows = jdbcTemplate.queryForList(query, queryByPKparams.toArray());
            return rowsToObjects(mainEntity, rows);
        } catch (final Exception e) {
            log.error("could not get elements of type " + entityType.getName(), e);
            throw e;
        }
    }

	public List<Map<String,Object>> getRecords(final String tableName, final int pageSize, final int currentPage) throws Exception {
		return getRecords(tableName, (Criteria)null, pageSize, currentPage);
	}

	public List<Map<String,Object>> getRecords(final String tableName, final Criteria criteria, final int pageSize, final int currentPage) throws Exception {
		return getRecords(tableName, new CriteriaGroup(Operator.AND, criteria), pageSize, currentPage, null);
	}

	public List<Map<String,Object>> getRecords(final String tableName, final CriteriaGroup criteriaGroup, final int pageSize, final int currentPage, final Sort sort) throws Exception {

		if (sort == null) {
			throw new Exception("a sort is required in order to use paging!");
		}
		final WherePart where = new WherePart(DB_PRODUCT, tableName, criteriaGroup);
		String stmt = "SELECT * FROM " + tableName + " WHERE " + where.toString() + sort.toString();
		final String count = "SELECT COUNT(*) FROM (" + stmt + ")";

		final Integer totalRows = jdbcTemplate.queryForObject(count, Integer.class);
		stmt = addPaging(stmt, currentPage, pageSize, totalRows);
		log.debug("Query: " + sqlPrettyPrint(stmt) + "\t" + where.getValues().toArray());

		return jdbcTemplate.queryForList(stmt, where.getValues().toArray());
	}

	private <T> List<T> rowsToObjects(DatabaseEntity baseEntity, final List<Map<String, Object>> rows) throws Exception {
		final Map<String,T> resultMap = new LinkedHashMap<String,T>();

		final Map<String,Object> alreadyFilledObjects = new HashMap<String, Object>();
		String pk;

		for (final Map<String,Object> row : rows) {

			pk = String.valueOf(getValueFromRow(baseEntity.getTableName(), baseEntity.getPrimaryKeyColumn(), row, false));
			if (!resultMap.containsKey(pk)) {
				baseEntity = baseEntity.getClass().getConstructor().newInstance();
				alreadyFilledObjects.clear();
			}
			else {
				baseEntity = (DatabaseEntity)resultMap.get(pk);
			}
			final String startPath = baseEntity.getTableName() == null ? "" : baseEntity.getTableName();
			rowToEntity(baseEntity, baseEntity.getTableName(), startPath, row, alreadyFilledObjects);
			resultMap.put(pk, (T)baseEntity);
		}
		return resultMap.values().stream().collect(Collectors.toList());
	}

	private void rowToEntity(final DatabaseEntity entity, final String alias, String path, final Map<String,Object> row, final Map<String,Object> alreadyFilledObjects) throws Exception {
		// first thing to do: retrieve pk value and build unique key
		final String pk = String.valueOf(getValueFromRow(alias, entity.getPrimaryKeyColumn(), row, false));
		final String uniqueKey = alias + "#" + pk;

		for (final Field field : entity.fields) {

			if (List.class.isAssignableFrom(field.getType())) {

				final Type genericType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
    			final Class<?> genericClass = Class.forName(genericType.getTypeName());

				if (DatabaseEntity.class.isAssignableFrom(genericClass)) {

	    			DatabaseEntity childEntity = (DatabaseEntity)genericClass.getConstructor().newInstance();
	    			final String subAlias;
	    			// this is allowed to happen if the database entity has a custom sql annotation
	    			if (entity.getTableName() == null) {
	    				if (entity.getClass().isAnnotationPresent(CustomSql.class)) {
	    					subAlias = field.getName();
						}
	    				else {
	    					throw new Exception("no table name defined! Either change 'getTableName' method of " + entity.getClass().getName() + " to return a value or use the @CustomSql annotation");
	    				}
	    			}
	    			else {
	    				subAlias = entity.getTableName() + SUB_FIELD_DELIMITER + field.getName();
	    			}

	    			final Object childPk = getValueFromRow(subAlias, childEntity.getPrimaryKeyColumn(), row, true);
	    			final String childUniqueKey = subAlias + "#" + childPk;

	    			if (childPk != null) {
	    				List<DatabaseEntity> list = (List)field.get(entity);
	    				if (list == null) {
							list = new ArrayList<>();
						}

	    				if (!alreadyFilledObjects.containsKey(childUniqueKey)) {
	    					field.set(entity, list);
	    					list.add(childEntity);
	    					alreadyFilledObjects.put(childUniqueKey, childEntity);
	    				}
	    				else {
	    					childEntity = (DatabaseEntity)alreadyFilledObjects.get(childUniqueKey);
	    				}
						// keep track of the current level in the tree
						path += "/" + childEntity.getTableName();
						rowToEntity(childEntity, subAlias, path, row, alreadyFilledObjects);
						path = path.substring(0, path.lastIndexOf("/"));
					}
				}
			}
			else if (DatabaseEntity.class.isAssignableFrom(field.getType())) {
				DatabaseEntity childEntity = (DatabaseEntity)field.get(entity);
				if (childEntity == null) {
					childEntity = (DatabaseEntity)field.getType().getConstructor().newInstance();
				}
				final String subAlias;
    			// this is allowed to happen if the database entity has a custom sql annotation
    			if (entity.getTableName() == null) {
    				if (entity.getClass().isAnnotationPresent(CustomSql.class)) {
    					subAlias = field.getName();
					}
    				else {
    					throw new Exception("no table name defined! Either change 'getTableName' method of " + entity.getClass().getName() + " to return a value or use the @CustomSql annotation");
    				}
    			}
    			else {
    				subAlias = entity.getTableName() + SUB_FIELD_DELIMITER + field.getName();
    			}

    			final Object childPk = getValueFromRow(subAlias, childEntity.getPrimaryKeyColumn(), row, true);
    			final String childUniqueKey = subAlias.replace("_", "#"+pk) + "#" + childPk;

    			if (childPk != null) {
    				if (!alreadyFilledObjects.containsKey(childUniqueKey)) {
    					field.set(entity, childEntity);
    				}
    				else {
    					childEntity = (DatabaseEntity)alreadyFilledObjects.get(childUniqueKey);
    				}
					// keep track of the current level in the tree
					path += "/" + childEntity.getTableName();
					rowToEntity(childEntity, subAlias, path, row, alreadyFilledObjects);
					path = path.substring(0, path.lastIndexOf("/"));
				}
			}
			else {
				final String colName = field.getAnnotation(Column.class) != null ? field.getAnnotation(Column.class).columnName() : field.getName();
				// this is a field of the main entity (on first level). Then we do not use aliases
				final String entityField;
				if (!path.contains("/") || alias == null) {
					entityField = colName;
				}
				else {
					entityField = alias + TABLE_COL_DELIMITER + colName;
				}
				for (final Map.Entry<String, Object> cell : row.entrySet()) {
					final String fullName = cell.getKey();

					if (fullName.equalsIgnoreCase(entityField)) {
						field.set(entity, convertTo(field.getType(), cell.getValue()));
						break;
					}
				}
			}
		}
		alreadyFilledObjects.put(uniqueKey, entity);
	}

	private Object getValueFromRow(final String alias, String fieldName, final Map<String,Object> row, final boolean useAlias) {
		if (alias != null && useAlias) {
			fieldName = alias + TABLE_COL_DELIMITER + fieldName;
		}
		for (final Map.Entry<String, Object> cell : row.entrySet()) {
			if (cell.getKey().equalsIgnoreCase(fieldName)) {
				return cell.getValue();
			}
		}
		return null;
	}

    public Long getElemCount(final Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElemCount((CriteriaGroup)null, entityType);
    }

    public Long getElemCount(final Criteria criteria, final Class<? extends DatabaseEntity> entityType) throws Exception {
    	return getElemCount(new CriteriaGroup(Operator.AND, criteria), entityType);
    }

    public Long getElemCount(final CriteriaGroup criteria, final Class<? extends DatabaseEntity> entityType) throws Exception {
    	try {
			final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();

			final ArrayList<Object> params = new ArrayList<Object>();
			final StringBuilder select = new StringBuilder();
			final StringBuilder from = new StringBuilder();
			final StringBuilder join = new StringBuilder();
			final StringBuilder where = new StringBuilder();
			buildQueryRecursively(mainEntity, criteria, select, from, join, where, new Sort(), params, 0);

			final String sql = "SELECT COUNT(DISTINCT " + mainEntity.getTableName() + "." + mainEntity.getPrimaryKeyColumn() + ")" + from.toString() + join.toString() + where.toString();

			log.debug("Query: " + sqlPrettyPrint(sql) + "\t[" + toCsv(params.toArray()) + "]");
			return jdbcTemplate.queryForObject(sql, params.toArray(), Long.class);

		} catch (final Exception e) {
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
    public <T> T addElem(final DatabaseEntity obj) throws Exception {
    	try {
			addElemRecursively(obj);
		} catch (final Exception e) {
			log.error("could not add element of type " + obj.getClass().getName(), e);
			throw e;
		}
    	return (T)obj;
    }

    private void addElemRecursively(final DatabaseEntity entity) throws Exception {

    	for (final Field field : entity.fields) {
			if (field.getAnnotation(MappingRelation.class) != null && field.getAnnotation(MappingRelation.class).mappingTableName().isEmpty()) {
				// these are required parent elements which will first be created if not existing
				if (DatabaseEntity.class.isAssignableFrom(field.getType())) {

					final DatabaseEntity elem = (DatabaseEntity)field.get(entity);
					if (elem != null) {
						if (elem.getPrimaryKeyValue() == null) {
							addElemRecursively(elem);
						}
					}
				}
			}
    	}
    	final SimpleJdbcInsert insert = new SimpleJdbcInsert(jdbcTemplate)
	            .withTableName(entity.getTableName())
	            .usingGeneratedKeyColumns(entity.getPrimaryKeyColumn())
	            .usingColumns(entity.getColumnNames(false).toArray(new String[0]));

		final Number key = insert.executeAndReturnKey(entity.toMap());
		entity.setPrimaryKeyValue(key);

		for (final Field field : entity.fields) {
			if (field.getAnnotation(MappingRelation.class) != null && field.getAnnotation(MappingRelation.class).mappingTableName().isEmpty()) {
				// these are dependent child elements which will be created after creating the parent element
				if (List.class.isAssignableFrom(field.getType())) {
	    			final List<DatabaseEntity> list = (List)field.get(entity);
	    			for (final DatabaseEntity child : list) {
	    				final Field fkField = child.fields.stream().filter(f -> f.getName().equalsIgnoreCase(field.getAnnotation(MappingRelation.class).joinedColumnName())).findFirst().orElse(null);
    					fkField.set(child, convertTo(fkField.getType(), entity.getPrimaryKeyValue()));

	    				if (child.getPrimaryKeyValue() == null) {
	    					addElemRecursively(child);
	    				}
	    			}
				}
			}
		}
    }

	public void updateElems(final List<DatabaseEntity> entities) throws Exception {
    	for (final DatabaseEntity entity : entities) {
    		updateElem(entity);
    	}
    }

	public void updateElem(final DatabaseEntity obj) throws Exception {
    	if (obj.getPrimaryKeyValue()== null || Integer.valueOf(String.valueOf(obj.getPrimaryKeyValue())) == -1) {
			addElem(obj);
		}
    	else {
    		try {
				// first, update the main entity
				final StringBuilder sb = new StringBuilder();
				for (final String col : obj.getColumnNames(false)) {
					sb.append(col.toLowerCase()).append(" = :").append(col.toLowerCase()).append(", ");
				}
				if (sb.length() > 1) {
					sb.setLength(sb.length()-2);	// crop last comma
				}
				final String stmt = "UPDATE " + obj.getTableName() + " SET " + sb.toString() + " WHERE " + obj.getPrimaryKeyColumn().toLowerCase() + " = :" + obj.getPrimaryKeyColumn().toLowerCase();
				log.debug("Query: " + sqlPrettyPrint(stmt) + "\t[" + obj.toMap() + "]");
				namedParameterJdbcTemplate.update(stmt, obj.toMap());

				// check if there is a list of sub entities which also need to be added or updated
				for (final Map.Entry<Class<? extends DatabaseEntity>,List<DatabaseEntity>> entry : getAllChildEntities(obj, MappingTableBehaviour.IGNORE).entrySet()) {
					final List<DatabaseEntity> elements = entry.getValue();
					for (final DatabaseEntity element : elements) {
						updateElem(element);
					}
				}
			} catch (final Exception e) {
				log.error("could not update element of type " + obj.getClass().getName(), e);
				throw e;
			}
    	}
    }

    /**
     * deletes all elements which expect the specified type and match the provided filter
     * @param criteria
     * @param entityType
     * @throws Exception
     */
	public void deleteElems(final Criteria criteria, final Class<? extends DatabaseEntity> entityType) throws Exception {
    	deleteElems(new CriteriaGroup(Operator.AND, criteria), entityType);
    }

    /**
     * deletes the main entity and all of its children by their primary key (this is the only field which needs to be provided)
     * @param advancedCriteria
     * @param entityType
     * @throws Exception
     */
	public void deleteElems(final CriteriaGroup advancedCriteria, final Class<? extends DatabaseEntity> entityType) throws Exception {
		try {
			final DatabaseEntity obj = entityType.getConstructor().newInstance();
			final WherePart whereClause = new WherePart(DB_PRODUCT, obj.getTableName(), advancedCriteria);
			final String stmt = "SELECT " + obj.getPrimaryKeyColumn() + " FROM " + obj.getTableName() + " WHERE " + whereClause.toString();

			deleteElemsRecursively(obj, stmt, whereClause.getValues());
		} catch (final Exception e) {
			log.error("could not delete element of type " + entityType.getName(), e);
			throw e;
		}
	}

	private void deleteElemsRecursively(final DatabaseEntity entity, final String stmt, final List<Object> params) throws Exception {

		for (final Field field : entity.fields) {
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

		// note: this must be done in two steps to be able to work in MySql (where a subquery for insert/update/delete operations cannot reference the main table)
		//final String query = "DELETE FROM " + entity.getTableName() + " WHERE " + entity.getPrimaryKeyColumn() + " IN (" + stmt + ")";
		final List<Long> idsToDelete = jdbcTemplate.query(stmt, new RowMapper<Long>() {
			@Override
			public Long mapRow(final ResultSet rs, final int rowNum) throws SQLException {
				try {
					return rs.getLong(entity.getPrimaryKeyColumn());
				} catch (final Exception e) {
					log.warn("could not retrieve ids of elements to delete", e);
				}
				return -1L;
			}
		}, params.toArray());

		if (!idsToDelete.isEmpty()) {
			final String query = "DELETE FROM " + entity.getTableName() + " WHERE " + entity.getPrimaryKeyColumn() + " IN (" +  toCsv(idsToDelete.toArray()) + ")";
			log.debug("Query: " + sqlPrettyPrint(query) + "\t[" + toCsv(idsToDelete.toArray()) + "]");
			jdbcTemplate.update(query);
		}
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

    protected String toCsv(final Collection<Object> values) {
    	final StringBuilder sb = new StringBuilder();
		for (final Object t : values) {
			sb.append(t).append(",");
		}
		if(sb.length() > 0) {
			sb.setLength(sb.length()-1); // crop last comma
		}
		return sb.toString();
	}

    protected String getColumnsCsv(final String tableAlias, final List<String> cols, final boolean useAlias) throws Exception {
    	final StringBuilder sb = new StringBuilder();
    	for (final String col : cols) {
    		validateColName(col);
    		final String s;
    		if (useAlias) {
    			s = tableAlias + "." + col + " AS \"" + tableAlias + TABLE_COL_DELIMITER + col + "\"";
    		}
    		else {
    			s = tableAlias + "." + col;
    		}
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
     * @param mappingTableBehaviour
     * @return
     * @throws Exception
     */
    protected Map<Class<? extends DatabaseEntity>,List<DatabaseEntity>> getAllChildEntities(final DatabaseEntity mainEntity, final MappingTableBehaviour mappingTableBehaviour) throws Exception {
    	final Map<Class<? extends DatabaseEntity>,List<DatabaseEntity>> typeToEntries = new HashMap<>();

    	for (final Field f : mainEntity.fields) {
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
    public String buildQuery(final DatabaseEntity entity, final CriteriaGroup filter, final ArrayList<Object> params) throws Exception {
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
    public String buildQuery(final DatabaseEntity entity, final CriteriaGroup filter, final Sort sort, final ArrayList<Object> params, final Integer fetchDepth) throws Exception {
		final StringBuilder select = new StringBuilder();
		final StringBuilder from = new StringBuilder();
		final StringBuilder join = new StringBuilder();
		final StringBuilder where = new StringBuilder();
		buildQueryRecursively(entity, "/", filter, select, from, join, where, sort, params, 0, fetchDepth == null ? -1 : fetchDepth);
		return select.toString() + from.toString() + join.toString() + where.toString() + (sort != null ? sort.toString() : "");
	}

	private void buildQueryRecursively(final DatabaseEntity entity, final CriteriaGroup filter, final StringBuilder select, final StringBuilder from, final StringBuilder join, final StringBuilder where, final Sort orderBy, final ArrayList<Object> params, final int fetchDepth) throws Exception {
		buildQueryRecursively(entity, "/", filter, select, from, join, where, orderBy, params, 0, fetchDepth);
	}

	private void buildQueryRecursively(final DatabaseEntity entity, String path, final CriteriaGroup filter, final StringBuilder select, final StringBuilder from, final StringBuilder join, final StringBuilder where, Sort orderBy, final ArrayList<Object> params, int currentDepth, final int fetchDepth) throws Exception {

		currentDepth++;

		// search for custom sql
		if (entity.getClass().isAnnotationPresent(CustomSql.class)) {
			final CustomSql customSql = entity.getClass().getAnnotation(CustomSql.class);
			select.setLength(0);
			select.append(customSql.selectQuery());
			if (filter != null && !filter.getCriterias().isEmpty()) {
				final WherePart wp = new WherePart(DB_PRODUCT, (String)null, filter);
				params.addAll(wp.getValues());
				where.append(" WHERE " + wp.toString());
			}
			return;
		}

		if (select.length() == 0) {
			select.append("SELECT " + getColumnsCsv(entity.getTableName(), entity.getColumnNames(true), false));
			from.append(" FROM " + entity.getTableName());
			if (filter != null && !filter.getCriterias().isEmpty()) {
				final WherePart wp = new WherePart(DB_PRODUCT, entity.getTableName(), filter);
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

		for (final Field field : entity.fields) {
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
    			final String childAlias = entity.getTableName() + SUB_FIELD_DELIMITER + field.getName();
    			aliasField.set(childEntity, childAlias);
    			select.append(", ").append(getColumnsCsv(childAlias, childEntity.getColumnNames(true), true));

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

    private Field findField(final Class<?> clazz, final String fieldName) {
    	for (final Field f : clazz.getDeclaredFields()) {
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
     * @param obj
     * @return
     */
    public static Object convertTo(final Class<?> type, final Object obj) {
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
    	if (IKodoEnum.class.isAssignableFrom(type) && obj instanceof String) {
    		for(final Enum constant : ((Class<Enum>) type).getEnumConstants()) {
    			if(((IKodoEnum) constant).getValue().equals(obj)) {
    				return constant;
    			}
    		}
    	}
    	if (String.class.isAssignableFrom(type)) {
    		obj.toString();
    	}
		return obj;
	}

	public static String sqlPrettyPrint(final String sql) {
		if (sql == null) {
			return null;
		}
		return sql.replaceAll("SELECT", "\n\tSELECT").replaceAll("FROM", "\n\tFROM").replaceAll("LEFT JOIN", "\n\tLEFT JOIN").replaceAll("WHERE", "\n\tWHERE");
	}

	public static void validateColName(final String colname) throws Exception {
		if(!VALID_COLNAME_PATTERN.matcher(colname).matches()) {
			throw new Exception("possible attempt of SQL Injection, invalid colname found: " + colname);
		}
	}

	private String addPaging(final String query, final int currentPage, final int pageSize, final int totalRows) throws Exception {

		final int startRow = (currentPage - 1) * pageSize;

		if (DB_PRODUCT.equals("Microsoft SQL Server")) {
			return query + " OFFSET " + startRow + " ROWS FETCH NEXT " + pageSize + " ROWS ONLY";
		}
		else {
			final int endRow = Math.min(startRow + pageSize, totalRows);
			return query + " LIMIT " + (endRow - startRow) + " OFFSET " + startRow;
		}
	}

	private String getProduct() {
	    return this.jdbcTemplate.execute(new ConnectionCallback<String>() {
	        @Override
	        public String doInConnection(final Connection connection) throws SQLException, DataAccessException {
	            return connection.getMetaData().getDatabaseProductName();
	        }
	    });
	}

}
