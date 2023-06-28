package com.segmeno.kodo.database;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

import com.segmeno.kodo.annotation.Column;
import com.segmeno.kodo.annotation.CustomSql;
import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.CriteriaGroup;
import com.segmeno.kodo.transport.IKodoEnum;
import com.segmeno.kodo.transport.Operator;
import com.segmeno.kodo.transport.Sort;
import com.segmeno.kodo.transport.Sort.SortDirection;

public class DataAccessManager {

	private static final Logger log = LogManager.getLogger(DataAccessManager.class);

	protected static final Pattern VALID_COLNAME_PATTERN = Pattern.compile("\\A[a-zA-Z_]{1}[0-9a-zA-Z_]*\\Z");
	protected static final String SUB_FIELD_DELIMITER = "_";
	protected static final String TABLE_COL_DELIMITER = ".";
	protected JdbcTemplate jdbcTemplate;
	protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	// H2, MySQL, Microsoft SQL Server, Oracle, PostgreSQL, Apache Derby, HSQL
	// Database Engine
	private final String DB_PRODUCT;

	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}

	public DataAccessManager(final JdbcTemplate jdbcTemplate) throws SQLException {
		this.jdbcTemplate = jdbcTemplate;
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate);
		this.DB_PRODUCT = getProduct();
	}

	/**
	 * returns all entities of entityType by performing a simple select without any
	 * filters
	 * 
	 * @param entityType
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElems((CriteriaGroup) null, entityType);
	}

	/**
	 * returns a list of the queried entity type, considering a criteria for
	 * filtering
	 * 
	 * @param criteria   the criteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final Criteria criteria, final Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElems(criteria, entityType, -1);
	}

	/**
	 * returns a list of the queried entity type, considering a criteria for
	 * filtering
	 * 
	 * @param criteria   the criteria for filtering the main entity
	 * @param entityType the main entity type to query
	 * @param fetchDepth - how deep to dig down in the hierarchy level. Pass in -1
	 *                   to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final Criteria criteria, final Class<? extends DatabaseEntity> entityType, final Integer fetchDepth)
			throws Exception {
		return getElems(new CriteriaGroup(Operator.AND, criteria), entityType, fetchDepth);
	}

	/**
	 * returns a list of the queried entity type, considering a criteria for
	 * filtering. Fills all sub elements and their children
	 * 
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType       the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final CriteriaGroup advancedCriteria, final Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElems(advancedCriteria, entityType, null, -1);
	}

	/**
	 * returns a list of the queried entity type, considering a criteria for
	 * filtering. Fills all sub elements and their children
	 * 
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType       the main entity type to query
	 * @param fetchDepth       - how deep to dig down in the hierarchy level. Pass
	 *                         in -1 to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final CriteriaGroup advancedCriteria, final Class<? extends DatabaseEntity> entityType,
			final Integer fetchDepth) throws Exception {
		return getElems(advancedCriteria, entityType, null, fetchDepth);
	}

	/**
	 * returns a list of the queried entity type, considering a criteria for
	 * filtering
	 * 
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType       the main entity type to query
	 * @param sort             sort options
	 * @param fetchDepth       - how deep to dig down in the hierarchy level. Pass
	 *                         in -1 to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElems(final CriteriaGroup advancedCriteria, final Class<? extends DatabaseEntity> entityType, final Sort sort,
			final Integer fetchDepth) throws Exception {
		try {
			final ArrayList<Object> params = new ArrayList<Object>();
			final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();
			final String query = buildQuery(mainEntity, advancedCriteria, sort, params, fetchDepth);

			if (log.isDebugEnabled()) {
				log.debug("Query: " + sqlPrettyPrint(query) + "\t" + params);
			}
			final List<Map<String, Object>> rows = jdbcTemplate.queryForList(query, params.toArray());
			if (log.isTraceEnabled()) {
				log.trace("Result: " + rows.stream().map(m -> m.toString()).collect(Collectors.joining("\n")));
			}
			return rowsToObjects(mainEntity, rows);
		} catch (final Exception e) {
			log.error("could not get elements of type " + entityType.getName(), e);
			throw e;
		}
	}

	/**
	 * returns a list of the queried entity type, which PrimaryKey is contained in
	 * the query given
	 * 
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType       the main entity type to query
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElemsByPkQuery(final String queryByPK, final ArrayList<Object> queryByPKparams,
			final Class<? extends DatabaseEntity> entityType) throws Exception {
		return getElemsByPkQuery(queryByPK, queryByPKparams, entityType, null, -1);
	}

	/**
	 * returns a list of the queried entity type, which PrimaryKey is contained in
	 * the query given
	 * 
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType       the main entity type to query
	 * @param fetchDepth       - how deep to dig down in the hierarchy level. Pass
	 *                         in -1 to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElemsByPkQuery(final String queryByPK, final ArrayList<Object> queryByPKparams,
			final Class<? extends DatabaseEntity> entityType, final Integer fetchDepth) throws Exception {
		return getElemsByPkQuery(queryByPK, queryByPKparams, entityType, null, fetchDepth);
	}

	/**
	 * returns a list of the queried entity type, which PrimaryKey is contained in
	 * the query given
	 * 
	 * @param advancedCriteria the advancedCriteria for filtering the main entity
	 * @param entityType       the main entity type to query
	 * @param sort             sort options
	 * @param fetchDepth       - how deep to dig down in the hierarchy level. Pass
	 *                         in -1 to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getElemsByPkQuery(final String queryByPK, final ArrayList<Object> queryByPKparams,
			final Class<? extends DatabaseEntity> entityType, final Sort sort, final Integer fetchDepth) throws Exception {

		try {
			final ArrayList<Object> params = new ArrayList<Object>();
			final DatabaseEntity mainEntity = entityType.getConstructor().newInstance();
			final ArrayList<Integer> fakeParams = new ArrayList<>();
			fakeParams.add(666);
			String query = buildQuery(mainEntity,
					new CriteriaGroup(Operator.AND, new Criteria(mainEntity.getPrimaryKeyColumn(), Operator.IN_SET, fakeParams)), sort,
					params, fetchDepth);

			query = query.replace("?", queryByPK);

			if (log.isDebugEnabled()) {
				log.debug("Query: " + sqlPrettyPrint(query) + "\t" + queryByPKparams);
			}
			final List<Map<String, Object>> rows = jdbcTemplate.queryForList(query, queryByPKparams.toArray());
			if (log.isTraceEnabled()) {
				log.trace("Result: " + rows.stream().map(m -> m.toString()).collect(Collectors.joining("\n")));
			}
			return rowsToObjects(mainEntity, rows);
		} catch (final Exception e) {
			log.error("could not get elements of type " + entityType.getName(), e);
			throw e;
		}
	}

	public List<Map<String, Object>> getRecords(final String tableName, final int pageSize, final int currentPage) throws Exception {
		return getRecords(tableName, (Criteria) null, pageSize, currentPage);
	}

	public List<Map<String, Object>> getRecords(final String tableName, final Criteria criteria, final int pageSize, final int currentPage)
			throws Exception {
		return getRecords(tableName, new CriteriaGroup(Operator.AND, criteria), pageSize, currentPage, null);
	}

	public List<Map<String, Object>> getRecords(final String tableName, final CriteriaGroup criteriaGroup, final int pageSize,
			final int currentPage, final Sort sort) throws Exception {

		if (sort == null) {
			throw new Exception("a sort is required in order to use paging!");
		}
		final WherePart where = new WherePart(DB_PRODUCT, tableName, criteriaGroup);
		String stmt = "SELECT * FROM " + tableName + " WHERE " + where.toString() + sort.toString();
		final String count = "SELECT COUNT(*) FROM (" + stmt + ")";

		final Integer totalRows = jdbcTemplate.queryForObject(count, Integer.class);
		stmt = addPaging(stmt, currentPage, pageSize, totalRows);
		if (log.isDebugEnabled()) {
			log.debug("Query: " + sqlPrettyPrint(stmt) + "\t" + where.getValues().toArray());
		}
		List<Map<String, Object>> rows = jdbcTemplate.queryForList(stmt, where.getValues().toArray());
		if (log.isTraceEnabled()) {
			log.trace("Result: " + rows.stream().map(m -> m.toString()).collect(Collectors.joining("\n")));
		}
		return rows;
	}

	private <T> List<T> rowsToObjects(final DatabaseEntity baseEntityTemplate, final List<Map<String, Object>> rows) throws Exception {
		final String startPath = baseEntityTemplate.getTableName() == null ? "" : baseEntityTemplate.getTableName();
		final Map<String, T> pk2entity = new LinkedHashMap<String, T>();
		final HashMap<String, HashMap<String, Object>> pk2alreadyFilledObjects = new HashMap<>();

		for (final Map<String, Object> row : rows) {
			final String pk = pkStr(getValueFromRow(baseEntityTemplate.getTableName(), baseEntityTemplate.getPrimaryKeyColumn(), row, false));
			if(pk == null) {
				throw new RuntimeException("Primary Key of root entity must not be null -> column " + baseEntityTemplate.getPrimaryKeyColumn());
			}
			
			final HashMap<String, Object> alreadyFilledObjects;
			DatabaseEntity baseEntity = (DatabaseEntity) pk2entity.get(pk);
			final boolean alreadyFilled;
			if (baseEntity == null) {
				baseEntity = baseEntityTemplate.getClass().getConstructor().newInstance();
				pk2entity.put(pk, (T) baseEntity);
				alreadyFilledObjects = new HashMap<>();
				pk2alreadyFilledObjects.put(pk, alreadyFilledObjects);
				alreadyFilled = false;
			} else {
				alreadyFilledObjects = pk2alreadyFilledObjects.get(pk);
				alreadyFilled = true;
			}
			
			rowToEntity(baseEntity, pk, baseEntity.getTableName(), startPath, row, alreadyFilledObjects, alreadyFilled);
		}
		pk2alreadyFilledObjects.clear();
		return pk2entity.values().stream().collect(Collectors.toList());
	}

	private String pkStr(Object pk) {
		return pk == null ? null : String.valueOf(pk);
	}

	private void rowToEntity(final DatabaseEntity entity, final String pk, final String alias, String path, final Map<String, Object> row,
			final Map<String, Object> alreadyFilledObjects, boolean entityWasAlreadyFilled) throws Exception {
		// first thing to do: retrieve pk value and build unique key
		final String uniqueKey = alias + "#" + pk;
		
		if(log.isDebugEnabled() && !entityWasAlreadyFilled) {
			log.debug("filling " + uniqueKey + " from " + row);
		}
		
		for (final Field field : entity.getCachedDbFields()) {
			if (List.class.isAssignableFrom(field.getType())) {
				final Type genericType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
				final Class<?> genericClass = Class.forName(genericType.getTypeName());

				if (DatabaseEntity.class.isAssignableFrom(genericClass)) {
					final String subAlias;
					// this is allowed to happen if the database entity has a custom sql annotation
					if (entity.getTableName() == null) {
						if (entity.getClass().isAnnotationPresent(CustomSql.class)) {
							subAlias = field.getName();
						} else {
							throw new Exception("no table name defined! Either change 'getTableName' method of "
									+ entity.getClass().getName() + " to return a value or use the @CustomSql annotation");
						}
					} else {
						subAlias = entity.getTableName() + SUB_FIELD_DELIMITER + field.getName();
					}

					final DatabaseEntity childEntityTemplate = (DatabaseEntity) genericClass.getConstructor().newInstance();
					final String childPk = pkStr(getValueFromRow(subAlias, childEntityTemplate.getPrimaryKeyColumn(), row, true));
					if (childPk != null) {
						final String childUniqueKey = subAlias + "#" + childPk;
						List<DatabaseEntity> list = (List) field.get(entity);
						if (list == null) {
							list = new ArrayList<>(0);
							field.set(entity, list);
						}

						DatabaseEntity childEntity = (DatabaseEntity) alreadyFilledObjects.get(childUniqueKey);
						final boolean alreadyFilled;
						if (childEntity == null) {
							childEntity = childEntityTemplate;
							list.add(childEntity);
							alreadyFilledObjects.put(childUniqueKey, childEntity);
							alreadyFilled = false;
						} else {
							alreadyFilled = true;
						}
						
						// keep track of the current level in the tree
						path += "/" + childEntity.getTableName();
						rowToEntity(childEntity, childPk, subAlias, path, row, alreadyFilledObjects, alreadyFilled);
						path = path.substring(0, path.lastIndexOf("/"));
					}
				}
			} else if (DatabaseEntity.class.isAssignableFrom(field.getType())) {
				DatabaseEntity childEntity = (DatabaseEntity) field.get(entity);
				final DatabaseEntity childEntityTemplate;
				if (childEntity == null) {
					childEntityTemplate = (DatabaseEntity) field.getType().getConstructor().newInstance();
				} else {
					childEntityTemplate = childEntity;
				}
				
				final String subAlias;
				// this is allowed to happen if the database entity has a custom sql annotation
				if (entity.getTableName() == null) {
					if (entity.getClass().isAnnotationPresent(CustomSql.class)) {
						subAlias = field.getName();
					} else {
						throw new Exception("no table name defined! Either change 'getTableName' method of " + entity.getClass().getName()
								+ " to return a value or use the @CustomSql annotation");
					}
				} else {
					subAlias = entity.getTableName() + SUB_FIELD_DELIMITER + field.getName();
				}

				final String childPk = pkStr(getValueFromRow(subAlias, childEntityTemplate.getPrimaryKeyColumn(), row, true));
				if (childPk != null) {
					final String childUniqueKey = subAlias.replace("_", "#" + pk) + "#" + childPk;
					final boolean alreadyFilled;
					if (childEntity == null) {
						childEntity = childEntityTemplate;
						field.set(entity, childEntity);
						alreadyFilledObjects.put(childUniqueKey, childEntity);
						alreadyFilled = false;
					} else {
						alreadyFilled = true;
					}
					
					// keep track of the current level in the tree
					path += "/" + childEntity.getTableName();
					rowToEntity(childEntity, childPk, subAlias, path, row, alreadyFilledObjects, alreadyFilled);
					path = path.substring(0, path.lastIndexOf("/"));
				}
			} else if(!entityWasAlreadyFilled) {
				final String colName = field.getAnnotation(Column.class) != null ? field.getAnnotation(Column.class).columnName()
						: field.getName();
				// this is a field of the main entity (on first level). Then we do not use
				// aliases
				final String entityField;
				if (!path.contains("/") || alias == null) {
					entityField = colName;
				} else {
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
	}

	private Object getValueFromRow(final String alias, String fieldName, final Map<String, Object> row, final boolean useAlias) {
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
		return getElemCount((CriteriaGroup) null, entityType);
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

			final String sql = "SELECT COUNT(DISTINCT " + mainEntity.getTableName() + "." + mainEntity.getPrimaryKeyColumn() + ")"
					+ from.toString() + join.toString() + where.toString();

			if (log.isDebugEnabled()) {
				log.debug("Query: " + sqlPrettyPrint(sql) + "\t[" + toCsv(params.toArray()) + "]");
			}
			final Long result = jdbcTemplate.queryForObject(sql, params.toArray(), Long.class);
			if (log.isTraceEnabled()) {
				log.trace("Result: " + result + " counted");
			}
			return result;
		} catch (final Exception e) {
			log.error("could not count elements of type " + entityType.getName(), e);
			throw e;
		}
	}

	/**
	 * adds the given element to the DB. If there are sub elements set without an
	 * ID, these will be inserted too. Many-To-Many mappings will be ignored
	 * 
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
		return (T) obj;
	}

	private void addElemRecursively(final DatabaseEntity entity) throws Exception {
		createChildrenBefore(entity);

		final SimpleJdbcInsert insert = new SimpleJdbcInsert(jdbcTemplate)
				.withTableName(entity.getTableName())
				.usingGeneratedKeyColumns(entity.getPrimaryKeyColumn())
				.usingColumns(entity.getColumnNames(false).toArray(new String[0]));
		final Map<String, Object> values = entity.toMap();

		if (log.isDebugEnabled()) {
			log.debug("INSERT INTO " + entity.getTableName() + " VALUES " + values);
		}
		final Number key = insert.executeAndReturnKey(values);
		if (log.isTraceEnabled()) {
			log.trace("Returned primary key = " + key);
		}
		entity.setPrimaryKeyValue(key);

		addChildren(entity, false);
	}

	private void createChildrenBefore(final DatabaseEntity entity) throws IllegalAccessException, Exception {
		for (final Field field : entity.getCachedDbFields()) {
			final MappingRelation mr = field.getAnnotation(MappingRelation.class);
			if (mr != null && mr.mappingTableName().isEmpty()) {
				// these are required parent elements which will first be created if not
				// existing
				if (DatabaseEntity.class.isAssignableFrom(field.getType())) {

					final DatabaseEntity elem = (DatabaseEntity) field.get(entity);
					if (elem != null) {
						if (elem.getPrimaryKeyValue() == null) {
							addElemRecursively(elem);
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

	public void updateElem(final DatabaseEntity entity) throws Exception {
		final Object pk = entity.getPrimaryKeyValue();
		if (pk == null || Integer.valueOf(String.valueOf(pk)) == -1) {
			addElem(entity);
		} else {
			try {
				createChildrenBefore(entity);

				deleteUnusedChildren(entity);

				// then update the main entity
				final StringBuilder sb = new StringBuilder();
				for (final String col : entity.getColumnNames(false)) {
					sb.append(col.toLowerCase()).append(" = :").append(col.toLowerCase()).append(", ");
				}
				if (sb.length() > 1) {
					sb.setLength(sb.length() - 2); // crop last comma
				}
				final String stmt = "UPDATE " + entity.getTableName() + " SET " + sb.toString() + " WHERE "
						+ entity.getPrimaryKeyColumn().toLowerCase() + " = :" + entity.getPrimaryKeyColumn().toLowerCase();
				if (log.isDebugEnabled()) {
					log.debug("Query: " + sqlPrettyPrint(stmt) + "\t[" + entity.toMap() + "]");
				}
				int result = namedParameterJdbcTemplate.update(stmt, entity.toMap());
				if (log.isTraceEnabled()) {
					log.trace("Result: " + result + " rows affected");
				}

				addChildren(entity, true);
			} catch (final Exception e) {
				log.error("could not update element of type " + entity.getClass().getName(), e);
				throw e;
			}
		}
	}

	private void addChildren(final DatabaseEntity entity, boolean isUpdate) throws IllegalAccessException, Exception {
		final Object pk = entity.getPrimaryKeyValue();
		for (final Field field : entity.getCachedDbFields()) {
			final MappingRelation mr = field.getAnnotation(MappingRelation.class);
			if (mr != null) {
				if (mr.mappingTableName().isEmpty()) {
					// these are dependent child elements which will be created after creating the
					// parent element
					if (List.class.isAssignableFrom(field.getType())) {
						final List<DatabaseEntity> list = (List) field.get(entity);
						if (list != null) {
							for (final DatabaseEntity child : list) {
								final Field fkField = child.getCachedDbFields().stream()
										.filter(f -> f.getName().equalsIgnoreCase(mr.joinedColumnName())).findFirst().orElse(null);
								fkField.set(child, convertTo(fkField.getType(), pk));

								// if they have a PK they were already created
								if (child.getPrimaryKeyValue() == null) {
									addElemRecursively(child);
								}
							}
						}
					}
				} else {
					// these are any to many relations to be inserted after main element was
					// created,
					// the linked objects are expected to have also been created before
					if (List.class.isAssignableFrom(field.getType())) {
						final List<DatabaseEntity> list = (List) field.get(entity);
						if (list != null && list.size() > 0) {
							HashSet<Object> alreadyThereList = new HashSet<>();
							if (isUpdate) {
								// in case of update only create the relations not already there
								final String sql = "SELECT DISTINCT " + mr.joinedColumnName() + " FROM " + mr.mappingTableName() + " WHERE "
										+ mr.masterColumnName() + " = ?";

								if (log.isDebugEnabled()) {
									log.debug("m2m " + sql + " " + pk);
								}
								jdbcTemplate.query(sql, new RowCallbackHandler() {
									@Override
									public void processRow(ResultSet rs) throws SQLException {
										alreadyThereList.add(rs.getObject(1));
									}
								}, pk);
								if (log.isTraceEnabled()) {
									log.trace("m2m Result: " + alreadyThereList);
								}
							}

							for (final DatabaseEntity child : list) {
								final Object cpk = child.getPrimaryKeyValue();
								if (cpk == null) {
									throw new RuntimeException(
											"With Many to Many Relations the linked objects have to exist (PK has to be set)!");
								}

								if (alreadyThereList.add(cpk)) {
									final SimpleJdbcInsert insertM2M = new SimpleJdbcInsert(jdbcTemplate)
											.withTableName(mr.mappingTableName())
											.usingColumns(new String[] {
													mr.masterColumnName(), mr.joinedColumnName()
											});

									final Map<String, Object> valuesM2M = new HashMap<>();
									valuesM2M.put(mr.masterColumnName(), pk);
									valuesM2M.put(mr.joinedColumnName(), cpk);

									if (log.isDebugEnabled()) {
										log.debug("m2m INSERT INTO " + mr.mappingTableName() + " VALUES " + valuesM2M);
									}
									final int resultM2M = insertM2M.execute(valuesM2M);
									if (log.isTraceEnabled()) {
										log.trace("m2m Result: " + resultM2M + " affected rows");
									}
								}
							}

							if (log.isTraceEnabled()) {
								final List<Map<String, Object>> rowsM2M = jdbcTemplate.queryForList(
										"SELECT * FROM " + mr.mappingTableName() + " WHERE " + mr.masterColumnName() + " = ?", pk);
								log.trace("m2m Result: " + rowsM2M.stream().map(m -> m.toString()).collect(Collectors.joining("\n")));
							}
						}

					}
				}
			}
		}
	}

	private void deleteUnusedChildren(DatabaseEntity entity) throws Exception {
		final Object pk = entity.getPrimaryKeyValue();
		for (final Field field : entity.getCachedDbFields()) {
			final MappingRelation mr = field.getAnnotation(MappingRelation.class);
			if (mr != null) {
				if (mr.mappingTableName().isEmpty()) {
					// these are dependent child elements which will be created after creating the
					// parent element
					// so we can delete them
					if (List.class.isAssignableFrom(field.getType())) {
						final List<DatabaseEntity> list = (List) field.get(entity);
						if (list != null && list.size() > 0) {
							final HashMap<String, ArrayList<Object>> table2pksO2M = new HashMap<>();
							final HashMap<String, String> table2pkField = new HashMap<>();
							for (final DatabaseEntity child : list) {
								final Object cpk = child.getPrimaryKeyValue();
								if (cpk != null) {
									ArrayList<Object> pksO2M = table2pksO2M.get(child.getTableName());
									if (pksO2M == null) {
										pksO2M = new ArrayList<>();
										table2pksO2M.put(child.getTableName(), pksO2M);
										table2pkField.put(child.getTableName(), child.getPrimaryKeyColumn());
									}

									pksO2M.add(cpk);
								}
							}

							for (final Entry<String, ArrayList<Object>> entry : table2pksO2M.entrySet()) {
								final ArrayList<Object> pksO2M = entry.getValue();

								final String sql = "DELETE FROM " + entry.getKey() + " WHERE " + table2pkField.get(entry.getKey()) + " NOT IN ("
										+ pksO2M.stream().map(v -> "?").collect(Collectors.joining(",")) + ") AND " + mr.joinedColumnName()
										+ " = ?";
								pksO2M.add(pk);

								if (log.isDebugEnabled()) {
									log.debug("o2m " + sql + " " + pksO2M);
								}
								final int result = jdbcTemplate.update(sql, pksO2M.toArray(new Object[pksO2M.size()]));
								if (log.isTraceEnabled()) {
									log.trace("o2m Result: " + result + " affected rows");
								}
							}
						}
					}
				} else {
					// these are any to many relations to be inserted after main element was
					// created,
					// the linked objects are expected to have their own lifespan, but we delete the
					// relation
					if (List.class.isAssignableFrom(field.getType())) {
						final List<DatabaseEntity> list = (List) field.get(entity);
						if (list != null && list.size() > 0) {
							final ArrayList<Object> pksM2M = new ArrayList<>(list.size());
							for (final DatabaseEntity child : list) {
								final Object cpk = child.getPrimaryKeyValue();
								if (cpk == null) {
									throw new RuntimeException(
											"With Many to Many Relations the linked objects have to exist (PK has to be set)!");
								}

								pksM2M.add(cpk);
							}

							final String sql = "DELETE FROM " + mr.mappingTableName() + " WHERE " + mr.joinedColumnName() + " NOT IN ("
									+ pksM2M.stream().map(v -> "?").collect(Collectors.joining(",")) + ") AND " + mr.masterColumnName()
									+ " = ?";
							pksM2M.add(pk);

							if (log.isDebugEnabled()) {
								log.debug("m2m " + sql + " " + pksM2M);
							}
							final int result = jdbcTemplate.update(sql, pksM2M.toArray(new Object[pksM2M.size()]));
							if (log.isTraceEnabled()) {
								log.trace("m2m Result: " + result + " affected rows");
							}
						}
					}
				}
			}
		}
	}

	/**
	 * deletes all elements which expect the specified type and match the provided
	 * filter
	 * 
	 * @param criteria
	 * @param entityType
	 * @throws Exception
	 */
	public void deleteElems(final Criteria criteria, final Class<? extends DatabaseEntity> entityType) throws Exception {
		deleteElems(new CriteriaGroup(Operator.AND, criteria), entityType);
	}

	/**
	 * deletes the main entity and all of its children by their primary key (this is
	 * the only field which needs to be provided)
	 * 
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
		for (final Field field : entity.getCachedDbFields()) {
			// discover all sub elements which are coming from sub tables
			final MappingRelation mr = field.getAnnotation(MappingRelation.class);
			if (mr != null) {
				// if there is an m:n mapping table, remove the entry first
				if (!mr.mappingTableName().isEmpty()) {
					final String nmDel = "DELETE FROM " + mr.mappingTableName() + " WHERE " + mr.masterColumnName() + " = (" + stmt + ")";
					if (log.isDebugEnabled()) {
						log.debug("Query: " + sqlPrettyPrint(nmDel) + "\t[" + toCsv(params.toArray()) + "]");
					}
					final int result = jdbcTemplate.update(nmDel, params.toArray());
					if (log.isTraceEnabled()) {
						log.trace("Result: " + result + " affected rows");
					}
				} else if (List.class.isAssignableFrom(field.getType())) {
					final Type genericType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
					final Class<?> genericClass = Class.forName(genericType.getTypeName());
					final DatabaseEntity childEntity = (DatabaseEntity) genericClass.getConstructor().newInstance();

					final String s = "SELECT " + childEntity.getPrimaryKeyColumn() + " FROM " + childEntity.getTableName() + " WHERE " +
							mr.joinedColumnName() + " IN (" + stmt + ")";

					deleteElemsRecursively(childEntity, s, params);
				}
			}
		}

		// note: this must be done in two steps to be able to work in MySql (where a
		// subquery for insert/update/delete operations cannot reference the main table)
		// final String query = "DELETE FROM " + entity.getTableName() + " WHERE " +
		// entity.getPrimaryKeyColumn() + " IN (" + stmt + ")";
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
			final String query = "DELETE FROM " + entity.getTableName() + " WHERE " + entity.getPrimaryKeyColumn() + " IN ("
					+ toCsv(idsToDelete.toArray()) + ")";
			if (log.isDebugEnabled()) {
				log.debug("Query: " + sqlPrettyPrint(query) + "\t[" + toCsv(idsToDelete.toArray()) + "]");
			}
			final int result = jdbcTemplate.update(query);
			if (log.isTraceEnabled()) {
				log.trace("Result: " + result + " affected rows");
			}
		}
	}

	protected <T> String toCsv(final T[] list) {
		final StringBuilder sb = new StringBuilder();
		for (final T t : list) {
			sb.append(t).append(",");
		}
		if (sb.length() > 0) {
			sb.setLength(sb.length() - 1); // crop last comma
		}
		return sb.toString();
	}

	protected String toCsv(final Collection<Object> values) {
		final StringBuilder sb = new StringBuilder();
		for (final Object t : values) {
			sb.append(t).append(",");
		}
		if (sb.length() > 0) {
			sb.setLength(sb.length() - 1); // crop last comma
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
			} else {
				s = tableAlias + "." + col;
			}
			sb.append(s).append(", ");
		}
		if (sb.length() > 2) {
			sb.setLength(sb.length() - 2); // crop last comma
		}
		return sb.toString();
	}

	/**
	 * builds a query from the entity and given filters
	 * 
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
	 * 
	 * @param entity     - the main entity
	 * @param filter     - criteria(s) to filter on the main entity
	 * @param params     - this list will be filled by the algorithm
	 * @param fetchDepth - how deep to dig down in the hierarchy level. Pass in -1
	 *                   to fetch all (sub)elements
	 * @return
	 * @throws Exception
	 */
	public String buildQuery(final DatabaseEntity entity, final CriteriaGroup filter, final Sort sort, final ArrayList<Object> params,
			final Integer fetchDepth) throws Exception {
		final StringBuilder select = new StringBuilder();
		final StringBuilder from = new StringBuilder();
		final StringBuilder join = new StringBuilder();
		final StringBuilder where = new StringBuilder();
		buildQueryRecursively(entity, "/", filter, select, from, join, where, sort, params, 0, fetchDepth == null ? -1 : fetchDepth);
		return select.toString() + from.toString() + join.toString() + where.toString() + (sort != null ? sort.toString() : "");
	}

	private void buildQueryRecursively(final DatabaseEntity entity, final CriteriaGroup filter, final StringBuilder select,
			final StringBuilder from, final StringBuilder join, final StringBuilder where, final Sort orderBy,
			final ArrayList<Object> params, final int fetchDepth) throws Exception {
		buildQueryRecursively(entity, "/", filter, select, from, join, where, orderBy, params, 0, fetchDepth);
	}

	private void buildQueryRecursively(final DatabaseEntity entity, String path, final CriteriaGroup filter, final StringBuilder select,
			final StringBuilder from, final StringBuilder join, final StringBuilder where, Sort orderBy, final ArrayList<Object> params,
			int currentDepth, final int fetchDepth) throws Exception {
		currentDepth++;

		// search for custom sql
		if (entity.getClass().isAnnotationPresent(CustomSql.class)) {
			final CustomSql customSql = entity.getClass().getAnnotation(CustomSql.class);
			select.setLength(0);
			select.append(customSql.selectQuery());
			if (filter != null && !filter.getCriterias().isEmpty()) {
				final WherePart wp = new WherePart(DB_PRODUCT, (String) null, filter);
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
		// always true for the main entity. We use this later for constructing the JOIN
		// part
		if (aliasField.get(entity) == null) {
			entityTableAlias = entity.getTableName();
		} else {
			entityTableAlias = String.valueOf(aliasField.get(entity));
		}

		for (final Field field : entity.getCachedDbFields()) {
			// only join children to the select if they are annotated with the
			// MappingRelation annotation
			final MappingRelation mr = field.getAnnotation(MappingRelation.class);
			if (mr != null) {
				// if this child element position exceeds the maximum depth of the joins, we do
				// not fetch it
				if (fetchDepth != -1 && currentDepth > fetchDepth) {
					continue;
				}
				final DatabaseEntity childEntity;

				if (List.class.isAssignableFrom(field.getType())) {
					final Type genericType = ((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
					final Class<?> genericClass = Class.forName(genericType.getTypeName());
					childEntity = (DatabaseEntity) genericClass.getConstructor().newInstance();
				} else if (DatabaseEntity.class.isAssignableFrom(field.getType())) {
					childEntity = (DatabaseEntity) field.getType().getConstructor().newInstance();
				} else {
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
				if (!mr.mappingTableName().isEmpty()) {
					join.append(" LEFT JOIN " + mr.mappingTableName() + " ON " + mr.mappingTableName() + "." + mr.masterColumnName() + " = "
							+ entityTableAlias + "." + entity.getPrimaryKeyColumn());
					join.append(" LEFT JOIN " + childEntity.getTableName() + " " + childAlias + " ON " + mr.mappingTableName() + "."
							+ mr.joinedColumnName() + " = " + childAlias + "." + childEntity.getPrimaryKeyColumn());
				} else {
					join.append(" LEFT JOIN " + childEntity.getTableName() + " " + childAlias + " ON " + childAlias + "."
							+ mr.joinedColumnName() + " = " + entityTableAlias + "." + mr.masterColumnName());
				}
				// keep track of the current level in the tree
				path += "/" + entity.getTableName();
				buildQueryRecursively(childEntity, path, filter, select, from, join, where, orderBy, params, currentDepth, fetchDepth);
				path = path.substring(0, path.lastIndexOf("/"));

			}
			// this is a misconfiguration
			else if (DatabaseEntity.class.isAssignableFrom(field.getType())) {
				throw new Exception("DatabaseEntity " + field.getType().getName() + " in " + entity.getClass().getName()
						+ " found, but the MappingRelation annotation is missing");
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
	 * 
	 * @param type
	 * @param obj
	 * @return
	 */
	public static Object convertTo(final Class<?> type, final Object obj) throws Exception {
		if (obj == null) {
			return obj;
		}
		if (Number.class.isAssignableFrom(type) && obj instanceof Number) {
			if (Long.class.isAssignableFrom(type)) {
				return ((Number) obj).longValue();
			}
			if (Integer.class.isAssignableFrom(type)) {
				return ((Number) obj).intValue();
			}
			if (Double.class.isAssignableFrom(type)) {
				return ((Number) obj).doubleValue();
			}
			if (Float.class.isAssignableFrom(type)) {
				return ((Number) obj).floatValue();
			}
		}
		if (IKodoEnum.class.isAssignableFrom(type) && obj instanceof String) {
			for (final Enum constant : ((Class<Enum>) type).getEnumConstants()) {
				if (((IKodoEnum) constant).getValue().equals(obj)) {
					return constant;
				}
			}
			throw new Exception("Cannot resolve '" + obj + "' to enum value of type " + type.getName());
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
		return sql.replaceAll("SELECT", "\n\tSELECT").replaceAll("FROM", "\n\tFROM").replaceAll("LEFT JOIN", "\n\tLEFT JOIN")
				.replaceAll("WHERE", "\n\tWHERE");
	}

	public static void validateColName(final String colname) throws Exception {
		if (!VALID_COLNAME_PATTERN.matcher(colname).matches()) {
			throw new Exception("possible attempt of SQL Injection, invalid colname found: " + colname);
		}
	}

	private String addPaging(final String query, final int currentPage, final int pageSize, final int totalRows) throws Exception {

		final int startRow = (currentPage - 1) * pageSize;

		if (DB_PRODUCT.equals("Microsoft SQL Server")) {
			return query + " OFFSET " + startRow + " ROWS FETCH NEXT " + pageSize + " ROWS ONLY";
		} else {
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
