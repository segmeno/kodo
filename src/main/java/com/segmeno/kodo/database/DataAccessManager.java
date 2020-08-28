package com.segmeno.kodo.database;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.transport.AdvancedCriteria;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.OperatorId;

public abstract class DataAccessManager {

protected final static Logger log = Logger.getLogger(DataAccessManager.class);
	
	protected DataSource dataSource;
	protected JdbcTemplate jdbcTemplate;
	protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
	protected abstract String getSelectByPrimaryKeyQuery(String tableName, String primaryKeyColumnName);
	
	protected abstract String getSelectByCriteriaQuery(String tableName, AdvancedCriteria advancedCriteria) throws Exception;
	
	protected abstract String getCountQuery(String tableName, AdvancedCriteria advancedCriteria) throws Exception;
	
	protected abstract String getUpdateQuery(String tableName, String params, String primaryKeyColumn);
	
	protected abstract String getDeleteByPrimaryKeyQuery(String tableName, String primaryKeyColumnName);
	
	protected abstract String getDeleteByPrimaryKeysQuery(String tableName, String primaryKeyColumnName, String[] primaryIds);
	
	protected abstract String getDeleteByParentKeyQuery(String tableName, String parentKeyColumnName);
	
	protected abstract String getDeleteByParentKeysQuery(String tableName, String parentKeyColumnName, String[] parentIds);
	
	public JdbcTemplate getJdbcTemplate() {
		return jdbcTemplate;
	}
	
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
	}
	
	public void afterPropertiesSet() throws Exception {
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getElem(Integer id, Class<? extends DatabaseEntity> model) throws Exception {
    	try {
	    	final DatabaseEntity de = model.getConstructor().newInstance();
	    	final Map<String,Object> m = jdbcTemplate.queryForMap(getSelectByPrimaryKeyQuery(de.getTableName(), de.getPrimaryKeyColumn()), id);
	    	de.fromMap(m);
	    	de.fillChildObjects(this);
	    	return (T)de;
		} 
    	catch (DataAccessException e) {
    		log.error("no object of type " + model.getName() + " for id " + id + " found");
    	} 
    	catch (Exception e1) {
			log.error("could not load object of type " + model.getName(), e1);
		}
    	return null;
    }
	
	public <T> List<T> getElems(Criteria criteria, Class<? extends DatabaseEntity> model) throws Exception {
    	final List<Criteria> list = new ArrayList<Criteria>();
    	list.add(criteria);
    	return getElems(new AdvancedCriteria(OperatorId.AND, list), model);
    }
    
	@SuppressWarnings("unchecked")
	public <T> List<T> getElems(AdvancedCriteria advancedCriteria, Class<? extends DatabaseEntity> model) throws Exception {
    	try {
	    	final DatabaseEntity de = model.getConstructor().newInstance();
	    	
	    	final List<T> result = new ArrayList<T>();
	    	jdbcTemplate.queryForList(getSelectByCriteriaQuery(de.getTableName(), advancedCriteria)).forEach(map -> {
				try {
					final DatabaseEntity obj = model.getConstructor().newInstance();
					obj.fromMap(map);
					obj.fillChildObjects(this);
		    		result.add((T)obj);
				} catch (Exception e) {
					log.error("could not instantiate object of type " + model.getName(), e);
				}
	    	});
	    	return result;
		} 
    	catch (DataAccessException e) {
    		log.error("no object of type " + model.getName() + " found");
    	} 
    	catch (Exception e1) {
			log.error("could not load object of type " + model.getName(), e1);
		}
    	return null;
    }
    
	public <T> List<T> getElems(Class <? extends DatabaseEntity> model) throws Exception {
    	return getElems((AdvancedCriteria)null, model);
    }

    public Integer getElemCount(Class<? extends DatabaseEntity> model) throws Exception {
    	return getElemCount(null, model);
    }
	
	public Integer getElemCount(AdvancedCriteria advancedCriteria, Class<? extends DatabaseEntity> model) throws Exception {
    	return jdbcTemplate.queryForObject(getCountQuery(model.getConstructor().newInstance().getTableName(), advancedCriteria), Integer.class);
    }
    
    @SuppressWarnings("unchecked")
	@Transactional(propagation = Propagation.REQUIRED)
    public <T> T addElem(DatabaseEntity obj) throws Exception {
    	
    	final SimpleJdbcInsert insert = new SimpleJdbcInsert(dataSource)
	            .withTableName(obj.getTableName())
	            .usingGeneratedKeyColumns(obj.getPrimaryKeyColumn())
	            .usingColumns(obj.getColumnNames(false));
    	
    	final Number key = insert.executeAndReturnKey(obj.toMap());
    	obj.setId(key.intValue());
    	
    	return (T)obj;
    }
    
    @Transactional(propagation = Propagation.REQUIRED)
	public void updateElem(DatabaseEntity obj) throws Exception {
    	if (obj.getId()== null || Integer.valueOf(String.valueOf(obj.getId())) == -1) {
			addElem(obj);
			return;
		}
    	
    	final StringBuilder sb = new StringBuilder();
    	for (Field f : obj.getClass().getDeclaredFields()) {
    		
    		if (f.getAnnotation(DbIgnore.class) != null || f.getAnnotation(PrimaryKey.class) != null) {
    			continue;
    		}
    		sb.append(f.getName().toLowerCase()).append(" = :").append(f.getName().toLowerCase()).append(", ");
    	}
    	if (sb.length() > 1) {
    		sb.setLength(sb.length()-2);	// crop last comma
    	}
    	
    	namedParameterJdbcTemplate.update(getUpdateQuery(obj.getTableName(), sb.toString(), obj.getPrimaryKeyColumn().toLowerCase()), obj.toMap());
    }
	
	@Transactional(propagation = Propagation.REQUIRED)
	public void deleteElem(Integer id, Class<? extends DatabaseEntity> model) throws Exception {
		try {
	    	final DatabaseEntity de = model.getConstructor().newInstance();
	    	jdbcTemplate.update(getDeleteByPrimaryKeyQuery(de.getTableName(), de.getPrimaryKeyColumn()), id);
		}
		 catch (Exception e1) {
			log.error("could not delete object of type " + model.getName(), e1);
		}
	}
    
    @Transactional(propagation = Propagation.REQUIRED)
	public void deleteChildElems(Integer parentId, Class<? extends DatabaseEntity> model) throws Exception {
		try {
	    	final DatabaseEntity de = model.getConstructor().newInstance();
	    	jdbcTemplate.update(getDeleteByParentKeyQuery(de.getTableName(), de.getParentKeyColunm()), parentId);
		}
		 catch (Exception e1) {
			log.error("could not delete object of type " + model.getName(), e1);
		}
	}
    
    @Transactional(propagation = Propagation.REQUIRED)
	public void deleteChildElems(String[] parentIds, Class<? extends DatabaseEntity> model) throws Exception {
		try {
	    	final DatabaseEntity de = model.getConstructor().newInstance();
	    	jdbcTemplate.update(getDeleteByParentKeysQuery(de.getTableName(), de.getParentKeyColunm(), parentIds));
		}
		 catch (Exception e1) {
			log.error("could not delete object of type " + model.getName(), e1);
		}
	}
    
    @Transactional(propagation = Propagation.REQUIRED)
	public void deleteElems(String[] ids, Class<? extends DatabaseEntity> model) throws Exception {
		try {
	    	final DatabaseEntity de = model.getConstructor().newInstance();
	    	jdbcTemplate.update(getDeleteByPrimaryKeysQuery(de.getTableName(), de.getPrimaryKeyColumn(), ids));
		}
		 catch (Exception e1) {
			log.error("could not delete object of type " + model.getName(), e1);
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
}
