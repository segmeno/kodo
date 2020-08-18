package com.segmeno.kodo.database.mysql;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.segmeno.kodo.annotation.DbIgnore;
import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.database.DatabaseEntity;
import com.segmeno.kodo.database.SqlManager;
import com.segmeno.kodo.transport.AdvancedCriteria;
import com.segmeno.kodo.transport.Criteria;
import com.segmeno.kodo.transport.OperatorId;

@Component
public class MySqlManager implements SqlManager, InitializingBean {

	protected final static Logger log = Logger.getLogger(MySqlManager.class);
	
	protected DataSource dataSource;
	protected JdbcTemplate jdbcTemplate;
	protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;
	
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
	    	final DatabaseEntity de = model.newInstance();
	    	final Map<String,Object> m = jdbcTemplate.queryForMap("SELECT * FROM " + de.getTableName() + " WHERE " + de.getPrimaryKeyColumn() + " = ?", id);
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
    
    @SuppressWarnings("unchecked")
	public <T> List<T> getElems(AdvancedCriteria advancedCriteria, Class<? extends DatabaseEntity> model) throws Exception {
    	try {
    		final MySqlWherePart wherePart = new MySqlWherePart(advancedCriteria);
	    	final DatabaseEntity de = model.newInstance();
	    	final List<T> result = new ArrayList<T>();
	    	jdbcTemplate.queryForList("SELECT * FROM " + de.getTableName() + wherePart.toString()).forEach(map -> {
				try {
					DatabaseEntity obj = model.newInstance();
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
    
	public <T> List<T> getElems(Criteria criteria, Class<? extends DatabaseEntity> model) throws Exception {
    	final List<Criteria> list = new ArrayList<Criteria>();
    	list.add(criteria);
    	return getElems(new AdvancedCriteria(OperatorId.AND, list), model);
    }
    
    @SuppressWarnings("unchecked")
	public <T> List<T> getElems(Class <? extends DatabaseEntity> model) {
		try {
			final DatabaseEntity de = model.newInstance();
	    	final List<T> result = new ArrayList<T>();
	    	jdbcTemplate.queryForList("SELECT * FROM " + de.getTableName()).forEach(map -> {
				try {
					DatabaseEntity obj = model.newInstance();
		    		obj.fromMap(map);
		    		obj.fillChildObjects(this);
		    		result.add((T)obj);
				} catch (Exception e) {
					log.error("could not instantiate object of type " + model.getName(), e);
				}
	    	});
	    	return result;
		} catch (Exception e1) {
			log.error("could not instantiate object of type " + model.getName(), e1);
		}
		return null;
    }
    
    public Integer getElemCount(Class<? extends DatabaseEntity> model) throws DataAccessException, InstantiationException, IllegalAccessException {
    	return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + model.newInstance().getTableName(), Integer.class);
    }
    
    @SuppressWarnings("unchecked")
	@Transactional(propagation = Propagation.REQUIRED)
    public <T> T addElem(DatabaseEntity obj) throws Exception {
    	
    	final SimpleJdbcInsert insert = new SimpleJdbcInsert(dataSource)
	            .withTableName(obj.getTableName())
	            .usingGeneratedKeyColumns(obj.getPrimaryKeyColumn())
	            .usingColumns(obj.getColumnNames());
    	
    	final Number key = insert.executeAndReturnKey(obj.toMap(false));
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
    	
    	final String stmt = "UPDATE " + obj.getTableName() + " SET " + sb.toString() + " WHERE " + obj.getPrimaryKeyColumn().toLowerCase() + " = :" + obj.getPrimaryKeyColumn().toLowerCase();
    	namedParameterJdbcTemplate.update(stmt, obj.toMap(false));
    }
	
    @Transactional(propagation = Propagation.REQUIRED)
	public void deleteElem(Integer id, Class<? extends DatabaseEntity> model) throws Exception {
		try {
	    	final DatabaseEntity de = model.newInstance();
			jdbcTemplate.update("DELETE FROM " + de.getTableName() + " WHERE " + de.getPrimaryKeyColumn() + " = ?", id);
			}
		 catch (Exception e1) {
			log.error("could not delete object of type " + model.getName(), e1);
		}
	}
    
    @Transactional(propagation = Propagation.REQUIRED)
	public void deleteChildElems(Integer parentId, Class<? extends DatabaseEntity> model) throws Exception {
		try {
	    	final DatabaseEntity de = model.newInstance();
			jdbcTemplate.update("DELETE FROM " + de.getTableName() + " WHERE " + de.getParentKeyColunm() + " = ?", parentId);
			}
		 catch (Exception e1) {
			log.error("could not delete object of type " + model.getName(), e1);
		}
	}
    
    @Transactional(propagation = Propagation.REQUIRED)
	public void deleteChildElems(String[] parentIds, Class<? extends DatabaseEntity> model) throws Exception {
		try {
	    	final DatabaseEntity de = model.newInstance();
			jdbcTemplate.update("DELETE FROM " + de.getTableName() + " WHERE " + de.getParentKeyColunm() + " IN (" + toCsv(parentIds) + ")");
			}
		 catch (Exception e1) {
			log.error("could not delete object of type " + model.getName(), e1);
		}
	}
    
    @Transactional(propagation = Propagation.REQUIRED)
	public void deleteElems(String[] ids, Class<? extends DatabaseEntity> model) throws Exception {
		try {
	    	final DatabaseEntity de = model.newInstance();
			jdbcTemplate.update("DELETE FROM " + de.getTableName() + " WHERE " + de.getPrimaryKeyColumn() + " IN (" + toCsv(ids) + ")");
			}
		 catch (Exception e1) {
			log.error("could not delete object of type " + model.getName(), e1);
		}
	}
    
    public static <T> String toCsv(final T[] list) {
		final StringBuilder sb = new StringBuilder();
		for (final T t : list) {
			sb.append(t).append(",");
		}
		if(sb.length() > 0) {
			sb.setLength(sb.length()-1); // crop last comma
		}
		return sb.toString();
	}

//    private String buildWhereClause(Map<String, Object> params) {
//    	final StringBuilder sb = new StringBuilder();
//    	for (Entry<String,Object> e : params.entrySet()) {
//			sb.append(e.getKey()).append(" = ");
//			if (e.getValue() instanceof String) {
//				sb.append("'").append(e.getValue()).append("' AND");
//			}
//			else if (e.getValue() instanceof Date) {
//				sb.append("'").append(SQL_DATE_TIME_FORMAT.format((Date)e.getValue())).append("',");
//			}
//			else {
//				sb.append(e.getValue()).append(",");
//			}
//		}
//		sb.setLength(sb.length()-1);	// crop last comma
//		sb.append(" WHERE " + primaryKeyCol + " = ?");
//    }
    
}