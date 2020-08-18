package com.segmeno.kodo.database;

import java.util.List;

import com.segmeno.kodo.transport.AdvancedCriteria;
import com.segmeno.kodo.transport.Criteria;

public interface SqlManager {

	public <T> T getElem(Integer id, Class<? extends DatabaseEntity> model) throws Exception;
	
	public <T> List<T> getElems(Class <? extends DatabaseEntity> model) throws Exception;
	
	public <T> List<T> getElems(AdvancedCriteria advancedCriteria, Class<? extends DatabaseEntity> model) throws Exception;
	
	public <T> List<T> getElems(Criteria criteria, Class<? extends DatabaseEntity> model) throws Exception;
	
	public Integer getElemCount(Class<? extends DatabaseEntity> model) throws Exception;
	
	public <T> T addElem(DatabaseEntity obj) throws Exception;
	
	public void updateElem(DatabaseEntity obj) throws Exception;
	
	public void deleteElem(Integer id, Class<? extends DatabaseEntity> model) throws Exception;
	
	public void deleteChildElems(Integer parentId, Class<? extends DatabaseEntity> model) throws Exception;
	
	public void deleteChildElems(String[] parentIds, Class<? extends DatabaseEntity> model) throws Exception;
	
	public void deleteElems(String[] ids, Class<? extends DatabaseEntity> model) throws Exception;
}
