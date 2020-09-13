package com.segmeno.kodo.entity;

import java.util.Date;

import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.database.DatabaseEntity;

public class TestRole extends DatabaseEntity {

	@PrimaryKey
	public Integer id;
	
	public String name;

	public String description;
	
	public Date createdAt;
	
	@MappingRelation(masterColumnName="primaryColorId", joinedColumnName="id")
	public TestType primaryColor;
	
	@MappingRelation(masterColumnName="secondaryColorId", joinedColumnName="id")
	public TestType secondaryColor;
	
	@Override
	public String getTableName() {
		return "tbRole";
	}

}
