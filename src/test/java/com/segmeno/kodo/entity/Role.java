package com.segmeno.kodo.entity;

import java.util.Date;

import com.segmeno.kodo.annotation.MappingRelation;
import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.database.DatabaseEntity;

public class Role extends DatabaseEntity {

	@PrimaryKey
	public Integer id;
	
	public String name;

	public String description;
	
	public Date createdAt;
	
	@MappingRelation(masterColumnName="primaryColorId", joinedColumnName="id")
	public Type primaryColor;
	
	@MappingRelation(masterColumnName="secondaryColorId", joinedColumnName="id")
	public Type secondaryColor;
	
	@Override
	public String getTableName() {
		return "tbRole";
	}

}
