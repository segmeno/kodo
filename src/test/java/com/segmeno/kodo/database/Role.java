package com.segmeno.kodo.database;

import com.segmeno.kodo.annotation.PrimaryKey;

public class Role extends DatabaseEntity {

	@PrimaryKey
	public Integer id;
	
	public String roleName;

	public String description;
	
	@Override
	public String getTableName() {
		return "tbRole";
	}

}
