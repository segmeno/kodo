package com.segmeno.kodo.entity;

import com.segmeno.kodo.annotation.PrimaryKey;
import com.segmeno.kodo.database.DatabaseEntity;

public class Type extends DatabaseEntity {

	@PrimaryKey
	public Integer id;
	
	public String name;

	@Override
	public String getTableName() {
		return "tbType";
	}

}
