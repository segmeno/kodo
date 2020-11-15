package com.segmeno.kodo.transport;

import com.segmeno.kodo.transport.Sort.SortDirection;

public class SortEntry {
	private String fieldName;
	private SortDirection sortDirection;
	
	public SortEntry() {}
	
	public SortEntry(String fieldName, SortDirection sortDirection) {
		this.fieldName = fieldName;
		this.sortDirection = sortDirection;
	}
	
	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public SortDirection getSortDirection() {
		return sortDirection;
	}
	public void setSortDirection(SortDirection sortDirection) {
		this.sortDirection = sortDirection;
	}		
}