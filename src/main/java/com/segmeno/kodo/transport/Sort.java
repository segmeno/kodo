package com.segmeno.kodo.transport;

import java.util.ArrayList;

public class Sort {
	
	private StringBuilder sortStmt = new StringBuilder();

	public enum SortDirection {
		ASC,
		DESC
	}
	
	ArrayList<SortEntry> sortFields = new ArrayList<>();
	
	/**
	 * convenience constructor (if there is only one field to sort by)
	 * @param fieldName
	 * @param sortDirection
	 */
	public Sort(String fieldName, SortDirection sortDirection) {
		sortFields.add(new SortEntry(fieldName, sortDirection));
		buildStmt();
	}
	
	public Sort(ArrayList<SortEntry> sortFields) {
		buildStmt();
	}
	
	public Sort(SortEntry entry) {
		buildStmt();
	}
	
	public Sort() {
	}
	
	private void buildStmt() {
		sortStmt.setLength(0);
		sortStmt.append(" ORDER BY");
		
		for (SortEntry entry : sortFields) {
			sortStmt.append(" ").append(entry.fieldName).append(" ").append(entry.sortDirection);
		}
	}
	
	public void addSortField(String fieldName, SortDirection sortDirection) {
		sortFields.add(new SortEntry(fieldName, sortDirection));
	}
	
	public ArrayList<SortEntry> getSortFields() {
		return sortFields;
	}
	
	public void setSortFields(ArrayList<SortEntry> sortFields) {
		this.sortFields = sortFields;
		buildStmt();
	}

	class SortEntry {
		private String fieldName;
		private SortDirection sortDirection;
		
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

	@Override
	public String toString() {
		return sortStmt.toString();
	}
	
}
