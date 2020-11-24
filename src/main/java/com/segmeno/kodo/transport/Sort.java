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
	public Sort(final String fieldName, final SortDirection sortDirection) {
		sortFields.add(new SortEntry(fieldName, sortDirection));
		buildStmt();
	}
	
	public Sort(final ArrayList<SortEntry> sortFields) {
		this.sortFields = sortFields;
		buildStmt();
	}
	
	public Sort(final SortEntry entry) {
		sortFields.add(entry);
		buildStmt();
	}
	
	public Sort() {
	}
	
	public void buildStmt() {
		sortStmt.setLength(0);
		sortStmt.append(" ORDER BY");
		
		boolean isFirst = true;
		for (final SortEntry entry : sortFields) {
			if(isFirst) {
				isFirst = false;
			} else {
				sortStmt.append(',');
			}
			sortStmt.append(' ').append(entry.fieldName).append(' ').append(entry.sortDirection);
		}
	}
	
	public void addSortField(final String fieldName, final SortDirection sortDirection) {
		sortFields.add(new SortEntry(fieldName, sortDirection));
	}
	
	public ArrayList<SortEntry> getSortFields() {
		return sortFields;
	}
	
	public void setSortFields(final ArrayList<SortEntry> sortFields) {
		this.sortFields = sortFields;
		buildStmt();
	}

	public static class SortEntry {
		private String fieldName;
		private SortDirection sortDirection;
		
		public SortEntry(final String fieldName, final SortDirection sortDirection) {
			this.fieldName = fieldName;
			this.sortDirection = sortDirection;
		}
		
		public String getFieldName() {
			return fieldName;
		}
		public void setFieldName(final String fieldName) {
			this.fieldName = fieldName;
		}
		public SortDirection getSortDirection() {
			return sortDirection;
		}
		public void setSortDirection(final SortDirection sortDirection) {
			this.sortDirection = sortDirection;
		}		
	}

	@Override
	public String toString() {
		return sortStmt.toString();
	}
}