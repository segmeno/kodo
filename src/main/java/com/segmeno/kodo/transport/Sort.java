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
		
		for (final SortEntry entry : sortFields) {
			if(sortStmt.length() == 0) {
				sortStmt.append(" ORDER BY ");
			} else {
				sortStmt.append(", ");
			}
			
			sortStmt.append(entry.getFieldName()).append(' ').append(entry.getSortDirection());
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

	@Override
	public String toString() {
		return sortStmt.toString();
	}
	
}
