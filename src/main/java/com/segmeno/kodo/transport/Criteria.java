package com.segmeno.kodo.transport;

import java.util.ArrayList;
import java.util.List;

public class Criteria {
	private CriteriaGroup criteriaGroup;
	private String fieldName;
	private Operator operator;
	private String stringValue; 
	private Number numberValue;
	private List<?> listValues = new ArrayList<>();
	
	public Criteria() {}
	
	public Criteria(final CriteriaGroup criteriaGroup) {
		this.criteriaGroup = criteriaGroup;
	}
	
	public Criteria(final String fieldName, final Operator operator) {
		this(fieldName, operator, null, null, null);
	}
	
	public Criteria(final String fieldName, final Operator operator, final String stringValue) {
		this(fieldName, operator, stringValue, null, null);
	}
	
	public Criteria(final String fieldName, final Operator operator, final Number numberValue) {
		this(fieldName, operator, null, numberValue, null);
	}
	
	public Criteria(final String fieldName, final Operator operator, final List<?> listValues) {
		this(fieldName, operator, null, null, listValues);
	}
	
	private Criteria(final String fieldName, final Operator operator, final String stringValue, final Number numberValue, final List<?> listValues) {
		this.fieldName = fieldName;
		this.operator = operator;
		this.stringValue = stringValue;
		this.numberValue = numberValue;
		this.setListValues(listValues);
	}
	
	public CriteriaGroup getCriteriaGroup() {
		return criteriaGroup;
	}

	public void setCriteriaGroup(final CriteriaGroup criteriaGroup) {
		this.criteriaGroup = criteriaGroup;
	}

	public String getFieldName() {
		return fieldName;
	}

	public Operator getOperator() {
		return operator;
	}

	public String getStringValue() {
		return stringValue;
	}
	
	public Number getNumberValue() {
		return numberValue;
	}

	public List<?> getListValues() {
		return listValues;
	}

	public void setListValues(final List<?> listValues) {
		this.listValues = listValues;
	}

	@Override
	public String toString() {
		return "Criteria [criteriaGroup=" + criteriaGroup + ", fieldName=" + fieldName + ", operator=" + operator + ", stringValue=" + stringValue
				+ ", numberValue=" + numberValue + ", listValues=" + listValues + "]";
	}

}
