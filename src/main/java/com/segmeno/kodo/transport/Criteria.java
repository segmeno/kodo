package com.segmeno.kodo.transport;

import java.util.ArrayList;
import java.util.List;

public class Criteria {
	
	private String fieldName;
	private Operator operator;
	private String stringValue; 
	private Number numberValue;
	private List<?> listValues = new ArrayList<>();
	
	public Criteria() {}
	
	public Criteria(String fieldName, Operator operator) {
		this(fieldName, operator, null, null, null);
	}
	
	public Criteria(String fieldName, Operator operator, String stringValue) {
		this(fieldName, operator, stringValue, null, null);
	}
	
	public Criteria(String fieldName, Operator operator, Number numberValue) {
		this(fieldName, operator, null, numberValue, null);
	}
	
	public Criteria(String fieldName, Operator operator, List<?> listValues) {
		this(fieldName, operator, null, null, listValues);
	}
	
	private Criteria(String fieldName, Operator operator, String stringValue, Number numberValue, List<?> listValues) {
		this.fieldName = fieldName;
		this.operator = operator;
		this.stringValue = stringValue;
		this.numberValue = numberValue;
		this.setListValues(listValues);
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

	public void setListValues(List<?> listValues) {
		this.listValues = listValues;
	}

	@Override
	public String toString() {
		return "Criteria [fieldName=" + fieldName + ", operator=" + operator + ", stringValue=" + stringValue
				+ ", numberValue=" + numberValue + ", listValues=" + listValues + "]";
	}

}
