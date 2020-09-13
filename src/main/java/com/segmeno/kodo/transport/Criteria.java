package com.segmeno.kodo.transport;

public class Criteria {
	
	private String fieldName;
	private Operator operator;
	private String stringValue; 
	private Number numberValue;
	
	public Criteria() {}
	
	public Criteria(String fieldName, Operator operator) {
		this(fieldName, operator, null, null);
	}
	
	public Criteria(String fieldName, Operator operator, String stringValue) {
		this(fieldName, operator, stringValue, null);
	}
	
	public Criteria(String fieldName, Operator operator, Number numberValue) {
		this(fieldName, operator, null, numberValue);
	}
	
	public Criteria(String fieldName, Operator operator, String stringValue, Number numberValue) {
		this.fieldName = fieldName;
		this.operator = operator;
		this.stringValue = stringValue;
		this.numberValue = numberValue;
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

}
