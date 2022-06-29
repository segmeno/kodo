package com.segmeno.kodo.transport;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Criteria {
	private CriteriaGroup criteriaGroup;
	private String fieldName;
	private Operator operator;
	private String stringValue;
	private Number numberValue;
	private Date dateValue;
	private List<?> listValues = new ArrayList<>();

	public Criteria() {}

	public Criteria(final CriteriaGroup criteriaGroup) {
		this.criteriaGroup = criteriaGroup;
	}

	public Criteria(final String fieldName, final Operator operator) {
		this(fieldName, operator, null, null, null, null);
	}

	public Criteria(final String fieldName, final Operator operator, final String stringValue) {
		this(fieldName, operator, stringValue, null, null, null);
	}

	public Criteria(final String fieldName, final Operator operator, final Number numberValue) {
		this(fieldName, operator, null, numberValue, null, null);
	}

    public Criteria(final String fieldName, final Operator operator, final Date dateValue) {
        this(fieldName, operator, null, null, dateValue, null);
    }

	public Criteria(final String fieldName, final Operator operator, final List<?> listValues) {
		this(fieldName, operator, null, null, null, listValues);
	}

	private Criteria(final String fieldName, final Operator operator, final String stringValue, final Number numberValue, final Date dateValue, final List<?> listValues) {
		this.fieldName = fieldName;
		this.operator = operator;
		this.stringValue = stringValue;
		this.numberValue = numberValue;
        this.dateValue = dateValue;
		this.setListValues(listValues);
	}
	
	public Criteria(Criteria toCopy) {
		if(toCopy == null) {
			throw new RuntimeException("toCopy cannot be null!");
		}
		
		this.fieldName = toCopy.fieldName;
		this.operator = toCopy.operator;
		this.stringValue = toCopy.stringValue;
		this.numberValue = toCopy.numberValue;
        this.dateValue = toCopy.dateValue;
		this.setListValues(new ArrayList(toCopy.listValues));

		this.criteriaGroup = toCopy.criteriaGroup == null ? null : new CriteriaGroup(toCopy.criteriaGroup);
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

	public void setStringValue(String stringValue) {
		this.stringValue = stringValue;
	}

	public String getStringValue() {
		return stringValue;
	}

	public void setNumberValue(Number numberValue) {
		this.numberValue = numberValue;
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

	public Date getDateValue() {
      return dateValue;
    }

    public void setDateValue(final Date dateValue) {
      this.dateValue = dateValue;
    }

    @Override
    public String toString() {
      return "Criteria [criteriaGroup=" + criteriaGroup + ", fieldName=" + fieldName + ", operator=" + operator + ", stringValue=" + stringValue +
          ", numberValue=" + numberValue + ", dateValue=" + dateValue + ", listValues=" + listValues + "]";
    }
}
