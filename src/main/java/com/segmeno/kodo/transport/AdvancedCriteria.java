package com.segmeno.kodo.transport;

import java.util.ArrayList;
import java.util.List;


public class AdvancedCriteria {
	private final  OperatorId operator;
	private final List<Criteria> criterias = new ArrayList<Criteria>();
	
	public AdvancedCriteria(OperatorId operator, List<Criteria> criterias) {
		this.operator = operator;
		this.criterias.addAll(criterias);
	}
	
	public AdvancedCriteria(OperatorId operator, Criteria criteria) {
		this.operator = operator;
		this.criterias.add(criteria);
	}
	
	public AdvancedCriteria(OperatorId operator) {
		this.operator = operator;
	}

	public OperatorId getOperator() {
		return operator;
	}
	
	public List<Criteria> getCriterias() {
		return criterias;
	}
	
	public AdvancedCriteria add(Criteria c) {
		criterias.add(c);
		return this;
	}
	
}
