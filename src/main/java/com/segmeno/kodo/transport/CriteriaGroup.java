package com.segmeno.kodo.transport;

import java.util.ArrayList;
import java.util.List;


public class CriteriaGroup {
	private final  Operator operator;
	private final List<Criteria> criterias = new ArrayList<Criteria>();
	
	public CriteriaGroup(Operator operator, List<Criteria> criterias) {
		this.operator = operator;
		this.criterias.addAll(criterias);
	}
	
	public CriteriaGroup(Operator operator, Criteria criteria) {
		this.operator = operator;
		this.criterias.add(criteria);
	}
	
	public CriteriaGroup(Operator operator) {
		this.operator = operator;
	}

	public Operator getOperator() {
		return operator;
	}
	
	public List<Criteria> getCriterias() {
		return criterias;
	}
	
	public CriteriaGroup add(Criteria c) {
		criterias.add(c);
		return this;
	}
	
}
