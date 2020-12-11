package com.segmeno.kodo.transport;

import java.util.ArrayList;
import java.util.List;


public class CriteriaGroup {
	private  Operator operator = Operator.AND;
	private final List<Criteria> criterias = new ArrayList<Criteria>();
	
	public CriteriaGroup(final Operator operator, final List<Criteria> criterias) {
		this.operator = operator;
		this.criterias.addAll(criterias);
	}
	
	public CriteriaGroup(final Operator operator, final Criteria criteria) {
		this.operator = operator;
		this.criterias.add(criteria);
	}
	
	public CriteriaGroup(final Operator operator) {
		this.operator = operator;
	}
	
	public CriteriaGroup() {
	}

	public Operator getOperator() {
		return operator;
	}
	
	public List<Criteria> getCriterias() {
		return criterias;
	}
	
	public CriteriaGroup add(final Criteria c) {
		criterias.add(c);
		return this;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder();
		criterias.forEach(c -> sb.append(c.toString()));
		return "CriteriaGroup [operator=" + operator + ", criterias=" + sb.toString() + "]";
	}
	
}
