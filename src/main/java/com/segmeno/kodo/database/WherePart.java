package com.segmeno.kodo.database;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import com.segmeno.kodo.transport.CriteriaGroup;
import com.segmeno.kodo.transport.Criteria;

public class WherePart {
	
	protected final static Logger log = Logger.getLogger(WherePart.class);
	
	protected StringBuilder sb = new StringBuilder();
	protected List<Object> params = new ArrayList<>();
	protected List<String> columnNames = new ArrayList<>();
	
	/**
	 * 
	 * @param tableAlias - the alias of the table
	 * @param adCrit - the filter settings to be used
	 * @throws Exception
	 */
	public WherePart(String tableAlias, CriteriaGroup adCrit) throws Exception {
		this(tableAlias, null, adCrit);
	}
	
	/**
	 * 
	 * @param tableAlias - the alias of the table
	 * @param columnNames - a list of all existing column names. If this parameter is set, sanity checks will be done while constructing the where part
	 * @param adCrit - the filter settings to be used
	 * @throws Exception
	 */
	public WherePart(String tableAlias, List<String> columnNames, CriteriaGroup adCrit) throws Exception {
		if (columnNames != null) {
			this.columnNames = columnNames.stream().map(col -> col.toUpperCase()).collect(Collectors.toList());
		}
		final List<Criteria> crits = adCrit.getCriterias();
		
		if (adCrit != null && crits.size() > 0) {
			sb.append("(");
			
			for (Criteria crit : crits) {
				// first check if the column name is really existing
				if (!this.columnNames.isEmpty()) {
					if (crit.getFieldName() != null && !this.columnNames.contains(crit.getFieldName().toUpperCase())) {
						final String s = "Check your filter settings: Column with Name '" + crit.getFieldName() + "' used in criteria, but not existing in table " + tableAlias;
						log.error(s);
						throw new Exception(s);
					}
				}
				
				switch (crit.getOperator()) {
				case CONTAINS:
					sb.append(contains(tableAlias, crit));
					break;
				case ICONTAINS:
					sb.append(icontains(tableAlias, crit));
					break;
				case NOT_CONTAINS:
					sb.append(notContains(tableAlias, crit));
					break;
				case INOT_CONTAINS:
					sb.append(notContains(tableAlias, crit));
					break;
				case EQUALS:
					sb.append(equals(tableAlias, crit));
					break;
				case IEQUALS:
					sb.append(iequals(tableAlias, crit));
					break;
				case GREATER_OR_EQUAL:
					sb.append(greaterOrEqual(tableAlias, crit));
					break;
				case GREATER_THAN:
					sb.append(greaterThan(tableAlias, crit));
					break;
				case LESS_OR_EQUAL:
					sb.append(lessOrEqual(tableAlias, crit));
					break;
				case LESS_THAN:
					sb.append(lessThan(tableAlias, crit));
					break;
				case STARTS_WITH:
					sb.append(startsWith(tableAlias, crit));
					break;
				case ISTARTS_WITH:
					sb.append(istartsWith(tableAlias, crit));
					break;
				case INOT_STARTS_WITH:
					sb.append(inotStartsWith(tableAlias, crit));
					break;
				case ENDS_WITH:
					sb.append(endsWith(tableAlias, crit));
					break;
				case IENDS_WITH:
					sb.append(iendsWith(tableAlias, crit));
					break;
				case INOT_ENDS_WITH:
					sb.append(inotEndsWith(tableAlias, crit));
					break;
				case IS_BLANK:
					sb.append(isBlank(tableAlias, crit));
					break;
				case NOT_BLANK:
					sb.append(notBlank(tableAlias, crit));
					break;
				case NOT_NULL:
					sb.append(notNull(tableAlias, crit));
					break;
				default:
					throw new Exception("unsupported OperatorId " + crit.getOperator() + "! Please extend this class: " + this.getClass());
				}
				
				sb.append(" ").append(adCrit.getOperator().getValue()).append(" ");
			}
			
			sb.setLength(sb.length() - (adCrit.getOperator().getValue().length() + 2));	// remove last ' OperatorId '
			sb.append(")");
		}
	}
	
	protected String contains(String tableAlias, Criteria criteria) {
		final Object param = criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue();
		params.add("%" + param + "%");
		return tableAlias + "." + criteria.getFieldName() + " LIKE ?";
	}
	
	protected String icontains(String tableAlias, Criteria criteria) {
		final Object param = criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue();
		params.add("%" + param + "%");
		return "LOWER(" + tableAlias + "." + criteria.getFieldName() + ") LIKE LOWER(?)";
	}
	
	protected String notContains(String tableAlias, Criteria criteria) {
		final Object param = criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue();
		params.add("%" + param + "%");
		return tableAlias + "." + criteria.getFieldName() + " NOT LIKE ?";
	}
	
	protected String equals(String tableAlias, Criteria criteria) {
		params.add(criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue());
		return tableAlias + "." + criteria.getFieldName() + " = ?";
	}
	
	protected String iequals(String tableAlias, Criteria criteria) {
		if (criteria.getStringValue() != null) {
			params.add(criteria.getStringValue());
			return "LOWER(" + tableAlias + "." + criteria.getFieldName() + ") LIKE LOWER(?)";
		}
		else {
			params.add(criteria.getNumberValue());
			return tableAlias + "." + criteria.getFieldName() + " LIKE ?";
		}
	}
	
	protected String greaterOrEqual(String tableAlias, Criteria criteria) {
		params.add(criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue());
		return tableAlias + "." + criteria.getFieldName() + " >= ?";
	}
	
	protected String greaterThan(String tableAlias, Criteria criteria) {
		params.add(criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue());
		return tableAlias + "." + criteria.getFieldName() + " > ?";
	}
	
	protected String lessOrEqual(String tableAlias, Criteria criteria) {
		params.add(criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue());
		return tableAlias + "." + criteria.getFieldName() + " <= ?";
	}
	
	protected String lessThan(String tableAlias, Criteria criteria) {
		params.add(criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue());
		return tableAlias + "." + criteria.getFieldName() + " < ?";
	}
	
	protected String startsWith(String tableAlias, Criteria criteria) {
		final Object param = criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue();
		params.add(param + "%");
		return tableAlias + "." + criteria.getFieldName() + " LIKE ?";
	}
	
	protected String istartsWith(String tableAlias, Criteria criteria) {
		final Object param = criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue();
		params.add(param + "%");
		return "LOWER(" + tableAlias + "." + criteria.getFieldName() + ") LIKE LOWER(?)";
	}
	
	protected String inotStartsWith(String tableAlias, Criteria criteria) {
		final Object param = criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue();
		params.add(param + "%");
		return "LOWER(" + tableAlias + "." + criteria.getFieldName() + ") NOT LIKE LOWER(?)";
	}
	
	protected String endsWith(String tableAlias, Criteria criteria) {
		final Object param = criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue();
		params.add("%" + param);
		return tableAlias + "." + criteria.getFieldName() + " LIKE ?";
	}
	
	protected String iendsWith(String tableAlias, Criteria criteria) {
		final Object param = criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue();
		params.add("%" + param);
		return "LOWER(" + tableAlias + "." + criteria.getFieldName() + ") LIKE LOWER(?)";
	}
	
	protected String inotEndsWith(String tableAlias, Criteria criteria) {
		final Object param = criteria.getStringValue() != null ? criteria.getStringValue() : criteria.getNumberValue();
		params.add("%" + param);
		return "LOWER(" + tableAlias + "." + criteria.getFieldName() + ") NOT LIKE LOWER(?)";
	}
	
	protected String isBlank(String tableAlias, Criteria criteria) {
		return "(" + tableAlias + "." + criteria.getFieldName() + " = '' OR " + tableAlias + "." + criteria.getFieldName() + " IS NULL)";
	}
	
	protected String notBlank(String tableAlias, Criteria criteria) {
		return "(" + tableAlias + "." + criteria.getFieldName() + " != '' AND " + tableAlias + "." + criteria.getFieldName() + " IS NOT NULL)";
	}
	
	protected String notNull(String tableAlias, Criteria criteria) {
		return tableAlias + "." + criteria.getFieldName() + " IS NOT NULL";
	}
	
	public boolean isEmpty() {
		return sb.length() == 0;
	}

	@Override
	public String toString() {
		return sb.toString();
	}
	
	public List<Object> getValues() {
		return params;
	}
}

