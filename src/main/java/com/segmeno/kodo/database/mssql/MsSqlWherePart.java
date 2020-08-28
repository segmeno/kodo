package com.segmeno.kodo.database.mssql;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.segmeno.kodo.transport.AdvancedCriteria;
import com.segmeno.kodo.transport.Criteria;

public class MsSqlWherePart {
	
	private StringBuilder sb = new StringBuilder();
	
	public MsSqlWherePart(AdvancedCriteria adCrit) throws Exception {
		this(adCrit, null);
	}

	public MsSqlWherePart(AdvancedCriteria adCrit, List<String> allowedFields) throws Exception {
		
		final List<Criteria> crits = getAllowedCriterias(adCrit, allowedFields);
		
		if (adCrit != null && crits.size() > 0) {
			
			for (Criteria crit : crits) {
				
				switch (crit.getOperator()) {
				case CONTAINS:
					sb.append(crit.getFieldName())
					.append(" LIKE '%")
					.append(crit.getStringValue() != null ? crit.getStringValue() : crit.getNumberValue())
					.append("%'");
					break;
				case ICONTAINS:
					sb.append("UPPER(")
					.append(crit.getFieldName())
					.append(") LIKE UPPER('%")
					.append(crit.getStringValue() != null ? crit.getStringValue() : crit.getNumberValue())
					.append("%')");
					break;
				case NOT_CONTAINS:
					sb.append(crit.getFieldName())
					.append(" NOT LIKE '%")
					.append(crit.getStringValue() != null ? crit.getStringValue() : crit.getNumberValue())
					.append("%'");
					break;
				case INOT_CONTAINS:
					sb.append("UPPER(")
					.append(crit.getFieldName())
					.append(" NOT LIKE UPPER('%")
					.append(crit.getStringValue() != null ? crit.getStringValue() : crit.getNumberValue())
					.append("%')");
					break;
				case EQUALS:
					if (crit.getStringValue() != null) {
						sb.append(crit.getFieldName())
						.append(" = '")
						.append(crit.getStringValue())
						.append("'");
					}
					else {
						sb.append(crit.getFieldName())
						.append(" = ")
						.append(crit.getNumberValue());
					}
					break;
				case IEQUALS:
					if (crit.getStringValue() != null) {
						sb.append(crit.getFieldName())
						.append(" LIKE '")
						.append(crit.getStringValue())
						.append("'");
					}
					else {
						sb.append(crit.getFieldName())
						.append(" LIKE ")
						.append(crit.getNumberValue());
					}
					break;
				case GREATER_OR_EQUAL:
					if (crit.getStringValue() != null) {
						sb.append(crit.getFieldName())
						.append(" >= '")
						.append(crit.getStringValue())
						.append("'");
					}
					else {
						sb.append(crit.getFieldName())
						.append(" >= ")
						.append(crit.getNumberValue());
					}
					break;
				case GREATER_THAN:
					if (crit.getStringValue() != null) {
						sb.append(crit.getFieldName())
						.append(" > '")
						.append(crit.getStringValue())
						.append("'");
					}
					else {
						sb.append(crit.getFieldName())
						.append(" > ")
						.append(crit.getNumberValue());
					}
					break;
				case LESS_OR_EQUAL:
					if (crit.getStringValue() != null) {
						sb.append(crit.getFieldName())
						.append(" <= '")
						.append(crit.getStringValue())
						.append("'");
					}
					else {
						sb.append(crit.getFieldName())
						.append(" <= ")
						.append(crit.getNumberValue());
					}
					break;
				case LESS_THAN:
					if (crit.getStringValue() != null) {
						sb.append(crit.getFieldName())
						.append(" < '")
						.append(crit.getStringValue())
						.append("'");
					}
					else {
						sb.append(crit.getFieldName())
						.append(" < ")
						.append(crit.getNumberValue());
					}
					break;
				case STARTS_WITH:
					sb.append("BINARY ")
					.append(crit.getFieldName())
					.append(" LIKE '")
					.append(crit.getStringValue() != null ? crit.getStringValue() : String.valueOf(crit.getNumberValue()))
					.append("%'");
					break;
				case ISTARTS_WITH:
					sb.append(crit.getFieldName())
					.append(" LIKE '")
					.append(crit.getStringValue() != null ? crit.getStringValue() : String.valueOf(crit.getNumberValue()))
					.append("%'");
					break;
				case INOT_STARTS_WITH:
					sb.append(crit.getFieldName())
					.append(" NOT LIKE '")
					.append(crit.getStringValue() != null ? crit.getStringValue() : String.valueOf(crit.getNumberValue()))
					.append("%'");
					break;
				case ENDS_WITH:
					sb.append("BINARY ")
					.append(crit.getFieldName())
					.append(" LIKE '%")
					.append(crit.getStringValue() != null ? crit.getStringValue() : String.valueOf(crit.getNumberValue()))
					.append("'");
					break;
				case IENDS_WITH:
					sb.append(crit.getFieldName())
					.append(" LIKE '%")
					.append(crit.getStringValue() != null ? crit.getStringValue() : String.valueOf(crit.getNumberValue()))
					.append("'");
					break;
				case INOT_ENDS_WITH:
					sb.append(crit.getFieldName())
					.append(" NOT LIKE '%")
					.append(crit.getStringValue() != null ? crit.getStringValue() : String.valueOf(crit.getNumberValue()))
					.append("'");
					break;
				case IS_BLANK:
					sb.append('(')
					.append(crit.getFieldName())
					.append(" = '' OR ")
					.append(crit.getFieldName())
					.append(" IS NULL)");
					break;
				case NOT_BLANK:
					sb.append('(')
					.append(crit.getFieldName())
					.append(" != '' AND ")
					.append(crit.getFieldName())
					.append(" IS NOT NULL)");
					break;
				default:
					throw new Exception("unsupported OperatorId " + crit.getOperator() + "! Please extend this class: " + this.getClass());
				}
				
				sb.append(" ").append(adCrit.getOperator().getValue()).append(" ");
			}
			
			sb.setLength(sb.length() - (adCrit.getOperator().getValue().length() + 2));	// remove last ' OperatorId '
		}
	}
	
	private List<Criteria> getAllowedCriterias(AdvancedCriteria adCrit, List<String> allowedFields) {
		// just return everything
		if (adCrit != null && allowedFields == null) {
			return adCrit.getCriterias();
		}
		List<Criteria> result = new ArrayList<Criteria>();
		if (adCrit != null && adCrit.getCriterias() != null) {
			result = adCrit.getCriterias()
					.stream()
					.filter(crit -> allowedFields.contains(crit.getFieldName()))
					.collect(Collectors.toList());
		}
		return result;
	}
	
	public boolean isEmpty() {
		return sb.length() == 0;
	}

	public String toString() {
		return sb.toString();
	}
}

