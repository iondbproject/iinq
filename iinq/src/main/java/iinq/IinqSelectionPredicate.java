package iinq;

import java.sql.Types;

public class IinqSelectionPredicate implements IinqPredicateOperators {
	protected String value;
	protected int operatorType;
	protected int fieldNum;
	protected boolean isKeyPredicate;

	public IinqSelectionPredicate(String value, int operatorType, Integer fieldType, Integer fieldNum, boolean isKeyPredicate) {
		if (fieldType == Types.INTEGER)
			this.value = "IONIZE(" + value + ", int)";
		else
			this.value = value;
		this.operatorType = operatorType;
		this.fieldNum = fieldNum;
		this.isKeyPredicate = isKeyPredicate;
	}

	public String toIinqConditionString() {
		return String.format("IINQ_CONDITION(%d, %s, %s)", fieldNum, getIinqBooleanOperator(operatorType), value);
	}

	public static String getIinqBooleanOperator(int operatorType) {
		switch (operatorType) {
			case NOT_EQUAL:
				return "iinq_not_equal";
			case EQUAL:
				return "iinq_equal";
			case GREATER_THAN:
				return "iinq_greater_than";
			case LESS_THAN:
				return "iinq_less_than";
			case GREATER_THAN_EQUAL:
				return "iinq_greater_than_equal_to";
			case LESS_THAN_EQUAL:
				return "iinq_less_than_equal_to";
			default:
				return null;
		}
	}
}
