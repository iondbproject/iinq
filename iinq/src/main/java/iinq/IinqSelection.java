package iinq;

import iinq.functions.select.operators.predicates.AllRecordsPredicate;
import iinq.functions.select.operators.predicates.EqualityPredicate;
import iinq.functions.select.operators.predicates.IonPredicate;
import iinq.functions.select.operators.predicates.RangePredicate;
import iinq.metadata.IinqTable;
import unity.annotation.SourceField;

import java.util.ArrayList;
import java.util.Iterator;

public class IinqSelection{
	protected ArrayList<IinqSelectionPredicate> selectionPredicates;
	protected boolean containsKeyPredicate;

	public IinqSelection() {
		this.selectionPredicates = new ArrayList<>();
		containsKeyPredicate = false;
	}

	public IinqSelection(ArrayList<String> predicateFields, IinqTable table) {
		this();

		if (predicateFields != null) {
			Iterator<String> it = predicateFields.iterator();

			while (it.hasNext()) {
				addPredicate(it.next(), table);
			}
		}
	}

	public void addPredicate(String predicateString, IinqTable table) {
		int pos = -1, len = -1;
		/* Set up value, operatorType, and predicate for each WHERE clause */
		int operatorType = -1;
		if (predicateString.contains("!=")) {
			pos = predicateString.indexOf("!=");
			len = 2;
			operatorType = IinqPredicateOperators.NOT_EQUAL;
		} else if (predicateString.contains("<>")) {
			pos = predicateString.indexOf("<>");
			len = 2;
			operatorType = IinqPredicateOperators.NOT_EQUAL;
		} else if (predicateString.contains("<=")) {
			pos = predicateString.indexOf("<=");
			len = 2;
			operatorType = IinqPredicateOperators.LESS_THAN_EQUAL;
		} else if (predicateString.contains(">=")) {
			pos = predicateString.indexOf(">=");
			len = 2;
			operatorType = IinqPredicateOperators.GREATER_THAN_EQUAL;
		} else if (predicateString.contains("=")) {
			pos = predicateString.indexOf("=");
			len = 1;
			operatorType = IinqPredicateOperators.EQUAL;
		} else if (predicateString.contains("<")) {
			pos = predicateString.indexOf("<");
			len = 1;
			operatorType = IinqPredicateOperators.LESS_THAN;
		} else if (predicateString.contains(">")) {
			pos = predicateString.indexOf(">");
			len = 1;
			operatorType = IinqPredicateOperators.GREATER_THAN;
		}

		String fieldName = predicateString.substring(0, pos).trim();
		String value =predicateString.substring(pos + len).trim();
		SourceField field = table.getField(fieldName);
		int fieldNum = field.getOrdinalPosition();
		int fieldType = field.getDataType();
		boolean isKeyPredicate = false;
		// Projection may have removed key field at this point, resulting in a NullPointerException.
		try {
			isKeyPredicate = table.getPrimaryKeyFields().contains(field);
		} catch (Exception e) {

		}

		if (!this.containsKeyPredicate && isKeyPredicate) {
			this.containsKeyPredicate = true;
		}

		selectionPredicates.add(new IinqSelectionPredicate(value, operatorType, fieldType, fieldNum, isKeyPredicate));
	}

	public int getNumPredicates() {
		return selectionPredicates.size();
	}

	public boolean containsKeyPredicate() {
		return containsKeyPredicate;
	}

	public String toIinqConditionListString() {
		if (getNumPredicates() == 0) {
			return null;
		} else {
			StringBuilder conditionList = new StringBuilder();
			conditionList.append("IINQ_CONDITION_LIST(");
			Iterator<IinqSelectionPredicate> it = selectionPredicates.iterator();
			while (it.hasNext()) {
				conditionList.append(it.next().toIinqConditionString());
				conditionList.append(", ");
			}
			conditionList.setLength(conditionList.length()-2);
			conditionList.append(")");

			return conditionList.toString();
		}
	}

	public ArrayList<IinqSelectionPredicate> getKeyPredicateConditions() {
		if (containsKeyPredicate) {
			ArrayList<IinqSelectionPredicate> keyPredicates = new ArrayList<>();
			Iterator<IinqSelectionPredicate> it = selectionPredicates.iterator();
			while (it.hasNext()) {
				IinqSelectionPredicate predicate = it.next();
				if (predicate.isKeyPredicate)
					keyPredicates.add(predicate);
			}

			return keyPredicates;
		} else {
			return null;
		}
	}

	public IonPredicate optimizePredicate() {
		ArrayList<IinqSelectionPredicate> keyPredicates = getKeyPredicateConditions();
		if (keyPredicates != null) {
			switch (keyPredicates.size()) {
				// TODO: add inequality predicate
				case 1: // only works for equality
					if (keyPredicates.get(0).operatorType == IinqPredicateOperators.EQUAL) {
						selectionPredicates.remove(keyPredicates.get(0));
						return new EqualityPredicate(keyPredicates.get(0).value);
					}
					break;
				case 2: // only works for range TODO: handle ranges where comparison operators are less than and greater than
					if (keyPredicates.get(0).operatorType == IinqPredicateOperators.LESS_THAN_EQUAL && keyPredicates.get(1).operatorType == IinqPredicateOperators.GREATER_THAN_EQUAL) {
						selectionPredicates.remove(keyPredicates.get(0));
						selectionPredicates.remove(keyPredicates.get(1));
						return new RangePredicate(keyPredicates.get(0).value, keyPredicates.get(1).value);
					} else if (keyPredicates.get(0).operatorType == IinqPredicateOperators.GREATER_THAN_EQUAL && keyPredicates.get(1).operatorType == IinqPredicateOperators.LESS_THAN_EQUAL) {
						selectionPredicates.remove(keyPredicates.get(0));
						selectionPredicates.remove(keyPredicates.get(1));
						return new RangePredicate(keyPredicates.get(1).value, keyPredicates.get(0).value);
					}
					break;
				default: // can only extract range (based on selectivity, equality is better in most cases when composite keys are used)
					Iterator<IinqSelectionPredicate> it = keyPredicates.iterator();
					IinqSelectionPredicate upperBound = null;
					IinqSelectionPredicate lowerBound = null;
					while (it.hasNext() && (lowerBound == null || upperBound == null)) {
						IinqSelectionPredicate predicate = it.next();
						if (predicate.operatorType == IinqPredicateOperators.GREATER_THAN_EQUAL) {
							lowerBound = predicate;
						} else if (predicate.operatorType == IinqPredicateOperators.LESS_THAN_EQUAL) {
							upperBound = predicate;
						}
					}
					if (lowerBound != null && upperBound != null) {
						selectionPredicates.remove(lowerBound);
						selectionPredicates.remove(upperBound);
						return new RangePredicate(lowerBound.value, upperBound.value);
					}

					break;
			}
		}
		return new AllRecordsPredicate();
	}
}
