package iinq.functions.select.operators.predicates;

public abstract class IonPredicate implements DictionaryPredicates {
	protected int predicateType;

	public int getPredicateType() {
		return predicateType;
	}

	public String getPredicateTypeIonName() {
		return getPredicateTypeIonName(predicateType);
	}

	public abstract String getPredicateArguments();

	public static String getPredicateTypeIonName(int type) {
		switch (type) {
			case ALL_RECORDS:
				return "predicate_all_records";
			case EQUALITY:
				return "predicate_equality";
			case RANGE:
				return "predicate_range";
			case PREDICATE:
				return "predicate_predicate";
			default:
				return null;
		}
	}

	public static String getPredicateSetup(int type) {
		switch (type) {
			case ALL_RECORDS:
				return AllRecordsPredicate.getPredicateSetup();
			case EQUALITY:
				return EqualityPredicate.getPredicateSetup();
			case RANGE:
				return RangePredicate.getPredicateSetup();
			case PREDICATE:
				return null;
			default:
				return null;
		}
	}
}
