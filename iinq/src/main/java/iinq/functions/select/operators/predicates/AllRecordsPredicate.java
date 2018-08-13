package iinq.functions.select.operators.predicates;

public class AllRecordsPredicate extends IonPredicate {
	public AllRecordsPredicate() {
		predicateType = ALL_RECORDS;
	}

	@Override
	public String getPredicateArguments() {
		return getPredicateTypeIonName();
	}

	public static String getPredicateSetup() {
		return "\terror = dictionary_build_predicate(predicate, predicate_type);\n";
	}
}
