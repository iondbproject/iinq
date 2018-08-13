package iinq.functions.select.operators.predicates;

public class RangePredicate extends IonPredicate {
	protected String upperBound;
	protected String lowerBound;

	public RangePredicate(String lowerBound, String upperBound) {
		this.lowerBound = lowerBound;
		this.upperBound = upperBound;
		predicateType = RANGE;
	}

	@Override
	public String getPredicateArguments() {
		return getPredicateTypeIonName() + ", " + lowerBound + ", " + upperBound;
	}

	public static String getPredicateSetup() {
		return "\tva_list arg_list;\n" +
				"\tva_start(arg_list, predicate_type);\n" +
				"\terror = dictionary_build_predicate(predicate, predicate_type, va_arg(arg_list, ion_key_t), va_arg(arg_list, ion_key_t));\n" +
				"\tva_end(arg_list);\n";
	}
}
