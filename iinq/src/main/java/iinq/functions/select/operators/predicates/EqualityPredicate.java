package iinq.functions.select.operators.predicates;

public class EqualityPredicate extends IonPredicate {
	protected String key;

	public EqualityPredicate(String key) {
		this.key = key;
		predicateType = EQUALITY;
	}

	@Override
	public String getPredicateArguments() {
		return getPredicateTypeIonName() + ", " + key.toString();
	}

	public static String getPredicateSetup() {
		return "\tva_list arg_list;\n" +
				"\tva_start(arg_list, predicate_type);\n" +
				"\terror = dictionary_build_predicate(predicate, predicate_type, va_arg(arg_list, ion_key_t));\n" +
				"\tva_end(arg_list);\n";
	}
}
