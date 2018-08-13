package iinq.functions.select.operators.init;

import iinq.functions.select.operators.predicates.IonPredicate;

import java.util.HashSet;
import java.util.Iterator;

public class DictionaryInitFunction extends OperatorInitFunction {
	protected HashSet<Integer> predicateTypes = new HashSet<>();

	public DictionaryInitFunction() {
		super("iinq_dictionary_init",
				"iinq_query_operator_t *iinq_dictionary_init(iinq_table_id_t table_id, iinq_field_num_t num_fields, ion_predicate_type_t predicate_type, ...);\n",
				null);
	}

	public void addPredicateType(int predicateType) {
		predicateTypes.add(predicateType);
	}

	public void addAllPredicateTypes(HashSet<Integer> predicateTypes) {
		this.predicateTypes.addAll(predicateTypes);
	}

	public void addAllPredicateTypes(DictionaryInitFunction initFunction) {
		addAllPredicateTypes(initFunction.predicateTypes);
	}

	public String generateDefinition() {
		StringBuilder def = new StringBuilder();

		def.append("iinq_query_operator_t *iinq_dictionary_init(iinq_table_id_t table_id, iinq_field_num_t num_fields, ion_predicate_type_t predicate_type, ...) {\n\n" +
				"\tint i;\n" +
				"\tion_err_t error;\n" +
				"\tiinq_dictionary_operator_t *dictionary_operator;\n" +
				"\tion_predicate_t *predicate;\n" +
				"\tion_dict_cursor_t *cursor = NULL;\n" +
				"\tion_record_t *record = NULL;\n" +
				"\tion_dictionary_t           *dictionary = NULL;\n" +
				"\tion_dictionary_handler_t   *handler = NULL;\n" +
				"\n" +
				"\tiinq_query_operator_t *operatorType = malloc(sizeof(iinq_query_operator_t));\n" +
				"\tif (NULL == operatorType) {\n" +
				"\t\treturn NULL;\n" +
				"\t}\n" +
				"\n" +
				"\toperatorType->instance = malloc(sizeof(iinq_dictionary_operator_t));\n" +
				"\tif (NULL == operatorType->instance) {\n" +
				"\t\tfree(operatorType);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n" +
				"\n" +
				"\tdictionary_operator = (iinq_dictionary_operator_t *) operatorType->instance;\n" +
				"\tdictionary_operator->super.type = iinq_dictionary_operator_e;\n" +
				"\tdictionary_operator->super.num_fields = num_fields;\n" +
				"\tpredicate = &dictionary_operator->predicate;\n");

		if (predicateTypes.size() == 1) {
			def.append(IonPredicate.getPredicateSetup(predicateTypes.iterator().next()));
		} else {
			Iterator<Integer> it = predicateTypes.iterator();
			def.append("\tswitch (predicate_type) {\n");
			while (it.hasNext()) {
				int type = it.next();
				def.append(String.format("\t\tcase %s: {\n", IonPredicate.getPredicateTypeIonName(type)));
				def.append("\t\t");
				def.append(IonPredicate.getPredicateSetup(type).replace("\n", "\n\t\t"));
				def.append("\tbreak;\n");
				def.append("\t\t}\n");
			}
			def.append("\t}\n");
		}
		def.append("\n" +
				"\tif (err_ok != error) {\n" +
				"\t\tfree(operatorType->instance);\n" +
				"\t\tfree(operatorType);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n" +
				"\n" +
				"\tdictionary = &dictionary_operator->dictionary;\n" +
				"\thandler = &dictionary_operator->handler;\n" +
				"\tdictionary->handler = handler;\n" +
				"\trecord = &dictionary_operator->record;\n" +
				"\n" +
				"\terror = iinq_open_source(table_id, dictionary, handler);\n" +
				"\n" +
				"\tif (err_ok != error) {\n" +
				"\t\tfree(operatorType->instance);\n" +
				"\t\tfree(operatorType);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n" +
				"\n" +
				"\terror = dictionary_find(dictionary, predicate, &cursor);\n" +
				"\n" +
				"\tif (err_ok != error) {\n" +
				"\t\tif (NULL != cursor) {\n" +
				"\t\t\tcursor->destroy(&cursor);\n" +
				"\t\t}\n" +
				"\t\tion_close_dictionary(&dictionary);\n" +
				"\t\tfree(operatorType->instance);\n" +
				"\t\tfree(operatorType);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n" +
				"\n" +
				"\tdictionary_operator->cursor = cursor;\n" +
				"\t\n" +
				"\tdictionary_operator->record.key     = malloc(dictionary->instance->record.key_size);\n" +
				"\tif (NULL == dictionary_operator->record.key) {\n" +
				"\t\tcursor->destroy(&cursor);\n" +
				"\t\tion_close_dictionary(&dictionary);\n" +
				"\t\tfree(operatorType->instance);\n" +
				"\t\tfree(operatorType);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n" +
				"\trecord->value   = malloc(dictionary->instance->record.value_size);\n" +
				"\tif (NULL == record->value) {\n" +
				"\t\tfree(record->key);\n" +
				"\t\tcursor->destroy(&cursor);\n" +
				"\t\tion_close_dictionary(&dictionary);\n" +
				"\t\tfree(operatorType->instance);\n" +
				"\t\tfree(operatorType);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n" +
				"\t\n" +
				"\tdictionary_operator->super.field_info = malloc(sizeof(iinq_field_info_t) * num_fields);\n" +
				"\tif (NULL == dictionary_operator->super.field_info) {\n" +
				"\t\tfree(record->value);\n" +
				"\t\tfree(record->key);\n" +
				"\t\tcursor->destroy(&cursor);\n" +
				"\t\tion_close_dictionary(&dictionary);\n" +
				"\t\tfree(operatorType->instance);\n" +
				"\t\tfree(operatorType);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n" +
				"\n" +
				"\tdictionary_operator->super.null_indicators = dictionary_operator->record.value;\n" +
				"\n" +
				"\tdictionary_operator->super.fields = malloc(sizeof(ion_value_t) * num_fields);\n" +
				"\tif (NULL == dictionary_operator->super.fields) {\n" +
				"\t\tfree(dictionary_operator->super.field_info);\n" +
				"\t\tfree(record->value);\n" +
				"\t\tfree(record->key);\n" +
				"\t\tcursor->destroy(&cursor);\n" +
				"\t\tion_close_dictionary(&dictionary);\n" +
				"\t\tfree(operatorType->instance);\n" +
				"\t\tfree(operatorType);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n" +
				"\n" +
				"\tfor (i = 0; i < num_fields; i++) {\n" +
				"\t\tdictionary_operator->super.field_info[i] = (iinq_field_info_t) {table_id, i+1};\n" +
				"\t\tdictionary_operator->super.fields[i] = (unsigned char *) record->value + iinq_calculate_offset(table_id, i + 1);\n" +
				"\t}\n" +
				"\n" +
				"\tion_close_master_table();\n" +
				"\toperatorType->next = iinq_dictionary_operator_next;\n" +
				"\toperatorType->destroy = iinq_dictionary_operator_destroy;\n" +
				"\toperatorType->status = ION_STATUS_OK(0);\n" +
				"\n" +
				"\treturn operatorType;\n" +
				"}\n\n");

		setDefinition(def.toString());

		return def.toString();
	}
}
