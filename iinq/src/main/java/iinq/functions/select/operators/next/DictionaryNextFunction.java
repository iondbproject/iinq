package iinq.functions.select.operators.next;

import iinq.functions.IinqFunction;

public class DictionaryNextFunction extends OperatorNextFunction {
	public DictionaryNextFunction() {
		super("iinq_dictionary_operator_next",
				"ion_boolean_t iinq_dictionary_operator_next(iinq_query_operator_t *query_operator);\n",
				"ion_boolean_t iinq_dictionary_operator_next(iinq_query_operator_t *query_operator) {\n" +
						"\tiinq_dictionary_operator_t *dict_op = (iinq_dictionary_operator_t *) query_operator->instance;\n" +
						"\tif (cs_cursor_active == dict_op->cursor->next(dict_op->cursor, &dict_op->record) || cs_cursor_initialized == dict_op->cursor->status) {\n" +
						"\t\tquery_operator->status.count++;\n" +
						"\t\treturn boolean_true;\n" +
						"\t}\n" +
						"\treturn boolean_false;\n" +
						"}\n\n");
	}
}
