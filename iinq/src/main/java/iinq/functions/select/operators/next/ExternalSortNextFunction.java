package iinq.functions.select.operators.next;

public class ExternalSortNextFunction extends OperatorNextFunction {
	public ExternalSortNextFunction() {
		super("iinq_external_sort_next",
				"ion_boolean_t iinq_external_sort_next(iinq_query_operator_t *operator);\n",
				"ion_boolean_t iinq_external_sort_next(iinq_query_operator_t *operator) {\n" +
						"\tiinq_external_sort_t *external_sort = (iinq_external_sort_t *) operator->instance;\n" +
						"\tif (err_ok != external_sort->cursor->next(external_sort->cursor, external_sort->record_buf) || cs_cursor_active != external_sort->cursor->status) {\n" +
						"\t\treturn boolean_false;\n" +
						"\t}\n" +
						"\n" +
						"\toperator->status.count++;\n" +
						"\treturn boolean_true;\n" +
						"}\n\n");
	}
}
