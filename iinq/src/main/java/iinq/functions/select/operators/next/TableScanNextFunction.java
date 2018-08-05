package iinq.functions.select.operators.next;

import iinq.functions.IinqFunction;

public class TableScanNextFunction extends OperatorNextFunction {
	public TableScanNextFunction() {
		super("iinq_simple_next",
				"ion_boolean_t iinq_table_scan_next(iinq_query_operator_t *query_operator);\n",
				"ion_boolean_t iinq_table_scan_next(iinq_query_operator_t *query_operator) {\n" +
						"\tiinq_table_scan_t *table_scan = (iinq_table_scan_t *) query_operator->instance;\n" +
						"\tif (cs_cursor_active == table_scan->cursor->next(table_scan->cursor, &table_scan->record) || cs_cursor_initialized == table_scan->cursor->status) {\n" +
/*						"\t\tif (!where(table_scan->super.field_info[0].table_id, &table_scan->record, 0, NULL))\n" +
						"\t\t\tcontinue;\n" +*/
						"\t\tquery_operator->status.count++;\n" +
						"\t\treturn boolean_true;\n" +
						"\t}\n" +
						"\treturn boolean_false;\n" +
						"}\n\n");
	}
}
