package iinq.functions.select.operators.next;

import iinq.functions.IinqFunction;

public class TableScanNextFunction extends OperatorNextFunction {
	public TableScanNextFunction() {
		super("iinq_simple_next",
				"ion_boolean_t iinq_table_scan_next(iinq_result_set *select);\n",
				"ion_boolean_t iinq_table_scan_next(iinq_result_set *select) {\n" +
						"\twhile(cs_cursor_active == select->dictionary_ref.cursor->next(select->dictionary_ref.cursor, &select->record) || cs_cursor_initialized == select->dictionary_ref.cursor->status) {\n" +
						"\t\tif (!where(select->table_id, &select->record, select->num_wheres, select->wheres))\n" +
						"\t\t\tcontinue;\n" +
						"\t\treturn boolean_true;\n" +
						"\t}\n" +
						"\treturn boolean_false;\n" +
						"}\n\n"
/*						"\t\tselect->status.count++;\n" +
						"\t\treturn boolean_true;\n\t}\n\n" +
						"\tselect->status.error = ion_init_master_table();\n" +
						"\tif (err_ok != select->status.error)\n" +
						"\t\treturn boolean_false;\n\n" +
						"\tion_dictionary_t dictionary;\n" +
						"\tion_dictionary_handler_t handler;\n" +
						"\tdictionary.handler = &handler;\n" +
						"\tselect->status.error = ion_open_dictionary(&handler, &dictionary, select->id);\n" +
						"\tif (err_ok != select->status.error)\n" +
						"\t\treturn boolean_false;\n\n" +
						"\tselect->status.error = ion_delete_dictionary(&dictionary, select->id);\n" +
						"\tif (err_ok != select->status.error)\n" +
						"\t\treturn boolean_false;\n\n" +
						"\tselect->status.error = ion_close_master_table();\n" +
						"\tif (err_ok != select->status.error)\n" +
						"\t\treturn boolean_false;\n\n" +
						"\tfree(select->value);\n" +
						"\tfree(select->fields);\n" +
						"\treturn boolean_false;\n}\n\n"*/);
	}
}
