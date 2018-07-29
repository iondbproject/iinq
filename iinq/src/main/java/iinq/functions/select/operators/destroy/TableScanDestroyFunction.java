package iinq.functions.select.operators.destroy;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class TableScanDestroyFunction extends OperatorDestroyFunction {
	public TableScanDestroyFunction() {
		super("iinq_table_scan_destroy",
				"void iinq_table_scan_destroy(iinq_query_operator_t **operator);\n",
				"void iinq_table_scan_destroy(iinq_query_operator_t **operator) {\n" +
						"\tiinq_table_scan_t *table_scan = (iinq_table_scan_t *) (*operator)->instance;\n" +
						"\n" +
						"\tif (NULL != table_scan->record.key) {\n" +
						"\t\tfree(table_scan->record.key);\n" +
						"\t}\n" +
						"\n" +
						"\tif (NULL != table_scan->record.value) {\n" +
						"\t\tfree(table_scan->record.value);\n" +
						"\t}\n" +
						"\n" +
						"\tion_close_dictionary(&table_scan->dictionary);\n" +
						"\n" +
						"\tif (NULL != table_scan->cursor) {\n" +
						"\t\ttable_scan->cursor->destroy(&table_scan->cursor);\n" +
						"\t}\n" +
						"\n" +
						"\tfree(table_scan);\n" +
						"\tfree(*operator);\n" +
						"\t*operator = NULL;\n" +
						"}\n\n"

		);
	}
}
