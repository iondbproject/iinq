package iinq.functions.select.operators.destroy;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class TableScanDestroyFunction extends OperatorDestroyFunction {
	public TableScanDestroyFunction() {
		super("iinq_table_scan_destroy",
				"void iinq_table_scan_destroy(iinq_query_operator_t **query_operator);\n",
				"void iinq_table_scan_destroy(iinq_query_operator_t **query_operator) {\n" +
						"\tif (NULL != *query_operator) {\n" +
						"\t\tif (NULL != (*query_operator)->instance){\n" +
						"\t\t\tiinq_table_scan_t *table_scan = (iinq_table_scan_t *) (*query_operator)->instance;\n" +
						"\n" +
						"\t\t\tif (NULL != table_scan->super.field_info) {\n" +
						"\t\t\t\tfree(table_scan->super.field_info);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != table_scan->super.fields) {\n" +
						"\t\t\t\tfree(table_scan->super.fields);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != table_scan->record.value) {\n" +
						"\t\t\t\tfree(table_scan->record.value);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != table_scan->record.key) {\n" +
						"\t\t\t\tfree(table_scan->record.key);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != table_scan->cursor) {\n" +
						"\t\t\t\ttable_scan->cursor->destroy(&table_scan->cursor);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tion_close_dictionary(&table_scan->dictionary);\n" +
						"\n" +
						"\t\t\tfree(table_scan);\n" +
						"\t\t}\n" +
						"\t\tfree(*query_operator);\n" +
						"\t\t*query_operator = NULL;\n" +
						"\t}" +
						"}\n\n"

		);
	}
}
