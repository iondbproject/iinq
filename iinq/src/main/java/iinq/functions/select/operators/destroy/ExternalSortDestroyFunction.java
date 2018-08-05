package iinq.functions.select.operators.destroy;

public class ExternalSortDestroyFunction extends OperatorDestroyFunction {
	public ExternalSortDestroyFunction() {
		super("iinq_external_sort_destroy",
				"void iinq_external_sort_destroy(iinq_query_operator_t **operator);\n",
				"void iinq_external_sort_destroy(iinq_query_operator_t **operator) {\n" +
						"\tif (NULL != *operator) {\n" +
						"\t\tif (NULL != (*operator)->instance) {\n" +
						"\t\t\tiinq_external_sort_t *external_sort = (iinq_external_sort_t *) (*operator)->instance;\n" +
						"\n" +
						"\t\t\tif (NULL != external_sort->cursor) {\n" +
						"\t\t\t\tion_external_sort_destroy_cursor(external_sort->cursor);\n" +
						"\t\t\t\tfree(external_sort->cursor);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != external_sort->buffer) {\n" +
						"\t\t\t\tfree(external_sort->buffer);\n" +
						"\t\t\t}\n" +
						"\t\t\t\n" +
						"\t\t\tif (NULL != external_sort->record_buf) {\n" +
						"\t\t\t\tfree(external_sort->record_buf);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != external_sort->es) {\n" +
						"\t\t\t\tfclose(external_sort->es->input_file);\n" +
						"\n" +
						"\t\t\t\tif (NULL != external_sort->es->context) {\n" +
						"\t\t\t\t\tfree(external_sort->es->context);\n" +
						"\t\t\t\t}\n" +
						"\n" +
						"\t\t\t\tfree(external_sort->es);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != external_sort->super.field_info) {\n" +
						"\t\t\t\tfree(external_sort->super.field_info);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tif (NULL != external_sort->super.fields) {\n" +
						"\t\t\t\tfree(external_sort->super.fields);\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\tfree(external_sort);\n" +
						"\t\t}\n" +
						"\n" +
						"\t\tfree(*operator);\n" +
						"\t\t*operator = NULL;\n" +
						"\t}\n" +
						"}\n\n");
	}
}
