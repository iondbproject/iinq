package iinq.functions.select.operators.destroy;

public class ExternalSortDestroyFunction extends OperatorDestroyFunction {
	public ExternalSortDestroyFunction() {
		super("iinq_external_sort_destroy",
				"void iinq_external_sort_destroy(iinq_query_operator_t **operator);\n",
				"void iinq_external_sort_destroy(iinq_query_operator_t **operator) {\n" +
						"\tif (NULL != *operator) {\n" +
						"\t\tif (NULL != (*operator)->instance) {\n" +
						"\t\t\tiinq_external_sort_t *external_sort = (iinq_external_sort_t *) (*operator)->instance;\n" +
						"\t\t\tif (external_sort->cursor != NULL) {\n" +
						"\t\t\t\tion_external_sort_destroy_cursor(external_sort->cursor);\n" +
						"\t\t\t}\n" +
						"\t\t\tif (external_sort->buffer != NULL) {\n" +
						"\t\t\t\tfree(external_sort->buffer);\n" +
						"\t\t\t}\n" +
						"\t\t\tif (external_sort->es != NULL) {\n" +
						"\t\t\t\tfclose(external_sort->es->input_file);\n" +
						"\t\t\t\tfree(external_sort->es);\n" +
						"\t\t\t}\n" +
						"\t\t\tfree(external_sort);\n" +
						"\t\t}\n" +
						"\t\tfree(*operator);\n" +
						"\t\t*operator = NULL;\n" +
						"\t}\n" +
						"}\n\n");
	}
}
