package iinq.functions.select.operators.destroy;

public class SelectionDestroyFunction extends OperatorDestroyFunction {
	public SelectionDestroyFunction() {
		super("iinq_selection_destroy",
				"void iinq_selection_destroy(iinq_query_operator_t **query_operator);\n",
				"void iinq_selection_destroy(iinq_query_operator_t **query_operator) {\n" +
						"\tif (NULL != *query_operator) {\n" +
						"\t\tif (NULL != (*query_operator)->instance) {\n" +
						"\t\t\tiinq_selection_t *selection = (iinq_selection_t *) (*query_operator)->instance;\n" +
						"\t\t\t\n" +
						"\t\t\tif (NULL != selection->conditions) {\n" +
						"\t\t\t\tint i;\n" +
						"\t\t\t\tfor (i = 0; i < selection->num_conditions; i++) {\n" +
						"\t\t\t\t\tif (NULL != selection->conditions[i].field_value) {\n" +
						"\t\t\t\t\t\tfree(selection->conditions[i].field_value);\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t}\n" +
						"\t\t\t\tfree(selection->conditions);" +
						"\t\t\t}\n" +
						"\t\t\t\n" +
						"\t\t\tif (NULL != selection->super.input_operators) {\n" +
						"\t\t\t\tselection->super.input_operators[0]->destroy(&selection->super.input_operators[0]);\n" +
						"\t\t\t\tfree(selection->super.input_operators);\n" +
						"\t\t\t}\n" +
						"\t\t\t\n" +
						"\t\t\tfree(selection);\n" +
						"\t\t}\n" +
						"\t\t\n" +
						"\t\tfree(*query_operator);\n" +
						"\t\t*query_operator = NULL;\n" +
						"\t}\n" +
						"}\n\n");
	}
}
