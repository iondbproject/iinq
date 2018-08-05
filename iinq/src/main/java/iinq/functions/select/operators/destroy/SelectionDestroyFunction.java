package iinq.functions.select.operators.destroy;

public class SelectionDestroyFunction extends OperatorDestroyFunction {
	public SelectionDestroyFunction() {
		super("iinq_selection_destroy",
				"void iinq_selection_destroy(iinq_query_operator_t **operator);\n",
				"void iinq_selection_destroy(iinq_query_operator_t **operator) {\n" +
						"\tif (NULL != *operator) {\n" +
						"\t\tif (NULL != (*operator)->instance) {\n" +
						"\t\t\tiinq_selection_t *selection = (iinq_selection_t *) (*operator)->instance;\n" +
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
						"\t\tfree(*operator);\n" +
						"\t\t*operator = NULL;\n" +
						"\t}\n" +
						"}\n\n");
	}
}
