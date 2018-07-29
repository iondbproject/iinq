package iinq.functions.select.operators.destroy;

public class ProjectionDestroyFunction extends OperatorDestroyFunction {
	public ProjectionDestroyFunction() {
		super("iinq_projection_destroy",
				"void iinq_projection_destroy(iinq_query_operator_t **operator);\n",
				"void iinq_projection_destroy(iinq_query_operator_t **operator) {\n" +
						"\tif (NULL != *operator) {\n" +
						"\t\tiinq_projection_t *projection = (iinq_projection_t *) (*operator)->instance;\n" +
						"\t\tif (NULL != projection->super.input_operators) {\n" +
						"\t\t\tprojection->super.input_operators[0]->destroy(projection->super.input_operators);\n" +
						"\t\t\tfree(projection->super.input_operators);\n" +
						"\t\t}\n" +
						"\t\t*operator = NULL;\n" +
						"\t}\n" +
						"}\n\n");
	}
}
