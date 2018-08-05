package iinq.functions.select.operators.destroy;

public class ProjectionDestroyFunction extends OperatorDestroyFunction {
	public ProjectionDestroyFunction() {
		super("iinq_projection_destroy",
				"void iinq_projection_destroy(iinq_query_operator_t **operator);\n",
				"void iinq_projection_destroy(iinq_query_operator_t **operator) {\n" +
						"\tif (*operator != NULL) {\n" +
						"\t\tif ((*operator)->instance != NULL) {\n" +
						"\t\t\tiinq_projection_t *projection = (iinq_projection_t *) (*operator)->instance;\n" +
						"\t\t\t\n" +
						"\t\t\tif (NULL != projection->input_field_nums) {\n" +
						"\t\t\t\tfree(projection->input_field_nums);\n" +
						"\t\t\t}\n" +
						"\t\t\t\n" +
						"\t\t\tif (NULL != projection->super.fields) {\n" +
						"\t\t\t\tfree(projection->super.fields);\n" +
						"\t\t\t}\n" +
						"\t\t\t\n" +
						"\t\t\tif (NULL != projection->super.field_info) {\n" +
						"\t\t\t\tfree(projection->super.field_info);\n" +
						"\t\t\t}\n" +
						"\t\t\t\n" +
						"\t\t\tif (NULL != projection->super.null_indicators) {\n" +
						"\t\t\t\tfree(projection->super.null_indicators);\n" +
						"\t\t\t}\n" +
						"\t\t\t\n" +
						"\t\t\tif (NULL != projection->super.input_operators) {\n" +
						"\t\t\t\tprojection->super.input_operators[0]->destroy(&projection->super.input_operators[0]);\n" +
						"\t\t\t\tfree(projection->super.input_operators);\n" +
						"\t\t\t}\n" +
						"\t\t\t\n" +
						"\t\t\tfree(projection);\n" +
						"\t\t}\n" +
						"\t\tfree(*operator);\n" +
						"\t\t*operator = NULL;\n" +
						"\t}\n" +
						"}\n\n");
	}
}
