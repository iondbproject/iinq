package iinq.functions.select.operators.next;

public class ProjectionNextFunction extends OperatorNextFunction {
	public ProjectionNextFunction() {
		super("iinq_projection_next",
				"ion_boolean_t iinq_projection_next(iinq_query_operator_t *query_operator);\n",
				"ion_boolean_t iinq_projection_next(iinq_query_operator_t *query_operator) {\n" +
						"\tion_boolean_t result = query_operator->instance->input_operators[0]->next(query_operator->instance->input_operators[0]);\n" +
						"\tif (result) {\n" +
						"\t\tint i;\n" +
						"\t\tiinq_projection_t *projection = (iinq_projection_t *) query_operator->instance;\n" +
						"\t\tfor (i = 0; i < projection->super.num_fields; i++) {\n" +
						"\t\t\tif (iinq_check_null_indicator(projection->super.input_operators[0]->instance->null_indicators, projection->input_field_nums[i])) {\n" +
						"\t\t\t\tiinq_set_null_indicator(projection->super.null_indicators, i+1);\n" +
						"\t\t\t} else {\n" +
						"\t\t\t\tiinq_clear_null_indicator(projection->super.null_indicators, i+1);\n" +
						"\t\t\t}\n" +
						"\t\t}\n" +
						"\t\tquery_operator->status.count++;\n" +
						"\t}\n" +
						"\treturn result;\n" +
						"}\n\n");
	}
}
