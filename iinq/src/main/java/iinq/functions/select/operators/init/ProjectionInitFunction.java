package iinq.functions.select.operators.init;

public class ProjectionInitFunction extends OperatorInitFunction {
	public ProjectionInitFunction() {
		super("iinq_projection_init",
				"iinq_query_operator_t *iinq_projection_init(iinq_query_operator_t *input_operator, iinq_field_num_t num_fields, iinq_field_num_t *field_nums);\n",
				"iinq_query_operator_t *iinq_projection_init(iinq_query_operator_t *input_operator, iinq_field_num_t num_fields, iinq_field_num_t *field_nums) {\n" +
						"\tif (NULL == input_operator) {\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\t\n" +
						"\tiinq_query_operator_t *operatorType = malloc(sizeof(iinq_query_operator_t));\n" +
						"\tif (NULL == operatorType) {\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\toperatorType->instance = malloc(sizeof(iinq_projection_t));\n" +
						"\tif (NULL == operatorType->instance) {\n" +
						"\t\tfree(operatorType);\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\toperatorType->next = iinq_projection_next;\n" +
						"\toperatorType->destroy = iinq_projection_destroy;\n" +
						"\n" +
						"\tiinq_projection_t *projection = (iinq_projection_t *) operatorType->instance;\n" +
						"\tprojection->super.type = iinq_projection_e;\n" +
						"\t\n" +
						"\tprojection->super.num_input_operators = 1;\n" +
						"\tprojection->super.input_operators = malloc(sizeof(iinq_query_operator_t));\n" +
						"\tif (NULL == projection->super.input_operators) {\n" +
						"\t\tfree(projection);\n" +
						"\t\tfree(operatorType);\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\t\n" +
						"\tprojection->super.input_operators[0] = input_operator;\n" +
						"\tprojection->super.num_fields = num_fields;\n" +
						"\n" +
						"\tprojection->input_field_nums = malloc(sizeof(iinq_field_num_t)*num_fields);\n" +
						"\tif (NULL == projection->input_field_nums) {\n" +
						"\t\tfree(projection->super.input_operators);\n" +
						"\t\tfree(projection);\n" +
						"\t\tfree(operatorType);\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\tmemcpy(projection->input_field_nums, field_nums, sizeof(iinq_field_num_t)*num_fields);\n" +
						"\n" +
						"\tprojection->super.null_indicators = malloc(IINQ_BITS_FOR_NULL(num_fields));\n" +
						"\tif (NULL == projection->super.null_indicators) {\n" +
						"\t\tfree(projection->input_field_nums);" +
						"\t\tfree(projection->super.input_operators);\n" +
						"\t\tfree(projection);\n" +
						"\t\tfree(operatorType);\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\t\n" +
						"\tprojection->super.field_info = malloc(sizeof(iinq_field_info_t)*num_fields);\n" +
						"\tif (NULL == projection->super.field_info) {\n" +
						"\t\tfree(projection->super.null_indicators);\n" +
						"\t\tfree(projection->super.input_operators);\n" +
						"\t\tfree(projection);\n" +
						"\t\tfree(operatorType);\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\tprojection->super.fields = malloc(sizeof(ion_value_t)*num_fields);\n" +
						"\tif (NULL == projection->super.fields) {\n" +
						"\t\tfree(projection->super.field_info);\n" +
						"\t\tfree(projection->super.null_indicators);\n" +
						"\t\tfree(projection->super.input_operators);\n" +
						"\t\tfree(projection);\n" +
						"\t\tfree(operatorType);\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\tint i;\n" +
						"\tfor (i = 0; i < num_fields; i++) {\n" +
						"\t\tprojection->super.field_info[i] = input_operator->instance->field_info[field_nums[i]-1];\n" +
						"\t\tprojection->super.fields[i] = input_operator->instance->fields[field_nums[i]-1];\n" +
						"\t}\n" +
						"\n" +
						"\toperatorType->status = ION_STATUS_OK(0);\n" +
						"\n" +
						"\treturn operatorType;\n" +
						"}\n\n");
	}
}
