package iinq.functions.select.operators.init;

public class ProjectionInitFunction extends OperatorInitFunction {
	public ProjectionInitFunction() {
		super("iinq_projection_init",
				"iinq_query_operator_t *iinq_projection_init(iinq_query_operator_t *input_operator, iinq_field_num_t num_fields, iinq_field_num_t *field_nums);\n",
				"iinq_query_operator_t *iinq_projection_init(iinq_query_operator_t *input_operator, iinq_field_num_t num_fields, iinq_field_num_t *field_nums) {\n" +
						"\tiinq_projection_t *projection = NULL;\n" +
						"\tiinq_query_operator_t *operator = malloc(sizeof(iinq_query_operator_t));\n" +
						"\tif (NULL == operator) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\n" +
						"\toperator->instance = malloc(sizeof(iinq_projection_t));\n" +
						"\tif (NULL == operator->instance) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\n" +
						"\toperator->next = iinq_projection_next;\n" +
						"\toperator->destroy = iinq_projection_destroy;\n" +
						"\n" +
						"\tprojection = operator->instance;\n" +
						"\tprojection->super.type = iinq_projection_e;\n" +
/*						"\tprojection->offset = NULL;\n" +
						"\tsize_t offset = malloc(sizeof(size_t)*num_fields);\n" +
						"\tif (NULL == offset) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\n" +
						"\tprojection->offset = offset;\n" +*/
						"\n" +
						"\tif (input_operator == NULL) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\n" +
						"\tprojection->super.num_input_operators = 1;\n" +
						"\tprojection->super.input_operators = malloc(sizeof(iinq_query_operator_t));\n" +
						"\tif (NULL == projection->super.input_operators) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\tprojection->super.input_operators[0] = input_operator;\n" +
						"\tprojection->super.num_fields = num_fields;\n" +
						"\n" +
						"\tprojection->input_field_nums = malloc(sizeof(iinq_field_num_t)*num_fields);\n" +
						"\tif (NULL == projection->input_field_nums) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\n" +
						"\tmemcpy(projection->input_field_nums, field_nums, sizeof(iinq_field_num_t)*num_fields);\n" +
						"\n" +
						"\tprojection->super.null_indicators = malloc(IINQ_BITS_FOR_NULL(num_fields));\n" +
						"\tif (NULL == projection->super.null_indicators) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
/*						"\tswitch (input_operator->instance->super.type) {\n" +
						"\t\tcase iinq_table_scan_e: {\n" +
						"\t\t\tiinq_table_scan_t *table_scan = (iinq_table_scan_t *) input_operator->instance;\n" +
						"\t\t\tprojection->value = table_scan->super.value;\n" +
						"\t\t}\n" +
						"\t\tcase iinq_selection_e: {\n" +
						"\t\t\tiinq_selection_t *selection = (iinq_selection_t *) input_operator->instance;\n" +
						"\t\t\tprojection->super.value = selection->super.value;\n" +
						"\n" +*/
						"\tprojection->super.field_info = malloc(sizeof(iinq_field_info_t)*num_fields);\n" +
						"\tif (NULL == projection->super.field_info) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\n" +
						"\tprojection->super.fields = malloc(sizeof(ion_value_t)*num_fields);\n" +
						"\tif (NULL == projection->super.fields) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\n" +
						"\tint i;\n" +
						"\tfor (i = 0; i < num_fields; i++) {\n" +
						"\t\tprojection->super.field_info[i] = input_operator->instance->field_info[field_nums[i]-1];\n" +
						"\t\tprojection->super.fields[i] = input_operator->instance->fields[field_nums[i]-1];\n" +
						"\t}\n" +
						"\n" +
						"\toperator->status = ION_STATUS_OK(0);\n" +
						"\n" +
						"\treturn operator;\n" +
						"\n" +
						"ERROR:\n" +
						"\tif (NULL != input_operator) {\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t}\n" +
						"\tif (NULL != operator) {\n" +
						"\t\tif (NULL != projection) {\n" +
						"\t\t\tif (NULL != projection->super.input_operators) {\n" +
						"\t\t\t\tfree(projection->super.input_operators);\n" +
						"\t\t\t}\n" +
/*						"\t\t\tif (NULL != projection->offset) {\n" +
						"\t\t\t\tfree(projection->offset);\n" +
						"\t\t\t}\n" +*/
						"\t\t\tfree(projection);\n" +
						"\t\t}\n" +
						"\t\tfree(operator);\n" +
						"\t}\n" +
						"\treturn NULL;\n" +
						"}\n\n");
	}
}
