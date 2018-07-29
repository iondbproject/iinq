package iinq.functions.select.operators.init;

public class SelectionInitFunction extends OperatorInitFunction {
	public SelectionInitFunction() {
		super("iinq_selection_init",
				"iinq_query_operator_t *iinq_selection_init(iinq_query_operator_t *input_operator, unsigned int num_conditions, iinq_where_params_t *conditions);\n",
				"iinq_query_operator_t *iinq_selection_init(iinq_query_operator_t *input_operator, unsigned int num_conditions, iinq_where_params_t *conditions) {\n" +
						"\tiinq_query_operator_t *operator = malloc(sizeof(iinq_query_operator_t));\n" +
						"\tif (NULL == operator) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\t\n" +
						"\toperator->instance = malloc(sizeof(iinq_selection_t));\n" +
						"\tif (NULL == operator->instance) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\t\n" +
						"\tiinq_selection_t *selection = (iinq_selection_t *) operator->instance;\n" +
						"\t\n" +
						"\tif (NULL == input_operator) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\t\n" +
						"\tselection->super.type = iinq_selection_e;\n" +
						"\tselection->super.num_input_operators = 1;\n" +
						"\tselection->conditions = NULL;\n" +
						"\tselection->super.input_operators = malloc(sizeof(iinq_query_operator_t *));\n" +
						"\tif (NULL == selection->super.input_operators) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\t\n" +
						"\tselection->super.input_operators[0] = input_operator;\n" +
						"\tselection->super.num_fields = input_operator->instance->num_fields;\n" +
						"\tselection->super.null_indicators = input_operator->instance->null_indicators;\n" +
						"\tselection->super.field_info = input_operator->instance->field_info;\n" +
						"\tselection->super.fields = input_operator->instance->fields;\n" +
						"\t\n" +
						"\tselection->num_conditions = num_conditions;\n" +
						"\tselection->conditions = malloc(sizeof(iinq_where_params_t)*num_conditions);\n" +
						"\tif (NULL == selection->conditions) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\t\n" +
						"\tint i;\n" +
						"\tfor (i = 0; i < num_conditions; i++) {\n" +
/*						"\t\tselection->conditions[i].field_info = malloc(sizeof(iinq_field_info_t));\n" +
						"\t\tif (NULL == selection->conditions[i].field_info) {\n" +
						"\t\t\tgoto ERROR;\n" +
						"\t\t}\n" +*/
						"\t\tselection->conditions[i].where_field = conditions[i].where_field;\n" +
						"\t\tselection->conditions[i].field_info.field_num = conditions[i].field_info.field_num;\n" +
						"\t\tsize_t value_size = iinq_calculate_offset(input_operator->instance->field_info[i].table_id, input_operator->instance->field_info[i].field_num+1)-iinq_calculate_offset(input_operator->instance->field_info[i].table_id, input_operator->instance->field_info[i].field_num);\n" +
						"\t\tselection->conditions[i].field_value = malloc(value_size);\n" +
						"\t\tif (NULL == selection->conditions[i].field_value) {\n" +
						"\t\t\tgoto ERROR;\n" +
						"\t\t}\n" +
						"\t\tselection->conditions[i].bool_operator = conditions[i].bool_operator;\n" +
						"\t\t\n" +
						"\t\tswitch (iinq_get_field_type(input_operator->instance->field_info[i].table_id, input_operator->instance->field_info[i].field_num)) {\n" +
						"\t\t\tcase iinq_int:\n" +
						"\t\t\t\t* (int *) selection->conditions[i].field_value = NEUTRALIZE(conditions[i].field_value, int);\n" +
						"\t\t\t\tbreak;\n" +
						"\t\t\tcase iinq_null_terminated_string:\n" +
						"\t\t\t\tstrncpy((char *) selection->conditions[i].field_value, (char *) conditions[i].field_value, value_size);\n" +
						"\t\t\t\tbreak;\n" +
						"\t\t\tdefault:\n" +
						"\t\t\t\tmemcpy(selection->conditions[i].field_value, conditions[i].field_value, value_size);\n" +
						"\t\t\t\tbreak;\n" +
						"\t\t}\t\t\t\n" +
						"\t\t\t\n" +
						"\t}\n" +
						"\t\n" +
						"\toperator->next = iinq_selection_next;\n" +
						"\toperator->destroy = iinq_selection_destroy;\n" +
						"\t\n" +
						"\treturn operator;\n" +
						"\t\n" +
						"\tERROR:\n" +
						"\t;\n" +
						"}\n\n");
	}
}
