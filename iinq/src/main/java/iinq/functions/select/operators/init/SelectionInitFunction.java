package iinq.functions.select.operators.init;

public class SelectionInitFunction extends OperatorInitFunction {
	public SelectionInitFunction() {
		super("iinq_selection_init",
				"iinq_query_operator_t *iinq_selection_init(iinq_query_operator_t *input_operator, unsigned int num_conditions, iinq_where_params_t *conditions);\n",
				"iinq_query_operator_t *iinq_selection_init(iinq_query_operator_t *input_operator, unsigned int num_conditions, iinq_where_params_t *conditions) {\n" +
						"\tif (NULL == input_operator) {\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\t\n" +
						"\tiinq_query_operator_t *operator = malloc(sizeof(iinq_query_operator_t));\n" +
						"\tif (NULL == operator) {\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\t\n" +
						"\toperator->instance = malloc(sizeof(iinq_selection_t));\n" +
						"\tif (NULL == operator->instance) {\n" +
						"\t\tfree(operator);\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\t\n" +
						"\tiinq_selection_t *selection = (iinq_selection_t *) operator->instance;\n" +
						"\t\n" +
						"\tselection->super.input_operators = malloc(sizeof(iinq_query_operator_t *));\n" +
						"\tif (NULL == selection->super.input_operators) {\n" +
						"\t\tfree(selection);\n" +
						"\t\tfree(operator);\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\tselection->super.type = iinq_selection_e;\n" +
						"\tselection->super.num_input_operators = 1;\n" +
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
						"\t\tfree(selection->super.input_operators);\n" +
						"\t\tfree(selection);\n" +
						"\t\tfree(operator);\n" +
						"\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\t\n" +
						"\tint i;\n" +
						"\tfor (i = 0; i < num_conditions; i++) {\n" +
						"\t\tiinq_table_id_t table_id = input_operator->instance->field_info[conditions[i].where_field-1].table_id;\n" +
						"\t\tiinq_field_num_t field_num = input_operator->instance->field_info[conditions[i].where_field-1].field_num;\n" +
						"\t\tselection->conditions[i].where_field = conditions[i].where_field;\n" +
						"\t\tsize_t value_size = iinq_calculate_offset(table_id, field_num + 1)-iinq_calculate_offset(table_id, field_num);\n" +
						"\t\tselection->conditions[i].field_value = malloc(value_size);\n" +
						"\t\tif (NULL == selection->conditions[i].field_value) {\n" +
						"\t\t\tint j;\n" +
						"\t\t\tfor (j = 0; j < i; j++) {\n" +
						"\t\t\t\tfree(selection->conditions[i].field_value);\n" +
						"\t\t\t}\n" +
						"\t\t\tfree(selection->conditions);\n" +
						"\t\t\tfree(selection->super.input_operators);\n" +
						"\t\t\tfree(selection);\n" +
						"\t\t\tfree(operator);\n" +
						"\t\t\tinput_operator->destroy(&input_operator);\n" +
						"\t\t\treturn NULL;\n" +
						"\t\t}\n" +
						"\t\tselection->conditions[i].bool_operator = conditions[i].bool_operator;\n" +
						"\t\t\n" +
						"\t\tswitch (iinq_get_field_type(table_id, field_num)) {\n" +
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
						"}\n\n");
	}
}
