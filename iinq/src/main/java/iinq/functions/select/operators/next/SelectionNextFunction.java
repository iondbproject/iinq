package iinq.functions.select.operators.next;

public class SelectionNextFunction extends OperatorNextFunction {
	public SelectionNextFunction() {
		super("iinq_selection_next",
				"ion_boolean_t iinq_selection_next(iinq_query_operator_t *operator);\n",
				"ion_boolean_t iinq_selection_next(iinq_query_operator_t *operator) {" +
						"\tint i;\n" +
						"\tiinq_selection_t *selection = (iinq_selection_t *) operator->instance;\n" +
						"\tion_boolean_t selection_result = boolean_false;\n" +
						"\twhile (!selection_result && selection->super.input_operators[0]->next(selection->super.input_operators[0])) {\n" +
						"\t\tion_value_t curr_value;\n" +
						"\t\tiinq_where_params_t *curr_condition;\n" +
						"\n" +
						"\t\tfor (i = 0; i < selection->num_conditions; i++) {\n" +
						"\t\t\tcurr_value = selection->super.fields[selection->conditions[i].where_field-1];" +
						"\t\t\tcurr_condition = &selection->conditions[i];\n" +
						"\t\t\tiinq_field_t\tfield_type\t\t\t\t= iinq_get_field_type(curr_condition->field_info.table_id, curr_condition->field_info.field_num);\n" +
						"\n" +
						"\t\t\tswitch (field_type) {\n" +
						"\t\t\t\tcase iinq_int: {\n" +
						"\t\t\t\t\tint comp_value = NEUTRALIZE(curr_condition->field_value, int);\n" +
						"\t\t\t\t\tswitch (curr_condition->bool_operator) {\n" +
						"\t\t\t\t\t\tcase iinq_equal:\n" +
						"\t\t\t\t\t\t\tif (NEUTRALIZE(curr_value, int) != comp_value) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_not_equal:\n" +
						"\t\t\t\t\t\t\tif (NEUTRALIZE(curr_value, int) == comp_value) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_less_than:\n" +
						"\t\t\t\t\t\t\tif (NEUTRALIZE(curr_value, int) >= comp_value) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_less_than_equal_to:\n" +
						"\t\t\t\t\t\t\tif (NEUTRALIZE(curr_value, int) > comp_value) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_greater_than_equal_to:\n" +
						"\t\t\t\t\t\t\tif (NEUTRALIZE(curr_value, int) < comp_value) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t\tbreak;\n" +
						"\t\t\t\t}\n" +
						"\t\t\t\tcase iinq_null_terminated_string: {\n" +
						"\t\t\t\t\tchar *comp_value = (char *) curr_condition->field_value;\n" +
						"\t\t\t\t\tsize_t value_size = iinq_calculate_offset(curr_condition->field_info.table_id, curr_condition->field_info.field_num + 1) - iinq_calculate_offset(curr_condition->field_info.table_id, curr_condition->field_info.field_num);\n" +
						"\t\t\t\t\tswitch (curr_condition->bool_operator) {\n" +
						"\t\t\t\t\t\tcase iinq_equal:\n" +
						"\t\t\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) != 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_not_equal:\n" +
						"\t\t\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) == 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_less_than:\n" +
						"\t\t\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) >= 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_less_than_equal_to:\n" +
						"\t\t\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) > 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_greater_than_equal_to:\n" +
						"\t\t\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) < 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\t}\n" +
						"\t\t\t\t\tbreak;\n" +
						"\t\t\t\t}\n" +
						"\t\t\t\tcase iinq_char_array: {\n" +
						"\t\t\t\t\tunsigned char *comp_value = (unsigned char *) curr_condition->field_value;\n" +
						"\t\t\t\t\tsize_t value_size = iinq_calculate_offset(curr_condition->field_info.table_id, curr_condition->field_info.field_num + 1) - iinq_calculate_offset(curr_condition->field_info.table_id, curr_condition->field_info.field_num);\n" +
						"\t\t\t\t\tswitch (curr_condition->bool_operator) {\n" +
						"\t\t\t\t\t\tcase iinq_equal:\n" +
						"\t\t\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) != 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_not_equal:\n" +
						"\t\t\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) == 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_less_than:\n" +
						"\t\t\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) >= 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_less_than_equal_to:\n" +
						"\t\t\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) > 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t\tcase iinq_greater_than_equal_to:\n" +
						"\t\t\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) < 0) {\n" +
						"\t\t\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\t\t}\n" +
						"\t\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t\tbreak;\n" +
						"\t\t\t\t}\n" +
						"\t\t\t}\n" +
						"\t\t}" +
						"\t}\n" +
						"\n" +
						"\treturn selection_result;\n" +
						"}\n\n");
	}
}
