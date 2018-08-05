package iinq.functions.select.operators.next;

public class SelectionNextFunction extends OperatorNextFunction {
	public SelectionNextFunction() {
		super("iinq_selection_next",
				"ion_boolean_t iinq_selection_next(iinq_query_operator_t *query_operator);\n",
				"ion_boolean_t iinq_selection_next(iinq_query_operator_t *query_operator) {\n" +
						"\tint i;\n" +
						"\tion_boolean_t selection_result;\n" +
						"\tiinq_selection_t *selection = (iinq_selection_t *) query_operator->instance;\n" +
						"\tdo {\n" +
						"\t\tif (!selection->super.input_operators[0]->next(selection->super.input_operators[0])) {\n" +
						"\t\t\treturn boolean_false;\n" +
						"\t\t}\n" +
						"\t\tion_value_t curr_value;\n" +
						"\t\tselection_result = boolean_true;\n" +
						"\t\tiinq_where_params_t *curr_condition;\n" +
						"\n" +
						"\t\tfor (i = 0; i < selection->num_conditions; i++) {\n" +
						"\t\t\tcurr_condition = &selection->conditions[i];\n" +
						"\t\t\tiinq_table_id_t table_id = query_operator->instance->input_operators[0]->instance->field_info[curr_condition->where_field-1].table_id;\n" +
						"\t\t\tiinq_field_num_t field_num = query_operator->instance->input_operators[0]->instance->field_info[curr_condition->where_field-1].field_num;\n" +
						"\t\t\tcurr_value = selection->super.fields[curr_condition->where_field-1];\n" +
						"\t\t\tiinq_field_t\tfield_type\t\t\t\t= iinq_get_field_type(table_id, field_num);\n" +
						"\n" +
						"\t\t\tif (field_type == iinq_int) {\n" +
						"\t\t\t\tint comp_value = NEUTRALIZE(curr_condition->field_value, int);\n" +
						"\t\t\t\tif (curr_condition->bool_operator == iinq_equal) {\n" +
						"\t\t\t\t\tif (NEUTRALIZE(curr_value, int) != comp_value) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_not_equal) {\n" +
						"\t\t\t\t\tif (NEUTRALIZE(curr_value, int) == comp_value) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_less_than) {\n" +
						"\t\t\t\t\tif (NEUTRALIZE(curr_value, int) >= comp_value) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_greater_than) {\n" +
						"\t\t\t\t\tif (NEUTRALIZE(curr_value, int) <= comp_value) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_less_than_equal_to) {\n" +
						"\t\t\t\t\tif (NEUTRALIZE(curr_value, int) > comp_value) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_greater_than_equal_to) {\n" +
						"\t\t\t\t\tif (NEUTRALIZE(curr_value, int) < comp_value) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t}\n" +
						"\t\t\t} else if (field_type == iinq_null_terminated_string) {\n" +
						"\t\t\t\tchar *comp_value = (char *) curr_condition->field_value;\n" +
						"\t\t\t\tsize_t value_size = iinq_calculate_offset(table_id, field_num + 1) - iinq_calculate_offset(table_id, field_num);\n" +
						"\t\t\t\tif (curr_condition->bool_operator == iinq_equal) {\n" +
						"\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) != 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_not_equal) {\n" +
						"\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) == 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_less_than) {\n" +
						"\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) >= 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_greater_than) {\n" +
						"\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) <= 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_less_than_equal_to) {\n" +
						"\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) > 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_greater_than_equal_to) {\n" +
						"\t\t\t\t\tif (strncmp((char *) curr_value, comp_value, value_size) < 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t}\n" +
						"\t\t\t} else if (field_type == iinq_char_array) {\n" +
						"\t\t\t\tunsigned char *comp_value = (char *) curr_condition->field_value;\n" +
						"\t\t\t\tsize_t value_size = iinq_calculate_offset(table_id, field_num + 1) - iinq_calculate_offset(table_id, field_num);\n" +
						"\t\t\t\tif (curr_condition->bool_operator == iinq_equal) {\n" +
						"\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) != 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_not_equal) {\n" +
						"\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) == 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_less_than) {\n" +
						"\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) >= 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_greater_than) {\n" +
						"\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) <= 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_less_than_equal_to) {\n" +
						"\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) > 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t} else if (curr_condition->bool_operator == iinq_greater_than_equal_to) {\n" +
						"\t\t\t\t\tif (memcmp((char *) curr_value, comp_value, value_size) < 0) {\n" +
						"\t\t\t\t\t\tselection_result = boolean_false;\n" +
						"\t\t\t\t\t\tbreak;\n" +
						"\t\t\t\t\t}\n" +
						"\t\t\t\t}\n" +
						"\t\t\t}\n" +
						"\t\t}\n" +
						"\t} while (!selection_result);\n" +
						"\n" +
						"\tif (selection_result)\n" +
						"\t\tquery_operator->status.count++;\n" +
						"\n" +
						"\treturn selection_result;\n" +
						"}\n\n");
	}
}
