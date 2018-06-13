package iinq.functions.SelectFunctions;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class SelectFunction extends IinqFunction {
	public SelectFunction() {
		super("iinq_select",
				"iinq_result_set iinq_select(iinq_table_id table_id, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_fields, ...);\n",
				"iinq_result_set iinq_select(iinq_table_id table_id, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_fields, ...) {\n\n" +
						"\tint i;\n" +
						"\tva_list valist, where_list;\n" +
						"\tva_start(valist, num_fields);\n" +
						"\tva_copy(where_list, valist);\n\n" +
						"\tion_err_t                  error;\n" +
						"\tion_dictionary_t           dictionary;\n" +
						"\tion_dictionary_handler_t   handler;\n\n" +
						"\tdictionary.handler = &handler;\n\n" +
						"\tiinq_result_set select = {0};\n" +
						"\tselect.status.error              = iinq_open_source(table_id, &dictionary, &handler);\n\n" +
						CommonCode.errorCheckResultSet("select") +
						"\tion_predicate_t predicate;\n" +
						"\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n" +
						"\tion_dict_cursor_t *cursor = NULL;\n" +
						"\tdictionary_find(&dictionary, &predicate, &cursor);\n\n" +
						"\tion_record_t ion_record;\n" +
						"\tion_record.key     = malloc(key_size);\n" +
						"\tion_record.value   = malloc(value_size);\n\n" +
						"\tion_cursor_status_t status;\n\n" +
						"\tint count = 0;\n" +
						"\tion_boolean_t condition_satisfied;\n\n" +
						"\tint fields[num_fields];\n\n" +
						"\tfor (i = 0; i < num_wheres; i++) {\n" +
						"\t\tva_arg(valist, void *);\n\t}\n\n" +
						"\tselect.num_fields = malloc(sizeof(int));\n" +
						"\t*(int *) select.num_fields = num_fields;\n" +
						"\tselect.fields = malloc(sizeof(int) * num_fields);\n" +
						"\tunsigned char *field_list = select.fields;\n" +
						"\tselect.num_recs = malloc(sizeof(int));\n\n" +
						"\tfor (i = 0; i < num_fields; i++) {\n" +
						"\t\tfields[i] = va_arg(valist, int);\n\n" +
						"\t\t*(int *) field_list = fields[i];\n\n" +
						"\t\tif (i < num_fields-1) {\n" +
						"\t\t\tfield_list += sizeof(int);\n" +
						"\t\t}\n\t}\n\n" +
						"\tva_end(valist);\n\n" +
						"\tion_dictionary_handler_t   handler_temp;\n" +
						"\tion_dictionary_t           dictionary_temp;\n\n" +
						"\tselect.status.error = iinq_create_source(255, key_type, (ion_key_size_t) key_size, (ion_value_size_t) value_size);\n\n" +
						CommonCode.errorCheckResultSet("select") +
						"\tdictionary_temp.handler = &handler_temp;\n\n" +
						"\tselect.status.error = iinq_open_source(255, &dictionary_temp, &handler_temp);\n\n" +
						CommonCode.errorCheckResultSet("select") +
						"\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n" +
						"\t\tcondition_satisfied = where(table_id, &ion_record, num_wheres, &where_list);\n\n" +
						"\t\tif (!condition_satisfied || num_wheres == 0) {\n" +
						"\t\t\tunsigned char *fieldlist = malloc(value_size);\n" +
						"\t\t\tunsigned char *data = fieldlist;\n\n" +
						"\t\t\tfor (i = 0; i < num_fields; i++) {\n\n" +
						"\t\t\t\tif (getFieldType(table_id, fields[i]) == iinq_int) {\n" +
						"\t\t\t\t\t*(int *) data = NEUTRALIZE(ion_record.value + calculateOffset(table_id, fields[i] - 1), int);\n" +
						"\t\t\t\t\tdata += sizeof(int);\n" +
						"\t\t\t\t}\n" +
						"\t\t\t\telse {\n" +
						"\t\t\t\t\tmemcpy(data, ion_record.value + calculateOffset(table_id, fields[i] - 1), calculateOffset(table_id, fields[i]) - calculateOffset(table_id, fields[i]-1));\n" +
						"\t\t\t\t\tdata += calculateOffset(table_id, fields[i]) - calculateOffset(table_id, fields[i]-1);\n" +
						"\t\t\t\t}\n\t\t\t}\n\n" +
						"\t\t\tselect.status.error = dictionary_insert(&dictionary_temp, IONIZE(count, int), fieldlist).error;\n\n" +
						CommonCode.errorCheckResultSet("select") +
						"\t\t\tcount++;\n\t\t\tfree(fieldlist);\n\t\t}\n\t}\n\n" +
						"\tcursor->destroy(&cursor);\n\n" +
						"\tselect.status.error = ion_close_dictionary(&dictionary);\n\n" +
						CommonCode.errorCheckResultSet("select") +
						"\tselect.status.error = ion_close_dictionary(&dictionary_temp);\n\n" +
						CommonCode.errorCheckResultSet("select") +
						"\t*(int *) select.num_recs = count;\n" +
						"\tselect.table_id = malloc(sizeof(int));\n" +
						"\t*(iinq_table_id *) select.table_id = table_id;\n" +
						"\tselect.value = malloc(value_size);\n" +
						"\tselect.count = malloc(sizeof(int));\n" +
						"\t*(int *) select.count = -1;\n\n" +
						"\tfree(ion_record.key);\n" +
						"\tfree(ion_record.value);\n\n" +
						"\treturn select;\n" +
						"}\n\n");
	}
}
