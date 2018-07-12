package iinq.functions.select.operators.init;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class TableScanInitFunction extends OperatorInitFunction {
	public TableScanInitFunction() {
		super("iinq_table_scan_init",
				"iinq_result_set *iinq_table_scan_init(iinq_table_id table_id, int num_wheres, iinq_field_num_t num_fields, ...);\n",
				"iinq_result_set *iinq_table_scan_init(iinq_table_id table_id, int num_wheres, iinq_field_num_t num_fields, ...) {\n\n" +
						"\tint i;\n" +
						"\tva_list valist;\n" +
						"\tiinq_where_params_t* where_list = NULL;\n" +
						"\tva_start(valist, num_fields);\n" +
						"\tiinq_result_set *result_set = malloc(sizeof(iinq_result_set));\n" +
						// TODO: should we have the user allocate the result set for us?
						"\tif (NULL == result_set)\n" +
						"\t\treturn NULL;\n" +
						"\tion_predicate_t *predicate = &result_set->dictionary_ref.predicate;\n" +
						"\tresult_set->status.error = dictionary_build_predicate(predicate, predicate_all_records);\n\n" +
						"\tif (err_ok != result_set->status.error) {\n" +
						"\t\treturn result_set;\n" +
						"\t}\n\n" +
						"\tion_dictionary_t           *dictionary = &result_set->dictionary_ref.dictionary;\n" +
						"\tion_dictionary_handler_t   *handler = &result_set->dictionary_ref.handler;\n\n" +
						"\tdictionary->handler = handler;\n" +
						"\tresult_set->status.error              = iinq_open_source(table_id, dictionary, handler);\n\n" +
						"\tif (err_ok != result_set->status.error) {\n" +
						"\t\treturn result_set;\n" +
						"\t}\n\n" +
						"\tion_dict_cursor_t *cursor = NULL;\n" +
						"\tresult_set->status.error = dictionary_find(dictionary, predicate, &cursor);\n\n" +
						"\tif (err_ok != result_set->status.error) {\n" +
						"\t\treturn result_set;\n" +
						"\t}\n\n" +
						"\tresult_set->record.key     = malloc(dictionary->instance->record.key_size);\n" +
						"\tif (NULL == result_set->record.key) {\n" +
						"\t\tresult_set->status.error = err_out_of_memory;\n" +
						"\t\treturn result_set;\n" +
						"\t}\n" +
						"\tresult_set->record.value   = malloc(dictionary->instance->record.value_size);\n" +
						"\tif (NULL == result_set->record.value) {\n" +
						"\t\tresult_set->status.error = err_out_of_memory;\n" +
						"\t\tfree(result_set->record.key);\n" +
						"\t\treturn result_set;\n" +
						"\t}\n" +
						"\tresult_set->dictionary_ref.cursor = cursor;\n" +
						"\tresult_set->table_id = table_id;\n" +
						"\tif (num_wheres > 0) {\n" +
						"\t\twhere_list = va_arg(valist, iinq_where_params_t*);\n" +
						"\t\tresult_set->wheres = where_list;\n" +
						"\t} else {\n" +
						"\t\tresult_set->wheres = NULL;\n" +
						"\t}\n" +
						"\tresult_set->num_wheres = num_wheres;\n" +
						"\tiinq_field_num_t *fields = va_arg(valist, iinq_field_num_t*);\n" +
						"\tresult_set->num_fields = num_fields;\n" +
						"\tresult_set->offset = malloc(sizeof(unsigned int) * num_fields);\n\n" +
						"\tif (NULL == result_set->offset) {\n" +
						"\t\tfree(result_set->record.value);\n" +
						"\t\tfree(result_set->record.key);\n" +
						"\t\tresult_set->status.error = err_out_of_memory;\n" +
						"\t\treturn result_set;\n" +
						"\t}\n" +
						"\tresult_set->fields = fields;\n\n" +
						"\tfor (i = 0; i < num_fields; i++) {\n" +
						"\t\tresult_set->offset[i] = calculateOffset(table_id, fields[i]);\n" +
						"\t}\n" +
						"\tva_end(valist);\n\n" +
						// TODO: determine if we need to materialize the query
/*						"\tion_dictionary_handler_t   handler_temp;\n" +
						"\tion_dictionary_t           dictionary_temp;\n\n" +
						"\tselect->status.error = ion_init_master_table();\n" +
						"\tiinq_select_handler_init(&handler_temp);\n" +
						"\tdictionary_temp.handler = &handler_temp;\n\n" */
						/*"\tselect->status.error = ion_master_table_create_dictionary(&handler_temp, &dictionary_temp, key_type_numeric_unsigned, sizeof(unsigned int), project_size, 10);\n" +
						CommonCode.errorCheckResultSet("select", true) +*/
						"\tion_close_master_table();\n" +
						"\tresult_set->next = iinq_table_scan_next;\n" +
						"\tresult_set->destroy = iinq_destroy_table_scan;\n" +
						"\tresult_set->dictionary_ref.temp_dictionary = boolean_false;\n\n" +
/*						"\twhile ((status = select->next(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n" +
						"\t\tcondition_satisfied = where(table_id, &ion_record, numWheres, where_list);\n\n" +
						"\t\tif (condition_satisfied) {\n" +
						"\t\t\tion_value_t fieldlist = malloc(project_size);\n" +
						"\t\t\tion_value_t data = fieldlist;\n\n" +
						"\t\t\tfor (i = 0; i < num_fields; i++) {\n\n" +
						"\t\t\t\tif (getFieldType(table_id, fields[i]) == iinq_int) {\n" +
						"\t\t\t\t\t*(int *) data = NEUTRALIZE(ion_record.value + calculateOffset(table_id, fields[i]), int);\n" +
						"\t\t\t\t\tdata += sizeof(int);\n" +
						"\t\t\t\t}\n" +
						"\t\t\t\telse {\n" +
						"\t\t\t\t\tstrncpy(data, ion_record.value + calculateOffset(table_id, fields[i]), calculateOffset(table_id, fields[i]+1) - calculateOffset(table_id, fields[i]));\n" +
						"\t\t\t\t\tdata += calculateOffset(table_id, fields[i]+1) - calculateOffset(table_id, fields[i]);\n" +
						"\t\t\t\t}\n\t\t\t}\n\n" +
						"\t\t\tselect->status.error = dictionary_insert(&dictionary_temp, IONIZE(count, unsigned int), fieldlist).error;\n\n" +
						CommonCode.errorCheckResultSet("select", true) +
						"\t\t\tcount++;\n\t\t\tfree(fieldlist);\n\t\t}\n\t}\n\n" +
						"\tcursor->destroy(&cursor);\n\n" +
						"\tselect->status.error = ion_close_dictionary(&dictionary);\n\n" +
						CommonCode.errorCheckResultSet("select", true) +
						"\tselect->status.error = ion_close_dictionary(&dictionary_temp);\n\n" +
						CommonCode.errorCheckResultSet("select",true) +
						"\tselect->num_recs = count;\n" +
						"\tselect->status.count = -1;\n" +
						"\tselect->table_id = table_id;\n" +
						"\tselect->value = malloc(project_size);\n" +*/
						"\treturn result_set;\n" +
						"}\n\n");
	}
}
