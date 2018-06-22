package iinq.functions;

public class UpdateFunction extends IinqFunction {
	public UpdateFunction() {
		super("update",
				"void update(iinq_table_id tableId, int numWheres, int num_update, ...);\n",
				"void update(iinq_table_id tableId, int numWheres, int num_update, ...) {\n\n" +
				"\tva_list valist;\n" +
				"\tva_start(valist, num_update);\n\n" +
				"\tion_err_t                  error;\n" +
				"\tion_dictionary_t           dictionary;\n" +
				"\tion_dictionary_handler_t   handler;\n\n" +
				"\tdictionary.handler = &handler;\n\n" +
				"\terror              = iinq_open_source(tableId, &dictionary, &handler);\n\n" +
				"\tif (err_ok != error) {\n" +
				"\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n" +
				"\t}\n\n" +
				"\tion_predicate_t predicate;\n" +
				"\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n" +
				"\tion_dict_cursor_t *cursor = NULL;\n" +
				"\tdictionary_find(&dictionary, &predicate, &cursor);\n\n" +
				"\tion_record_t ion_record;\n" +
				"\tion_record.key     = malloc(dictionary.instance->record.key_size);\n" +
				"\tion_record.value   = malloc(dictionary.instance->record.value_size);\n\n" +
				"\tion_cursor_status_t status;\n\n" +
				"\tion_dictionary_handler_t handler_temp;\n" +
				"\tion_dictionary_t dictionary_temp;\n" +
				"\tiinq_update_handler_init(&handler_temp);\n" +
				"\tdictionary_temp.handler = &handler_temp;\n\n" +
				"\terror = ion_init_master_table();\n" +
				"\terror = ion_master_table_create_dictionary(&handler_temp, &dictionary_temp, dictionary.instance->key_type, dictionary.instance->record.key_size, dictionary.instance->record.value_size, 10);\n" +
				"\tif (err_ok != error) {\n" +
				"\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n" +
				"\t}\n\n" +
				"\tion_boolean_t condition_satisfied;\n\n" +
				"\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n" +
				"\t\tcondition_satisfied = where(tableId, &ion_record, numWheres, &valist);\n\n" +
				"\t\tif (condition_satisfied) {\n" +
				"\t\t\terror = dictionary_insert(&dictionary_temp, ion_record.key, ion_record.value).error;\n\n" +
				"\t\t\tif (err_ok != error) {\n" +
				"\t\t\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n\t\t\t}\n" +
				"\t\t}\n\t}\n\n" +
				"\tcursor->destroy(&cursor);\n\n" +
				"\tint i;\n\n" +
				"\tiinq_update_params_t updates[num_update];\n" +
				"\tfor (i = 0; i < numWheres; i++) {\n" +
				"\t\tva_arg(valist, void *);\n\t}\n\n" +
				"\tfor (i = 0; i < num_update; i++) {\n" +
				"\t\tupdates[i] = va_arg(valist, iinq_update_params_t);\n\t}\n\n" +
				"\tva_end(valist);\n\n" +
				"\tion_predicate_t predicate_temp;\n" +
				"\tdictionary_build_predicate(&predicate_temp, predicate_all_records);\n\n" +
				"\tion_dict_cursor_t *cursor_temp = NULL;\n" +
				"\tdictionary_find(&dictionary_temp, &predicate_temp, &cursor_temp);\n\n" +
				"\twhile ((status = iinq_next_record(cursor_temp, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n" +
				"\t\tfor (i = 0; i < num_update; i++) {\n" +
				"\t\t\tunsigned char *value;\n" +
				"\t\t\tif (updates[i].implicit_field != 0) {\n" +
				"\t\t\t\tint new_value;\n" +
				"\t\t\t\tvalue = ion_record.value + calculateOffset(tableId, updates[i].implicit_field);\n\n" +
				"\t\t\t\tswitch (updates[i].operator) {\n" +
				"\t\t\t\t\tcase iinq_add :\n" +
				"\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) + NEUTRALIZE(updates[i].field_value, int));\n" +
				"\t\t\t\t\t\tbreak;\n" +
				"\t\t\t\t\tcase iinq_subtract :\n" +
				"\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) - NEUTRALIZE(updates[i].field_value, int));\n" +
				"\t\t\t\t\t\tbreak;\n" +
				"\t\t\t\t\tcase iinq_multiply :\n" +
				"\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) * NEUTRALIZE(updates[i].field_value, int));\n" +
				"\t\t\t\t\t\tbreak;\n" +
				"\t\t\t\t\tcase iinq_divide :\n" +
				"\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) / NEUTRALIZE(updates[i].field_value, int));\n" +
				"\t\t\t\t\t\tbreak;\n\t\t\t\t}\n" +
				"\t\t\t\tvalue = ion_record.value + calculateOffset(tableId, updates[i].update_field);\n" +
				"\t\t\t\t*(int *) value = new_value;\n\t\t\t}\n" +
				"\t\t\telse {\n" +
				"\t\t\t\tvalue = ion_record.value + calculateOffset(tableId, updates[i].update_field);\n" +
				"\t\t\t\tif (getFieldType(tableId, updates[i].update_field) == iinq_int) {\n" +
				"\t\t\t\t\t*(int *) value = NEUTRALIZE(updates[i].field_value, int);\n\t\t\t\t}\n" +
				"\t\t\t\telse {\n" +
				"\t\t\t\t\tstrncpy(value, updates[i].field_value, calculateOffset(tableId, updates[i].update_field+1) - calculateOffset(tableId, updates[i].update_field));\n\t\t\t\t}\n\t\t\t}\n\t\t}\n\n" +
				"\t\terror = dictionary_update(&dictionary, ion_record.key, ion_record.value).error;\n\n" +
				"\t\tif (err_ok != error) {\n" +
				"\t\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n" +
				"\t\t}\n\t}\n\n" +
				"\tcursor_temp->destroy(&cursor_temp);\n" +
				"\tion_delete_dictionary(&dictionary_temp, dictionary_temp.instance->id);\n" +
				"\tion_close_master_table();\n" +
				"\tfree(ion_record.key);\n" +
				"\tfree(ion_record.value);\n" +
				"}\n\n");
	}
}
