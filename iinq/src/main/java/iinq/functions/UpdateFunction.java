package iinq.functions;

// TODO: redo error checking
public class UpdateFunction extends IinqFunction {
	public UpdateFunction() {
		super("update",
				"void update(iinq_table_id table_id, int num_wheres, int num_update, ...);\n",
				"void update(iinq_table_id table_id, int num_wheres, int num_update, ...) {\n\n" +
				"\tva_list valist;\n" +
				"\tva_start(valist, num_update);\n\n" +
				"\tion_err_t                  error;\n" +
				"\tion_dictionary_t           dictionary;\n" +
				"\tion_dictionary_handler_t   handler;\n\n" +
				"\tdictionary.handler = &handler;\n\n" +
				"\terror              = iinq_open_source(table_id, &dictionary, &handler);\n\n" +
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
				"\tion_key_t new_key = malloc(dictionary.instance->record.key_size);\n" +
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
				"\tiinq_where_params_t *wheres = NULL;\n" +
				"\tif (num_wheres > 0)\n" +
				"\t\twheres = va_arg(valist, iinq_where_params_t*);\n" +
				"\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n" +
				"\t\tcondition_satisfied = where(table_id, &ion_record, num_wheres, wheres);\n\n" +
				"\t\tif (condition_satisfied) {\n" +
				"\t\t\terror = dictionary_insert(&dictionary_temp, ion_record.key, ion_record.value).error;\n\n" +
				"\t\t\tif (err_ok != error) {\n" +
				"\t\t\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n\t\t\t}\n" +
				"\t\t}\n" +
				"\t}\n\n" +
				"\tcursor->destroy(&cursor);\n\n" +
				"\tint i;\n\n" +
				"\tiinq_update_params_t *updates = va_arg(valist, iinq_update_params_t*);\n" +
				"\tva_end(valist);\n\n" +
				"\tion_predicate_t predicate_temp;\n" +
				"\tdictionary_build_predicate(&predicate_temp, predicate_all_records);\n\n" +
				"\tion_dict_cursor_t *cursor_temp = NULL;\n" +
				"\tdictionary_find(&dictionary_temp, &predicate_temp, &cursor_temp);\n\n" +
				"\twhile ((status = iinq_next_record(cursor_temp, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n" +
				/* Always copy the original key in case we change it */
				"\t\tion_boolean_t key_changed = boolean_false;\n" +
				"\t\tmemcpy(new_key, ion_record.key, dictionary.instance->record.key_size);\n" +
				"\t\tfor (i = 0; i < num_update; i++) {\n" +
				"\t\t\tunsigned char *value;\n" +
				"\t\t\tif (updates[i].implicit_field != 0) {\n" +
				"\t\t\t\tint new_value;\n" +
				"\t\t\t\tvalue = (char *) ion_record.value + calculateOffset(table_id, updates[i].implicit_field);\n\n" +
				"\t\t\t\tswitch (updates[i].math_operator) {\n" +
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
				"\t\t\t\tif (iinq_is_key_field(table_id, updates[i].update_field)) {\n" +
				"\t\t\t\t\t*(int *) ((char *) new_key+iinq_calculate_key_offset(table_id, updates[i].update_field)) = new_value;\n" +
				"\t\t\t\t\tkey_changed = boolean_true;\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\tvalue = (char *) ion_record.value + calculateOffset(table_id, updates[i].update_field);\n" +
				"\t\t\t\t*(int *) value = new_value;\n\t\t\t}\n" +
				"\t\t\telse {\n" +
				"\t\t\t\tvalue = (char *) ion_record.value + calculateOffset(table_id, updates[i].update_field);\n" +
				"\t\t\t\tif (getFieldType(table_id, updates[i].update_field) == iinq_int) {\n" +
				"\t\t\t\t\tif (iinq_is_key_field(table_id, updates[i].update_field)) {\n" +
				"\t\t\t\t\t\t*(int *) ((char *) new_key+iinq_calculate_key_offset(table_id, updates[i].update_field)) = NEUTRALIZE(updates[i].field_value, int);\n" +
				"\t\t\t\t\t\tkey_changed = boolean_true;\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t\t*(int *) value = NEUTRALIZE(updates[i].field_value, int);\n\t\t\t\t}\n" +
				"\t\t\t\telse {\n" +
				"\t\t\t\t\tsize_t size = calculateOffset(table_id, updates[i].update_field+1)-calculateOffset(table_id, updates[i].update_field);\n" +
				"\t\t\t\t\tif (iinq_is_key_field(table_id, updates[i].update_field)) {\n" +
				"\t\t\t\t\t\tstrncpy((char *) new_key+iinq_calculate_key_offset(table_id, updates[i].update_field), updates[i].field_value, size);\n" +
				"\t\t\t\t\t\tkey_changed = boolean_true;\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t\tstrncpy(value, updates[i].field_value, size);\n\t\t\t\t}\n\t\t\t}\n\t\t}\n\n" +
				"\t\tif (key_changed) {\n" +
				"\t\t\tion_predicate_t dup_predicate;\n" +
				"\t\t\tion_dict_cursor_t *dup_cursor = NULL;\n" +
				"\t\t\tdictionary_build_predicate(&dup_predicate, predicate_equality, new_key);\n" +
				"\t\t\tdictionary_find(&dictionary, &dup_predicate, &dup_cursor);\n" +
				"\t\t\tif (cs_end_of_results != dup_cursor->status) {\n" +
				"\t\t\t\terror = err_duplicate_key;\n" +
				"\t\t\t} else {\n" +
				"\t\t\t\tion_status_t ion_status;\n" +
				"\t\t\t\tion_status = dictionary_delete(&dictionary, ion_record.key);\n" +
				"\t\t\t\tion_status = dictionary_insert(&dictionary, new_key, ion_record.value);\n" +
				"\t\t\t\terror = err_ok;\n" +
				"\t\t\t}\n" +
				"\t\t\tdup_cursor->destroy(&dup_cursor);\n" +
				"\t\t} else {\n" +
				"\t\t\terror = dictionary_update(&dictionary, ion_record.key, ion_record.value).error;\n\n" +
				"\t\t}\n" +
				"\t\tif (err_ok != error) {\n" +
				"\t\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n" +
				"\t\t}\n\t}\n\n" +
				"\tcursor_temp->destroy(&cursor_temp);\n" +
				"\tion_delete_dictionary(&dictionary_temp, dictionary_temp.instance->id);\n" +
				"\tion_close_master_table();\n" +
				"\tfree(new_key);\n" +
				"\tfree(ion_record.key);\n" +
				"\tfree(ion_record.value);\n" +
				"}\n\n");
	}
}
