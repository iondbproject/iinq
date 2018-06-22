package iinq.functions;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;


public class DeleteFunction extends IinqFunction {
	public DeleteFunction() {
		super("delete",
				"void delete_record(iinq_table_id tableId, int numWheres, ...);\n",
				"void delete_record(iinq_table_id tableId, int numWheres, ...) {\n\n" +
						"\tva_list valist;\n" +
						"\tva_start(valist, numWheres);\n\n" +
						"\tion_err_t                  error;\n" +
						"\tion_dictionary_t           dictionary;\n" +
						"\tion_dictionary_handler_t   handler;\n\n" +
						"\tdictionary.handler = &handler;\n\n" +
						"\terror              = iinq_open_source(tableId, &dictionary, &handler);\n\n" +
						CommonCode.error_check() +
						"\tion_predicate_t predicate;\n" +
						"\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n" +
						"\tion_dict_cursor_t *cursor = NULL;\n" +
						"\tdictionary_find(&dictionary, &predicate, &cursor);\n\n" +
						"\tion_record_t ion_record;\n" +
						"\tion_record.key     = malloc(dictionary.instance->record.key_size);\n" +
						"\tion_record.value   = malloc(dictionary.instance->record.value_size);\n\n" +
						"\tion_cursor_status_t status;\n\n" +
						"\tion_dictionary_t           dictionary_temp;\n" +
						"\tion_dictionary_handler_t   handler_temp;\n\n" +
						"\tdictionary_temp.handler = &handler_temp;\n\n" +
						"\terror = ion_init_master_table();\n" +
						CommonCode.error_check() +
						"\tiinq_delete_handler_init(&handler_temp);\n" +
						"\tdictionary_temp.handler = &handler_temp;\n\n" +
						"\terror = ion_master_table_create_dictionary(&handler_temp, &dictionary_temp, dictionary.instance->key_type, dictionary.instance->record.key_size, 1, 10);\n" +
						CommonCode.error_check() +
						CommonCode.error_check() +
						"\tion_boolean_t condition_satisfied;\n\n" +
						"\tiinq_where_params_t *wheres = NULL;\n" +
						"\tif (numWheres > 0)\n" +
						"\t\twheres = va_arg(valist, iinq_where_params_t*);\n" +
						"\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n" +
						"\t\tcondition_satisfied = where(tableId, &ion_record, numWheres, wheres);\n\n" +
						"\t\tif (condition_satisfied) {\n" +
						"\t\t\terror = dictionary_insert(&dictionary_temp, ion_record.key, IONIZE(0, char)).error;\n\n" +
						CommonCode.error_check(2) +
						"\t\t}\n" +
						"\t}\n\n" +
						"\tva_end(valist);\n" +
						"\tcursor->destroy(&cursor);\n\n" +
						"\tion_predicate_t predicate_temp;\n" +
						"\tdictionary_build_predicate(&predicate_temp, predicate_all_records);\n\n" +
						"\tion_dict_cursor_t *cursor_temp = NULL;\n" +
						"\tdictionary_find(&dictionary_temp, &predicate_temp, &cursor_temp);\n\n" +
						"\twhile ((status = iinq_next_record(cursor_temp, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n" +
						"\t\terror = dictionary_delete(&dictionary, ion_record.key).error;\n\n" +
						CommonCode.error_check(1) +
						"\t}\n\n" +
						"\tcursor_temp->destroy(&cursor_temp);\n" +
						"\terror = ion_delete_dictionary(&dictionary_temp, dictionary_temp.instance->id);\n\n" +
						CommonCode.error_check() +
						"\tion_close_master_table();\n" +
						"\tfree(ion_record.key);\n" +
						"\tfree(ion_record.value);\n" +
						"}\n\n");
	}
}
