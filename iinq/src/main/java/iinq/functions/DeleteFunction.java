package iinq.functions;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;


public class DeleteFunction extends IinqFunction {
	public DeleteFunction() {
		super("delete",
				"void delete_record(iinq_table_id table_id, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_fields, ...);\n",
				"void delete_record(iinq_table_id table_id, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_fields, ...) {\n\n" +
						"\tva_list valist;\n" +
						"\tva_start(valist, num_fields);\n\n" +
						"\tion_err_t                  error;\n" +
						"\tion_dictionary_t           dictionary;\n" +
						"\tion_dictionary_handler_t   handler;\n\n" +
						"\tdictionary.handler = &handler;\n\n" +
						"\terror              = iinq_open_source(table_id, &dictionary, &handler);\n\n" +
						CommonCode.error_check() +
						"\tion_predicate_t predicate;\n" +
						"\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n" +
						"\tion_dict_cursor_t *cursor = NULL;\n" +
						"\tdictionary_find(&dictionary, &predicate, &cursor);\n\n" +
						"\tion_record_t ion_record;\n" +
						"\tion_record.key     = malloc(key_size);\n" +
						"\tion_record.value   = malloc(value_size);\n\n" +
						"\tion_cursor_status_t status;\n\n" +
						"\terror = iinq_create_source(255, key_type, (ion_key_size_t) key_size, (ion_value_size_t) sizeof(int));\n\n" +
						CommonCode.error_check() +
						"\tion_dictionary_t           dictionary_temp;\n" +
						"\tion_dictionary_handler_t   handler_temp;\n\n" +
						"\tdictionary_temp.handler = &handler_temp;\n\n" +
						"\terror              = iinq_open_source(255, &dictionary_temp, &handler_temp);\n\n" +
						CommonCode.error_check() +
						"\tion_boolean_t condition_satisfied;\n\n" +
						"\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n" +
						"\t\tcondition_satisfied = where(table_id, &ion_record, num_fields, &valist);\n\n" +
						"\t\tif (condition_satisfied) {\n" +
						"\t\t\terror = dictionary_insert(&dictionary_temp, ion_record.key, IONIZE(0, int)).error;\n\n" +
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
						"\terror = ion_close_dictionary(&dictionary);\n\n" +
						CommonCode.error_check() +
						"\tiinq_drop(255);\n" +
						"\tfree(ion_record.key);\n" +
						"\tfree(ion_record.value);\n" +
						"}\n\n");
	}
}
