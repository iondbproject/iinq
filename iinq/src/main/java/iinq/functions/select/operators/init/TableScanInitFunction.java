package iinq.functions.select.operators.init;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class TableScanInitFunction extends OperatorInitFunction {
	public TableScanInitFunction() {
		super("iinq_table_scan_init",
				"iinq_query_operator_t *iinq_table_scan_init(iinq_table_id_t table_id, iinq_field_num_t num_fields);\n",
				"iinq_query_operator_t *iinq_table_scan_init(iinq_table_id_t table_id, iinq_field_num_t num_fields) {\n\n" +
						"\tint i;\n" +
						"\tion_err_t error;\n" +
						"\tiinq_table_scan_t *table_scan;\n" +
						"\tion_predicate_t *predicate;\n" +
						"\tion_dict_cursor_t *cursor = NULL;\n" +
						"\tion_record_t *record = NULL;\n" +
						"\tion_dictionary_t           *dictionary = NULL;\n" +
						"\tion_dictionary_handler_t   *handler = NULL;\n" +
						"\n" +
						"\tiinq_query_operator_t *operator = malloc(sizeof(iinq_query_operator_t));\n" +
						"\tif (NULL == operator) {\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\toperator->instance = malloc(sizeof(iinq_table_scan_t));\n" +
						"\tif (NULL == operator->instance) {\n" +
						"\t\tfree(operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\ttable_scan = (iinq_table_scan_t *) operator->instance;\n" +
						"\ttable_scan->super.type = iinq_table_scan_e;\n" +
						"\ttable_scan->super.num_fields = num_fields;\n" +
						"\tpredicate = &table_scan->predicate;\n" +
						"\terror = dictionary_build_predicate(predicate, predicate_all_records);\n" +
						"\n" +
						"\tif (err_ok != error) {\n" +
						"\t\tfree(operator->instance);\n" +
						"\t\tfree(operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\tdictionary = &table_scan->dictionary;\n" +
						"\thandler = &table_scan->handler;\n" +
						"\tdictionary->handler = handler;\n" +
						"\trecord = &table_scan->record;\n" +
						"\n" +
						"\terror = iinq_open_source(table_id, dictionary, handler);\n" +
						"\n" +
						"\tif (err_ok != error) {\n" +
						"\t\tfree(operator->instance);\n" +
						"\t\tfree(operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\terror = dictionary_find(dictionary, predicate, &cursor);\n" +
						"\n" +
						"\tif (err_ok != error) {\n" +
						"\t\tif (NULL != cursor) {\n" +
						"\t\t\tcursor->destroy(&cursor);\n" +
						"\t\t}\n" +
						"\t\tion_close_dictionary(&dictionary);\n" +
						"\t\tfree(operator->instance);\n" +
						"\t\tfree(operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\ttable_scan->cursor = cursor;\n" +
						"\t\n" +
						"\ttable_scan->record.key     = malloc(dictionary->instance->record.key_size);\n" +
						"\tif (NULL == table_scan->record.key) {\n" +
						"\t\tcursor->destroy(&cursor);\n" +
						"\t\tion_close_dictionary(&dictionary);\n" +
						"\t\tfree(operator->instance);\n" +
						"\t\tfree(operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\trecord->value   = malloc(dictionary->instance->record.value_size);\n" +
						"\tif (NULL == record->value) {\n" +
						"\t\tfree(record->key);\n" +
						"\t\tcursor->destroy(&cursor);\n" +
						"\t\tion_close_dictionary(&dictionary);\n" +
						"\t\tfree(operator->instance);\n" +
						"\t\tfree(operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\t\n" +
						"\ttable_scan->super.field_info = malloc(sizeof(iinq_field_info_t) * num_fields);\n" +
						"\tif (NULL == table_scan->super.field_info) {\n" +
						"\t\tfree(record->value);\n" +
						"\t\tfree(record->key);\n" +
						"\t\tcursor->destroy(&cursor);\n" +
						"\t\tion_close_dictionary(&dictionary);\n" +
						"\t\tfree(operator->instance);\n" +
						"\t\tfree(operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\ttable_scan->super.null_indicators = table_scan->record.value;\n" +
						"\n" +
						"\ttable_scan->super.fields = malloc(sizeof(ion_value_t) * num_fields);\n" +
						"\tif (NULL == table_scan->super.fields) {\n" +
						"\t\tfree(table_scan->super.field_info);\n" +
						"\t\tfree(record->value);\n" +
						"\t\tfree(record->key);\n" +
						"\t\tcursor->destroy(&cursor);\n" +
						"\t\tion_close_dictionary(&dictionary);\n" +
						"\t\tfree(operator->instance);\n" +
						"\t\tfree(operator);\n" +
						"\t\treturn NULL;\n" +
						"\t}\n" +
						"\n" +
						"\tfor (i = 0; i < num_fields; i++) {\n" +
						"\t\ttable_scan->super.field_info[i] = (iinq_field_info_t) {table_id, i+1};\n" +
						"\t\ttable_scan->super.fields[i] = (unsigned char *) record->value + iinq_calculate_offset(table_id, i + 1);\n" +
						"\t}\n" +
						"\n" +
						"\tion_close_master_table();\n" +
						"\toperator->next = iinq_table_scan_next;\n" +
						"\toperator->destroy = iinq_table_scan_destroy;\n" +
						"\toperator->status = ION_STATUS_OK(0);\n" +
						"\n" +
						"\treturn operator;\n" +
						"}\n\n");
	}
}
