package iinq.functions.select.operators.init;

public class ExternalSortInitFunction extends OperatorInitFunction {
	public ExternalSortInitFunction() {

		super("iinq_external_sort_init",
				"iinq_operator_t *iinq_external_sort_init(iinq_table_id table_id, iinq_result_set *table_scan, int num_orderby, iinq_order_by_field_t *order_by_fields);\n",
				"iinq_operator_t *iinq_external_sort_init(iinq_query_operator_t *input_operator, int num_orderby, iinq_order_by_field_t *order_by_fields) {\n" +
						"\tint total_orderby_size = 0;\n" +
						"\tiinq_order_part_t *orderby_order_parts = malloc(sizeof(iinq_order_part_t) * orderby_n);\n" +
						"\tif (NULL == orderby_order_parts) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\n" +
						"\tfor (i = 0; i < orderby_n; i++) {\n" +
						"\t\t//iinq_init_order_by_pointers(it, &order_by_field[i], orderby_order_parts, num_orderby);\n" +
						"\t\torderby_order_parts[i].pointer = ((unsigned char *) result_scan->record.value) + calculateOffset(table_id, order_by_fields[i].field_num);\n" +
						"\n" +
						"\t\torderby_order_parts[i].direction = order_by_fields[i].direction;\n" +
						"\t\torderby_order_parts[i].size = calculateOffset(table_id, order_by_fields[i].field_num+1) - calculateOffset(table_id, order_by_fields[i].field_num);\n" +
						"\n" +
						"\t\t// TODO: can we get rid of the order types and just use standard iinq types?\n" +
						"\t\tswitch (getFieldType(table_id, order_by_fields[i].field_num)) {\n" +
						"\t\t\tcase iinq_int:\n" +
						"\t\t\t\torderby_order_parts[i].type = IINQ_ORDERTYPE_INT;\n" +
						"\t\t\t\tbreak;\n" +
						"\n" +
						"\t\t\tcase iinq_unsigned_int:\n" +
						"\t\t\t\torderby_order_parts[i].type = IINQ_ORDERTYPE_UINT;\n" +
						"\t\t\t\tbreak;\n" +
						"\n" +
						"\t\t\tcase iinq_float:\n" +
						"\t\t\t\torderby_order_parts[i].type = IINQ_ORDERTYPE_FLOAT;\n" +
						"\t\t\t\tbreak;\n" +
						"\n" +
						"\t\t\tcase iinq_null_terminated_string:\n" +
						"\t\t\tcase iinq_char_array:\n" +
						"\t\t\tdefault:\n" +
						"\t\t\t\torderby_order_parts[i].type = IINQ_ORDERTYPE_OTHER;\n" +
						"\t\t\t\tbreak;\n" +
						"\t\t}\n" +
						"\n" +
						"\t\ttotal_orderby_size += orderby_order_parts[i].size;\n" +
						"\t}\n" +
						"\n" +
						"\tion_dictionary_t dictionary;\n" +
						"\tion_dictionary_handler_t handler;\n" +
						"\tiinq_open_source(table_id, &dictionary, &handler);\n" +
						"\tion_key_size_t key_size = dictionary.instance->record.key_size;\n" +
						"\tion_value_size_t value_size = dictionary.instance->record.value_size;\n" +
						"\n" +
						"\t//iinq_order_by_write_to_file(it, orderby_n, orderby_order_parts, location);\n" +
						"\n" +
						"\tFILE *file = fopen(\"orderby\", \"wb\");\n" +
						"\n" +
						"\tint i;\n" +
						"\tint write_page_remaining = IINQ_PAGE_SIZE;\n" +
						"\n" +
						"\t/* Filter before sorting. Use existing table scan operator*/\n" +
						"\twhile (input_operator->next(input_operator) {\n" +
						"\t\tif (write_page_remaining < (total_orderby_size + key_size + value_size)) {\n" +
						"\t\t\tchar x = 0;\n" +
						"\n" +
						"\t\t\tfor (i = 0; i < write_page_remaining; i++) {\n" +
						"\t\t\t\tif (1 != fwrite(&x, 1, 1, file)) {\n" +
						"\t\t\t\t\tbreak;\n" +
						"\t\t\t\t}\n" +
						"\t\t\t}\n" +
						"\n" +
						"\t\t\twrite_page_remaining = IINQ_PAGE_SIZE;\n" +
						"\t\t}\n" +
						"\n" +
						"\t\tfor (i = 0; i < orderby_n; i++) {\n" +
						"\t\t\tif (1 != fwrite(orderby_order_parts[i].pointer, orderby_order_parts[i].size, 1, file)) {\n" +
						"\t\t\t\tbreak;\n" +
						"\t\t\t} else {\n" +
						"\t\t\t\twrite_page_remaining -= orderby_order_parts[i].size;\n" +
						"\t\t\t}\n" +
						"\t\t}\n" +
						"\n" +
						"\t\t// TODO: why do we need to write the key?\n" +
						"\t\t/* write the key from the record into the file.*/\n" +
						"\t\tif (1 != fwrite(table_scan->record.key, key_size, 1, file)) {\n" +
						"\t\t\tbreak;\n" +
						"\t\t} else {\n" +
						"\t\t\twrite_page_remaining -= key_size;\n" +
						"\t\t}\n" +
						"\n" +
						"\t\t/* If the sort is on a table, write the value from the record into the file.\n" +
						"\t\t * If the sort is on a GROUP BY, write the value from the buffer. */\n" +
						"\t\tfor (j = 0; j < input_operator->instance.num_fields; j++) {\n" +
						"\t\t\tsize_t field_size = iinq_calculate_offset(input_operator->instance->field_info[i].table_id, input_operator->instance->field_info[i].field_num+1) - iinq_calculate_offset(input_operator->instance->field_info[i].table_id, input_operator->instance->field_info[i].field_num);\n" +
						"\t\t\tif (1 != fwrite(input_operator->fields[j], size, 1, file)) {\n" +
						"\t\t\t\tbreak;\n" +
						"\t\t\t} else {\n" +
						"\t\t\t\twrite_page_remaining -= size;\n" +
						"\t\t}\n" +
						"\t}\n" +
						"\n" +
						"\tinput_operator->destroy(&input_operator);\n" +
						"\n" +
						"\t/* All records have been written, so close file */\n" +
						"\tif (NULL != file) {\n" +
						"\t\tfclose(file);\n" +
						"\t}\n" +
						"\n" +
						"\tiinq_query_operator_t *operator = malloc(sizeof(iinq_query_operator_t));\n" +
						"\toperator->instance = malloc(sizeof(iinq_external_sort_t));\n" +
						"\n" +
						"\t// Open file in read mode for sorting\n" +
						"\tFILE *file = fopen(\"orderby\", \"rb\");\n" +
						"\n" +
						"\tiinq_sort_context_t *context = malloc(sizeof(iinq_sort_context_t));\n" +
						"\n" +
						"\tcontext->parts \t= orderby_order_parts;\n" +
						"\tcontext->n\t\t= num_orderby;\n" +
						"\n" +
						"\tion_external_sort_t *es = malloc(sizeof(ion_external_sort_t));\n" +
						"\n" +
						"\terror = ion_external_sort_init(es, file, context, iinq_sort_compare, key_size + value_size,\n" +
						"\t\t\t\t\t\t\t\t   key_size + value_size + total_orderby_size, IINQ_PAGE_SIZE, boolean_false,\n" +
						"\t\t\t\t\t\t\t\t   ION_FILE_SORT_FLASH_MINSORT);\n" +
						"\n" +
						"\tuint16_t buffer_size = ion_external_sort_bytes_of_memory_required(es, 0, boolean_false);\n" +
						"\n" +
						"\tchar *buffer = malloc(buffer_size);\n" +
						"\n" +
						"\t// recordbuf needs enough room for the sort field and the table tuple (sort field is stored twice)\n" +
						"\t// projection is done afterward TODO: why not before?\n" +
						"\tresult_set->sort.record_buf = malloc((total_orderby_size + key_size + value_size));\n" +
						"\n" +
						"\tresult_set->cursor = malloc(sizeof(ion_external_sort_cursor_t));\n" +
						"\n" +
						"\terror = ion_external_sort_init_cursor(es, result_set->cursor, buffer, buffer_size);\n" +
						"\n" +
						"\treturn result_set;"

		);

	}
}
