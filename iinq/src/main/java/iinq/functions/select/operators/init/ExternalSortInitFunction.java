package iinq.functions.select.operators.init;

public class ExternalSortInitFunction extends OperatorInitFunction {
	public ExternalSortInitFunction() {

		super("iinq_external_sort_init",
				"iinq_query_operator_t *iinq_external_sort_init(iinq_query_operator_t *input_operator, int num_orderby, iinq_order_by_field_t *order_by_fields);\n",
				"iinq_query_operator_t *iinq_external_sort_init(iinq_query_operator_t *input_operator, int num_orderby, iinq_order_by_field_t *order_by_fields) {\n" +
						"\tint total_orderby_size = 0;\n" +
						"\tiinq_field_num_t num_fields = input_operator->instance->num_fields;\n" +
						"\tiinq_order_part_t *orderby_order_parts = malloc(sizeof(iinq_order_part_t) * num_orderby);\n" +
						"\tif (NULL == orderby_order_parts) {\n" +
						"\t\tgoto ERROR;\n" +
						"\t}\n" +
						"\n" +
						"\tint i;\n" +
						"\tion_value_size_t value_size = 0;\n" +
						"\tfor (i = 0; i < num_fields; i++) {\n" +
						"\t\tiinq_table_id_t table_id = input_operator->instance->field_info[i].table_id;\n" +
						"\t\tiinq_field_num_t field_num = input_operator->instance->field_info[i].field_num;\n" +
						"\t\tvalue_size += iinq_calculate_offset(table_id, field_num + 1) - iinq_calculate_offset(table_id, field_num);\n" +
						"\t}\n" +
						"\n" +
						"\tfor (i = 0; i < num_orderby; i++) {\n" +
						"\t\t//iinq_init_order_by_pointers(it, &order_by_field[i], orderby_order_parts, num_orderby);\n" +
						"\t\torderby_order_parts[i].pointer = input_operator->instance->fields[order_by_fields[i].field_num-1];\n" +
						"\n" +
						"\t\tiinq_field_num_t field_num = input_operator->instance->field_info[order_by_fields[i].field_num-1].field_num;\n" +
						"\t\tiinq_table_id_t table_id = input_operator->instance->field_info[order_by_fields[i].field_num-1].table_id;\n" +
						"\t\torderby_order_parts[i].direction = order_by_fields[i].direction;\n" +
						"\t\torderby_order_parts[i].size = iinq_calculate_offset(table_id, field_num + 1) - iinq_calculate_offset(table_id, field_num);\n" +
						"\n" +
						"\t\t// TODO: can we get rid of the order types and just use standard iinq types?\n" +
						"\t\tswitch (iinq_get_field_type(table_id, field_num)) {\n" +
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
						"\t//iinq_order_by_write_to_file(it, orderby_n, orderby_order_parts, location);\n" +
						"\n" +
						"\tFILE *file = fopen(\"orderby\", \"wb\");\n" +
						"\n" +
						"\tint write_page_remaining = IINQ_PAGE_SIZE;\n" +
						"\n" +
						"\t/* Filter before sorting. Use existing table scan operator*/\n" +
						"\twhile (input_operator->next(input_operator)) {\n" +
						"\t\tif (write_page_remaining < (total_orderby_size + IINQ_BITS_FOR_NULL(num_fields) + value_size)) {\n" +
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
						"\t\tfor (i = 0; i < num_orderby; i++) {\n" +
						"\t\t\tif (1 != fwrite(orderby_order_parts[i].pointer, orderby_order_parts[i].size, 1, file)) {\n" +
						"\t\t\t\tbreak;\n" +
						"\t\t\t} else {\n" +
						"\t\t\t\twrite_page_remaining -= orderby_order_parts[i].size;\n" +
						"\t\t\t}\n" +
						"\t\t}\n" +
						"\n" +
						"\t\tif (1 != fwrite(input_operator->instance->null_indicators, IINQ_BITS_FOR_NULL(num_fields), 1, file)) {\n" +
						"\t\t\tbreak;\n" +
						"\t\t} else {\n" +
						"\t\t\twrite_page_remaining -= IINQ_BITS_FOR_NULL(num_fields);\n" +
						"\t\t}\n" +
						"\n" +
						"\t\tint j;\n" +
						"\t\tfor (j = 0; j < num_fields; j++) {\n" +
						"\t\t\tiinq_field_num_t field_num = input_operator->instance->field_info[j].field_num;\n" +
						"\t\t\tiinq_table_id_t table_id = input_operator->instance->field_info[j].table_id;\n" +
						"\t\t\tsize_t field_size = iinq_calculate_offset(table_id, field_num + 1) - iinq_calculate_offset(table_id, field_num);\n" +
						"\t\t\tif (1 != fwrite(input_operator->instance->fields[j], field_size, 1, file)) {\n" +
						"\t\t\t\tbreak;\n" +
						"\t\t\t} else {\n" +
						"\t\t\t\twrite_page_remaining -= field_size;\n" +
						"\t\t\t}\n" +
						"\t\t}\n" +
						"\t}\n" +
						"\n" +
						"\n" +
						"\t/* All records have been written, so close file */\n" +
						"\tif (NULL != file) {\n" +
						"\t\tfclose(file);\n" +
						"\t}\n" +
						"\n" +
						"\tiinq_query_operator_t *operator = malloc(sizeof(iinq_query_operator_t));\n" +
						"\toperator->instance = malloc(sizeof(iinq_external_sort_t));\n" +
						"\toperator->instance->type = iinq_external_sort_e;\n" +
						"\tiinq_external_sort_t *external_sort = (iinq_external_sort_t *) operator->instance;\n" +
						"\n" +
						"\tion_external_sort_t *es = malloc(sizeof(ion_external_sort_t));\n" +
						"\texternal_sort->es = es;\n" +
						"\n" +
						"\t// Open file in read mode for sorting\n" +
						"\tfile = fopen(\"orderby\", \"rb\");\n" +
						"\n" +
						"\tiinq_sort_context_t *context = malloc(sizeof(iinq_sort_context_t));\n" +
						"\n" +
						"\tcontext->parts \t= orderby_order_parts;\n" +
						"\tcontext->n\t\t= num_orderby;\n" +
						"\n" +
						"\n" +
						"\tion_err_t error = ion_external_sort_init(es, file, context, iinq_sort_compare, 0,\n" +
						"\t\t\t\t\t\t\t\t   IINQ_BITS_FOR_NULL(num_fields) + value_size + total_orderby_size, IINQ_PAGE_SIZE, boolean_false,\n" +
						"\t\t\t\t\t\t\t\t   ION_FILE_SORT_FLASH_MINSORT);\n" +
						"\n" +
						"\tuint16_t buffer_size = ion_external_sort_bytes_of_memory_required(es, 0, boolean_false);\n" +
						"\n" +
						"\tchar *buffer = malloc(buffer_size);\n" +
						"\t// recordbuf needs enough room for the sort field and the table tuple (sort field is stored twice)\n" +
						"\tchar *record_buf = malloc((total_orderby_size + IINQ_BITS_FOR_NULL(num_fields) + value_size));\n" +
						"\toperator->instance->null_indicators = record_buf + total_orderby_size;\n" +
						"\n" +
						"\toperator->instance->num_fields = num_fields;\n" +
						"\toperator->instance->field_info = malloc(sizeof(iinq_field_info_t)*num_fields);\n" +
						"\tmemcpy(operator->instance->field_info, input_operator->instance->field_info, sizeof(iinq_field_info_t)*num_fields);\n" +
						"\toperator->instance->fields = malloc(sizeof(ion_value_t)*num_fields);\n" +
						"\n" +
						"\tsize_t offset = total_orderby_size + IINQ_BITS_FOR_NULL(num_fields);\n" +
						"\tfor (i = 0; i < num_fields; i++) {\n" +
						"\t\tiinq_table_id_t table_id = operator->instance->field_info[i].table_id;\n" +
						"\t\tiinq_field_num_t field_num = operator->instance->field_info[i].field_num;\n" +
						"\t\toperator->instance->fields[i] = record_buf + offset;\n" +
						"\t\toffset += iinq_calculate_offset(table_id, field_num + 1) - iinq_calculate_offset(table_id, field_num);\n" +
						"\t}\n" +
						"\n" +
						"\tinput_operator->destroy(&input_operator);\n" +
						"\n" +
						"\n" +
						"\texternal_sort->buffer = buffer;\n" +
						"\n" +
						"\texternal_sort->record_buf = record_buf;\n" +
						"\n" +
						"\tion_external_sort_cursor_t *cursor = malloc(sizeof(ion_external_sort_cursor_t));\n" +
						"\n" +
						"\terror = ion_external_sort_init_cursor(es, cursor, buffer, buffer_size);\n" +
						"\texternal_sort->cursor = cursor;\n" +
						"\n" +
						"\toperator->next = iinq_external_sort_next;\n" +
						"\toperator->destroy = iinq_external_sort_destroy;\n" +
						"\n" +
						"\treturn operator;\n" +
						"\n" +
						"ERROR:\n" +
						"\treturn NULL;\n" +
						"}\n\n"

		);

	}
}
