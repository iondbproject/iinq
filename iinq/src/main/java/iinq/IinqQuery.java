package iinq;

import com.sun.xml.internal.bind.v2.TODO;
import unity.annotation.SourceField;
import unity.annotation.SourceTable;
import unity.engine.Attribute;
import unity.generic.query.WebQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.jar.Attributes;


/**
 * Stores a representation of an IINQ query.
 */
public class IinqQuery extends WebQuery {
	// Enables comments with in code output
	private static final boolean DEBUG = true;


	/**
	 * Checks whether the query contains an GROUP BY clause
	 *
	 * @return true if it contains an GROUP BY clause, false if it does not
	 */
	public boolean hasGroupBy() {
		/* TODO: implement check for GROUP BY clause */
		return false;
	}

	/**
	 * Checks whether the query contains an ORDER BY clause
	 *
	 * @return true if it contains an ORDER BY clause, false if it does not
	 */
	public boolean hasOrderBy() {
		return this.parameters.containsKey("sort");
	}

	/**
	 * Checks whether a schema was given for the query.
	 *
	 * @return true if a schema was given, false if it was not.
	 */
	public boolean hasSchema() {
		return (this.proj.getDatabase().getDatabase().getSchemaFile() != null) ? true : false;
	}

	/**
	 * Constructs an IINQ query.
	 *
	 * @param url query URL
	 */
	public IinqQuery(String url) {
		super(url);
	}

	/**
	 * Generates the C code for this SQL query.
	 *
	 * @return code to execute query
	 */
	public String generateCode() {
		/* Currently follows similar structure to MATERIALIZED_QUERY macro		 */
		String tableName = getTableName();
		StringBuilder code = new StringBuilder();

		/* Included for all queries */
		code.append("\tdo {\n" +
				"\t\tion_err_t error;\n" +
				"\t\tion_iinq_result_t result;\n" +
				"\t\tint jmp_r;\n" +
				/* selectbuf not used for materialized queries*/
				"\t\tjmp_buf selectbuf;\n" +
				"\t\tresult.raw_record_size = 0;\n" +
				"\t\tresult.num_bytes = 0;\n"
		);

		/* Original macros required the schema to be declared prior to the macro call
		 * This does not declare the fields in the same order as the source table */
		/*if (hasSchema()) {
			code.append(generateSchemaDefinition());
		}*/

		/* additional code for materialized queries */
		code.append("\t\tint read_page_remaining = IINQ_PAGE_SIZE;\n" +
				"\t\tint write_page_remaining = IINQ_PAGE_SIZE;\n" +
				"\t\tFILE *input_file;\n" +
				"\t\tFILE *output_file;\n");

		/* for aggregate expressions,
		 * probably will not need */
		/*code.append("\t\tint agg_n = 0;\n" +
				"\t\tint i_agg = 0;\n");*/

		/* from_clause */
		code.append(generateFromCode());

		/* _ORDERING_DECLARE(groupby) */
		/* GROUP BY not supported */
		/*code.append(generateOrderingDeclare("groupby"));*/

		/* _ORDERING_DECLARE(orderby) */
		/* if ORDER BY was in query, "sort" parameter exist */
		if (hasOrderBy())
			code.append(generateOrderingDeclare("orderby"));

		/* aggregate_exprs
		 * not supported */
		/*code.append(generateAggregateExprsCode());*/

		/* select_clause */
		code.append(generateSelectCode());

		/* groupby_clause
		 * not supported */
		/*code.append(generateGroupByCode());*/

		/* orderby_clause */
		if (hasOrderBy())
			code.append(generateOrderByCode());

		/* Wrap from in do-while loop */
		code.append("\t\tdo {\n"
				/* GROUP BY and aggregates
				 * not supported yet
				 * TODO: remove these if statements (only generate needed code)*/
				/*"\t\t\tif (agg_n > 0 || groupby_n > 0) {\n" +
				*//* _OPEN_ORDERING_FILE_WRITE(groupby, 0, 1, 0, result, groupby) *//*
				"\t\t\t\toutput_file = fopen(\"groupby\", \"wb\");\n" +
				"\t\t\t\tif (NULL == output_file) {\n" +
				"\t\t\t\t\terror = err_file_open_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\twrite_page_remaining = IINQ_PAGE_SIZE;\n" +
				"\t\t\t\tif ((int) write_page_remaining < (int) (total_groupby_size + (result.raw_record_size))) {\n" +
				"\t\t\t\t\terror = err_record_size_too_large;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t}\n" +
				"\t\t\t}\n" +

				*//* Just GROUP BY, invalid query *//*
				*//* TODO: why is this even an else if? Doesn't the first if make sure this is never a case *//*
				"\t\t\telse if (groupby_n > 0) {\n" +
				"\t\t\t\terror = err_illegal_state;\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t}\n"*/);

				/* Just ORDER BY */
		if (hasOrderBy()) {
			code.append("\t\t\t{\n");
			if (this.DEBUG) code.append("\t\t\t\t/* _OPEN_ORDERING_FILE_WRITE(orderby, 0, 1, 0, result, orderby) */\n");
			code.append("\t\t\t\toutput_file = fopen(\"orderby\", \"wb\");\n" +
					"\t\t\t\tif (NULL == output_file) {\n" +
					"\t\t\t\t\terror = err_file_open_error;\n" +
					"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t\t}\n" +
					"\t\t\t\twrite_page_remaining = IINQ_PAGE_SIZE;\n" +
					"\t\t\t\tif ((int) write_page_remaining < (int) (total_orderby_size + (result.raw_record_size))) {\n" +
					"\t\t\t\t\terror = err_record_size_too_large;\n" +
					"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t\t}\n" +
					"\t\t\t}\n");
		}

		code.append("\t\twhile (1) {\n");

		if (this.DEBUG) {
			code.append("\t\t\t/* _FROM_ADVANCE_CURSORS */\n");
		}

		code.append("\t\t\tif (NULL == ref_cursor) { break; }\n" +
				"\t\t\tlast_cursor = ref_cursor;\n" +
				"\t\t\twhile (NULL != ref_cursor && (cs_cursor_active !=\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t  (ref_cursor->reference->cursor_status = ref_cursor->reference->cursor->next(\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t  ref_cursor->reference->cursor,\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t  &ref_cursor->reference->ion_record)) &&\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t  cs_cursor_initialized != ref_cursor->reference->cursor_status)) {\n" +
				"\t\t\t\tref_cursor->reference->cursor->destroy(&ref_cursor->reference->cursor);\n" +
				"\t\t\t\tdictionary_find(&ref_cursor->reference->dictionary, &ref_cursor->reference->predicate,\n" +
				"\t\t\t\t\t\t\t\t&ref_cursor->reference->cursor);\n" +
				"\t\t\t\tif ((cs_cursor_active != (ref_cursor->reference->cursor_status = ref_cursor->reference->cursor->next(\n" +
				"\t\t\t\t\t\tref_cursor->reference->cursor, &ref_cursor->reference->ion_record)) &&\n" +
				"\t\t\t\t\t cs_cursor_initialized != ref_cursor->reference->cursor_status)) { goto IINQ_QUERY_CLEANUP; }\n" +
				"\t\t\t\tref_cursor = ref_cursor->last;\n" +
				"\t\t\t}\n" +
				"\t\t\tif (NULL == ref_cursor) { \n" +
				"\t\t\t\tbreak;\n" +
				"\t\t\t} else if (last_cursor != ref_cursor) {\n" +
				"\t\t\t\tref_cursor = last;\n" +
				"\t\t\t }\n"
		);

		if (this.DEBUG) {
			code.append("\t\t\t/* end of _FROM_ADVANCE_CURSORS */\n");
		}

		/* Still in while(1) loop
		 * if (!WHERE) { continue; } */
		try {
			code.append(generateWhereCode());
		} catch (RequiresSchemaException ex) {
			ex.printStackTrace();
		}

		/* Copy data after a join (before grouping/aggregating/sorting)
		 * Not sure if we will need this code, so leaving it commented out
		 * _COPY_EARLY_RESULT_ALL */
		/*code.append("do {\n" +
				"\t\t\t\t\tion_iinq_result_size_t result_loc = 0;\n" +
				"\t\t\t\t\tion_iinq_cleanup_t *copyer = first;\n" +
				"\t\t\t\t\twhile (NULL != copyer) {\n" +
				"\t\t\t\t\t\tmemcpy(result.data + (result_loc), copyer->reference->key,\n" +
				"\t\t\t\t\t\t\t   copyer->reference->dictionary.instance->record.key_size);\n" +
				"\t\t\t\t\t\tresult_loc += copyer->reference->dictionary.instance->record.key_size;\n" +
				"\t\t\t\t\t\tmemcpy(result.data + (result_loc), copyer->reference->value,\n" +
				"\t\t\t\t\t\t\t   copyer->reference->dictionary.instance->record.value_size);\n" +
				"\t\t\t\t\t\tresult_loc += copyer->reference->dictionary.instance->record.value_size;\n" +
				"\t\t\t\t\t\tcopyer = copyer->next;\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\twhile (0);");*/

		/* Also not sure if we will need this yet*/
		/* If GROUP BY or aggregates */
		/*code.append("\t\t\t\tif (agg_n > 0 || groupby_n > 0) {\n" +
				*//* Write out groupby records to disk for sorting *//*
				"\t\t\t\t\tgoto IINQ_COMPUTE_GROUPBY;\n" +
				"\t\t\t\t\tIINQ_DONE_COMPUTE_GROUPBY:;\n" +

				*//* _WRITE_ORDERING_RECORD(groupby, 0, 1, 0, result) *//*
				"\t\t\t\t\tif ((int) write_page_remaining <\n" +
				"\t\t\t\t\t\t(int) (total_groupby_size + (result.raw_record_size) + (8 * agg_n))) {\n" +
				"\t\t\t\t\t\tint i = 0;\n" +
				"\t\t\t\t\t\tchar x = 0;\n" +
				"\t\t\t\t\t\tfor (; i < write_page_remaining; i++) { if (1 != fwrite(&x, 1, 1, output_file)) { break; }}\n" +
				"\t\t\t\t\t\twrite_page_remaining = IINQ_PAGE_SIZE;\n" +
				"\t\t\t\t\t};" +
				"\t\t\t\t\tfor (i_groupby = 0; i_groupby < groupby_n; i_groupby++) {\n" +
				"\t\t\t\t\t\tif (1 != fwrite(groupby_order_parts[i_groupby].pointer, groupby_order_parts[i_groupby].size, 1,\n" +
				"\t\t\t\t\t\t\t\t\t\toutput_file)) { break; }\n" +
				"\t\t\t\t\t\telse { write_page_remaining -= groupby_order_parts[i_groupby].size; }\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t\tif (1 != fwrite(result.data, result.raw_record_size, 1, output_file)) { break; }\n" +
				"\t\t\t\t\telse { write_page_remaining -= result.raw_record_size; }\n" +
				"\t\t\t\t}");*/

		/* If ORDER BY */
		if (hasOrderBy()) {
			code.append("\t\t\t\t{\n" +
					"\t\t\t\t\tgoto IINQ_COMPUTE_ORDERBY;\n" +
					"\t\t\t\t\tIINQ_DONE_COMPUTE_ORDERBY:;\n" +

				/* Not needed for select all,
				 * come back to look at this later */
					"\t\t\t\t\tif (1 == jmp_r) { goto IINQ_DONE_COMPUTE_ORDERBY_1; }\n" +
					"\t\t\t\t\telse if (2 == jmp_r) { goto IINQ_DONE_COMPUTE_ORDERBY_2; }\n" +
					"\t\t\t\t\tjmp_r = 3;\n" +
					"\t\t\t\t\tgoto COMPUTE_SELECT;\n" +
					"\t\t\t\t\tDONE_COMPUTE_SELECT_3:;\n" +

					"\t\t\t\t\tif ((int) write_page_remaining < (int) (total_orderby_size + (result.num_bytes) + (8 * agg_n))) {\n" +
					"\t\t\t\t\t\tint i = 0;\n" +
					"\t\t\t\t\t\tchar x = 0;\n" +
					"\t\t\t\t\t\tfor (; i < write_page_remaining; i++) { if (1 != fwrite(&x, 1, 1, output_file)) { break; }}\n" +
					"\t\t\t\t\t\twrite_page_remaining = 512;\n" +
					"\t\t\t\t\t};\n" +
					"\t\t\t\t\tfor (i_orderby = 0; i_orderby < orderby_n; i_orderby++) {\n" +
					"\t\t\t\t\t\tif (1 != fwrite(orderby_order_parts[i_orderby].pointer, orderby_order_parts[i_orderby].size, 1,\n" +
					"\t\t\t\t\t\t\t\t\t\toutput_file)) { break; }\n" +
					"\t\t\t\t\t\telse { write_page_remaining -= orderby_order_parts[i_orderby].size; }\n" +
					"\t\t\t\t\t}\n" +
					"\t\t\t\t\tif (1 != fwrite(result.processed, result.num_bytes, 1, output_file)) { break; }\n" +
					"\t\t\t\t\telse { write_page_remaining -= result.num_bytes; }\n" +
					"\t\t\t\t}\n");
		} else {
			code.append("\t\t\t\tresult.processed = alloca(result.num_bytes);\n" +

				"\t\t\t\tgoto COMPUTE_SELECT;\n" +
				"\t\t\t\tDONE_COMPUTE_SELECT:;\n" +
/*				"\t\t\t\tif (1 == jmp_r) { goto DONE_COMPUTE_SELECT_1; }\n" +
				"\t\t\t\telse if (2 == jmp_r) { goto DONE_COMPUTE_SELECT_2; }\n" +
				"\t\t\t\telse if (3 == jmp_r) { goto DONE_COMPUTE_SELECT_3; }\n" +*/

					"\t\t\t\t(&processor)->execute(&result, (&processor)->state);\n" +
					"\t\t\t}\n");
				/* end if-else*/
		}

		/* free memory */
		code.append(generateCleanupCode());

		/* sort the file if there are groupby elements
		 *  have not implemented group by yet so leaving this commented out */
		/* code.append("\t\tif (groupby_n > 0) {\n" +
				"\t\t\tinput_file = fopen(\"groupby\", \"rb\");\n" +
				"\t\t\tif (NULL == input_file) {\n" +
				"\t\t\t\terror = err_file_open_error;\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t}\n" +
				"\t\t\tread_page_remaining = 512;\n" +
				"\t\t\tif ((int) read_page_remaining < (int) (total_groupby_size + result.raw_record_size)) {\n" +
				"\t\t\t\terror = err_record_size_too_large;\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t};\n" +
				"\t\t\toutput_file = fopen(\"sortedgb\", \"wb\");\n" +
				"\t\t\tif (NULL == output_file) {\n" +
				"\t\t\t\terror = err_file_open_error;\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t}\n" +
				"\t\t\twrite_page_remaining = 512;\n" +
				"\t\t\tif ((int) write_page_remaining < (int) (total_groupby_size + (result.raw_record_size))) {\n" +
				"\t\t\t\terror = err_record_size_too_large;\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t};\n" +
				"\t\t\tion_external_sort_t es;\n" +
				"\t\t\tiinq_sort_context_t context = ((iinq_sort_context_t) {groupby_order_parts, groupby_n});\n" +
				"\t\t\tif (err_ok !=\n" +
				"\t\t\t\t(error = ion_external_sort_init(&es, input_file, &context, iinq_sort_compare, result.raw_record_size,\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\tresult.raw_record_size + total_groupby_size, 512, boolean_false,\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\tION_FILE_SORT_FLASH_MINSORT))) {\n" +
				"\t\t\t\tif (0 != fclose(input_file)) {\n" +
				"\t\t\t\t\terror = err_file_close_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t};\n" +
				"\t\t\t\tif (0 != fclose(output_file)) {\n" +
				"\t\t\t\t\terror = err_file_close_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t};\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t}\n" +
				"\t\t\tuint16_t buffer_size = ion_external_sort_bytes_of_memory_required(&es, 0, boolean_true);\n" +
				"\t\t\tchar *buffer = alloca((buffer_size));\n" +
				"\t\t\tif (err_ok != (error = ion_external_sort_dump_all(&es, output_file, buffer, buffer_size))) {\n" +
				"\t\t\t\tif (0 != fclose(input_file)) {\n" +
				"\t\t\t\t\terror = err_file_close_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t};\n" +
				"\t\t\t\tif (0 != fclose(output_file)) {\n" +
				"\t\t\t\t\terror = err_file_close_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t};\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t}\n" +
				"\t\t\tif (0 != fclose(input_file)) {\n" +
				"\t\t\t\terror = err_file_close_error;\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t};\n" +
				"\t\t\tif (0 != fclose(output_file)) {\n" +
				"\t\t\t\terror = err_file_close_error;\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t};\n" +
				"\t\t\tif (0 != remove(\"groupby\")) {\n" +
				"\t\t\t\terror = err_file_delete_error;\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t};\n" +
				"\t\t}")*/

		/* if aggregates or groupby
		 * have not implemented so commented out */
		/*code.append("\t\tif (agg_n > 0 || groupby_n > 0) {\n" +
				"\t\t\tif (groupby_n > 0) {\n" +
				"\t\t\t\tinput_file = fopen(\"sortedgb\", \"rb\");\n" +
				"\t\t\t\tif (NULL == input_file) {\n" +
				"\t\t\t\t\terror = err_file_open_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\tread_page_remaining = 512;\n" +
				"\t\t\t\tif ((int) read_page_remaining < (int) (total_groupby_size + result.raw_record_size)) {\n" +
				"\t\t\t\t\terror = err_record_size_too_large;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t};\n" +
				"\t\t\t}\n" +
				"\t\t\telse {\n" +
				"\t\t\t\tinput_file = fopen(\"groupby\", \"rb\");\n" +
				"\t\t\t\tif (NULL == input_file) {\n" +
				"\t\t\t\t\terror = err_file_open_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\tread_page_remaining = 512;\n" +
				"\t\t\t\tif ((int) read_page_remaining < (int) (total_groupby_size + result.raw_record_size)) {\n" +
				"\t\t\t\t\terror = err_record_size_too_large;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t};\n" +
				"\t\t\t}\n" +
				"\t\t\tif (orderby_n > 0) {\n" +
				"\t\t\t\toutput_file = fopen(\"orderby\", \"wb\");\n" +
				"\t\t\t\tif (NULL == output_file) {\n" +
				"\t\t\t\t\terror = err_file_open_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\twrite_page_remaining = 512;\n" +
				"\t\t\t\tif ((int) write_page_remaining < (int) (total_orderby_size + (8 * agg_n) + (result.num_bytes))) {\n" +
				"\t\t\t\t\terror = err_record_size_too_large;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t};\n" +
				"\t\t\t}\n" +
				"\t\t\tion_boolean_t is_first = boolean_true;\n" +
				"\t\t\tchar *old_key = (total_groupby_size > 0) ? alloca((total_groupby_size)) : NULL;\n" +
				"\t\t\tchar *cur_key = (total_groupby_size > 0) ? alloca((total_groupby_size)) : NULL;\n" +
				"\t\t\tread_page_remaining = 512;\n" +
				"\t\t\tresult.processed = alloca((result.num_bytes));\n" +
				"\t\t\twhile (1) {\n" +
				"\t\t\t\tif ((int) read_page_remaining < (int) (total_groupby_size + (result.raw_record_size) +\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t   ((NULL != NULL) ? 8 * agg_n : 0))) {\n" +
				"\t\t\t\t\tif (0 != fseek(input_file, read_page_remaining, 1)) { break; }\n" +
				"\t\t\t\t\tread_page_remaining = 512;\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\tif (NULL != cur_key) {\n" +
				"\t\t\t\t\tif (0 == total_groupby_size ||\n" +
				"\t\t\t\t\t\t1 != fread((cur_key), total_groupby_size, 1, input_file)) { break; }\n" +
				"\t\t\t\t\telse { read_page_remaining -= total_groupby_size; }\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\telse {\n" +
				"\t\t\t\t\tif (0 != fseek(input_file, total_groupby_size, 1)) { break; }\n" +
				"\t\t\t\t\telse { read_page_remaining -= total_groupby_size; }\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\tif (NULL != NULL) {\n" +
				"\t\t\t\t\tif (1 != fread((NULL), 8 * agg_n, 1, input_file)) { break; }\n" +
				"\t\t\t\t\telse {\n" +
				"\t\t\t\t\t\tread_page_remaining -= 8 * agg_n;\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\tif (1 != fread(result.data, result.raw_record_size, 1, input_file)) { break; }\n" +
				"\t\t\t\tread_page_remaining -= result.raw_record_size;;\n" +
				"\t\t\t\tif (total_groupby_size > 0 && boolean_false == is_first && equal != iinq_sort_compare(\n" +
				"\t\t\t\t\t\t&((iinq_sort_context_t) {groupby_order_parts, groupby_n}), cur_key, old_key)) {\n" +
				"\t\t\t\t\tjmp_r = 1;\n" +
				"\t\t\t\t\tgoto IINQ_COMPUTE_ORDERBY;\n" +
				"\t\t\t\t\tIINQ_DONE_COMPUTE_ORDERBY_1:;\n" +
				"\t\t\t\t\tif (!(having_____)) { continue; }\n" +
				"\t\t\t\t\tjmp_r = 1;\n" +
				"\t\t\t\t\tgoto COMPUTE_SELECT;\n" +
				"\t\t\t\t\tDONE_COMPUTE_SELECT_1:;\n" +
				"\t\t\t\t\tif (orderby_n > 0) {\n" +
				"\t\t\t\t\t\tif ((int) write_page_remaining <\n" +
				"\t\t\t\t\t\t\t(int) (total_orderby_size + (result.num_bytes) + (8 * agg_n))) {\n" +
				"\t\t\t\t\t\t\tint i = 0;\n" +
				"\t\t\t\t\t\t\tchar x = 0;\n" +
				"\t\t\t\t\t\t\tfor (; i < write_page_remaining; i++) { if (1 != fwrite(&x, 1, 1, output_file)) { break; }}\n" +
				"\t\t\t\t\t\t\twrite_page_remaining = 512;\n" +
				"\t\t\t\t\t\t};\n" +
				"\t\t\t\t\t\tfor (i_orderby = 0; i_orderby < orderby_n; i_orderby++) {\n" +
				"\t\t\t\t\t\t\tif (1 != fwrite(orderby_order_parts[i_orderby].pointer, orderby_order_parts[i_orderby].size,\n" +
				"\t\t\t\t\t\t\t\t\t\t\t1, output_file)) { break; }\n" +
				"\t\t\t\t\t\t\telse { write_page_remaining -= orderby_order_parts[i_orderby].size; }\n" +
				"\t\t\t\t\t\t}\n" +
				"\t\t\t\t\t\tfor (i_agg = 0; i_agg < agg_n; i_agg++) {\n" +
				"\t\t\t\t\t\t\tif (1 != fwrite(&(uint64_t) {\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t(IINQ_AGGREGATE_TYPE_INT == aggregates[(i_agg)].type ? aggregates[(i_agg)].value.i64\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t : (IINQ_AGGREGATE_TYPE_UINT ==\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\taggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t? aggregates[(i_agg)].value.u64\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t: aggregates[(i_agg)].value.f64))},\n" +
				"\t\t\t\t\t\t\t\t\t\t\tsizeof((IINQ_AGGREGATE_TYPE_INT == aggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t? aggregates[(i_agg)].value.i64 : (IINQ_AGGREGATE_TYPE_UINT ==\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t   aggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t   ? aggregates[(i_agg)].value.u64\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t   : aggregates[(i_agg)].value.f64))),\n" +
				"\t\t\t\t\t\t\t\t\t\t\t1, output_file)) { break; }\n" +
				"\t\t\t\t\t\t\telse {\n" +
				"\t\t\t\t\t\t\t\twrite_page_remaining -= sizeof((IINQ_AGGREGATE_TYPE_INT == aggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t? aggregates[(i_agg)].value.i64 : (\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tIINQ_AGGREGATE_TYPE_UINT ==\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\taggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t? aggregates[(i_agg)].value.u64\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t: aggregates[(i_agg)].value.f64)));\n" +
				"\t\t\t\t\t\t\t}\n" +
				"\t\t\t\t\t\t}\n" +
				"\t\t\t\t\t\tif (1 != fwrite(result.processed, result.num_bytes, 1, output_file)) { break; }\n" +
				"\t\t\t\t\t\telse { write_page_remaining -= result.num_bytes; }\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t\telse { (processor)->execute(&result, (processor)->state); }\n" +
				"\t\t\t\t\tfor (i_agg = 0; i_agg < agg_n; i_agg++) {\n" +
				"\t\t\t\t\t\taggregates[i_agg].status = 0;\n" +
				"\t\t\t\t\t\taggregates[i_agg].value.i64 = 0;\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\tfor (i_groupby = 0; i_groupby < groupby_n; i_groupby++) {\n" +
				"\t\t\t\t\tmemcpy(groupby_order_parts[i_groupby].pointer, cur_key, groupby_order_parts[i_groupby].size);\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\tgoto IINQ_COMPUTE_AGGREGATES;\n" +
				"\t\t\t\tIINQ_DONE_COMPUTE_AGGREGATES:;\n" +
				"\t\t\t\tif (total_groupby_size > 0) { memcpy(old_key, cur_key, total_groupby_size); }\n" +
				"\t\t\t\tis_first = boolean_false;\n" +
				"\t\t\t}\n" +
				"\t\t\tif (boolean_false == is_first) {\n" +
				"\t\t\t\tjmp_r = 2;\n" +
				"\t\t\t\tgoto IINQ_COMPUTE_ORDERBY;\n" +
				"\t\t\t\tIINQ_DONE_COMPUTE_ORDERBY_2:;\n" +
				"\t\t\t\tif (!(having_____)) { goto IINQ_CLEANUP_AGGREGATION; }\n" +
				"\t\t\t\tjmp_r = 2;\n" +
				"\t\t\t\tgoto COMPUTE_SELECT;\n" +
				"\t\t\t\tDONE_COMPUTE_SELECT_2:;\n" +
				"\t\t\t\tif (orderby_n > 0) {\n" +
				"\t\t\t\t\tif ((int) write_page_remaining < (int) (total_orderby_size + (result.num_bytes) + (8 * agg_n))) {\n" +
				"\t\t\t\t\t\tint i = 0;\n" +
				"\t\t\t\t\t\tchar x = 0;\n" +
				"\t\t\t\t\t\tfor (; i < write_page_remaining; i++) { if (1 != fwrite(&x, 1, 1, output_file)) { break; }}\n" +
				"\t\t\t\t\t\twrite_page_remaining = 512;\n" +
				"\t\t\t\t\t};\n" +
				"\t\t\t\t\tfor (i_orderby = 0; i_orderby < orderby_n; i_orderby++) {\n" +
				"\t\t\t\t\t\tif (1 != fwrite(orderby_order_parts[i_orderby].pointer, orderby_order_parts[i_orderby].size, 1,\n" +
				"\t\t\t\t\t\t\t\t\t\toutput_file)) { break; }\n" +
				"\t\t\t\t\t\telse { write_page_remaining -= orderby_order_parts[i_orderby].size; }\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t\tfor (i_agg = 0; i_agg < agg_n; i_agg++) {\n" +
				"\t\t\t\t\t\tif (1 != fwrite(&(uint64_t) {\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t(IINQ_AGGREGATE_TYPE_INT == aggregates[(i_agg)].type ? aggregates[(i_agg)].value.i64 : (\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\tIINQ_AGGREGATE_TYPE_UINT == aggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t? aggregates[(i_agg)].value.u64 : aggregates[(i_agg)].value.f64))},\n" +
				"\t\t\t\t\t\t\t\t\t\tsizeof((IINQ_AGGREGATE_TYPE_INT == aggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t? aggregates[(i_agg)].value.i64 : (IINQ_AGGREGATE_TYPE_UINT ==\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t   aggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t   ? aggregates[(i_agg)].value.u64\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t   : aggregates[(i_agg)].value.f64))),\n" +
				"\t\t\t\t\t\t\t\t\t\t1, output_file)) { break; }\n" +
				"\t\t\t\t\t\telse {\n" +
				"\t\t\t\t\t\t\twrite_page_remaining -= sizeof((IINQ_AGGREGATE_TYPE_INT == aggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t? aggregates[(i_agg)].value.i64 : (\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tIINQ_AGGREGATE_TYPE_UINT == aggregates[(i_agg)].type\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t? aggregates[(i_agg)].value.u64\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t: aggregates[(i_agg)].value.f64)));\n" +
				"\t\t\t\t\t\t}\n" +
				"\t\t\t\t\t}\n" +
				"\t\t\t\t\tif (1 != fwrite(result.processed, result.num_bytes, 1, output_file)) { break; }\n" +
				"\t\t\t\t\telse { write_page_remaining -= result.num_bytes; }\n" +
				"\t\t\t\t}\n" +
				"\t\t\t\telse { (processor)->execute(&result, (processor)->state); }\n" +
				"\t\t\t}\n" +
				"\t\t\tIINQ_CLEANUP_AGGREGATION:;\n" +
				"\t\t\tif (orderby_n > 0) {\n" +
				"\t\t\t\tif (0 != fclose(output_file)) {\n" +
				"\t\t\t\t\terror = err_file_close_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t}\n" +
				"\t\t\t}\n" +
				"\t\t\tif (0 != fclose(input_file)) {\n" +
				"\t\t\t\terror = err_file_close_error;\n" +
				"\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t}\n" +
				"\t\t\tif (groupby_n > 0) {\n" +
				"\t\t\t\tif (0 != remove(\"sortedgb\")) {\n" +
				"\t\t\t\t\terror = err_file_delete_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t};\n" +
				"\t\t\t}\n" +
				"\t\t\telse {\n" +
				"\t\t\t\tif (0 != remove(\"groupby\")) {\n" +
				"\t\t\t\t\terror = err_file_delete_error;\n" +
				"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
				"\t\t\t\t};\n" +
				"\t\t\t}\n" +
				"\t\t\tif (boolean_true == is_first) { goto IINQ_QUERY_END; }\n" +
				"\t\t}");*/

		/* close do-while(0) */
		code.append("\t\t} while (0);\n");

		/* if orderby or aggregates
		 * ORDERBY handling */
		code.append(generateOrderByHandlingCode());

		/* end do-while(0) */
		code.append("\t\tIINQ_QUERY_END:;\n" +
				"\t} while (0);\n");

		return code.toString();


	}

	public String generateFromCode() {
		StringBuilder fromCode = new StringBuilder();
		String tableName = this.getParameter("source");

		if (this.DEBUG) {
			fromCode.append("\t\t/* FROM(0, test1)\n" +
					"\t\t * first argument is with_schemas but the query macro would check for with_schema\n" +
					"\t\t * the IF_ELSE macro always substituted as (),\n" +
					"\t\t * pretty sure there was a typo in the macro */\n");
		}

		fromCode.append("\t\tion_iinq_cleanup_t *first;\n" +
				"\t\tion_iinq_cleanup_t *last;\n" +
				"\t\tion_iinq_cleanup_t *ref_cursor;\n" +
				"\t\tion_iinq_cleanup_t *last_cursor;\n" +
				"\t\tfirst = NULL;\n" +
				"\t\tlast = NULL;\n" +
				"\t\tref_cursor = NULL;\n" +
				"\t\tlast_cursor = NULL;\n");

		if (this.DEBUG) {
			fromCode.append("\t\t/* FROM_SOURCES(test1)\n" +
					"\t\t * substituted with FROM_SOURCE_SINGLE(test1) */\n");
		}

		fromCode.append("\t\tion_iinq_source_t " + tableName + ";\n" +
				"\t\t" + tableName + ".cleanup.next = NULL;\n" +
				"\t\t" + tableName + ".cleanup.last = last;\n" +
				"\t\t" + tableName + ".cleanup.reference = &" + tableName + ";\n" +
				"\t\tif (NULL == first) { first = &" + tableName + ".cleanup; }\n" +
				"\t\tif (NULL != last) { last->next = &" + tableName + ".cleanup; }\n" +
				"\t\tlast = &" + tableName + ".cleanup;\n" +
				"\t\t" + tableName + ".cleanup.next = NULL;\n" +
				"\t\t" + tableName + ".dictionary.handler = &" + tableName + ".handler;\n" +
				"\t\terror = iinq_open_source(\"" + tableName + "\" \".inq\", &(" + tableName + ".dictionary), &(" + tableName + ".handler));\n" +
				"\t\tif (err_ok != error) { break; }\n" +
				"\t\tresult.raw_record_size += " + tableName + ".dictionary.instance->record.key_size;\n" +
				"\t\tresult.raw_record_size += " + tableName + ".dictionary.instance->record.value_size;\n" +
				"\t\tresult.num_bytes += " + tableName + ".dictionary.instance->record.key_size;\n" +
				"\t\tresult.num_bytes += " + tableName + ".dictionary.instance->record.value_size;\n" +
				"\t\terror = dictionary_build_predicate(&(" + tableName + ".predicate), predicate_all_records);\n" +
				"\t\tif (err_ok != error) { break; }\n" +
				"\t\tdictionary_find(&" + tableName + ".dictionary, &" + tableName + ".predicate, &" + tableName + ".cursor);\n");

		if (this.DEBUG) {
			fromCode.append("\t\t/* end of FROM_SOURCES(test1),\n" +
					"\t\t * FROM(0, test1) continued */\n");
		}

		fromCode.append("\t\tresult.data = alloca(result.raw_record_size);\n" +
				"\t\tresult.processed = result.data;\n");

		if (this.DEBUG) {
			fromCode.append("\t\t/* _FROM_SETUP_POINTERS(test1),\n" +
					"\t\t * substituted to _FROM_GET_OVERRIDE(test1)\n" +
					"\t\t * substituted to _FROM_SETUP_POINTERS_SINGLE(test1) */\n");
		}

		fromCode.append("\t\t" + tableName + ".key = result.processed;\n" +
				"\t\tresult.processed += " + tableName + ".dictionary.instance->record.key_size;\n" +
				"\t\t" + tableName + ".value = result.processed;\n" +
				"\t\tresult.processed += " + tableName + ".dictionary.instance->record.value_size;\n" +
				"\t\t" + tableName + ".ion_record.key = " + tableName + ".key;\n" +
				"\t\t" + tableName + ".ion_record.value = test1.value;\n" +
				"\t\tstruct iinq_" + tableName + "_schema *" + tableName + "_tuple;\n" +
				"\t\t" + tableName + "_tuple = " + tableName + ".value;\n");

		fromCode.append(
				"\t\tref_cursor = first;\n" +
						"\t\twhile (ref_cursor != last) {\n" +
						"\t\t\tif (NULL == ref_cursor || (cs_cursor_active !=\n" +
						"\t\t\t\t\t\t\t\t\t\t\t   (ref_cursor->reference->cursor_status = ref_cursor->reference->cursor->next(\n" +
						"\t\t\t\t\t\t\t\t\t\t\t\t\t   ref_cursor->reference->cursor,\n" +
						"\t\t\t\t\t\t\t\t\t\t\t\t\t   &ref_cursor->reference->ion_record)) && cs_cursor_initialized !=\n" +
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t   ref_cursor->reference->cursor_status)) { break; }\n" +
						"\t\t\tref_cursor = ref_cursor->next;\n" +
						"\t\t}\n" +
						"\t\tref_cursor = last;\n"
		);

		return fromCode.toString();
	}

	/**
	 * Constructs C code to handle the ORDER BY clause
	 *
	 * @return C code to execute the ORDER BY clause (sort)
	 */
	public String generateOrderByHandlingCode() {
		StringBuilder code = new StringBuilder();
		/* if orderby or aggregates
		 * if there are aggregates projection has already occurred
		 * TODO: add aggregates to the if statement */
		if (hasOrderBy()) {
			code.append("\t\t{\n");

			if (this.DEBUG) {
				code.append("\t\t\t/* _OPEN_ORDERING_FILE_READ(orderby, 1, 0, 1, result, orderby); */\n");
			}

			code.append("\t\t\tinput_file = fopen(\"orderby\", \"rb\");\n" +
					"\t\t\tif (NULL == input_file) {\n" +
					"\t\t\t\terror = err_file_open_error;\n" +
					"\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t}\n" +
					"\t\t\tread_page_remaining = 512;\n" +
					"\t\t\tif ((int) read_page_remaining < (int) (total_orderby_size + (8 * agg_n) + result.num_bytes)) {\n" +
					"\t\t\t\terror = err_record_size_too_large;\n" +
					"\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t};\n" +
					"\t\t}\n");

			if (this.DEBUG) {
				code.append("\t\t/* end of _OPEN_ORDERING_FILE_READ(orderby, 1, 0, 1, result, orderby); */\n");
			}

			code.append("\t\t\tion_external_sort_t es;\n");

			if (this.DEBUG) {
				code.append("\t\t\t/* iinq_sort_context_t context = _IINQ_SORT_CONTEXT(orderby); */\n");
			}

			code.append("\t\t\tiinq_sort_context_t context = ((iinq_sort_context_t) {orderby_order_parts, orderby_n});\n");

			if (this.DEBUG) {
				code.append("\t\t\t/* if (err_ok != (error = ion_external_sort_init(&es, input_file, &context, iinq_sort_compare, _RESULT_ORDERBY_RECORD_SIZE, _RESULT_ORDERBY_RECORD_SIZE + total_orderby_size + (8 * agg_n), IINQ_PAGE_SIZE, boolean_false, ION_FILE_SORT_FLASH_MINSORT)))*/\n");
			}

			code.append("\t\t\tif (err_ok != (error = ion_external_sort_init(&es, input_file, &context, iinq_sort_compare,\n" +
					"\t\t\t\t\t\t\t\t\t\t\t\t\t\t  (agg_n > 0 ? result.num_bytes : result.raw_record_size),\n" +
					"\t\t\t\t\t\t\t\t\t\t\t\t\t\t  (agg_n > 0 ? result.num_bytes : result.raw_record_size) +\n" +
					"\t\t\t\t\t\t\t\t\t\t\t\t\t\t  total_orderby_size + (8 * agg_n), 512, boolean_false,\n" +
					"\t\t\t\t\t\t\t\t\t\t\t\t\t\t  ION_FILE_SORT_FLASH_MINSORT))) {\n");

			if (this.DEBUG) {
				code.append("\t\t\t\t/* _CLOSE_ORDERING_FILE(input_file); */\n");
			}

			code.append("\t\t\t\tif (0 != fclose(input_file)) {\n" +
					"\t\t\t\t\terror = err_file_close_error;\n" +
					"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t\t};\n");

			if (this.DEBUG) {
				code.append("\t\t\t\t/* end of _CLOSE_ORDERING_FILE(input_file); */\n");
			}

			code.append("\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t}\n" +
					"\t\t\tuint16_t buffer_size = ion_external_sort_bytes_of_memory_required(&es, 0, boolean_false);\n" +
					"\t\t\tchar *buffer = alloca(buffer_size);\n" +
					"\t\t\tchar *record_buf = alloca((total_orderby_size + 8 * agg_n + result.num_bytes));\n" +
					"\t\t\tresult.processed = (unsigned char *) (record_buf + total_orderby_size + (8 * agg_n));\n" +
					"\t\t\tion_external_sort_cursor_t cursor;\n" +
					"\t\t\tif (err_ok != (error = ion_external_sort_init_cursor(&es, &cursor, buffer, buffer_size))) {\n");

			if (this.DEBUG) {
				code.append("\t\t\t\t/* _CLOSE_ORDERING_FILE(input_file); */\n");
			}

			code.append("\t\t\t\tif (0 != fclose(input_file)) {\n" +
					"\t\t\t\t\terror = err_file_close_error;\n" +
					"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t\t};\n");

			if (this.DEBUG) {
				code.append("\t\t\t\t/* end of _CLOSE_ORDERING_FILE(input_file); */\n");
			}

			code.append("\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t}\n" +
					"\t\t\tif (err_ok != (error = cursor.next(&cursor, record_buf))) {\n");

			if (this.DEBUG) {
				code.append("\t\t\t\t/* _CLOSE_ORDERING_FILE(input_file); */\n");
			}

			code.append("\t\t\t\tif (0 != fclose(input_file)) {\n" +
					"\t\t\t\t\terror = err_file_close_error;\n" +
					"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t\t};\n");

			if (this.DEBUG) {
				code.append("\t\t\t\t/* end of _CLOSE_ORDERING_FILE(input_file); */\n");
			}

			code.append("\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t}\n");

		/* load aggregate values into their place
		 * aggregates not currently implemented */
		/*code.append("\t\t\tfor (i_agg = 0; i_agg < agg_n; i_agg++) {\n" +
				"\t\t\t\taggregates[i_agg].value.i64 = *((int64_t * )(record_buf + (8 * i_agg)));\n" +
				"\t\t\t}\n");*/

		/* execute function in the processor */
			code.append("\t\t\twhile (cs_cursor_active == cursor.status) {\n" +
					"\t\t\t\t(&processor)->execute(&result, (&processor)->state);\n" +
					"\t\t\t\tif (err_ok != (error = cursor.next(&cursor, record_buf))) {\n");

			if (this.DEBUG) {
				code.append("\t\t\t\t\t/* _CLOSE_ORDERING_FILE(input_file); */\n");
			}

			code.append("\t\t\t\t\tif (0 != fclose(input_file)) {\n" +
					"\t\t\t\t\t\terror = err_file_close_error;\n" +
					"\t\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
					"\t\t\t\t\t};\n");

			if (this.DEBUG) {
				code.append("\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
						"\t\t\t\t}\n");

		/* load the aggregate values into their place
		 * aggregates are not currently implemented */
		/*code.append("\t\t\t\tfor (i_agg = 0; i_agg < agg_n; i_agg++) {\n" +
				"\t\t\t\t\taggregates[i_agg].value.i64 = *((int64_t * )(record_buf + (8 * i_agg)));\n" +
				"\t\t\t\t}\n");*/

				code.append("\t\t\t}\n" +
						"\t\t\tion_external_sort_destroy_cursor(&cursor);\n");

				if (this.DEBUG) {
					code.append("\t\t\t/* _CLOSE_ORDERING_FILE(input_file); */\n");
				}

				code.append("\t\t\tif (0 != fclose(input_file)) {\n" +
						"\t\t\t\terror = err_file_close_error;\n" +
						"\t\t\t\tgoto IINQ_QUERY_END;\n" +
						"\t\t\t};\n");

				if (this.DEBUG) {
					code.append("\t\t\t/* end of _CLOSE_ORDERING_FILE(input_file); */\n");
					code.append("\t\t\t/* _REMOVE_ORDERING_FILE(orderby); */\n");
				}


				code.append("\t\t\tif (0 != remove(\"orderby\")) {\n" +
						"\t\t\t\terror = err_file_delete_error;\n" +
						"\t\t\t\tgoto IINQ_QUERY_END;\n" +
						"\t\t\t};\n");

				if (this.DEBUG) {
					code.append("\t\t\t/* end of _REMOVE_ORDERING_FILE(orderby); */\n");
				}

				code.append("\t\t}");
			}

			return code.toString();
		} else
			return "";

}

	/**
	 * Constructs C code to free memory used in the query
	 *
	 * @return C code to free memory
	 */
	public String generateCleanupCode() {

		StringBuilder cleanup = new StringBuilder("\t\tIINQ_QUERY_CLEANUP:\n");
		/* if there are aggregates, groupby, or orderby */
		/* TODO: add if aggregates or groupby when supported */
		if (hasOrderBy()) {
			cleanup.append(
					"\t\t\t\tif (0 != fclose(output_file)) {\n" +
							"\t\t\t\t\terror = err_file_close_error;\n" +
							"\t\t\t\t\tgoto IINQ_QUERY_END;\n" +
							"\t\t\t\t};\n");
		}

				/* free all memory */
		cleanup.append("\t\twhile (NULL != first) {\n" +
				"\t\t\tfirst->reference->cursor->destroy(&first->reference->cursor);\n" +
				"\t\t\tion_close_dictionary(&first->reference->dictionary);\n" +
				"\t\t\tfirst = first->next;\n" +
				"\t\t}\n" +

				/* something went wrong goto end */
				"\t\t\tif (err_ok != error) { goto IINQ_QUERY_END; }\n");

		return cleanup.toString();
	}

	/**
	 * Constructs C code for aggregate expressions
	 *
	 * @return C code needed to execute queries with aggregation
	 */
	public String generateAggregateExprsCode() {
		/* TODO: generate C code for aggregate expressions */
		return "";
	}

	/**
	 * Constructs C code to define the schema for a table. Does not declare fields in the same order as an already existing table.
	 *
	 * @return code to define the schema
	 */
	public String generateSchemaDefinition() {
		if (!hasSchema()) {
			/* Schema wasn't given. Return empty string */
			return "";
		} else {
			/* Schema was given. Define the schema for the table */
			StringBuilder code = new StringBuilder();
			String tableName = getTableName();

			if (this.DEBUG) {
				code.append("\t\t/* DEFINE_SCHEMA(source_name, source_definition) */\n");
			}

			code.append(String.format("\t\tDEFINE_SCHEMA(%s, {\n", tableName));
			Attribute[] attributes = this.relation.getAttributes();
			SourceTable table = this.proj.getDatabase().getDatabase().getSourceTables().get(tableName);
			/* source_definition */
			for (SourceField field : table.getSourceFieldList()) {
				switch (field.getDataTypeName()) {
					case "CHAR":
					case "VARCHAR":
						/* char array data type
						char fieldName[fieldSize];
						 */
						code.append(String.format("\t\t\tchar %s[%d];\n", field.getColumnName(), field.getColumnSize()));
						break;
					case "INTEGER":
						/* int data type
						int fieldname;
						 */
						code.append(String.format("\t\t\tint %s;\n", field.getColumnName()));
						break;
					case "DECIMAL":
						/*  double data type
						double fieldname;
						 */
						code.append(String.format("\t\t\tdouble %s;\n", field.getColumnName()));
				}
			}
			/* Close the struct definition */
			code.append("\t\t});\n");
			return code.toString();
		}
	}

	/**
	 * Constructs the C code for the SELECT clause.
	 *
	 * @return code for the SELECT clause
	 */
	public String generateSelectCode() {


		/* Code generated by macro. Initially skipped but goto COMPUTE_SELECT is called from the nested do-while loop */
		/* TODO: check if the goto can be eliminated (move the select clause to where it won't be skipped)*/
		StringBuilder selectCode = new StringBuilder();
		if (this.DEBUG) {
			selectCode.append("\t\t/* select_clause */\n");
		}

		selectCode.append("\t\tgoto SKIP_COMPUTE_SELECT;\n" +
				"\t\tCOMPUTE_SELECT:;\n");

		if (this.proj.isSelectAll()) {

			if (this.DEBUG) {
				selectCode.append("\t\t/* SELECT_ALL */\n");
			}

			selectCode.append("\t\tmemcpy(result.processed, result.data, result.num_bytes);\n");

		} else {
			/* field list */
			/* TODO: generate code for other SELECT clauses */
			if (this.DEBUG) {
				selectCode.append("\t\t/* SELECT(...) */\n");
			}

			selectCode.append("\t\tdo {\n" +
					"\t\t\tion_iinq_result_size_t select_byte_index = 0;\n");

			String tableName = getTableName();
			/* Key was not copied over in original macro unless specified in the select clause.
			 * We are copying it regardless, otherwise we would have to include the key in the GlobalSchema */
			selectCode.append(String.format("\t\t\tmemcpy(result.processed, %s.key, %s.dictionary.instance->record.key_size);\n" +
					"\t\t\tselect_byte_index += %s.dictionary.instance->record.key_size;\n", tableName, tableName, tableName));

			String fields[] = this.getParameter("fields").split(", ");
			for (int i = 0; i < fields.length; i++) {
				String tuplePointer = getTableName() + "_tuple->" + fields[i];
				/* TODO: use createStackAddress for expressions, not simple field lists
				/*selectCode.append(String.format("\t\t\tmemcpy(result.processed + select_byte_index, %s, sizeof(%s));\n" +
						"\t\t\tselect_byte_index += sizeof(%s);\n", createStackAddress(tuplePointer), tuplePointer, tuplePointer));*/
				selectCode.append(String.format("\t\tmemcpy(result.processed + select_byte_index, &(%s), sizeof(%s));\n" +
						"\t\t\tselect_byte_index += sizeof(%s);\n", tuplePointer, tuplePointer, tuplePointer));

			}
			selectCode.append("\t\t\tresult.num_bytes = select_byte_index;\n" +
					"\t\t} while (0);\n");
		}

		selectCode.append("\t\tgoto DONE_COMPUTE_SELECT;\n" +
				"\t\tSKIP_COMPUTE_SELECT:;\n");

		if (this.DEBUG) {
			selectCode.append("\t\t/* end of select_clause */\n");
		}

		return selectCode.toString();
	}

	/**
	 * Constructs the C code for the WHERE clause.
	 *
	 * @return code for the WHERE filter
	 */
	public String generateWhereCode() throws RequiresSchemaException{

		StringBuilder code = new StringBuilder();

		if (this.parameters.containsKey("filter")) {

			if (this.DEBUG) {
				code.append("\t\t\t\t/* if (!conditions) { continue; } */\n");
			}

			code.append("if (!(");
			Object filter = this.parameters.get("filter");
			if (filter instanceof ArrayList) {
				if (((ArrayList) filter).get(0) instanceof String) {
					ArrayList<String> filterList = (ArrayList<String>) filter;
					/* Loop through each filter in the where clause */
					for (String s : filterList) {
						code.append(generateComparisonCode(s));
					}
				}
			} else if (filter instanceof String) {
				code.append(generateComparisonCode((String) filter));
			}
			/* Remove the unused && */
			code.setLength(code.length() - 4);
			code.append("){\n");
			code.append("\tcontinue;\n}\n");
		}
		return code.toString();
	}

	/**
	 * Constructs the code needed for a comparison filter in the WHERE clause
	 *
	 * @param filter Single filter as created in IinqBuilder
	 * @return C code for a single comparison filter in the WHERE clause
	 */
	public String generateComparisonCode(String filter) throws RequiresSchemaException{
		if (!hasSchema()) {
			throw new RequiresSchemaException("Comparison requires schema. ");
		}
		String tableName = getTableName();
		SourceTable sourceTable = getTable(tableName);
		StringBuilder condition = new StringBuilder();
		String operator = null;
		condition.append("(");
		int i = 0;
		boolean foundOperator = false;
		String fieldName = "";
		while (i < filter.length() && !foundOperator) {
			switch (filter.charAt(i)) {
				case '=':
				case '>':
					foundOperator = true;
					fieldName = filter.substring(0, i);
					break;
				case '!':
					if (filter.charAt(i + 1) == '=') {
						foundOperator = true;
						operator = "!=";
						fieldName = filter.substring(0, i);
						i += 2;
					}
						break;
				case '<':
					foundOperator = true;
					fieldName = filter.substring(0, i);
					if (filter.charAt(i + 1) == '>') {
						operator = "!=";
						i += 2;
					}
						break;
				default:
					i++;
			}
		}
		if (!foundOperator) {
			System.err.println("Operator not found in filter: " + filter);
			return "";
		}
		if (sourceTable.getField(fieldName).getDataTypeName().contains("CHAR")) {
							/* Field has a string data type:
							 * strcmp(source_tuple->field, compareString) > 0 */
			condition.append(String.format("strcmp(%s_tuple->%s", tableName, fieldName));
			if (null == operator) {
								/* operator was not <> or != */
				if (filter.charAt(i + 1) == '=') {
									/* Operator is two characters e.g <=, >= */
					operator = filter.substring(i, i + 2);
					i += 2;
				} else {
									/* Operator is a single character e.g <, >, = */
					if (filter.charAt(i) == '=') {
										/* Equality operator */
						operator = "==";
					} else {
										/* <, > operators */
						operator = filter.substring(i, i + 1);
					}
					i++;
				}
			}
			/* compareString */
			condition.append(String.format(", %s) %s 0)", filter.substring(i), operator));
		} else {
			/* Field has numeric data type:
			 * source_tuple->field > 0 */
			if (null == operator) {
				/* Operator was not <> or != */
				if (filter.charAt(i + 1) == '=') {
					/* Operator is two characters e.g <=, >= */
					operator = filter.substring(i, i + 2);
					i += 2;
				} else {
					/* Operator is a single character e.g <, >, = */
					if (filter.charAt(i) == '=') {
						/* Equality operator */
						operator = "==";
					} else {
						/* <, > operators */
						operator = filter.substring(i, i + 1);
					}
					i++;
				}

			}
			condition.append(String.format("%s_tuple->%s %s %s)", tableName, fieldName, operator, filter.substring((i))));
		}

		/* Close the condition */
		condition.append(") && ");

		return condition.toString();
	}

	/**
	 * Generates C code to declare variables used in either GROUP BY or ORDER BY clauses
	 *
	 * @param name Part of the variable names that will be declared. Should be "groupby" or "orderby"
	 * @return C code that will declare variables for either the GROUP BY or ORDER BY clauses
	 */
	public String generateOrderingDeclare(String name) {
		/* _ORDERING_DECLARE(name) */
		/* 	int name ## _n = 0;
			int					i_ ## name = 0;
			int					total_ ## name ## _size = 0;
			iinq_order_part_t	*name ## _order_parts = NULL; */

		StringBuilder code = new StringBuilder();
		code.append(String.format("\t\tint %s_n = 0;\n", name));
		code.append(String.format("\t\tint i_%s = 0;\n", name));
		code.append(String.format("\t\tint total_%s_size = 0;\n", name));
		code.append(String.format("\t\tiinq_order_part_t *%s_orderparts = NULL;\n", name));

		return code.toString();
	}


	/**
	 * Constructs C code for the GROUP BY clause
	 *
	 * @return code for the GROUP BY clause
	 */
	public String generateGroupByCode() {
		/* GROUPBY_NONE
		*  Gets skipped, not sure if neccessary yet*/
		/*if (!this.parameters.containsKey("stats"))
			return "\t\tgoto IINQ_SKIP_COMPUTE_GROUPBY;\n" +
					"\t\tIINQ_COMPUTE_GROUPBY:; \n" +
					"\t\tgoto IINQ_DONE_COMPUTE_GROUPBY; \n" +
					"\t\tIINQ_SKIP_COMPUTE_GROUPBY:;\n";
		else*/
			/* TODO: add GROUP BY code */
		return "";
	}

	/**
	 * Constructs the C code for the ORDER BY clause.
	 *
	 * @return code for the ORDER BY clause
	 */
	public String generateOrderByCode() {
		/* ORDERBY(...), e.g. ORDERBY(ASCENDING_INT(field_name)) */
		String tableName = getTableName();
		SourceTable sourceTable = getTable(tableName);
		StringBuilder code = new StringBuilder();

		/* TODO: add support for sort modes */
		if (hasOrderBy()) {
			String[] sortFields;
			StringBuilder stackMemory = new StringBuilder();
			/* Split based on delimeters + and - (asc and desc), first element is sort mode */
			sortFields = this.getParameter("sort").split("((?=\\+)|(?=-))");
			code.append(String.format("name_n = %d\n", sortFields.length - 1));
			code.append("total_orderby_size = 0\n");
			for (int i = 1; i < sortFields.length; i++) {
				int direction = (sortFields[i].charAt(0) == '+') ? 1 : -1;
				String sortField = sortFields[i].substring(1);
				SourceField field = sourceTable.getField(sortField);
				String orderType;
				String expr;
				/* Check if the order is a field.
				 * We should only be using field names at this point in development
				 * TODO: add code to handle expressions*/
				if (null == field) {
					orderType = "IINQ_ORDERTYPE_OTHER";
					expr = "";
				} else {
					switch (field.getDataTypeName()) {
						case "INTEGER":
							orderType = "IINQ_ORDERTYPE_INT";
							break;
						case "DECIMAL":
							orderType = "IINQ_ORDERTYPE_FLOAT";
							break;
						default:
							orderType = "IINQ_ORDERTYPE_OTHER";
							break;
					}
					/* Order by column name from schema */
					expr = sourceTable.getTableName() + "_tuple->" + sortField;
				}
				code.append(String.format("orderby_order_parts[%d].direction = %d;\n", i, direction));
				code.append(String.format("orderby_order_parts[%d].size = sizeof();\n", i, sortField));
				code.append(String.format("orderby_order_parts[%d].type = %s;\n", i, orderType));
				code.append(String.format("total_orderby_size += orderby_order_parts[%d].size;\n", i));

					/* Declare a space in the memory big enough for for an expression
					*  _CREATE_MEMCPY_STACK_ADDRESS_FOR_NUMERICAL_EXPRESSION(expr) */
				if (this.DEBUG) {
					stackMemory.append(String.format("\t/* orderby_order_parts[%d].pointer = CREATE_MEMCPY_STACK_ADDRESS_FOR_NUMERICAL_EXPRESSION(%s)\n", i, expr));
				}
				stackMemory.append(String.format("\torderby_order_parts[%d].pointer = (\n", i));
				stackMemory.append(createStackAddress(expr));

			}
			/* Skipped but then gone back to later.
			*  Could potentially move this later in the code */
			code.append("\tgoto IINQ_SKIP_COMPUTE_ORDERBY;\n");
			code.append("\tIINQ_COMPUTE_ORDERBY:;\n");
			code.append(stackMemory.toString());
			code.append("\tgoto IINQ_DONE_COMPUTE_ORDERBY;\n");
			code.append("\tIINQ_SKIP_COMPUTE_ORDERBY:;");
		}

		return code.toString();
	}

	public String createStackAddress(String expr) {
		return String.format("(\n\t\t8 == sizeof(%s) ? (void *) (&(uint64_t) { (%s) }\n" +
				"\t\t\t) : \n" +
				"\t\t\t(\n" +
				"\t\t\t\t4 == sizeof(%s) ? (void *) (&(uint32_t) { (%s) }\n" +
				"\t\t\t\t) :\n" +
				"\t\t\t\t(\n" +
				"\t\t\t\t\t2 == sizeof(%s) ? (void *) (&(uint16_t) { (%s) } \n" +
				"\t\t\t\t) : (void *) (&(uint8_t) { (%s) } \n" +
				"\t\t\t\t) \n" +
				"\t\t\t) \n" +
				"\t\t) \n" +
				"\t)", expr, expr, expr, expr, expr, expr, expr);

}


	/**
	 * Retrieves the name of the relation for the query.
	 *
	 * @return name of the relation
	 */
	public String getTableName() {
		/* TODO: add support for multiple tables */
		return this.getRelation().getProperty("name");
	}

	/**
	 * Retrieves the SourceTable with a given name.
	 *
	 * @param tableName name of the table to retrieve
	 * @return SourceTable with the given name
	 */
	public SourceTable getTable(String tableName) {
		return this.proj.getDatabase().getDatabase().getSourceTables().get(tableName);
	}
}