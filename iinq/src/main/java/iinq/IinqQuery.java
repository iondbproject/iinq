package iinq;

import com.sun.xml.internal.bind.v2.TODO;
import unity.annotation.SourceField;
import unity.annotation.SourceTable;
import unity.engine.Attribute;
import unity.generic.query.WebQuery;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.jar.Attributes;


/**
 * Stores a representation of an IINQ query.
 */
public class IinqQuery extends WebQuery {
	// Enables comments with in code output
	private static final boolean DEBUG = true;
	private HashMap<String, Object> queryCode;

	public void writeQueryToSource(String source) throws IOException {
		File file = new File(source);
		File temp = File.createTempFile("temp-source-file", ".tmp");
		BufferedReader in = new BufferedReader(new FileReader(file));
		PrintWriter out = new PrintWriter(new FileWriter(temp));
		String line, trimmedLine;
		while ((line = in.readLine()) != null) {
			trimmedLine = line.trim();
			// Skip comments & #includes
			if (trimmedLine.isEmpty() || trimmedLine.charAt(0) == '#' || trimmedLine.substring(0, 2).equals("//")) {
				// empty line, preprocessor command, or single line comment
				out.println(line);
				continue;
			} else if (trimmedLine.substring(0, 1).equals("/*")) {
				// multi line comment
				Boolean multiLineComment = true;
				while (multiLineComment) {
					if (trimmedLine.contains("*/")) {
						multiLineComment = false;
					} else {
						line = in.readLine();
						trimmedLine = line.trim();
					}
					out.println(line);
				}
			} else {
				// end of comments and #includes
				break;
			}
		}

		// write out predicate definition
		PredicateFunction predicateFunction;
		if ((predicateFunction = (PredicateFunction) this.queryCode.get("predicate")) != null) {
			out.println(predicateFunction.getDefinition());
		}

		out.println(line);

		// continue until we get to the init function
		while ((line = in.readLine()) != null) {
			trimmedLine = line.trim();
			if (trimmedLine.startsWith("init(")) {
				// print out the line as a comment
				out.println("/* LINE REPLACED: " + line.trim() + " */");
				// get the name of the iterator to be initialized
				String iteratorName = trimmedLine.substring(trimmedLine.indexOf("(") + 1, trimmedLine.indexOf(","));
				// replace the line with the appropriate init function
				out.println(String.format("%s(%s, %s, %s, %s);", (String) this.queryCode.get("select_init"), iteratorName, (String) this.queryCode.get("table_name"), (String) this.queryCode.get("next"), (predicateFunction == null) ? null : predicateFunction.getName()));

			} else {
				out.println(line);
			}
		}

		in.close();
		out.close();
		file.delete();
		temp.renameTo(file);
	}

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
	 * Checks whether the query has SELECT * as its SELECT clause.
	 *
	 * @return true if it is SELECT *, false if it is not
	 */
	public boolean isSelectAll() {
		return ((String) this.parameters.get("fields")).equals("*");
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
	 * Generates the C code for a predicate definition.
	 *
	 * @param identifier A number added to the end of the function name to make it unique.
	 * @return code to evaluate a predicate
	 */
	public PredicateFunction generatePredicateFunction(int identifier) throws RequiresSchemaException {

		String name = String.format("generatedPredicate%d", identifier);
		// First line will be the return type for the predicate
		StringBuilder definition = new StringBuilder(String.format("ion_boolean_t\n" +
				// Next line is the function name and parameter list
				"%s(iinq_iterator_t *it) {\n", name));

		definition.append("\tif (!(");
		Object filter = this.parameters.get("filter");
		if (filter instanceof ArrayList) {
			if (((ArrayList) filter).get(0) instanceof String) {
				ArrayList<String> filterList = (ArrayList<String>) filter;
					/* Loop through each filter in the where clause */
				for (String s : filterList) {
					definition.append(generateComparisonCode(s));
				}
			}
		} else if (filter instanceof String) {
			definition.append(generateComparisonCode((String) filter));
		}
		definition.setLength(definition.length() - 3);
		definition.append("{\n" +
				"\t\treturn boolean_false;\n" +
				"\t} else {\n" +
				"\t\treturn boolean_true;\n" +
				"\t}\n" +
				"}\n\n");


		return new PredicateFunction(name, definition.toString());
	}

	/**
	 * Generates the C code for this SQL query.
	 *
	 * @return code to execute query
	 */
	public HashMap<String, Object> generateCode() {
		/* Currently follows similar structure to MATERIALIZED_QUERY macro		 */
		String tableName = getTableName();
		StringBuilder code = new StringBuilder();
		HashMap<String, Object> returnValue = new HashMap<>();

		// first thing we will need is the table name for the query
		returnValue.put("table_name", this.parameters.get("source"));

		// if there is a predicate, create a function definietion for it
		if (this.parameters.containsKey("filter")) {
			try {
				returnValue.put("predicate", generatePredicateFunction(0));
			} catch (RequiresSchemaException e) {
				e.printStackTrace();
			}
		}

		// is select all
		if (isSelectAll()) {
			if (hasOrderBy()) {

			} else if (hasGroupBy()) {

			} else {
				// No sorting, projection is done on the table directly
				returnValue.put("select_init", "iinq_query_init_select_all_from_table");
			}
		} else {
			// select field list
			if (hasOrderBy()) {

			} else if (hasGroupBy()) {

			} else {
				// No sorting, projection is done on the table directly
				// Split the field list into an array
				String fields[] = this.getParameter("fields").split(", ");
				// Field list will be an iinq_field_list_t array. We use a macro in the C code to improve readibility.
				StringBuilder fieldList = new StringBuilder("IINQ_FIELD_LIST(");
				for (int i = 0; i < fields.length; i++) {
				/* First number is table index (hardcoded as 0 since multiple tables are unsupported)
				 * Second number is the field number of that table (requires the ordinal positions in the schema to be correct) */
					fieldList.append(String.format("{ 0, %d }, ", this.getRelation().getAttributeIndexByName(fields[i])));
				}
				// remove the last comma
				fieldList.setLength(fieldList.length() - 2);
				// close off the field list
				fieldList.append(")");
				returnValue.put("field_list", fieldList.toString());
				returnValue.put("select_init", "iinq_query_init_select_field_list_from_table");
			}
		}

		return this.queryCode = returnValue;
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

			code.append("\t\t\tif (err_ok != (error = ion_external_sort_init(&es, input_file, &context, iinq_sort_compare,\n");
			/* raw_record_size does not work with SELECT field list FROM table WHERE ORDERBY (doesn't take into account the fields/padding removed), */
			/*		"\t\t\t\t\t\t\t\t\t\t\t\t\t\t  (agg_n > 0 ? result.num_bytes : result.raw_record_size),\n" +
					"\t\t\t\t\t\t\t\t\t\t\t\t\t\t  (agg_n > 0 ? result.num_bytes : result.raw_record_size) +\n" +*/
			code.append("\t\t\t\t\t\t\t\t\t\t\t\t\t\t result.num_bytes, result.num_bytes +\n" +
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
	public String generateWhereCode() throws RequiresSchemaException {

		StringBuilder code = new StringBuilder();

		if (this.parameters.containsKey("filter")) {

			if (this.DEBUG) {
				code.append("\t\t\t/* if (!conditions) { continue; } */\n");
			}

			code.append("\t\t\tif (!(");
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
			code.append("\t\t\tcontinue;\n}\n");
		}
		return code.toString();
	}

	/**
	 * Constructs the code needed for a comparison filter in the WHERE clause
	 *
	 * @param filter Single filter as created in IinqBuilder
	 * @return C code for a single comparison filter in the WHERE clause
	 */
	public String generateComparisonCode(String filter) throws RequiresSchemaException {
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
		SourceField field = sourceTable.getField(fieldName);
		int fieldIndex = this.getRelation().getAttributeIndexByName(fieldName);
		if (field.getDataTypeName().contains("CHAR")) {
							/* Field has a string data type:
							 * strcmp(source_tuple->field, compareString) > 0 */
			condition.append(String.format("strcmp(get_string(*it, %d)", fieldIndex));
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
			condition.append(String.format("get_int(*it, %d) %s %s", fieldIndex, operator, filter.substring((i))));
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

		String code = String.format("\t\tint %s_n = 0;\n", name) +
				String.format("\t\tint i_%s = 0;\n", name) +
				String.format("\t\tint total_%s_size = 0;\n", name) +
				String.format("\t\tiinq_order_part_t *%s_order_parts = NULL;\n", name);

		return code;
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
			ArrayList<String> sortFields;
			StringBuilder stackMemory = new StringBuilder();
			/* Split based on delimeters + and - (asc and desc), first element is sort mode */
			sortFields = new ArrayList<String>(Arrays.asList(this.getParameter("sort").split("((?=\\+)|(?=-))")));
			/* Remove the sort mode,
			 * allows us to index from 0 for the next part */
			sortFields.remove(0);
			code.append(String.format("\t\torderby_n = %d;\n", sortFields.size()));
			code.append("\t\ttotal_orderby_size = 0;\n");
			code.append(String.format("\t\torderby_order_parts = alloca(sizeof(iinq_order_part_t)*%d);\n", sortFields.size()));
			for (int i = 0, n = sortFields.size(); i < n; i++) {
				int direction = (sortFields.get(i).charAt(0) == '+') ? 1 : -1;
				String sortField = sortFields.get(i).substring(1);
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
				code.append(String.format("\t\torderby_order_parts[%d].direction = %d;\n", i, direction));
				code.append(String.format("\t\torderby_order_parts[%d].size = sizeof(%s);\n", i, expr));
				code.append(String.format("\t\torderby_order_parts[%d].type = %s;\n", i, orderType));
				code.append(String.format("\t\ttotal_orderby_size += orderby_order_parts[%d].size;\n", i));

					/* Declare a space in the memory big enough for for an expression
					*  _CREATE_MEMCPY_STACK_ADDRESS_FOR_NUMERICAL_EXPRESSION(expr) */
				if (this.DEBUG) {
					//stackMemory.append(String.format("\t/* orderby_order_parts[%d].pointer = CREATE_MEMCPY_STACK_ADDRESS_FOR_NUMERICAL_EXPRESSION(%s) */\n", i, expr));
				}
				stackMemory.append(String.format("\t\torderby_order_parts[%d].pointer = ", i));
				/* TODO: implement orderby for expressions */
				// stackMemory.append(createStackAddress(expr));
				stackMemory.append(String.format("&(%s_tuple->%s);\n", tableName, sortField));

			}
			/* Skipped but then gone back to later.
			*  Could potentially move this later in the code */
			code.append("\t\tgoto IINQ_SKIP_COMPUTE_ORDERBY;\n");
			code.append("\t\tIINQ_COMPUTE_ORDERBY:;\n");
			code.append(stackMemory.toString());
			code.append("\t\tgoto IINQ_DONE_COMPUTE_ORDERBY;\n");
			code.append("\t\tIINQ_SKIP_COMPUTE_ORDERBY:;");
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
		this.getRelation();
		return this.proj.getDatabase().getDatabase().getSourceTables().get(tableName);
	}
}