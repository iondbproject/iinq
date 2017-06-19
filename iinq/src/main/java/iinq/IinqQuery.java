package iinq;

import unity.generic.query.WebQuery;


/**
 * Stores a representation of an IINQ query.
 */
public class IinqQuery extends WebQuery
{    		
	/**
	 * Constructs an IINQ query.
	 * 
	 * @param url
	 * 		query URL 
	 */
	public IinqQuery(String url)
	{
	    super(url);			
	}    
	
	/**
	 * Generates the C code for this SQL query.
	 * 
	 * @return
	 * 		code to execute query
	 */
	public String generateCode()
	{
		StringBuilder code = new StringBuilder();

		/* Included for all queries */
		code.append(
				"\tdo {\n" +
				"\t\tion_err_t error;\n" +
				"\t\tion_iinq_result_t result;\n" +
				"\t\tint jmp_r;\n" +
				"\t\tjmp_buf selectbuf;\n" +
				"\t\tresult.raw_record_size = 0;\n" +
				"\t\tresult.num_bytes = 0;\n"
		);

		code.append(this.getParameter("from"));

		/* SELECT_ALL */
		if (this.proj.isSelectAll()) {
			code.append(
					"\t\tgoto SKIP_COMPUTE_SELECT;\n" +
					"\t\tCOMPUTE_SELECT:;\n" +
					"\t\tmemcpy(result.processed, result.data, result.num_bytes);\n" +
					"\t\tgoto DONE_COMPUTE_SELECT;\n" +
					"\t\tSKIP_COMPUTE_SELECT:;\n");
		}

		/* _FROM_ADVANCE_CURSORS */
		code.append(
				"\t\twhile (1) {\n" +
				"\t\t\tif (NULL == ref_cursor) { break; }\n" +
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
				"\t\t\tif (NULL == ref_cursor) { break; } else if (last_cursor != ref_cursor) { ref_cursor = last; }\n"
		);

		if (this.parameters.containsKey("filter")) {
			/* if (!condition) { continue; } */
		}

		code.append(
				"\t\t\tjmp_r = setjmp(selectbuf);\n" +
				"\t\t\tgoto COMPUTE_SELECT;\n" +
				"\t\t\tDONE_COMPUTE_SELECT:;\n" +
				"\t\t\t(&processor)->execute(&result, (&processor)->state);\n" +
				"\t\t}\n" +
				"\t\tIINQ_QUERY_CLEANUP:\n" +
				"\t\twhile (NULL != first) {\n" +
				"\t\t\tfirst->reference->cursor->destroy(&first->reference->cursor);\n" +
				"\t\t\tion_close_dictionary(&first->reference->dictionary);\n" +
				"\t\t\tfirst = first->next;\n" +
				"\t\t}\n" +
				"\t}\n" +
				"\twhile (0);"
		);


		return code.toString();
	}
}