package iinq.functions.select.operators.destroy;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class TableScanDestroyFunction extends OperatorDestroyFunction {
	public TableScanDestroyFunction() {
		super("iinq_destroy_table_scan",
				"void iinq_destroy_table_scan(iinq_result_set **result_set);\n",
				"void iinq_destroy_table_scan(iinq_result_set **result_set) {\n" +
						CommonCode.freeMemory("(*result_set)->record.key") +
						CommonCode.freeMemory("(*result_set)->record.value") +
						"\tif ((*result_set)->dictionary_ref.temp_dictionary) {\n" +
						"\t\tion_init_master_table();\n" +
						"\t\tion_delete_dictionary(&(*result_set)->dictionary_ref.dictionary, (*result_set)->dictionary_ref.dictionary.instance->id);\n" +
						"\t} else {\n" +
						"\t\tion_close_dictionary(&(*result_set)->dictionary_ref.dictionary);\n" +
						"\t}\n\n" +
						CommonCode.destroyCursor("(*result_set)->dictionary_ref.cursor") +
						CommonCode.freeMemory("(*result_set)->offset") +
						"\tfree(*result_set);\n" +
						"\t*result_set = NULL;\n" +
						"}\n\n"

		);
	}
}
