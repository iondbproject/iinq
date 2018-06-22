package iinq.functions.select;

import iinq.functions.IinqFunction;

class NextFunction extends IinqFunction {
	public NextFunction() {
		super("next",
				"ion_boolean_t next(iinq_result_set *select);\n",
				"ion_boolean_t next(iinq_result_set *select) {\n" +
						"\tif (select->status.count < select->num_recs-1) {\n" +
						"\t\tselect->status.count++;\n" +
						"\t\treturn boolean_true;\n\t}\n\n" +
						"\tselect->status.error = ion_init_master_table();\n" +
						"\tif (err_ok != select->status.error)\n" +
						"\t\treturn boolean_false;\n\n" +
						"\tion_dictionary_t dictionary;\n" +
						"\tion_dictionary_handler_t handler;\n" +
						"\tdictionary.handler = &handler;\n" +
						"\tselect->status.error = ion_open_dictionary(&handler, &dictionary, select->id);\n" +
						"\tif (err_ok != select->status.error)\n" +
						"\t\treturn boolean_false;\n\n" +
						"\tselect->status.error = ion_delete_dictionary(&dictionary, select->id);\n" +
						"\tif (err_ok != select->status.error)\n" +
						"\t\treturn boolean_false;\n\n" +
						"\tselect->status.error = ion_close_master_table();\n" +
						"\tif (err_ok != select->status.error)\n" +
						"\t\treturn boolean_false;\n\n" +
						"\tfree(select->value);\n" +
						"\tfree(select->fields);\n" +
						"\treturn boolean_false;\n}\n\n");
	}
}
