package iinq.functions.SelectFunctions;

import iinq.functions.IinqFunction;

public class NextFunction extends IinqFunction {
	public NextFunction() {
		super("next",
				"ion_boolean_t next(iinq_result_set *select);\n",
				"ion_boolean_t next(iinq_result_set *select) {\n" +
						"\tif (*(int *) select->count < (*(int *) select->num_recs) - 1) {\n" +
						"\t\t*(int *) select->count = (*(int *) select->count) + 1;\n" +
						"\t\treturn boolean_true;\n\t}\n\n" +
						"\tion_err_t error = iinq_drop(255);\n\n" +
						"\tif (err_ok != error) {\n" +
						"\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n" +
						"\t}\n\n" +
						"\tfree(select->value);\n" +
						"\tfree(select->fields);\n" +
						"\tfree(select->count);\n" +
						"\tfree(select->table_id);\n" +
						"\tfree(select->num_recs);\n" +
						"\tfree(select->num_fields);\n" +
						"\treturn boolean_false;\n}\n\n");
	}
}
