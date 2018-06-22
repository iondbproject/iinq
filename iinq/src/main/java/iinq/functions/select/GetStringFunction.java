package iinq.functions.select;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

class GetStringFunction extends IinqFunction {
	public GetStringFunction() {
		super("getString",
				"char* getString(iinq_result_set *select, int field_num);\n",
				"char* getString(iinq_result_set *select, int field_num) {\n" +
						"\tiinq_field_num_t i, count = 0;\n\n" +
						"\tion_err_t                  error;\n" +
						"\tion_dictionary_t           dictionary;\n" +
						"\tion_dictionary_handler_t   handler;\n\n" +
						"\tdictionary.handler = &handler;\n\n" +
						"\tselect->status.error = ion_init_master_table();\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_STRING) +
						"\tselect->status.error              = ion_open_dictionary(&handler, &dictionary, select->id);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_STRING) +
						"\tselect->status.error = ion_close_master_table();\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_STRING) +
						"\tdictionary_get(&dictionary, IONIZE(select->status.count,int), select->value);\n\n" +
						"\terror = ion_close_dictionary(&dictionary);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_STRING) +

						"\treturn (char*) (select->value + select->offset[field_num-1]);\n}\n\n");
	}
}
