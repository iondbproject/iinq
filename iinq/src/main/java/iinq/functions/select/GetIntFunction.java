package iinq.functions.select;

import iinq.functions.CommonCode;
import iinq.functions.IinqFunction;

public class GetIntFunction extends IinqFunction {
	public GetIntFunction() {
		super("getInt",
				"int getInt(iinq_result_set *select, int field_num);\n",
				"int getInt(iinq_result_set *select, int field_num) {\n" +
						"\tiinq_field_num_t i;\n\n" +
						"\tion_err_t                  error;\n" +
						"\tion_dictionary_t           dictionary;\n" +
						"\tion_dictionary_handler_t   handler;\n\n" +
						"\tdictionary.handler = &handler;\n\n" +
						"\tselect->status.error = ion_init_master_table();\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_NUMERIC) +
						"\tselect->status.error              = ion_open_dictionary(&handler, &dictionary, select->id);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_NUMERIC) +
						"\tselect->status.error = ion_close_master_table();\n" +
						"\tdictionary_get(&dictionary, IONIZE(select->status.count,int), select->value);\n\n" +
						"\tselect->status.error = ion_close_dictionary(&dictionary);\n\n" +
						CommonCode.errorCheckWithReturn("select->status.error", CommonCode.ReturnType.EMPTY_NUMERIC) +

						"\treturn NEUTRALIZE(select->value + select->offset[field_num-1], int);\n}\n\n");
	}
}
