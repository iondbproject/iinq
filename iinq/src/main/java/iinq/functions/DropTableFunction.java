package iinq.functions;

public class DropTableFunction extends IinqFunction {
	public DropTableFunction() {
		super("drop_table",
				"ion_err_t drop_table(iinq_table_id_t table_id);\n",
				"ion_err_t drop_table(iinq_table_id_t table_id) {\n\n" +
						"\tion_dictionary_t dictionary;\n" +
						"\tion_dictionary_handler_t handler;\n" +
						"\tion_err_t error;\n\n" +
						"\terror = iinq_open_source(table_id, &dictionary, &handler);\n\n" +
						"\tif (err_ok != error)\n" +
						"\t\treturn error;\n\n" +
						"\tion_close_dictionary(&dictionary);\n" +
						"\terror = iinq_drop(table_id);\n" +
						"\treturn error;\n" +
						"\n}\n\n");
	}
}
