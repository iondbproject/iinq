package iinq.functions;

public class DropTableFunction extends IinqFunction {
	public DropTableFunction() {
		super("drop_table",
				"ion_err_t drop_table(iinq_table_id *tableId);\n",
				"ion_err_t drop_table(iinq_table_id *tableId) {\n\n" +
						"\tion_err_t error;\n\n" +
						"\terror = iinq_drop(tableId);" +
						"\treturn error;\n" +
						"\n}\n\n");
	}
}
