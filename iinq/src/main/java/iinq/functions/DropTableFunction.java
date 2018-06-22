package iinq.functions;

public class DropTableFunction extends IinqFunction {
	public DropTableFunction() {
		super("drop_table",
				"void drop_table(iinq_table_id *tableId);\n",
				"void drop_table(iinq_table_id *tableId) {\n\n" +
						"\tion_err_t error;\n\n" +
						"\terror = iinq_drop(tableId);" +
						CommonCode.error_check() +
						"\tprintf(\"Table %d has been deleted." + "\\" + "n" + "\", tableId);" +
						"\n}\n\n");
	}
}
