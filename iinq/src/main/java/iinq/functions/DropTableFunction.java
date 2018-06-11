package iinq.functions;

public class DropTableFunction extends IinqFunction {
	public DropTableFunction() {
		super("drop_table",
				"void drop_table(iinq_table_id *table_id);\n",
				"void drop_table(iinq_table_id *table_id) {\n\n" +
						"\tion_err_t error;\n\n" +
						"\terror = iinq_drop(table_id);" +
						CommonCode.error_check() +
						"\tprintf(\"Table %d has been deleted." + "\\" + "n" + "\", table_id);" +
						"\n}\n\n");
	}
}
