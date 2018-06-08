package iinq.functions;

public class DropTableFunction extends IinqFunction {
	public DropTableFunction() {
		super("drop_table",
				"void drop_table(char *table_name);\n",
				"void drop_table(char *table_name) {\n\n" +
						"\tion_err_t error;\n\n" +
						"\terror = iinq_drop(table_name);" +
						CommonCode.error_check() +
						"\tprintf(\"Table %s has been deleted." + "\\" + "n" + "\", table_name);" +
						"\n}\n\n");
	}
}
