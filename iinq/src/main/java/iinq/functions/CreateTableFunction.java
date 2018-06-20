package iinq.functions;

import static iinq.functions.CommonCode.*;

public class CreateTableFunction extends IinqFunction {
	public CreateTableFunction() {
		super("create_table", "void create_table(iinq_table_id table_id, ion_key_type_t key_type, ion_key_size_t key_size, ion_value_size_t project_size);\n",
				"void create_table(iinq_table_id table_id, ion_key_type_t key_type, ion_key_size_t key_size, ion_value_size_t project_size) {\n" +
				"\tion_err_t error = iinq_create_source(table_id, key_type, key_size, project_size);\n" +
				error_check() + "}\n\n");
	}
}
