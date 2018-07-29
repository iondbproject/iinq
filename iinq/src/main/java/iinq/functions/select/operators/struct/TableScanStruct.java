package iinq.functions.select.operators.struct;

import java.util.Arrays;
import java.util.LinkedHashSet;

public class TableScanStruct extends OperatorStruct {
	public TableScanStruct() {
		super("iinq_table_scan",
				new LinkedHashSet<>(Arrays.asList(
						new StructMember("dictionary", "ion_dictionary_t"),
						new StructMember("handler", "ion_dictionary_handler_t"),
						new StructMember("cursor", "ion_dict_cursor_t *"),
						new StructMember("predicate", "ion_predicate_t"),
						new StructMember("record", "ion_record_t")
				)));
	}
}
