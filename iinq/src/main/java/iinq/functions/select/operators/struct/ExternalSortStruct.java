package iinq.functions.select.operators.struct;

import java.util.Arrays;
import java.util.LinkedHashSet;

public class ExternalSortStruct extends OperatorStruct {
	public ExternalSortStruct() {
		super("iinq_external_sort",
				new LinkedHashSet<>(Arrays.asList(
						new StructMember("es", "ion_external_sort_t *"),
						new StructMember("buffer", "char *"),
						new StructMember("record_buf", "char *"),
						new StructMember("cursor", "ion_external_sort_cursor_t *")
				)));
	}
}
