package iinq.metadata;

import unity.annotation.AnnotatedSourceTable;
import unity.annotation.GlobalSchema;
import unity.annotation.SourceTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class IinqSchema extends GlobalSchema {
	protected HashSet<String> iinqTableIdentifiers = new HashSet<>();

	public IinqSchema() {
		super();
	}


	public AnnotatedSourceTable getNewlyCreatedTable(String databaseName) {
		AnnotatedSourceTable table = null;
		for (Map.Entry<String, ArrayList<AnnotatedSourceTable>> entry: this.tableIdentifiers.entrySet()) {
			if (!this.iinqTableIdentifiers.contains(entry.getKey())) {
				iinqTableIdentifiers.add(entry.getKey());
				// TODO: can a table identifier id more than one table (ArrayList)?
				table = entry.getValue().get(0);
			}
		}
		return table;
	}
}
