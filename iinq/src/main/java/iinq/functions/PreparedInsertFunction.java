package iinq.functions;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.metadata.IinqDatabase;
import iinq.metadata.IinqTable;
import unity.jdbc.UnityPreparedStatement;
import unity.query.GQFieldRef;
import unity.query.GlobalUpdate;
import unity.query.LQExprNode;
import unity.query.LQInsertNode;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;

import static iinq.functions.SchemaKeyword.*;

public class PreparedInsertFunction extends IinqFunction {
	private static boolean setParamsWritten = false;
	private GlobalUpdate globalUpdate;
	private IinqDatabase iinqDatabase;
	private boolean preparedStatement;

	public PreparedInsertFunction(GlobalUpdate globalUpdate, IinqDatabase iinqDatabase) throws SQLException, InvalidArgumentException {
		this.iinqDatabase = iinqDatabase;
		this.globalUpdate = globalUpdate;
		generatePreparedFunction();
	}

	private void generatePreparedFunction() throws InvalidArgumentException, SQLException {
		StringBuilder header = new StringBuilder();
		StringBuilder definition = new StringBuilder();

		LQInsertNode insertNode = (LQInsertNode) globalUpdate.getPlan().getLogicalQueryTree().getRoot();
		IinqTable table = iinqDatabase.getIinqTable(insertNode.getSourceTable().getTable().getTableName());

		this.setName("iinq_insert_" + table.getTableId());

		/* Number of fields specified for the insert */
		int count = insertNode.getInsertFields().size();
		int totalFields = table.getNumFields();

		boolean[] prep_fields = new boolean[count];

		/* Check if the INSERT statement is a prepared statement */
		UnityPreparedStatement stmt = iinqDatabase.prepareUnityStatement(insertNode.generateSQL());
		preparedStatement = stmt.getParameters().size() > 0;

		String[] fields = new String[count];

		ArrayList<Integer> keyIndices = table.getPrimaryKeyIndices();

		header.append("iinq_prepared_sql *");
		header.append(getName());
		header.append("(");

		for (int j = 1; j <= totalFields; j++) {
			if (table.getFieldType(j) == Types.INTEGER) {
				header.append("int *value_").append(j);
			} else {
				header.append("char *value_").append(j);
			}
			header.append(", ");
		}

		header.setLength(header.length()-2);

		// TODO: can this be combined with the other for loop?
		for (int j = 0; j < count; j++) {
			GQFieldRef fieldNode = insertNode.getInsertFields().get(j);
			fields[j] = ((LQExprNode) insertNode.getInsertValues().get(j)).getContent().toString();

			prep_fields[j] = fields[j].equals("?");
		}

		header.append(")");
		definition.append(header.toString());
		header.append(";\n");
		definition.append(" {\n");
		definition.append("\tiinq_prepared_sql *p = malloc(sizeof(iinq_prepared_sql));\n");
		definition.append("\tif(NULL == p) {\n" +
				"\t\treturn NULL;\n" +
				"\t}\n\n");
		definition.append("\tp->table = ").append(table.getTableId()).append(";\n");
		definition.append(String.format("\tp->value = malloc(%s + IINQ_BITS_FOR_NULL(%d));\n", table.generateIonValueSize(), totalFields));
		definition.append("\tif (NULL == p->value) {\n" +
				"\t\tfree(p);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n\n");
		definition.append("\tiinq_null_indicator_t *null_indicators = p->value;\n");
		definition.append(String.format("\tunsigned char *data = ((char *) p->value + IINQ_BITS_FOR_NULL(%d));\n", table.getNumFields()));
		definition.append(String.format("\tp->key = malloc(%s);\n", table.generateIonKeySize()));
		definition.append("\tif (NULL == p->key) {\n" +
				"\t\tfree(p->value);\n" +
				"\t\tfree(p);\n" +
				"\t\treturn NULL;\n" +
				"\t}\n\n");

		Iterator<Integer> it = keyIndices.iterator();
		StringBuilder offset = new StringBuilder();
		offset.append("0");
		while (it.hasNext()) {
			int keyField = it.next();
			String fieldSize = table.getIonFieldSize(keyField);
			definition.append(String.format("\tif(NULL != value_%d) {\n", keyField));
			if (table.getFieldType(keyField) == Types.INTEGER) {
				definition.append(String.format("\t\t* (int *) ((char*)p->key+%s) = NEUTRALIZE(value_%d, int);\n", offset, keyField));
			} else {
				definition.append(String.format("\t\tstrncpy((char *)p->key+%s, value_%d, %s);\n", offset.toString(), keyField, fieldSize));
			}
			definition.append("\t}\n\n");
			offset.append("+").append(fieldSize);
		}

		for (int i = 1; i <= totalFields; i++) {
			int fieldType = table.getFieldType(i);
			String fieldSize = table.getIonFieldSize(i);

			definition.append(String.format("\tif (NULL == value_%d) {\n", i));
			definition.append(String.format("\t\tiinq_set_null_indicator(null_indicators, %d);\n", i));
			definition.append("\t} else {\n");
			definition.append(String.format("\t\tiinq_clear_null_indicator(null_indicators, %d);\n", i));

			// TODO: add support for byte arrays
			switch (fieldType) {
				case Types.CHAR:
				case Types.VARCHAR:
					definition.append(String.format("\t\tstrncpy(data, value_%d, %s);\n", i, fieldSize));
					break;
				case Types.INTEGER:
					definition.append(String.format("\t\t*(int *) data = NEUTRALIZE(value_%d, int);\n", i));
					break;
				default:
					throw new SQLFeatureNotSupportedException("Non-supported data type encountered.");
			}

			if (i < totalFields) {
				definition.append(String.format("\tdata += %s;\n\n", fieldSize));
			}
			definition.append("\t}\n");
		}

		definition.append("\n\treturn p;\n");
		definition.append("}\n\n");

		setDefinition(definition.toString());
		setHeader(header.toString());
	}

	public boolean isPreparedStatement() {
		return preparedStatement;
	}

	public static boolean isSetParamsWritten() {
		return setParamsWritten;
	}
}