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

		header.append("iinq_prepared_sql ");
		header.append(getName());
		header.append("(");

		for (int j = 1; j <= totalFields; j++) {
			if (table.getFieldType(j) == Types.INTEGER) {
				header.append("int value_").append(j);
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
		definition.append("\tiinq_prepared_sql p = {0};\n");

		definition.append("\tp.table = ").append(table.getTableId()).append(";\n");
		definition.append("\tp.value = malloc(").append(table.getSchemaValue(VALUE_SIZE)).append(");\n");
		definition.append("\tunsigned char\t*data = p.value;\n");
		definition.append("\tp.key = malloc(").append(table.generateIonKeySize()).append(");\n");

		Iterator<Integer> it = keyIndices.iterator();
		StringBuilder offset = new StringBuilder();
		offset.append("0");
		while (it.hasNext()) {
			int keyField = it.next();
			String fieldSize = table.getIonFieldSize(keyField);
			if (table.getFieldType(keyField) == Types.INTEGER) {
				definition.append(String.format("\t*(int *) (p.key+%s) = value_%d;\n", offset, keyField));
			} else {
				definition.append(String.format("\tstrncpy(p.key+%s, value_%d, %s);\n", offset.toString(), keyField, fieldSize));
			}
			offset.append("+").append(fieldSize);
		}

		definition.append("\n");

		for (int i = 1; i <= totalFields; i++) {
			int fieldType = table.getFieldType(i);
			String fieldSize = table.getIonFieldSize(i);

			// TODO: add support for byte arrays
			switch (fieldType) {
				case Types.CHAR:
				case Types.VARCHAR:
					definition.append(String.format("\tstrncpy(data, value_%d, %s);\n", i, fieldSize));
					break;
				case Types.INTEGER:
					definition.append(String.format("\t*(int *) data = value_%d;\n", i));
					break;
				default:
					throw new SQLFeatureNotSupportedException("Non-supported data type encountered.");
			}

			if (i < totalFields) {
				definition.append(String.format("\tdata += %s;\n\n", fieldSize));
			}
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