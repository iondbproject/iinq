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
import java.sql.Types;

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

		boolean[] prep_fields = new boolean[count];

		/* Check if the INSERT statement is a prepared statement */
		UnityPreparedStatement stmt = iinqDatabase.prepareUnityStatement(insertNode.generateSQL());
		preparedStatement = stmt.getParameters().size() > 0;

		String[] fields = new String[count];
		String field_type;

		String key_type = table.getSchemaValue(PRIMARY_KEY_TYPE);
		String key_field_num = table.getSchemaValue(PRIMARY_KEY_FIELD);

		header.append("iinq_prepared_sql ");
		header.append(getName());
		header.append("(");

		// TODO: can this be combined with the other for loop?
		for (int j = 0; j < count; j++) {
			GQFieldRef fieldNode = insertNode.getInsertFields().get(j);
			fields[j] = ((LQExprNode) insertNode.getInsertValues().get(j)).getContent().toString();

			field_type = fieldNode.getField().getDataTypeName();

			if (j > 0) {
				header.append(", ");
			}

			if (field_type.contains("CHAR")) {

				header.append("char *value_").append(j + 1);

			} else {

				header.append("int value_").append(j + 1);
			}

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

		// TODO: allow composite keys
		if (Integer.parseInt(key_type) == Types.INTEGER) {
			definition.append("\t*(int *) p.key = value_").append(Integer.parseInt(key_field_num)).append(";\n\n");
		} else {
			definition.append("\tmemcpy(p.key, value_").append(Integer.parseInt(key_field_num)).append(", ").append(key_type).append(");\n\n");
		}

		for (int i = 0; i < fields.length; i++) {
			field_type = table.getFieldTypeName(i+1);
			String value_size = table.getSchemaValue(FIELD_SIZE, i+1);

			// TODO: add support for byte arrays
			if (field_type.contains("CHAR")) {
				definition.append("\tstrcpy(data, value_").append(i + 1).append(");\n");
			} else {
				definition.append("\t*(int *) data = value_").append(i + 1).append(";\n");
			}

			if (i < fields.length - 1) {
				definition.append("\tdata += ").append(value_size).append(";\n\n");
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