package iinq.metadata;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.functions.SchemaKeyword;
import unity.annotation.AnnotatedSourceTable;
import unity.annotation.SourceField;
import unity.annotation.SourceKey;

import java.sql.Types;
import java.util.*;

public class IinqTable extends AnnotatedSourceTable {
	private boolean createTableStatementSet = false;
	private boolean printFunctionWritten = false;
	private int tableId = -1;
	private boolean insertWritten = false;
	private AnnotatedSourceTable annotatedSourceTable;

	public IinqTable(AnnotatedSourceTable table) {
		super(table.getCatalogName(), table.getSchemaName(), table.getTableName(), table.getComment(), table.getSourceFields(), table.getPrimaryKey());
		annotatedSourceTable = table;
		// change field parent tables back to the AnnotatedSourceTable to prevent validation from breaking
		ArrayList<SourceField> fieldList = getSourceFieldList();
		Iterator<SourceField> it = fieldList.iterator();
		while (it.hasNext()) {
			it.next().setParentTable(table);
		}
	}

	public IinqTable(IinqTable table) {
		super(table.getCatalogName(), table.getSchemaName(), table.getTableName(), table.getComment(), new HashMap<>(), null);
		HashMap<String, SourceField> sourceFields = new HashMap<>();
		Iterator<SourceField> it = table.getSourceFieldList().iterator();
		while (it.hasNext()) {
			SourceField field = it.next();
			SourceField fieldCopy = new SourceField(field.getTableCatalog(), field.getTableSchema(),
					field.getTableName(), field.getColumnName(), field.getDataType(), field.getDataTypeName(),
					field.getColumnSize(), field.getDecimalDigits(), field.getNumberPrecisionRadix(),
					field.getNullable(), field.getRemarks(), field.getDefaultValue(), field.getCharacterOctetLength(),
					field.getOrdinalPosition(), null);
			this.addField(fieldCopy);
		}
		this.copyKey(table.getPrimaryKey());
	}

	public void copyKey(SourceKey key) {
		Iterator<SourceField> it = key.getFields().iterator();
		ArrayList<SourceField> copiedKeyFields = new ArrayList<>();
		while (it.hasNext()) {
			copiedKeyFields.add(getField(it.next().getColumnName()));
		}
		this.setPrimaryKey(new SourceKey(copiedKeyFields, key.getKeyType(), key.getName()));
	}

	private AnnotatedSourceTable getAnnotatedSourceTable() {
		return annotatedSourceTable;
	}

	public IinqTable() {

	}

	public boolean isInsertWritten() {
		return insertWritten;
	}

	public void setInsertWritten(boolean insertWritten) {
		this.insertWritten = insertWritten;
	}

	public ArrayList<String> getIonKeyFieldSizes() {
		ArrayList<Integer> indices = getPrimaryKeyIndices();
		//Collections.sort(indices);
		ArrayList<String> sizes = new ArrayList<>(indices.size());
		Iterator<Integer> it = indices.iterator();
		while (it.hasNext()) {
			sizes.add(getIonFieldSize(it.next()));
		}
		return sizes;
	}

	public ArrayList<String> getIonFieldSizes() {
		ArrayList<String> sizes = new ArrayList<>(getNumFields());
		for (int i = 1, n = getNumFields(); i <= n; i++) {
			sizes.add(getIonFieldSize(i));
		}
		return sizes;
	}

	public String getIonFieldSize(int fieldNum) {
		switch (this.getFieldType(fieldNum)) {
			case Types.INTEGER:
				return "sizeof(int)";
			case Types.CHAR:
			case Types.VARCHAR:
				return "(sizeof(char) * " + this.getFieldSize(fieldNum) + ")";
			case Types.BOOLEAN:
				return "sizeof(ion_boolean_t)";
			case Types.FLOAT:
			case Types.DECIMAL:
			case Types.NUMERIC:
				return "sizeof(float)";
			default:
				return null;
		}
	}

	public String getIonFieldSize(String fieldName) {
		int fieldNum = this.getSourceFields().get(fieldName.toLowerCase()).getOrdinalPosition();
		return (getIonFieldSize(fieldNum));
	}

	public int getFieldType(int fieldNum) {
		return this.getSourceFieldsByPosition().get(fieldNum-1).getDataType();
	}

	public String generateIonValueSize() {
		StringBuilder value_calculation = new StringBuilder();
		int int_count = 0;
		boolean char_present = false;
		int char_multiplier = 0;

		for (int i = 1, n = this.getNumFields(); i <= n; i++) {
			switch (this.getFieldType(i)) {
				case Types.INTEGER :
					int_count++;
					break;
				case Types.CHAR :
				case Types.VARCHAR :
				default : // Any other field type is treated as a char array
					char_multiplier += this.getFieldSize(i);
					char_present = true;
					break;
			}
		}

		// TODO: add support for more data types
		value_calculation.append(String.format("IINQ_BITS_FOR_NULL(%d)+",getNumFields()));
		if (int_count > 0) {
			value_calculation.append("(sizeof(int) * ").append(int_count).append(")+");
		}
		if (char_present) {
			value_calculation.append("(sizeof(char) * ").append(char_multiplier).append(")+");
		}
		value_calculation.setLength(value_calculation.length() - 1); // remove last "+"

		return value_calculation.toString();
	}

	public String generateIonKeySize() {
		// TODO: Add support for composite keys
		ArrayList<SourceField> keyFields = this.getPrimaryKey().getFields();
		StringBuilder returnValue = new StringBuilder();
		for (int i = 0, n = keyFields.size(); i < n; i++) {
			returnValue.append(this.getIonFieldSize(keyFields.get(i).getOrdinalPosition()));
			returnValue.append("+");
		}
		returnValue.setLength(returnValue.length()-1); // remove trailing "+"
		return returnValue.toString();
	}

	private int getKeyDataType() {
		// TODO: add support for composite keys
		return this.getPrimaryKey().getFields().get(0).getDataType();
	}

	public String getIonKeyType() {
		ArrayList<SourceField> keyFields = this.getPrimaryKey().getFields();
		// Composite keys stored as char array
		if (keyFields.size() > 1) {
			return "key_type_char_array";
		} else {
			switch (keyFields.get(0).getDataType()) {
				case Types.INTEGER:
					return "key_type_numeric_signed";
				case Types.CHAR:
				case Types.VARCHAR:
					return "key_type_char_array";
				default:
					return null;
			}
		}
	}

	public String getKeyIonization() {
		ArrayList<SourceField> keyFields = this.getPrimaryKey().getFields();
		if (keyFields.size() > 1) {
			return "p.key";
		} else {
			switch (keyFields.get(0).getDataType()) {
				case Types.INTEGER:
					return "IONIZE(*(int *) p.key, int)";
				default:
					return "p.key";
			}
		}
	}

	public boolean isPrintFunctionWritten() {
		return printFunctionWritten;
	}

	public void setPrintFunctionWritten(boolean printFunctionWritten) {
		this.printFunctionWritten = printFunctionWritten;
	}

	public int getTableId() {
		return tableId;
	}

	public void setTableId(int tableId) {
		this.tableId = tableId;
	}

	public ArrayList<SourceField> getPrimaryKeyFields() {
		return this.getPrimaryKey().getFields();
	}

	public ArrayList<Integer> getPrimaryKeyIndices() {
		ArrayList<SourceField> keyFields = this.getPrimaryKey().getFields();
		ArrayList<Integer> indexList = new ArrayList<>(keyFields.size());
		for (int i = 0, n = keyFields.size(); i < n; i++) {
			indexList.add(keyFields.get(i).getOrdinalPosition());
		}
		return indexList;
	}

	public String getTableName() {
		return super.getTableName().toLowerCase();
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getFieldName(int i) {
		return this.getSourceFieldsByPosition().get(i-1).getColumnName();
	}

	public String getFieldTypeName(int i) {
		return this.getSourceFieldsByPosition().get(i-1).getDataTypeName();
	}

	public int getFieldSize(int i) {
		return this.getSourceFieldsByPosition().get(i-1).getColumnSize();
	}

	public String getIonFieldType(int i) {
		switch (this.getSourceFieldsByPosition().get(i-1).getDataType()) {
			case Types.INTEGER:
				return "iinq_int";
			case Types.VARCHAR:
			case Types.CHAR:
				return "iinq_null_terminated_string";
			default:
				return "iinq_char_array";
		}
	}

	public int getFieldPosition(String fieldName) {
		return this.getField(fieldName).getOrdinalPosition();
	}

	public String toSQL() {
		return createTableStatementSet ? this.getComment() : null;
	}

	public String getSchemaValue(SchemaKeyword keyword, int field_num) throws InvalidArgumentException {
		switch (keyword) {
			case PRIMARY_KEY_TYPE: {
				return Integer.toString(this.getKeyDataType());
			}
			// TODO: update for composite keys
			case PRIMARY_KEY_FIELD: {
				return this.getPrimaryKeyIndices().get(0).toString();
			}
			case PRIMARY_KEY_SIZE: {
				return this.generateIonKeySize();
			}
			case NUMBER_OF_RECORDS: {
				return Integer.toString(this.getNumTuples());
			}
			case VALUE_SIZE: {
				return this.generateIonValueSize();
			}
			case NUMBER_OF_FIELDS: {
				return Integer.toString(this.getNumFields());
			}
			case ION_KEY_TYPE: {
				return this.getIonKeyType();
			}
			case FIELD_NAME: {
				return this.getFieldName(field_num);
			}
			case FIELD_TYPE: {
				return Integer.toString(this.getFieldType(field_num));
			}
			case FIELD_SIZE: {
				return this.getIonFieldSize(field_num);
			}
			default:
				throw new InvalidArgumentException(new String[]{String.format("%s is not a valid keyword.", keyword)});
		}

	}

	public String getSchemaValue(SchemaKeyword keyword) throws InvalidArgumentException {
		return getSchemaValue(keyword, -1);
	}

	public void setCreateTableStatement(String sql) {
		getAnnotatedSourceTable().setComment(sql);
		createTableStatementSet = true;
	}

	public String getIonKeyCast() {
		switch (getKeyDataType()) {
			case Types.INTEGER:
				return "IONIZE(*(int *) p.key, int)";
			default:
				return "p.key";
		}
	}

	// TODO: change to use multipliers
	public String generateProjectionSize(ArrayList<Integer> fields) {
		StringBuilder size = new StringBuilder();
		Iterator<Integer> it = fields.iterator();
		while (it.hasNext()) {
			size.append(getIonFieldSize(it.next()));
			size.append("+");
		}
		size.setLength(size.length()-1);
		return size.toString();
	}
}
