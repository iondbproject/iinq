package iinq;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class IinqTable implements Iterable<IinqField> {
	private ArrayList<IinqField> fields;
	private int numFields;
	private int primaryKeyIndex;
	private int primaryKeyType;
	private String primaryKey;
	private String tableName;
	private boolean written_table;

	private int table_id;

	public IinqTable() {
		fields = new ArrayList<>();
		numFields = 0;
		written_table = false;
		table_id = -1;
	}

	public boolean isWritten_table() {
		return written_table;
	}

	public void setWritten_table(boolean written_table) {
		this.written_table = written_table;
	}

	/* Setters and getters */
	public int getTable_id() {
		return table_id;
	}

	public void setTable_id(int table_id) {
		this.table_id = table_id;
	}

	public int getPrimaryKeyIndex() {
		return primaryKeyIndex;
	}

	public void setPrimaryKeyIndex(int primaryKeyIndex) {
		this.primaryKeyIndex = primaryKeyIndex;
	}

	public int getPrimaryKeyType() {
		return primaryKeyType;
	}

	public void setPrimaryKeyType(int primaryKeyType) {
		this.primaryKeyType = primaryKeyType;
	}

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getFieldName(int i) {
		return fields.get(i).getFieldName();
	}

	public int getFieldType(int i) {
		return fields.get(i).getFieldType();
	}

	public String getFieldTypeName(int i) {
		return fields.get(i).getFieldTypeName();
	}

	public int getFieldSize(int i) {
		return fields.get(i).getFieldSize();
	}

	/* Add a field to the table */
	public void addField(String fieldName, int fieldType, String fieldTypeName, int fieldSize) {
		fields.add(new IinqField(fieldName, fieldType, fieldTypeName, fieldSize));
		numFields++;
	}

	@Override
	public Iterator<IinqField> iterator() {
		Iterator<IinqField> it = new Iterator() {
			private int currentIndex = 0;
			private Iterator<IinqField> iinqFieldIterator = fields.iterator();

			@Override
			public boolean hasNext() {
				return iinqFieldIterator.hasNext();
			}

			@Override
			public Object next() throws NoSuchElementException {
				return iinqFieldIterator.next();
			}
		};
		return it;
	}
}
