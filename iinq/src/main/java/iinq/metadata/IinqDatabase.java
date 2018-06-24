package iinq.metadata;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.*;
import iinq.callable.*;
import iinq.callable.update.IinqUpdate;
import iinq.functions.*;
import iinq.functions.calculated.CalculatedFunctions;
import iinq.functions.select.SelectFunctions;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import unity.annotation.*;
import unity.jdbc.UnityConnection;
import unity.jdbc.UnityPreparedStatement;
import unity.jdbc.UnityStatement;

import javax.management.relation.RelationNotFoundException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;
import java.io.*;
import java.sql.*;
import java.util.*;

public class IinqDatabase {
	private IinqSchema schema;
	private UnityConnection unityConnection;
	private Connection javaConnection;
	private String databaseName;
	private String directory;
	private int tableCount = 0;
	private IinqExecutor executor;
	private boolean createWritten = false;
	private boolean updateWritten = false;
	private boolean deleteWritten = false;
	private boolean dropWritten = false;
	private boolean selectWritten = false;
	private boolean preparedStatements = false;
	private HashMap<Integer, PreparedInsertFunction> insertFunctions = new HashMap<>();
	private ArrayList<IinqCreateTable> createTables = new ArrayList<>();
	private ArrayList<IinqInsert> inserts = new ArrayList<>();
	private ArrayList<IinqUpdate> updates = new ArrayList<>();
	private ArrayList<IinqDelete> deletes = new ArrayList<>();
	private ArrayList<IinqSelect> selects = new ArrayList<>();
	private HashMap<String, IinqFunction> functions = new HashMap<>();
	private ArrayList<Integer> droppedTableIds = new ArrayList<>();
	private HashMap<Integer, String> tableIds = new HashMap<>();
	private HashMap<String, IinqTable> iinqTables = new HashMap<>();
	private CalculatedFunctions calculatedFunctions = null;

	public IinqDatabase(String directory, String databaseName) throws ClassNotFoundException, SQLException {
		this.schema = new IinqSchema();
		this.databaseName = databaseName;
		this.directory = directory;
		Class.forName("org.hsqldb.jdbc.JDBCDriver");
		this.javaConnection = DriverManager.getConnection("jdbc:hsqldb:mem:.");
		AnnotatedSourceDatabase db = new AnnotatedSourceDatabase(databaseName, "", "HSQL Database Engine", "2.2.0", "jdbc:hsqldb:mem:.", "HSQL Database Engine Driver", '\"');
		db.setUserId("sa");
		db.setPassword("");
		db.setSchemaFile(getFullSchemaFileName());
		db.setJavaDriverClassName("org.hsqldb.jdbcDriver");
		this.schema.addDatabase(db);
		this.unityConnection = new UnityConnection(this.schema, new Properties());
		this.executor = new IinqExecutor(this);
	}

	public IinqCreateTable getCreateTable(int i) {
		return createTables.get(i);
	}

	public boolean insertFunctionExists(int tableId) {
		return insertFunctions.containsKey(tableId);
	}

	public PreparedInsertFunction getInsertFunction(int tableId) {
		return insertFunctions.get(tableId);
	}

	public void generateCalculatedDefinitions() {
		calculatedFunctions.generateDefinitions();
	}

	public void addIinqTable(IinqTable table) {
		iinqTables.put(table.getTableName().toLowerCase(), table);
	}

	private static String getFullSchemaFileName(String directory, String databaseName) {
		return directory + File.separator + databaseName + ".xml";
	}

	private String
	getSchemaValue(String table_name, SchemaKeyword keyword, int field_num) throws InvalidArgumentException {
		IinqTable table = this.getIinqTable(table_name);
		return table.getSchemaValue(keyword, field_num);
	}

	public String getSchemaValue(String table_name, SchemaKeyword keyword) throws InvalidArgumentException {
		return getSchemaValue(table_name, keyword, -1);
	}

	private String getFullSchemaFileName() {
		return getFullSchemaFileName(getDirectory(), getDatabaseName());
	}

	public IinqTable executeCreateTable(String sql) throws SQLException, IOException, InvalidArgumentException {
		IinqFunction func;
		IinqTable table = executor.executeCreateTable(sql);
		iinqTables.put(table.getTableName().toLowerCase(), table);
		if (!createWritten) {
			func = new CreateTableFunction();
			functions.put(func.getName(), func);
			createWritten = true;
		}
		if (calculatedFunctions == null) {
			calculatedFunctions = new CalculatedFunctions();
			functions.putAll(calculatedFunctions.getFunctions());
		}

		calculatedFunctions.addTable(table);
		createTables.add(new IinqCreateTable(table));

		return table;
	}

	public IinqSchema getSchema() {
		return this.schema;
	}

	public AnnotatedSourceDatabase getUnityDB() {
		return this.schema.getDB(this.databaseName);
	}

	public IinqInsert executeInsertStatement(String sql) throws SQLException, InvalidArgumentException {
		IinqInsert insert =  executor.executeInsertStatement(sql);
		inserts.add(insert);
		if (!insert.isDuplicate()) {
			insertFunctions.put(insert.getTableId(), insert.getInsertFunction());
			functions.put(insert.getInsertFunction().getName(), insert.getInsertFunction());
		}
		if (!containsPreparedStatements() && insert.isPreparedStatement()) {
			preparedStatements = true;
			IinqFunction func = new SetPreparedParametersFunction();
			functions.put(func.getName(), func);
		}

		return insert;
	}

	public void executeUpdateStatement(String sql) throws SQLException, RelationNotFoundException, IOException, InvalidArgumentException {
		IinqUpdate update = executor.executeUpdateStatement(sql);
		updates.add(update);
		if (!updateWritten) {
			IinqFunction func = new UpdateFunction();
			functions.put(func.getName(), func);
			updateWritten = true;
		}
	}

	public void executeDeleteStatement(String sql) throws SQLException, InvalidArgumentException, RelationNotFoundException, IOException {
		deletes.add(executor.executeDeleteStatement(sql));
		if (!deleteWritten) {
			IinqFunction func = new DeleteFunction();
			functions.put(func.getName(), func);
			deleteWritten = true;
		}
	}

	public void executeDropTable(String sql) throws SQLException {
		int droppedTable = executor.executeDropTable(sql);
		droppedTableIds.add(droppedTable);
		if (!dropWritten) {
			IinqFunction func = new DropTableFunction();
			functions.put(func.getName(), func);
			dropWritten = true;
		}
	}

	public IinqInsert getInsert(int i) {
		return inserts.get(i);
	}

	public int getNumInserts() {
		return inserts.size();
	}

	public void updateSource() throws SQLException {
		this.schema.updateSource(databaseName);
	}

	private void updateSchemaFile() throws IOException {
		this.schema.updateSchemaFile(databaseName);
	}

	public void reloadTablesFromXML() throws SQLException, IOException, ParserConfigurationException, SAXException, InvalidArgumentException {
		//this.schema.parseSourcesFile(new BufferedReader(new FileReader(new File(getFullSchemaFileName()))), getFullUnityUrl(), new Properties());
		//this.schema.parseSources(getFullSchemaFileName(), new Properties());
		Statement stmt = javaConnection.createStatement();
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		TransformerFactory transFactory = TransformerFactory.newInstance();
		Document xml = builder.parse(getFullSchemaFileName());
		NodeList tableNodes = xml.getElementsByTagName("TABLE");

		for (int i = 0, n = tableNodes.getLength(); i < n; i++) {
			/* Create table statements are stored as a comment in the schema */
			String sql = ((Element) tableNodes.item(i)).getElementsByTagName("comment").item(0).getTextContent();
			this.executeCreateTable(sql);
		}

		this.updateSchemaFile();
	}

	public IinqTable getIinqTable(String tableName) {
		return this.iinqTables.get(tableName.toLowerCase());
	}

	public String getFullUnityUrl() {
		return "jdbc:unity://" + getFullSchemaFileName();
	}

	public String getDatabaseName() {
		return this.databaseName;
	}

	private String getDirectory() {
		return this.directory;
	}

	public UnityStatement createUnityStatement() throws SQLException {
		return (UnityStatement) this.unityConnection.createStatement();
	}

	public UnityPreparedStatement prepareUnityStatement(String sql) throws SQLException {
		return (UnityPreparedStatement) this.unityConnection.prepareStatement(sql);
	}

	public Statement createJavaStatement() throws SQLException {
		return this.javaConnection.createStatement();
	}

	public void updateSourceTables() throws SQLException {
		this.schema.addTable(this.databaseName, null, this.javaConnection);
	}

	public IinqTable getNewlyCreatedIinqTable() {
		SourceTable table = schema.getNewlyCreatedTable();

		/* Before we create a new IinqTable object, we must add one to each CHAR and VARCHAR field size to compensate for the NULL character */
		ArrayList<SourceField> sourceFields = table.getSourceFieldList();
		Iterator<SourceField> it = sourceFields.iterator();
		while (it.hasNext()) {
			SourceField field = it.next();
			switch (field.getDataType()) {
				case Types.CHAR:
				case Types.VARCHAR:
					field.setColumnSize(field.getColumnSize()+1);
			}
		}

		if (table != null) {
			IinqTable iinqTable = new IinqTable((AnnotatedSourceTable) table);
			iinqTable.setTableId(tableCount++);
			tableIds.put(iinqTable.getTableId(), iinqTable.getTableName());
			return iinqTable;
		} else {
			return null;
		}
	}

	public void close() throws SQLException {
		if (unityConnection != null) {
			unityConnection.close();
		}
	}

	public String getExecutionHeader() {
		return executor.getExFunc().getHeader();
	}

	public String getExecutionDefinition() {
		return executor.getExFunc().generateDefinition();
	}

	private boolean containsPreparedStatements() {
		return preparedStatements;
	}

	public int getTableCount() {
		return tableCount;
	}

	public IinqTable getIinqTableFromId(int id) {
		return getIinqTable(tableIds.get(id));
	}

	public String getFunctionHeaders() {
		Iterator<Map.Entry<String, IinqFunction>> it = functions.entrySet().iterator();
		StringBuilder headers = new StringBuilder(300);
		String header;
		while (it.hasNext()) {
			header = it.next().getValue().getHeader();
			if (header != null) {
				headers.append(header);
			}
		}
		return headers.toString();
	}

	public String getFunctionDefinitions() {
		Iterator<Map.Entry<String, IinqFunction>> it = functions.entrySet().iterator();
		StringBuilder definitions = new StringBuilder(2000);
		String definition;
		while (it.hasNext()) {
			definition = it.next().getValue().getDefinition();
			if (definition != null) {
				definitions.append(definition);
			}
		}
		return definitions.toString();
	}

	public void removeIinqTable(IinqTable table) {
		schema.removeIinqIdentifiers(table);
		iinqTables.remove(table.getTableName().toLowerCase());
	}

	public IinqDelete getDelete(int i) {
		return deletes.get(i);
	}

	public IinqUpdate getUpdate(int i) {
		return updates.get(i);
	}

	public ArrayList<Integer> getDroppedTableIds() {
		return droppedTableIds;
	}

	public void executeQuery(String sql, String return_val) throws SQLException, RelationNotFoundException, IOException, InvalidArgumentException {
		IinqSelect select = executor.executeQuery(sql);
		select.return_value = return_val;
		selects.add(select);
		if (!selectWritten) {
			SelectFunctions selectFunctions = new SelectFunctions();
			functions.putAll(selectFunctions.getFunctions());
			selectWritten = true;
		}
	}

	public IinqSelect getSelect(int i) {
		return selects.get(i);
	}
}
