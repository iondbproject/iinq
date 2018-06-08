package iinq.metadata;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.*;
import iinq.functions.*;
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
import java.util.concurrent.LinkedBlockingDeque;

public class IinqDatabase {
	protected IinqSchema schema;
	protected UnityConnection unityConnection;
	protected Connection javaConnection;
	protected String databaseName;
	protected String directory;
	protected int tableCount = 0;
	protected IinqExecutor executor;
	protected boolean createWritten = false;
	protected boolean updateWritten = false;
	protected boolean deleteWritten = false;
	protected boolean dropWritten = false;
	protected boolean preparedStatements = false;
	protected ArrayList<PreparedInsertFunction> inserts = new ArrayList<>();
	protected ArrayList<IinqUpdate> updates = new ArrayList<>();
	protected ArrayList<delete_fields> deletes = new ArrayList<>();
	protected ArrayList<IinqFunction> functions = new ArrayList<>();
	// TODO: change to table ids after removing table names
	protected ArrayList<String> droppedTables = new ArrayList<>();
	protected HashMap<Integer, String> tableIds = new HashMap<>();
	protected HashMap<String, IinqTable> iinqTables = new HashMap<>();

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

	public void addIinqTable(IinqTable table) {
		iinqTables.put(table.getTableName().toLowerCase(), table);
	}

	public static String getFullSchemaFileName(String directory, String databaseName) {
		return directory + File.separator + databaseName + ".xml";
	}

	public String
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

	public IinqTable executeCreateTable(String sql) throws SQLException, IOException {
		IinqTable table = executor.executeCreateTable(sql);
		iinqTables.put(table.getTableName().toLowerCase(), table);
		if (!createWritten) {
			functions.add(new CreateTableFunction());
			createWritten = true;
		}
		return table;
	}

	public IinqSchema getSchema() {
		return this.schema;
	}

	public AnnotatedSourceDatabase getUnityDB() {
		return this.schema.getDB(this.databaseName);
	}

	public PreparedInsertFunction executeInsertStatement(String sql) throws SQLException, InvalidArgumentException {
		PreparedInsertFunction insert =  executor.executeInsertStatement(sql);
		inserts.add(insert);
		functions.add(insert);
		if (insert.isPreparedStatement())
			preparedStatements = true;
		return insert;
	}

	public void executeUpdateStatement(String sql) throws SQLException, RelationNotFoundException, IOException, InvalidArgumentException {
		IinqUpdate update = executor.executeUpdateStatement(sql);
		updates.add(update);
		if (!updateWritten) {
			functions.add(new UpdateFunction());
			updateWritten = true;
		}
	}

	public void executeDeleteStatement(String sql) throws SQLException, InvalidArgumentException, RelationNotFoundException, IOException {
		deletes.add(executor.executeDeleteStatement(sql));
		if (!deleteWritten) {
			functions.add(new DeleteFunction());
			deleteWritten = true;
		}
	}

	public void executeDropTable(String sql) throws SQLException {
		String droppedTable = executor.executeDropTable(sql);
		droppedTables.add(droppedTable);
		if (!dropWritten) {
			functions.add(new DropTableFunction());
			dropWritten = true;
		}
	}

	public PreparedInsertFunction getInsert(int i) {
		return inserts.get(i);
	}

	public int getNumInserts() {
		return inserts.size();
	}

	public void updateSource() throws SQLException {
		this.schema.updateSource(databaseName);
	}

	public void updateSchemaFile() throws IOException {
		this.schema.updateSchemaFile(databaseName);
	}

	public void reloadTablesFromXML() throws SQLException, IOException, ParserConfigurationException, SAXException {
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

	public String getDirectory() {
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
		SourceTable table = schema.getNewlyCreatedTable(databaseName);
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

	public String getInsertHeaders() throws IOException {
		StringBuilder headers = new StringBuilder();
		for (int i = 0, n = getNumInserts(); i < n; i++) {
			PreparedInsertFunction insertFunction = getInsert(i);
			if (!insertFunction.isDuplicate()) {
				headers.append(getInsert(i).getHeader());
			}
		}
		return headers.toString();
	}

	public String getExecutionHeader() {
		return executor.getExFunc().getHeader();
	}

	public String getExecutionDefinition() {
		return executor.getExFunc().generateDefinition();
	}

	public boolean containsPreparedStatements() {
		return preparedStatements;
	}

	public int getTableCount() {
		return tableCount;
	}

	public IinqTable getIinqTableFromId(int id) {
		return getIinqTable(tableIds.get(id));
	}

	public String getFunctionHeaders() {
		Iterator<IinqFunction> it = functions.iterator();
		StringBuilder headers = new StringBuilder(300);
		String header;
		while (it.hasNext()) {
			header = it.next().getHeader();
			if (header != null) {
				headers.append(header);
			}
		}
		return headers.toString();
	}

	public String getFunctionDefinitions() {
		Iterator<IinqFunction> it = functions.iterator();
		StringBuilder definitions = new StringBuilder(2000);
		String definition;
		while (it.hasNext()) {
			definition = it.next().getDefinition();
			if (definition != null) {
				definitions.append(definition);
			}
		}
		return definitions.toString();
	}

	public void removeIinqTable(IinqTable table) {
		schema.removeIinqIdentifiers(table);
		iinqTables.remove(table.getTableName());
	}
}
