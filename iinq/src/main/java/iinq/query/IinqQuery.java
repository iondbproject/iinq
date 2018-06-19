package iinq.query;

import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.IinqWhere;
import iinq.functions.PredicateFunction;
import iinq.metadata.IinqDatabase;
import iinq.metadata.IinqTable;
import iinq.query.RequiresSchemaException;
import unity.annotation.SourceField;
import unity.annotation.SourceTable;
import unity.generic.query.WebQuery;
import unity.query.LQExprNode;

import javax.management.relation.RelationNotFoundException;
import java.io.*;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;


/**
 * Stores a representation of an IINQ query.
 */
public class IinqQuery extends WebQuery {
	// Enables comments with in code output
	private static final boolean DEBUG = true;
	private IinqDatabase database;
	private HashMap<String, Object> queryCode;

	/**
	 *  Writes a query function to a C source file that contains the dummy function call (query_init_SQL). Any generated functions are declared within the single source file.
	 *  File needs to be parsed for the query already. Only does one query at a time.
	 *
	 * @param source
	 * 		The path to the source file that contains the dummy function call
	 * @param destination
	 * 		The path where the updated source file will be written
	 * @throws IOException
	 */
	public void writeQueryToFile(String source, String destination) throws IOException {
		File inFile = new File(source);
		File outFile;
		// Check whether we are overwriting the original source file
		if (source.equals(destination)) {
			// Create a temp file so that we aren't reading and writing with the same file
			outFile = File.createTempFile("temp-source-file", ".tmp");
		} else {
			outFile = new File(destination);
		}

		// More complicated way of parsing the original file than in IinqExecute. Allows skipping of multi-line comments and single line comments using "//".
		BufferedReader in = new BufferedReader(new FileReader(inFile));
		PrintWriter out = new PrintWriter(new FileWriter(outFile));
		String line, trimmedLine;
		while ((line = in.readLine()) != null) {
			trimmedLine = line.trim();
			// Skip comments & #includes
			if (trimmedLine.isEmpty() || trimmedLine.charAt(0) == '#' || trimmedLine.substring(0, 2).equals("//")) {
				// empty line, preprocessor command, or single line comment
				out.println(line);
				continue;
			} else if (trimmedLine.substring(0, 1).equals("/*")) {
				// multi line comment
				Boolean multiLineComment = true;
				while (multiLineComment) {
					if (trimmedLine.contains("*/")) {
						multiLineComment = false;
					} else {
						line = in.readLine();
						trimmedLine = line.trim();
					}
					out.println(line);
				}
			} else {
				// end of comments and #includes
				break;
			}
		}

		// write out predicate definition
		PredicateFunction predicateFunction;
		if ((predicateFunction = (PredicateFunction) this.queryCode.get("predicate")) != null) {
			out.println(predicateFunction.getDefinition());
		}

		out.println(line);

		// continue until we get to the init function query_init_SQL(&iterator, "SQL")
		while ((line = in.readLine()) != null) {
			trimmedLine = line.trim();
			if (trimmedLine.startsWith("query_init_SQL(")) {
				// print out the line as a comment
				out.println("/* LINE REPLACED: " + line.trim() + " */");

				// get the name of the iterator to be initialized
				String iteratorName = trimmedLine.substring(trimmedLine.indexOf("(") + 1, trimmedLine.indexOf(","));

				// replace the line with the appropriate init function
				out.printf("%s(%s, %s, %s", (String) this.queryCode.get("select_init"), iteratorName, (String) this.queryCode.get("table_name"), (predicateFunction == null) ? "NULL" : predicateFunction.getName());

				// if there is a field list add it to the init
				if (null != this.queryCode.get("field_list")) {
					out.printf(", %s", this.queryCode.get("field_list"));
				}
				// Close the init function
				out.println(");");
			} else {
				out.println(line);
			}
		}

		in.close();
		out.close();

		// if the destination is the same as the source file we need to overwrite it
		if (source.equals(destination)) {
			// Delete the original source file
			inFile.delete();
			// Rename the temp file to replace the original source file
			outFile.renameTo(inFile);
		}
	}

	/**
	 * Checks whether the query contains an GROUP BY clause
	 *
	 * @return true if it contains an GROUP BY clause, false if it does not
	 */
	public boolean hasGroupBy() {
		/* TODO: implement check for GROUP BY clause */
		return false;
	}

	/**
	 * Checks whether the query contains an ORDER BY clause
	 *
	 * @return true if it contains an ORDER BY clause, false if it does not
	 */
	public boolean hasOrderBy() {
		return this.parameters.containsKey("sort");
	}

	/**
	 * Checks whether the query has SELECT * as its SELECT clause.
	 *
	 * @return true if it is SELECT *, false if it is not
	 */
	public boolean isSelectAll() {
		return ((String) this.parameters.get("fields")).equals("*");
	}

	/**
	 * Checks whether a schema was given for the query.
	 *
	 * @return true if a schema was given, false if it was not.
	 */
	public boolean hasSchema() {
		return (this.proj.getDatabase().getDatabase().getSchemaFile() != null) ? true : false;
	}

	/**
	 * Constructs an IINQ query.
	 *
	 * @param url query URL
	 * @param database
	 */
	public IinqQuery(String url, IinqDatabase database) {
		super(url);
		this.database = database;
	}

	/**
	 * Generates the C code for a predicate definition.
	 *
	 * @param identifier A number added to the end of the function name to make it unique.
	 * @return code to evaluate a predicate
	 */
	public PredicateFunction generatePredicateFunction(int identifier) throws RequiresSchemaException {

		String name = String.format("generatedPredicate%d", identifier);
		// First line will be the return type for the predicate
		StringBuilder definition = new StringBuilder(String.format("ion_boolean_t\n" +
				// Next line is the function name and parameter list
				"%s(iinq_iterator_t *it) {\n", name));
		String header = "ion_boolean_t\n%s(iinq_iterator_t *it);";
		definition.append("\tif (!(");
		Object filter = this.parameters.get("filter");
		if (filter instanceof ArrayList) {
			if (((ArrayList) filter).get(0) instanceof String) {
				ArrayList<String> filterList = (ArrayList<String>) filter;
					/* Loop through each filter in the where clause */
				for (String s : filterList) {
					definition.append(generateComparisonCode(s));
				}
			}
		} else if (filter instanceof String) {
			definition.append(generateComparisonCode((String) filter));
		}
		definition.setLength(definition.length() - 3);
		definition.append("{\n" +
				"\t\treturn boolean_false;\n" +
				"\t} else {\n" +
				"\t\treturn boolean_true;\n" +
				"\t}\n" +
				"}\n\n");


		return new PredicateFunction(name, header, definition.toString());
	}

	/**
	 * Generates the C code for this SQL query.
	 *
	 * @return code to execute query
	 */
	public HashMap<String, Object> generateCode() throws InvalidArgumentException, RelationNotFoundException, SQLFeatureNotSupportedException, IOException {
		/* Currently follows similar structure to MATERIALIZED_QUERY macro		 */
		String tableName = getTableName();
		StringBuilder code = new StringBuilder();
		HashMap<String, Object> returnValue = new HashMap<>();

		// first thing we will need is the table name for the query
		returnValue.put("table_name", this.parameters.get("source"));

		// TODO: move this to appropriate spot
		ArrayList<LQExprNode> expList = this.proj.getExpressions();
		Iterator<LQExprNode> it = expList.iterator();
		ArrayList<String> fieldList = new ArrayList<>();
		ArrayList<Integer> fieldListNums = new ArrayList<>();
		while (it.hasNext()) {
			LQExprNode expNode = it.next();
			fieldList.add(expNode.getFieldReference().getName());
			fieldListNums.add(expNode.getFieldReference().getField().getOrdinalPosition());
		}
		this.parameters.put("fieldList", fieldList);
		this.parameters.put("fieldListNums", fieldListNums);

		// if there is a predicate, create a function definition for it
		if (this.parameters.containsKey("filter")) {
			/*try {
				returnValue.put("predicate", generatePredicateFunction(0));
			} catch (RequiresSchemaException e) {
				e.printStackTrace();
			}*/
			Object filter = this.parameters.get("filter");
			if (filter instanceof ArrayList) {
				if (((ArrayList) filter).get(0) instanceof String) {
					ArrayList<String> filterList = (ArrayList<String>) filter;
					IinqWhere where = new IinqWhere(filterList.size());
					IinqTable table = database.getIinqTable(tableName);
					where.generateWhere(filterList.toArray(new String[filterList.size()]),table);
					returnValue.put("where", where);
				}
			}
		}

/*		// is select all
		if (isSelectAll()) {
			if (hasOrderBy()) {

			} else if (hasGroupBy()) {

			} else {
				// No sorting, projection is done on the table directly
				returnValue.put("select_init", "iinq_query_init_select_all_from_table");
			}
		} else {
			// select field list
			if (hasOrderBy()) {

			} else if (hasGroupBy()) {

			} else {
				// No sorting, projection is done on the table directly
				// Split the field list into an array
				String fields[] = this.getParameter("fields").split(", ");
				// Field list will be an iinq_field_list_t array. We use a macro in the C code to improve readibility.
				StringBuilder fieldList = new StringBuilder("IINQ_FIELD_LIST(");
				for (int i = 0; i < fields.length; i++) {
				*//* First number is table index (hardcoded as 0 since multiple tables are unsupported)
				 * Second number is the field number of that table (requires the ordinal positions in the schema to be correct) *//*
					fieldList.append(String.format("{ 0, %d }, ", this.getRelation().getAttributeIndexByName(fields[i])));
				}
				// remove the last comma
				fieldList.setLength(fieldList.length() - 2);
				// close off the field list
				fieldList.append(")");
				returnValue.put("field_list", fieldList.toString());
				returnValue.put("select_init", "iinq_query_init_select_field_list_from_table");
			}
		}*/

		return this.queryCode = returnValue;
	}

	/**
	 * Constructs C code for aggregate expressions
	 *
	 * @return C code needed to execute queries with aggregation
	 */
	public String generateAggregateExprsCode() {
		/* TODO: generate C code for aggregate expressions */
		return "";
	}

	/**
	 * Constructs the code needed for a comparison filter in the WHERE clause
	 *
	 * @param filter Single filter as created in IinqBuilder
	 * @return C code for a single comparison filter in the WHERE clause
	 */
	public String generateComparisonCode(String filter) throws RequiresSchemaException {
		if (!hasSchema()) {
			throw new RequiresSchemaException("Comparison requires schema. ");
		}
		String tableName = getTableName();
		SourceTable sourceTable = getTable(tableName);
		StringBuilder condition = new StringBuilder();
		String operator = null;
		condition.append("(");
		int i = 0;
		boolean foundOperator = false;
		String fieldName = "";
		while (i < filter.length() && !foundOperator) {
			switch (filter.charAt(i)) {
				case '=':
				case '>':
					foundOperator = true;
					fieldName = filter.substring(0, i);
					break;
				case '!':
					if (filter.charAt(i + 1) == '=') {
						foundOperator = true;
						operator = "!=";
						fieldName = filter.substring(0, i);
						i += 2;
					}
					break;
				case '<':
					foundOperator = true;
					fieldName = filter.substring(0, i);
					if (filter.charAt(i + 1) == '>') {
						operator = "!=";
						i += 2;
					}
					break;
				default:
					i++;
			}
		}
		if (!foundOperator) {
			System.err.println("Operator not found in filter: " + filter);
			return "";
		}
		SourceField field = sourceTable.getField(fieldName);
		int fieldIndex = this.getRelation().getAttributeIndexByName(fieldName);
		if (field.getDataTypeName().contains("CHAR")) {
							/* Field has a string data type:
							 * strcmp(source_tuple->field, compareString) > 0 */
			condition.append(String.format("strcmp(get_string(*it, %d)", fieldIndex));
			if (null == operator) {
								/* operator was not <> or != */
				if (filter.charAt(i + 1) == '=') {
									/* Operator is two characters e.g <=, >= */
					operator = filter.substring(i, i + 2);
					i += 2;
				} else {
									/* Operator is a single character e.g <, >, = */
					if (filter.charAt(i) == '=') {
										/* Equality operator */
						operator = "==";
					} else {
										/* <, > operators */
						operator = filter.substring(i, i + 1);
					}
					i++;
				}
			}
			/* compareString */
			condition.append(String.format(", %s) %s 0)", filter.substring(i), operator));
		} else {
			/* Field has numeric data type:
			 * source_tuple->field > 0 */
			if (null == operator) {
				/* Operator was not <> or != */
				if (filter.charAt(i + 1) == '=') {
					/* Operator is two characters e.g <=, >= */
					operator = filter.substring(i, i + 2);
					i += 2;
				} else {
					/* Operator is a single character e.g <, >, = */
					if (filter.charAt(i) == '=') {
						/* Equality operator */
						operator = "==";
					} else {
						/* <, > operators */
						operator = filter.substring(i, i + 1);
					}
					i++;
				}

			}
			condition.append(String.format("get_int(*it, %d) %s %s", fieldIndex, operator, filter.substring((i))));
		}

		/* Close the condition */
		condition.append(") && ");

		return condition.toString();
	}

	/**
	 * Constructs C code for the GROUP BY clause
	 *
	 * @return code for the GROUP BY clause
	 */
	public String generateGroupByCode() {
			/* TODO: add GROUP BY code */
		return "";
	}

	/**
	 * Retrieves the name of the relation for the query.
	 *
	 * @return name of the relation
	 */
	public String getTableName() {
		/* TODO: add support for multiple tables */
		return this.getRelation().getProperty("name");
	}

	/**
	 * Retrieves the SourceTable with a given table_id.
	 *
	 * @param tableName name of the table to retrieve
	 * @return SourceTable with the given name
	 */
	public SourceTable getTable(String tableName) {
		this.getRelation();
		return this.proj.getDatabase().getDatabase().getSourceTables().get(tableName);
	}
}