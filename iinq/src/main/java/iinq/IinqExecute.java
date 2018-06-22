/******************************************************************************/
/**
 @file IinqExecute.java
 @author Dana Klamut, Kai Neubauer
 @see        For more information, refer to dictionary.h.
 @copyright Copyright 2018
 The University of British Columbia,
 IonDB Project Contributors (see AUTHORS.md)
 @par Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions are met:

 @par 1.Redistributions of source code must retain the above copyright notice,
 this list of conditions and the following disclaimer.

 @par 2.Redistributions in binary form must reproduce the above copyright notice,
 this list of conditions and the following disclaimer in the documentation
 and/or other materials provided with the distribution.

 @par 3.Neither the name of the copyright holder nor the names of its contributors
 may be used to endorse or promote products derived from this software without
 specific prior written permission.

 @par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 POSSIBILITY OF SUCH DAMAGE.
 */
/******************************************************************************/

package iinq;

import javax.xml.parsers.ParserConfigurationException;

import iinq.callable.*;
import iinq.callable.update.IinqUpdate;
import iinq.functions.*;
import iinq.metadata.IinqDatabase;

import com.sun.javaws.exceptions.InvalidArgumentException;
import org.xml.sax.SAXException;
import unity.util.StringFunc;

import javax.management.relation.RelationNotFoundException;
import java.io.*;
import java.sql.*;

public class IinqExecute {

	/* JVM options */
	private static String user_file;
	/**
	 * < Path to iinq_user.c source file. Mandatory for iinq to run.
	 */
	private static String function_file;
	/**
	 * < Path to iinq_user_functions.c source file. Mandatory for iinq to run.
	 */
	private static String function_header_file;
	/**
	 * < Path to iinq_user_functions.h source file. Mandatory for iinq to run.
	 */
	private static String directory;
	/**
	 * < Path to directory to output UnityJDBC schema files. Mandatory for iinq to run.
	 */
	private static boolean use_existing = false;
	/**
	 * < Optional JVM option to use pre-existing database files (i.e. Use tables generated from an earlier IinqExecute).
	 */

	private static IinqDatabase iinqDatabase;

	public static void main(String args[]) {

		FileInputStream in = null;
		FileOutputStream out = null;

		/* Determine whether the user wants to use an existing database */
		if (System.getProperty("USE_EXISTING") != null) {
			use_existing = Boolean.parseBoolean(System.getProperty("USE_EXISTING"));
		}

		/* Get file names and directories passed in as JVM options. */
		user_file = System.getProperty("USER_FILE");
		function_file = System.getProperty("FUNCTION_FILE");
		function_header_file = System.getProperty("FUNCTION_HEADER_FILE");
		directory = System.getProperty("DIRECTORY");

		if (user_file == null || function_file == null || function_header_file == null || directory == null) {
			System.err.println("Missing JVM options: USER_FILE, FUNCTION_FILE, FUNCTION_HEADER_FILE, and DIRECTORY " +
					"are all required.\nExiting Iinq.");
			System.exit(-1);
		}

		try {
			/* Create a new database if we have to */
			iinqDatabase = new IinqDatabase(directory, "IinqDB");


			/* Reload the CREATE TABLE statements if we are using an existing database */
			if (use_existing) {
				reload_tables();
			}

			in = new FileInputStream(user_file);

			/* Create output file */
			File output_file = new File(function_file);
			output_file.createNewFile();
			out = new FileOutputStream(output_file, false);

			BufferedReader buff_in = new BufferedReader(new InputStreamReader(in));
			BufferedWriter buff_out = new BufferedWriter(new OutputStreamWriter(out));

			String sql;
			buff_out.write("#include \"iinq_user_functions.h\"\n\n");

			main_setup();

			/* File is read line by line */
			// TODO: make this more robust
			while (((sql = buff_in.readLine()) != null) && !sql.contains("return 0;")) {
				/* Verify file contents are as expected*/
				System.out.println(sql);

				if (!sql.contains("printf")&& !sql.contains("/*") && !sql.contains("//")) {

					/* CREATE TABLE statements exists in code that is not a comment */
					if ((sql.toUpperCase()).contains("CREATE TABLE")) {
						create_table(sql);
					}

					/* INSERT statements exists in code that is not a comment */
					else if ((sql.toUpperCase()).contains("INSERT INTO")) {
						insert(sql);
					}

					/* UPDATE statements exists in code that is not a comment */
					else if ((sql.toUpperCase()).contains("UPDATE")) {
						update(sql);
					}

					/* DELETE statements exists in code that is not a comment */
					else if ((sql.toUpperCase()).contains("DELETE FROM")) {
						delete(sql);
					}

					/* DROP TABLE statements exists in code that is not a comment */
					else if ((sql.toUpperCase()).contains("DROP TABLE")) {
						drop_table(sql);
					} else if ((sql.toUpperCase()).contains("SELECT")) {
						select(sql);
					}
				}
			}

			calculate_functions(buff_out);

			buff_in.close();
			buff_out.close();

			write_headers();
			// TODO: combine into single function
			create_setup();
			insert_setup();
			delete_setup();
			update_setup();
			select_setup();
			drop_setup();
			function_close();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != in) {
					in.close();
				}
				if (null != out) {
					out.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void reload_tables() throws SQLException, ParserConfigurationException, IOException, SAXException, InvalidArgumentException {
		iinqDatabase.reloadTablesFromXML();
	}

	private static void
	main_setup() throws IOException {
		/* Comment out old IINQ functions */
		String path = user_file;
		BufferedReader file = new BufferedReader(new FileReader(path));

		String contents = "";
		String line;

		while (null != (line = file.readLine())) {
			if ((line.contains("create_table") || line.contains("insert")
					|| line.contains("update") || line.contains("delete_record") || line.contains("iinq_select")
					|| line.contains("drop_table")) && !line.contains("/*")) {
				contents += "/* " + line + " */\n";
			} else {
				contents += line + '\n';
			}
		}

		File ex_output_file = new File(path);
		FileOutputStream out = new FileOutputStream(ex_output_file, false);

		out.write(contents.getBytes());

		file.close();
		out.close();
	}

	private static void
	print_top_header(FileOutputStream out) throws IOException {
		String contents = "";
		contents += "/********************************************************************/\n";
		contents += "/*              Code generated by IinqExecute.java                  */\n";
		contents += "/********************************************************************/\n\n";
		contents += "#if !defined(IINQ_USER_FUNCTIONS_H_)\n" + "#define IINQ_USER_FUNCTIONS_H_\n\n";
		contents += "#if defined(__cplusplus)\n" + "extern \"C\" {\n" + "#endif\n\n";

		/* Include other headers*/
		// TODO: make the includes work from other directories
		contents += "#include \"../../dictionary/dictionary_types.h\"\n" +
				"#include \"../../dictionary/dictionary.h\"\n" +
				"#include \"../iinq.h\"\n" + "#include \"iinq_functions.h\"\n\n";

		out.write(contents.getBytes());
	}

	private static void
	write_headers() throws IOException {
		/* Write header file */
		String header_path = function_header_file;

		/* Create schema table header file */
		String contents = "";

		File output_file = new File(header_path);

		/* Create header file if it does not previously exist*/
		if (!output_file.exists()) {
			output_file.createNewFile();
		}

		FileOutputStream header = new FileOutputStream(output_file, false);

		print_top_header(header);

		contents += iinqDatabase.getFunctionHeaders();

		contents += "\n#if defined(__cplusplus)\n" + "}\n" + "#endif\n" + "\n" + "#endif\n";

		header.write(contents.getBytes());

		header.close();
	}

	private static void
	insert_setup() throws IOException {

		/* Add new functions to be run to executable */
		String ex_path = user_file;
		BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

		String contents = "";
		String line;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("INSERT") && !line.contains("/*") && !line.contains("//") && !line.contains("printf")) {
				contents += "/* " + line + " */\n";

				int index = line.indexOf("SQL_prepare");
				if (index != -1) {
					contents += line.substring(0,index) + iinqDatabase.getInsert(count).generateFunctionCall();
				} else if (line.contains("SQL_execute")) {
					contents += "\t" + CommonCode.wrapInExecuteFunction(iinqDatabase.getInsert(count).generateFunctionCall());
				}

				contents += ";\n";
				count++;
			} else {
				contents += line + '\n';
			}
		}

		File ex_output_file = new File(ex_path);
		FileOutputStream ex_out = new FileOutputStream(ex_output_file, false);

		ex_out.write(contents.getBytes());

		ex_file.close();
		ex_out.close();
	}

	private static void
	delete_setup() throws IOException {

		/* Add new functions to be run to executable */
		String ex_path = user_file;
		BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

		StringBuilder contents = new StringBuilder();
		String line;
		IinqDelete delete;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("DELETE") && line.contains("SQL_execute") && !line.contains("/*") && !line.contains("//") && !line.contains("printf")) {
				contents.append("/* ").append(line).append(" */\n");

				delete = iinqDatabase.getDelete(count);

				if (delete != null) {
					contents.append("\t" + delete.generateFunctionCall() + ";\n");
					count++;
				}
			} else {
				contents.append(line + '\n');
			}
		}

		File ex_output_file = new File(ex_path);
		FileOutputStream ex_out = new FileOutputStream(ex_output_file, false);

		ex_out.write(contents.toString().getBytes());

		ex_file.close();
		ex_out.close();
	}

	private static void
	update_setup() throws IOException {

		/* Add new functions to be run to executable */
		String ex_path = user_file;
		BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

		StringBuilder contents = new StringBuilder();
		String line;
		IinqUpdate update;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("UPDATE") && !line.contains("/*") && !line.contains("//") && !line.contains("printf")) {
				contents.append("/* " + line + " */\n");

				update = iinqDatabase.getUpdate(count);

				if (update != null) {
					contents.append ("\t" + update.generateFunctionCall() + ";\n");

					count++;
				}
			} else {
				contents.append(line + '\n');
			}
		}

		File ex_output_file = new File(ex_path);
		FileOutputStream ex_out = new FileOutputStream(ex_output_file, false);

		ex_out.write(contents.toString().getBytes());

		ex_file.close();
		ex_out.close();
	}

	private static void
	select_setup() throws IOException {

		/* Add new functions to be run to executable */
		String ex_path = user_file;
		BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

		StringBuilder contents = new StringBuilder();
		String line;
		IinqSelect select;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("SELECT") && !line.contains("/*") && !line.contains("//") && !line.contains("printf")) {
				contents.append("/* " + line + " */\n");

				select = iinqDatabase.getSelect(count);

				if (select != null) {
					contents.append("\t" + select.return_value + " = iinq_select(" + select.table_id +  ", "
							+ select.project_size + ", " + select.num_wheres + ", " + select.num_fields);

					if (select.num_wheres > 0) {
						contents.append(", ");
						contents.append(select.where.generateIinqConditionList());
					}

					contents.append(", IINQ_SELECT_LIST(");
					for (int i = 0; i < select.num_fields; i++) {
						contents.append(select.fields.get(i) + ", ");
					}
					contents.setLength(contents.length()-2);

					contents.append("));\n");
					count++;
				}
			} else {
				contents.append(line + '\n');
			}
		}

		File ex_output_file = new File(ex_path);
		FileOutputStream ex_out = new FileOutputStream(ex_output_file, false);

		ex_out.write(contents.toString().getBytes());

		ex_file.close();
		ex_out.close();
	}

	private static void
	create_setup() throws IOException {

		/* Add new functions to be run to executable */
		String ex_path = user_file;
		BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

		String contents = "";
		String line;
		IinqCreateTable create;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("CREATE TABLE") && !line.contains("/*") && !line.contains("//")&& !line.contains("printf")) {
				contents += "/* " + line + " */\n";

				create = iinqDatabase.getCreateTable(count);

				if (create != null) {
					contents += "\t" + create.generateFunctionCall() + ";\n";

					count++;
				}
			} else {
				contents += line + '\n';
			}
		}

		File ex_output_file = new File(ex_path);
		FileOutputStream ex_out = new FileOutputStream(ex_output_file, false);

		ex_out.write(contents.getBytes());

		ex_file.close();
		ex_out.close();
	}

	private static void
	drop_setup() throws IOException {

		/* Add new functions to be run to executable */
		String ex_path = user_file;
		BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

		String contents = "";
		String line;
		int table_id;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("DROP TABLE") && !line.contains("/*") && !line.contains("//") && !line.contains("printf")) {
				contents += "/* " + line + " */\n";

				try {
					table_id = iinqDatabase.getDroppedTableIds().get(count);
					contents += "\tdrop_table(" + table_id + ");\n";
				} catch (NullPointerException e) {
					e.printStackTrace();
				} finally {
					count++;
				}
			} else {
				contents += line + '\n';
			}
		}

		File ex_output_file = new File(ex_path);
		FileOutputStream ex_out = new FileOutputStream(ex_output_file, false);

		ex_out.write(contents.getBytes());

		ex_file.close();
		ex_out.close();
	}

	private static void
	create_table(String sql) throws Exception {
		sql = sql.substring(sql.toUpperCase().indexOf("CREATE"), sql.indexOf(";"));
		sql = StringFunc.verifyTerminator(sql);    // Make sure SQL is terminated by semi-colon properly

		// Create the table using HSQLDB to avoid string parsing
		iinqDatabase.executeCreateTable(sql);
	}

	private static void
	insert(String sql) throws IOException, InvalidArgumentException, RelationNotFoundException, SQLException {
		System.out.println("insert statement");

		sql = sql.substring(sql.toUpperCase().indexOf("INSERT"), sql.indexOf(";")).trim();
		sql = StringFunc.verifyTerminator(sql);    // Make sure SQL is terminated by semi-colon properly

		// Use UnityJDBC to parse the insert statement (metadata is required to verify fields)
		iinqDatabase.executeInsertStatement(sql);
	}

	private static void
	calculate_functions(BufferedWriter out) throws IOException {
		if (iinqDatabase.getTableCount() > 0) {
			iinqDatabase.generateCalculatedDefinitions();
			out.write(iinqDatabase.getFunctionDefinitions());
		}
	}

	private static void
	update(String sql) throws IOException, InvalidArgumentException, RelationNotFoundException, SQLException {
		System.out.println("update statement");

		sql = sql.substring(sql.toUpperCase().indexOf("UPDATE"), sql.indexOf(";"));
		sql = StringFunc.verifyTerminator(sql);

		iinqDatabase.executeUpdateStatement(sql);
	}

	private static void
	select(String sql) throws IOException, SQLException, RelationNotFoundException, InvalidArgumentException {
		System.out.println("select statement");

		String return_val = sql.substring(0, sql.indexOf("=") - 1);

		sql = sql.substring(sql.indexOf("("));
		sql = sql.substring(sql.toUpperCase().indexOf("SELECT"), sql.indexOf(";")).trim();
		sql = StringFunc.verifyTerminator(sql);    // Make sure SQL is terminated by semi-colon properly

		iinqDatabase.executeQuery(sql, return_val);


	}

	private static void
	delete(String sql) throws IOException, InvalidArgumentException, RelationNotFoundException, SQLException {
		System.out.println("delete statement");

		sql = sql.substring(sql.toUpperCase().indexOf("DELETE"), sql.indexOf(";"));
		sql = StringFunc.verifyTerminator(sql);

		iinqDatabase.executeDeleteStatement(sql);

	}

	private static void
	drop_table(String sql) throws Exception {
		System.out.println("drop statement");

		sql = sql.substring(sql.toUpperCase().indexOf("DROP"));
		sql = sql.substring(0, sql.indexOf(";"));
		sql = StringFunc.verifyTerminator(sql);

		iinqDatabase.executeDropTable(sql);
	}

	private static void
	function_close() throws IOException {
		/* Closes insert functions because there do not exist any more commands to be read */
		String path = function_file;
		BufferedReader file = new BufferedReader(new FileReader(path));

		String contents = "";
		String line;

		while (null != (line = file.readLine())) {
			if (!((line.contains("/* INSERT 1 */")) || (line.contains("/* INSERT 2 */"))
					|| (line.contains("/* INSERT 3 */")) || (line.contains("/* INSERT 4 */")))) {
				contents += line + '\n';
			}
		}

		File ex_output_file = new File(path);
		FileOutputStream out = new FileOutputStream(ex_output_file, false);

		out.write(contents.getBytes());

		file.close();
		out.close();
	}
}
