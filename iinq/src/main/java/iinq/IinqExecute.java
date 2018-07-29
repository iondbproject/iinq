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
import iinq.query.IinqQuery;
import org.xml.sax.SAXException;
import unity.util.StringFunc;

import javax.management.relation.RelationNotFoundException;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;

public class IinqExecute {

	/* JVM options */
	private static String iinqUserParsedSourceFile;
	/**
	 * < Path to iinq_user.c source file. Mandatory for iinq to run.
	 */
	private static String iinqFunctionOutputName;
	/**
	 * < Path to iinq_user_functions.c source file. Mandatory for iinq to run.
	 */
	private static String iinqFunctionOutputDirectory;
	/**
	 * < Path to iinq_user_functions.h source file. Mandatory for iinq to run.
	 */
	private static String iinqInterfaceDirectory;
	/**
	 * < Path to directory to output UnityJDBC schema files. Mandatory for iinq to run.
	 */
	private static boolean useExistingIinqDatabase = false;
	/**
	 * < Optional JVM option to use pre-existing database files (i.e. Use tables generated from an earlier IinqExecute).
	 */
	private static String defaultFunctionOutputName = "iinq_user_functions";

	private static boolean commentOutExistingFunctions = true;

	private static boolean debug = false;

	private static IinqDatabase iinqDatabase;

	public static void main(String args[]) {

		FileInputStream in = null;
		FileOutputStream out = null;

		/* Determine whether the user wants to use an existing database */
		if (System.getProperty("USE_EXISTING") != null) {
			useExistingIinqDatabase = Boolean.parseBoolean(System.getProperty("USE_EXISTING"));
		}

		if (System.getProperty("COMMENT_OUT_EXISTING_FUNCTIONS") != null) {
			commentOutExistingFunctions = Boolean.parseBoolean(System.getProperty("COMMENT_OUT_EXISTING_FUNCTIONS"));
		}

		if (System.getProperty("DEBUG") != null) {
			debug = Boolean.parseBoolean(System.getProperty("DEBUG"));
		}

		/* Get file names and directories passed in as JVM options. */
		iinqUserParsedSourceFile = System.getProperty("USER_FILE");
		iinqFunctionOutputDirectory = System.getProperty("OUTPUT_DIRECTORY");
		iinqFunctionOutputName = System.getProperty("OUTPUT_NAME");
		iinqInterfaceDirectory = System.getProperty("INTERFACE_DIRECTORY");

		if (iinqFunctionOutputName == null) {
			iinqFunctionOutputName = defaultFunctionOutputName;
		}

		if (iinqUserParsedSourceFile == null || iinqFunctionOutputDirectory == null || iinqInterfaceDirectory == null) {
			System.err.println("Missing JVM options: USER_FILE, OUTPUT_DIRECTORY, and INTERFACE_DIRECTORY " +
					"are all required.\nExiting Iinq.");
			System.exit(-1);
		}

		try {
			/* Create a new database if we have to */
			iinqDatabase = new IinqDatabase(iinqInterfaceDirectory, "IinqDB", debug);


			/* Reload the CREATE TABLE statements if we are using an existing database */
			if (useExistingIinqDatabase) {
				reload_tables();
			}

			in = new FileInputStream(iinqUserParsedSourceFile);

			/* Create output file */
			new File(iinqFunctionOutputDirectory).mkdirs();
			File output_file = Paths.get(iinqFunctionOutputDirectory, iinqFunctionOutputName + ".c").toFile();
			output_file.createNewFile();
			out = new FileOutputStream(output_file, false);

			BufferedReader buff_in = new BufferedReader(new InputStreamReader(in));
			BufferedWriter buff_out = new BufferedWriter(new OutputStreamWriter(out));

			String sql;
			buff_out.write("#include \""+ iinqFunctionOutputName +".h\"\n\n");

			/* File is read line by line */
			// TODO: make this more robust
			while (((sql = buff_in.readLine()) != null) && !sql.contains("return 0;")) {
				/* Verify file contents are as expected*/
				System.out.println(sql);

				if (!sql.contains("printf")&& !sql.contains("/*") && !sql.contains("//") && sql.contains("SQL_")) {

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
						if (sql.contains("SELECT * FROM Table1 WHERE ID <= 50;")){
							System.out.println();
						}
						select(sql);
					}
				}
			}

			calculateFunctions(buff_out);

			buff_in.close();
			buff_out.close();

			writeFunctionHeaderFile();
			overwriteUserFile();

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

	// TODO: move overwriteUserFile code into other iinq functions (to allow iinq to run in a single pass)
	private static void overwriteUserFile() throws IOException {
		String path = iinqUserParsedSourceFile;
		BufferedReader inFile = new BufferedReader(new FileReader(path));

		StringBuilder contents = new StringBuilder();
		String line;

		int insertCount = 0;
		int deleteCount = 0;
		int createTableCount = 0;
		int updateCount = 0;
		int selectCount = 0;
		int dropCount = 0;

		while (null != (line = inFile.readLine())) {
			if (!line.contains("/*") && !line.contains("//") && !line.contains("printf")) {
				/* Comment out old iinq function call */
				if (commentOutExistingFunctions && (line.contains("create_table(") || line.contains("iinq_insert")
						|| line.contains("update(") || line.contains("delete_record(") || line.contains("iinq_select(")
						|| line.contains("drop_table("))) {
					contents.append("/* ").append(line).append(" */\n");
				} else if (line.contains("SQL_execute") || line.contains("SQL_prepare")) {
					if ((line.toUpperCase()).contains("INSERT")) {
						/* Add insert function call */
						contents.append("/* ").append(line).append(" */\n");
						int index = line.indexOf("SQL_prepare");
						if (index != -1) {
							contents.append(line, 0, index).append(iinqDatabase.getInsert(insertCount).generateFunctionCall());
						} else {
							contents.append("\t").append(CommonCode.instantaneousExecution(iinqDatabase.getInsert(insertCount).generateFunctionCall()));
						}

						contents.append(";\n");
						insertCount++;

					} else if (line.toUpperCase().contains("DELETE")) {
						/* Add delete function call */
						contents.append("/* ").append(line).append(" */\n");

						IinqDelete delete = iinqDatabase.getDelete(deleteCount);

						if (delete != null) {
							contents.append("\t").append(delete.generateFunctionCall()).append(";\n");
							deleteCount++;
						}
					} else if ((line.toUpperCase()).contains("CREATE TABLE")) {
						/* Add create table function call */
						contents.append("/* ").append(line).append(" */\n");

						IinqCreateTable create = iinqDatabase.getCreateTable(createTableCount);

						if (create != null) {
							contents.append("\t").append(create.generateFunctionCall()).append(";\n");
						}

						createTableCount++;
					} else if (line.toUpperCase().contains("UPDATE")) {
						/* Add update function call */
						contents.append("/* ").append(line).append(" */\n");

						IinqUpdate update = iinqDatabase.getUpdate(updateCount);

						if (update != null) {
							contents.append("\t").append(update.generateFunctionCall()).append(";\n");
						}

						updateCount++;
					} else if (line.toUpperCase().contains("DROP TABLE")) {
						/* Add drop table function call */
						contents.append("/* ").append(line).append(" */\n");

						try {
							int table_id = iinqDatabase.getDroppedTableIds().get(dropCount);
							contents.append("\tdrop_table(").append(table_id).append(");\n");
						} catch (NullPointerException e) {
							e.printStackTrace();
						} finally {
							dropCount++;
						}

					}

				} else if (line.contains("SQL_select") && line.toUpperCase().contains("SELECT")) {
					/* Add select function call */
					contents.append("/* " + line + " */\n");

					IinqQuery select = iinqDatabase.getQuery(selectCount);

					if (select != null) {
						contents.append(line.substring(0, line.indexOf("SQL_select")));
						contents.append(select.getFunctionCall());
						contents.append(";\n");
					}
					selectCount++;
				} else {
					contents.append(line).append('\n');
				}


			} else {
				contents.append(line).append('\n');
			}
		}
		inFile.close();
		FileOutputStream outFile = new FileOutputStream(new File(path), false);

		outFile.write(contents.toString().getBytes());
		outFile.close();
	}

	private static String
	generateTopHeader(String outputDirectory, String libraryDirectory) throws IOException {
		StringBuilder contents = new StringBuilder();
		contents.append("/********************************************************************/\n" +
				"/*              Code generated by IinqExecute.java                  */\n" +
				"/********************************************************************/\n\n" +
				"#if !defined(IINQ_USER_FUNCTIONS_H_)\n" + "#define IINQ_USER_FUNCTIONS_H_\n\n" +
				"#if defined(__cplusplus)\n" + "extern \"C\" {\n" + "#endif\n\n");

		contents.append("#include <limits.h>\n");

		if (File.separatorChar != '/') {
			Path libraryRelativeToOutput = Paths.get(outputDirectory).relativize(Paths.get(libraryDirectory)).normalize();
			contents.append(String.format("#include \"%s\"\n", Paths.get(libraryRelativeToOutput.toString(), "../../dictionary/dictionary_types.h").normalize()).replace(File.separatorChar, '/'));
			contents.append(String.format("#include \"%s\"\n", Paths.get(libraryRelativeToOutput.toString(), "../../dictionary/dictionary.h").normalize()).replace(File.separatorChar, '/'));
			contents.append(String.format("#include \"%s\"\n", Paths.get(libraryRelativeToOutput.toString(), "../iinq.h").normalize()).replace(File.separatorChar, '/'));
			contents.append(String.format("#include \"%s\"\n\n", Paths.get(libraryRelativeToOutput.toString(), "iinq_functions.h").normalize()).replace(File.separatorChar, '/'));
		} else {
			Path libraryRelativeToOutput = Paths.get(outputDirectory).relativize(Paths.get(libraryDirectory)).normalize();
			contents.append(String.format("#include \"%s\"\n", Paths.get(libraryRelativeToOutput.toString(), "../../dictionary/dictionary_types.h").normalize()));
			contents.append(String.format("#include \"%s\"\n", Paths.get(libraryRelativeToOutput.toString(), "../../dictionary/dictionary.h").normalize()));
			contents.append(String.format("#include \"%s\"\n", Paths.get(libraryRelativeToOutput.toString(), "../iinq.h").normalize()));
			contents.append(String.format("#include \"%s\"\n\n", Paths.get(libraryRelativeToOutput.toString(), "iinq_functions.h").normalize()));
		}

		return contents.toString();
	}

	private static void
	writeFunctionHeaderFile() throws IOException {
		/* Write header file */
		Path headerFilePath = Paths.get(iinqFunctionOutputDirectory, iinqFunctionOutputName + ".h");

		/* Create header file for generated functions*/
		StringBuilder contents = new StringBuilder();

		File output_file = new File(headerFilePath.toUri());

		FileOutputStream header = new FileOutputStream(output_file, false);

		contents.append(generateTopHeader(iinqFunctionOutputDirectory, iinqInterfaceDirectory));

		if (debug) {
			contents.append("#define IINQ_DEBUG		1\n");
		}

		contents.append(iinqDatabase.getOperatorStructDefinitions());
		contents.append(iinqDatabase.getFunctionHeaders());
		contents.append("\n#if defined(__cplusplus)\n" + "}\n" + "#endif\n" + "\n" + "#endif\n");

		header.write(contents.toString().getBytes());

		header.close();
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
	calculateFunctions(BufferedWriter out) throws IOException {
		iinqDatabase.generateCalculatedDefinitions();
		iinqDatabase.addOperatorFunctions();
		out.write(iinqDatabase.getFunctionDefinitions());
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
}
