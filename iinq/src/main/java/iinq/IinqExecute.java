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

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;

import iinq.functions.*;
import iinq.metadata.IinqDatabase;
import iinq.metadata.IinqTable;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sun.javaws.exceptions.InvalidArgumentException;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import unity.annotation.*;
import unity.jdbc.UnityConnection;
import unity.parser.GlobalParser;
import unity.query.*;
import unity.util.StringFunc;

import javax.management.relation.RelationNotFoundException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.sql.*;
import java.util.*;

import static iinq.functions.SchemaKeyword.*;

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

	private static boolean param_written = false;
	private static boolean delete_written = false;
	private static boolean update_written = false;
	private static boolean select_written = false;
	private static boolean create_written = false;
	private static boolean drop_written = false;
	private static boolean prepared_written = false;

	/* Variables for INSERT supported prepared statements on multiple tables */
	private static ArrayList<String> table_names = new ArrayList<>();
	private static ArrayList<IinqInsert> inserts = new ArrayList<>();
	private static ArrayList<IinqInsertFields> IinqInsertFields = new ArrayList<>();
	private static ArrayList<String> function_headers = new ArrayList<>();
	private static ArrayList<tableInfo> calculateInfo = new ArrayList<>();
	private static ArrayList<delete_fields> delete_fields = new ArrayList<>();
	private static ArrayList<IinqUpdate> IinqUpdate = new ArrayList<>();
	private static ArrayList<select_fields> select_fields = new ArrayList<>();
	private static ArrayList<create_fields> create_fields = new ArrayList<>();
	private static ArrayList<String> drop_tables = new ArrayList<>();

	private static boolean new_table;
	private static String written_table;

	private static IinqDatabase iinqDatabase;

	private static ArrayList<String> xml_schemas = new ArrayList<>();

	private static Connection conUnity = null;
	/**
	 * < Connection for UnityJDBC.
	 */
	private static Connection conJava = null;
	/**
	 * < Connection for HSQLDB
	 */
	private static GlobalSchema metadata = null;
	/**
	 * < Metadata object for Iinq tables
	 */
	private static String urlUnity = null;
	/**
	 * < Url to use for UnityJDBC connection
	 */
	private static String urlJava = null;
	/**
	 * < Url to use for HSQLDB
	 */

	private static HashMap<String, IinqTable> tables = new HashMap<>();

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

		urlUnity = "jdbc:unity://" + directory + "iinq_sources.xml";
		urlJava = "jdbc:hsqldb:mem:.";

		try {
			/* Create a new database if we have to */
			iinqDatabase = new IinqDatabase(directory, "IinqDB");

			//getUnityConnection(urlUnity);
			//getJavaConnection(urlJava);

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
			while (((sql = buff_in.readLine()) != null) && (!sql.contains("return 0;"))) {
				/* Verify file contents are as expected*/
				System.out.println(sql);

				/* CREATE TABLE statements exists in code that is not a comment */
				if ((sql.toUpperCase()).contains("CREATE TABLE") && !sql.contains("/*") && !sql.contains("//")) {
					create_table(sql, buff_out);
				}

				/* INSERT statements exists in code that is not a comment */
				else if ((sql.toUpperCase()).contains("INSERT INTO") && !sql.contains("/*") && !sql.contains("//")) {
					insert(sql, buff_out);
				}

				/* UPDATE statements exists in code that is not a comment */
				else if ((sql.toUpperCase()).contains("UPDATE") && !sql.contains("/*") && !sql.contains("//")) {
					update(sql, buff_out);
				}

				/* DELETE statements exists in code that is not a comment */
				else if ((sql.toUpperCase()).contains("DELETE FROM") && !sql.contains("/*") && !sql.contains("//")) {
					delete(sql, buff_out);
				}

				/* DROP TABLE statements exists in code that is not a comment */
				else if ((sql.toUpperCase()).contains("DROP TABLE") && !sql.contains("/*") && !sql.contains("//")) {
					drop_table(sql, buff_out);
				} else if ((sql.toUpperCase()).contains("SELECT") && !sql.contains("/*") && !sql.contains("//")) {
					select(sql, buff_out);
				}
			}

			calculate_functions(buff_out);

			buff_in.close();
			buff_out.close();

			params();
			write_headers();
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
				if (null != conUnity) {
					conUnity.close();
				}
				if (null != conJava) {
					conJava.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private static void write_delete_method(BufferedWriter out) throws IOException {
		out.write("void delete_record(int id, char *name, iinq_print_table_t print_function, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_fields, ...) {\n\n");
		out.write("\tva_list valist;\n");
		out.write("\tva_start(valist, num_fields);\n\n");
		out.write("\tunsigned char *table_id = malloc(sizeof(int));\n");
		out.write("\t*(int *) table_id = id;\n\n");
		out.write("\tchar *table_name = malloc(sizeof(char)*ION_MAX_FILENAME_LENGTH);\n");
		out.write("\tstrcpy(table_name, name);\n\n");

		out.write("\tion_err_t                  error;\n");
		out.write("\tion_dictionary_t           dictionary;\n");
		out.write("\tion_dictionary_handler_t   handler;\n\n");

		out.write("\tdictionary.handler = &handler;\n\n");
		out.write("\terror              = iinq_open_source(table_name, &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");

		out.write("\tion_predicate_t predicate;\n");
		out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");

		out.write("\tion_dict_cursor_t *cursor = NULL;\n");
		out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n\n");
		out.write("\tion_record_t ion_record;\n");
		out.write("\tion_record.key     = malloc(key_size);\n");
		out.write("\tion_record.value   = malloc(value_size);\n\n");

		out.write("\tion_cursor_status_t status;\n\n");
		out.write("\terror = iinq_create_source(\"DEL\", key_type, (ion_key_size_t) key_size, (ion_value_size_t) sizeof(int));\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tion_dictionary_t           dictionary_temp;\n");
		out.write("\tion_dictionary_handler_t   handler_temp;\n\n");

		out.write("\tdictionary_temp.handler = &handler_temp;\n\n");
		out.write("\terror              = iinq_open_source(\"DEL\", &dictionary_temp, &handler_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tion_boolean_t condition_satisfied;\n\n");

		out.write("\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
		out.write("\t\tcondition_satisfied = where(table_id, &ion_record, num_fields, &valist);\n\n");
		out.write("\t\tif (!condition_satisfied || num_fields == 0) {\n");
		out.write("\t\t\terror = dictionary_insert(&dictionary_temp, ion_record.key, IONIZE(0, int)).error;\n\n");
		out.write("\t\t\tif (err_ok != error) {\n");
		out.write("\t\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n\t\t\t}\n");
		out.write("\t\t}\n\t}\n\n");

		out.write("\tva_end(valist);\n");
		out.write("\tcursor->destroy(&cursor);\n\n");
		out.write("\tion_predicate_t predicate_temp;\n");
		out.write("\tdictionary_build_predicate(&predicate_temp, predicate_all_records);\n\n");
		out.write("\tion_dict_cursor_t *cursor_temp = NULL;\n");
		out.write("\tdictionary_find(&dictionary_temp, &predicate_temp, &cursor_temp);\n\n");
		out.write("\twhile ((status = iinq_next_record(cursor_temp, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
		out.write("\t\terror = dictionary_delete(&dictionary, ion_record.key).error;\n\n");
		out.write("\t\tif (err_ok != error) {\n");
		out.write("\t\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n\t\t}\n");
		out.write("\t}\n\n");

		out.write("\tcursor_temp->destroy(&cursor_temp);\n");
		out.write("\tprint_function(&dictionary);\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\terror = dictionary_delete_dictionary(&dictionary_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tfremove(\"DEL.inq\");\n");
		out.write("\tfree(table_id);\n");
		out.write("\tfree(table_name);\n");
		out.write("\tfree(ion_record.key);\n");
		out.write("\tfree(ion_record.value);\n");
		out.write("}\n\n");

		function_headers.add("void delete_record(int id, char *name, iinq_print_table_t print_function, ion_key_type_t key_type, size_t key_size, size_t value_size, int fields, ...);\n");
	}

	private static void write_update_method(BufferedWriter out) throws IOException {
		out.write("void update(int id, char *name, iinq_print_table_t print_function, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_update, ...) {\n\n");
		out.write("\tva_list valist;\n");
		out.write("\tva_start(valist, num_update);\n\n");
		out.write("\tunsigned char *table_id = malloc(sizeof(int));\n");
		out.write("\t*(int *) table_id = id;\n\n");
		out.write("\tchar *table_name = malloc(sizeof(char)*ION_MAX_FILENAME_LENGTH);\n");
		out.write("\tstrcpy(table_name, name);\n\n");

		out.write("\tion_err_t                  error;\n");
		out.write("\tion_dictionary_t           dictionary;\n");
		out.write("\tion_dictionary_handler_t   handler;\n\n");

		out.write("\tdictionary.handler = &handler;\n\n");
		out.write("\terror              = iinq_open_source(table_name, &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");

		out.write("\tion_predicate_t predicate;\n");
		out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");

		out.write("\tion_dict_cursor_t *cursor = NULL;\n");
		out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n\n");
		out.write("\tion_record_t ion_record;\n");
		out.write("\tion_record.key     = malloc(key_size);\n");
		out.write("\tion_record.value   = malloc(value_size);\n\n");

		out.write("\tion_cursor_status_t status;\n\n");
		out.write("\terror = iinq_create_source(\"UPD\", key_type, (ion_key_size_t) key_size, (ion_value_size_t) value_size);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tion_dictionary_t           dictionary_temp;\n");
		out.write("\tion_dictionary_handler_t   handler_temp;\n\n");

		out.write("\tdictionary_temp.handler = &handler_temp;\n\n");
		out.write("\terror              = iinq_open_source(\"UPD\", &dictionary_temp, &handler_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tion_boolean_t condition_satisfied;\n\n");

		out.write("\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
		out.write("\t\tcondition_satisfied = where(table_id, &ion_record, num_wheres, &valist);\n\n");
		out.write("\t\tif (!condition_satisfied || num_wheres == 0) {\n");
		out.write("\t\t\terror = dictionary_insert(&dictionary_temp, ion_record.key, ion_record.value).error;\n\n");
		out.write("\t\t\tif (err_ok != error) {\n");
		out.write("\t\t\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n\t\t\t}\n");
		out.write("\t\t}\n\t}\n\n");

		out.write("\tcursor->destroy(&cursor);\n\n");

		out.write("\tint i;\n\n");

		out.write("\tiinq_update_params_t updates[num_update];");

		out.write("\tfor (i = 0; i < num_wheres; i++) {\n");
		out.write("\t\tva_arg(valist, void *);\n\t}\n\n");

		out.write("\tfor (i = 0; i < num_update; i++) {\n");
		out.write("\t\tupdates[i] = va_arg(valist, iinq_update_params_t);\n\t}\n\n");

		out.write("\tva_end(valist);\n\n");
		out.write("\tion_predicate_t predicate_temp;\n");
		out.write("\tdictionary_build_predicate(&predicate_temp, predicate_all_records);\n\n");
		out.write("\tion_dict_cursor_t *cursor_temp = NULL;\n");
		out.write("\tdictionary_find(&dictionary_temp, &predicate_temp, &cursor_temp);\n\n");

		out.write("\twhile ((status = iinq_next_record(cursor_temp, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
		out.write("\t\tfor (i = 0; i < num_update; i++) {\n");
		out.write("\t\t\tunsigned char *value;\n");
		out.write("\t\t\tif (updates[i].implicit_field != 0) {\n");
		out.write("\t\t\t\tint new_value;\n");
		out.write("\t\t\t\tvalue = ion_record.value + calculateOffset(table_id, updates[i].implicit_field - 1);\n\n");

		out.write("\t\t\t\tswitch (updates[i].operator) {\n");
		out.write("\t\t\t\t\tcase iinq_add :\n");
		out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) + (int) updates[i].field_value);\n");
		out.write("\t\t\t\t\t\tbreak;\n");
		out.write("\t\t\t\t\tcase iinq_subtract :\n");
		out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) - (int) updates[i].field_value);\n");
		out.write("\t\t\t\t\t\tbreak;\n");
		out.write("\t\t\t\t\tcase iinq_multiply :\n");
		out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) * (int) updates[i].field_value);\n");
		out.write("\t\t\t\t\t\tbreak;\n");
		out.write("\t\t\t\t\tcase iinq_divide :\n");
		out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) / (int) updates[i].field_value);\n");
		out.write("\t\t\t\t\t\tbreak;\n\t\t\t\t}\n");
		out.write("\t\t\t\tvalue = ion_record.value + calculateOffset(table_id, updates[i].update_field - 1);\n");
		out.write("\t\t\t\t*(int *) value = new_value;\n\t\t\t}\n");

		out.write("\t\t\telse {\n");
		out.write("\t\t\t\tvalue = ion_record.value + calculateOffset(table_id, updates[i].update_field - 1);\n");
		out.write("\t\t\t\tif (getFieldType(table_id, updates[i].update_field) == iinq_int) {\n");
		out.write("\t\t\t\t\t*(int *) value = (int) updates[i].field_value;\n\t\t\t\t}\n");
		out.write("\t\t\t\telse {\n");
		// TODO: why is this line dependent on the previous update?
		out.write("\t\t\t\t\tmemcpy(value, updates[i].field_value, calculateOffset(table_id, updates[i].update_field) - calculateOffset(table_id, updates[i - 1].update_field));\n\t\t\t\t}\n\t\t\t}\n\t\t}\n\n");
		out.write("\t\terror = dictionary_update(&dictionary, ion_record.key, ion_record.value).error;\n\n");
		out.write("\t\tif (err_ok != error) {\n");
		out.write("\t\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t\t}\n\t}\n\n");
		out.write("\tcursor_temp->destroy(&cursor_temp);\n");
		out.write("\tprint_function(&dictionary);\n\n");
		out.write("\terror = dictionary_delete_dictionary(&dictionary_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");

		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");

		out.write("\tfremove(\"UPD.inq\");\n");
		out.write("\tfree(table_id);\n");
		out.write("\tfree(table_name);\n");
		out.write("\tfree(ion_record.key);\n");
		out.write("\tfree(ion_record.value);\n");
		out.write("}\n\n");

		function_headers.add("void update(int id, char *name, iinq_print_table_t print_function, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_update, ...);\n");

	}

	private static void write_select_method(BufferedWriter out) throws IOException {
		out.write("iinq_result_set iinq_select(int id, char *name, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_fields, ...) {\n\n");
		out.write("\tint i;\n");
		out.write("\tva_list valist, where_list;\n");
		out.write("\tva_start(valist, num);\n");
		out.write("\tva_copy(where_list, valist);\n\n");

		out.write("\tunsigned char *table_id = malloc(sizeof(int));\n");
		out.write("\t*(int *) table_id = id;\n\n");

		out.write("\tchar *table_name = malloc(sizeof(char)*ION_MAX_FILENAME_LENGTH);\n");
		out.write("\tstrcpy(table_name, name);\n\n");

		out.write("\tion_err_t                  error;\n");
		out.write("\tion_dictionary_t           dictionary;\n");
		out.write("\tion_dictionary_handler_t   handler;\n\n");

		out.write("\tdictionary.handler = &handler;\n\n");
		out.write("\terror              = iinq_open_source(table_name, &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");

		out.write("\tion_predicate_t predicate;\n");
		out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");

		out.write("\tion_dict_cursor_t *cursor = NULL;\n");
		out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n\n");

		out.write("\tion_record_t ion_record;\n");
		out.write("\tion_record.key     = malloc(key_size);\n");
		out.write("\tion_record.value   = malloc(value_size);\n\n");

		out.write("\tion_cursor_status_t status;\n\n");
		out.write("\tint count = 0;\n");
		out.write("\tion_boolean_t condition_satisfied;\n\n");

		out.write("\tint fields[num_fields];\n\n");
		out.write("\tfor (i = 0; i < num_wheres; i++) {\n");
		out.write("\t\tva_arg(valist, void *);\n\t}\n\n");

		out.write("\tiinq_result_set select = {0};\n");
		out.write("\tselect.num_fields = malloc(sizeof(int));\n");
		out.write("\t*(int *) select.num_fields = num_fields;\n");
		out.write("\tselect.fields = malloc(sizeof(int) * num_fields);\n");
		out.write("\tunsigned char *field_list = select.fields;\n");
		out.write("\tselect.num_recs = malloc(sizeof(int));\n\n");

		out.write("\tfor (i = 0; i < num_fields; i++) {\n");
		out.write("\t\tfields[i] = va_arg(valist, int);\n\n");
		out.write("\t\t*(int *) field_list = fields[i];\n\n");
		out.write("\t\tif (i < num_fields-1) {\n");
		out.write("\t\t\tfield_list += sizeof(int);\n");
		out.write("\t\t}\n\t}\n\n");

		out.write("\tva_end(valist);\n\n");
		out.write("\tion_dictionary_handler_t   handler_temp;\n");
		out.write("\tion_dictionary_t           dictionary_temp;\n\n");
		out.write("\terror = iinq_create_source(\"SEL\", key_type, (ion_key_size_t) key_size, (ion_value_size_t) value_size);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tdictionary_temp.handler = &handler_temp;\n\n");
		out.write("\terror = iinq_open_source(\"SEL\", &dictionary_temp, &handler_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");

		out.write("\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
		out.write("\t\tcondition_satisfied = where(table_id, &ion_record, num_wheres, &where_list);\n\n");
		out.write("\t\tif (!condition_satisfied || num_wheres == 0) {\n");
		out.write("\t\t\tunsigned char *fieldlist = malloc(value_size);\n");
		out.write("\t\t\tunsigned char *data = fieldlist;\n\n");

		out.write("\t\t\tfor (i = 0; i < num_fields; i++) {\n\n");
		out.write("\t\t\t\tif (getFieldType(table_id, fields[i]) == iinq_int) {\n");
		out.write("\t\t\t\t\t*(int *) data = NEUTRALIZE(ion_record.value + calculateOffset(table_id, fields[i] - 1), int);\n");
		out.write("\t\t\t\t\tdata += sizeof(int);\n");
		out.write("\t\t\t\t}\n");
		out.write("\t\t\t\telse {\n");
		out.write("\t\t\t\t\tmemcpy(data, ion_record.value + calculateOffset(table_id, fields[i] - 1), calculateOffset(table_id, fields[i]) - calculateOffset(table_id, fields[i]-1));\n");
		out.write("\t\t\t\t\tdata += calculateOffset(table_id, fields[i]) - calculateOffset(table_id, fields[i]-1);\n");
		out.write("\t\t\t\t}\n\t\t\t}\n\n");
		out.write("\t\t\terror = dictionary_insert(&dictionary_temp, IONIZE(count, int), fieldlist).error;\n\n");
		out.write("\t\t\tif (err_ok != error) {\n");
		out.write("\t\t\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t\t\t}\n\n");
		out.write("\t\t\tcount++;\n\t\t\tfree(fieldlist);\n\t\t}\n\t}\n\n");

		out.write("\tcursor->destroy(&cursor);\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");

		out.write("\t*(int *) select.num_recs = count;\n");
		out.write("\tselect.table_id = malloc(sizeof(int));\n");
		out.write("\t*(int *) select.table_id = id;\n");
		out.write("\tselect.value = malloc(value_size);\n");
		out.write("\tselect.count = malloc(sizeof(int));\n");
		out.write("\t*(int *) select.count = -1;\n\n");

		out.write("\tfree(table_id);\n");
		out.write("\tfree(table_name);\n\n");
		out.write("\tfree(ion_record.key);\n");
		out.write("\tfree(ion_record.value);\n\n");
		out.write("\treturn select;\n");
		out.write("}\n\n");

		out.write("ion_boolean_t next(iinq_result_set *select) {\n");
		out.write("\tif (*(int *) select->count < (*(int *) select->num_recs) - 1) {\n");
		out.write("\t\t*(int *) select->count = (*(int *) select->count) + 1;\n");
		out.write("\t\treturn boolean_true;\n\t}\n\n");
		out.write("\tion_err_t error = iinq_drop(\"SEL\");\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tfree(select->value);\n");
		out.write("\tfree(select->fields);\n");
		out.write("\tfree(select->count);\n");
		out.write("\tfree(select->table_id);\n");
		out.write("\tfree(select->num_recs);\n");
		out.write("\tfree(select->num_fields);\n");
		out.write("\treturn boolean_false;\n}\n\n");

		out.write("char* getString(iinq_result_set *select, int field_num) {\n");
		out.write("\tint i, count = 0;\n\n");
		out.write("\tion_err_t                  error;\n");
		out.write("\tion_dictionary_t           dictionary;\n");
		out.write("\tion_dictionary_handler_t   handler;\n\n");
		out.write("\tdictionary.handler = &handler;\n\n");
		out.write("\terror              = iinq_open_source(\"SEL\", &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tdictionary_get(&dictionary, select->count, select->value);\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tfor (i = 0; i < *(int *) select->num_fields; i++) {\n");
		out.write("\t\tint field = *(int *) (select->fields + sizeof(int)*i);\n\n");
		out.write("\t\tif (getFieldType(select->table_id, field) == iinq_null_terminated_string) {\n");
		out.write("\t\t\tcount++;\n\t\t}\n\n");
		out.write("\t\tif (count == field_num) {\n");
		out.write("\t\t\treturn (char *) (select->value + calculateOffset(select->table_id, field-1));\n");
		out.write("\t\t}\n\t}\n\n\treturn \"\";\n}\n\n");

		out.write("int getInt(iinq_result_set *select, int field_num) {\n");
		out.write("\tint i, count = 0;\n\n");
		out.write("\tion_err_t                  error;\n");
		out.write("\tion_dictionary_t           dictionary;\n");
		out.write("\tion_dictionary_handler_t   handler;\n\n");
		out.write("\tdictionary.handler = &handler;\n\n");
		out.write("\terror              = iinq_open_source(\"SEL\", &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tdictionary_get(&dictionary, select->count, select->value);\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		out.write("\t}\n\n");
		out.write("\tfor (i = 0; i < *(int *) select->num_fields; i++) {\n");
		out.write("\t\tint field = *(int *) (select->fields + sizeof(int)*i);\n\n");
		out.write("\t\tif (getFieldType(select->table_id, field) == iinq_int) {\n");
		out.write("\t\t\tcount++;\n\t\t}\n\n");
		out.write("\t\tif (count == field_num) {\n");
		out.write("\t\t\treturn NEUTRALIZE(select->value + calculateOffset(select->table_id, field-1), int);\n");
		out.write("\t\t}\n\t}\n\n\treturn 0;\n}\n\n");

		function_headers.add("iinq_result_set iinq_select(int id, char *name, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_fields, ...);\n");
		function_headers.add("ion_boolean_t next(iinq_result_set *select);\n");
		function_headers.add("char* getString(iinq_result_set *select, int field_num);\n");
		function_headers.add("int getInt(iinq_result_set *select, int field_num);\n");

	}

	private static void reload_tables() throws SQLException, ParserConfigurationException, IOException, SAXException {
		iinqDatabase.reloadTablesFromXML();
	}

	public static void create_empty_database(String path) throws Exception {
		create_xml_source(path, "iinq_sources.xml");
		create_database(path, "iinq_database.xml");
	}

	public static void add_table_to_database(String path, String filename, IinqTable table, String sql) throws Exception {
/*		String fullPath = path + "/" + filename;

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		TransformerFactory transFactory = TransformerFactory.newInstance();
		Transformer trans = transFactory.newTransformer();
		Document xml = builder.parse(fullPath);
		StreamResult result;
		Element node;

		Element root = xml.getDocumentElement();
		Element tableNode = xml.createElement("TABLE");

		node = xml.createElement("semanticTableName");
		node.appendChild(xml.createTextNode(xml.getElementsByTagName("databaseName").item(0).getTextContent() +
				"." + table.getTableName()));
		tableNode.appendChild(node);

		node = xml.createElement("tableName");
		node.appendChild(xml.createTextNode(table.getTableName()));
		tableNode.appendChild(node);

		node = xml.createElement("schemaName");
		tableNode.appendChild(node);

		node = xml.createElement("catalogName");
		tableNode.appendChild(node);

		// add the CREATE TABLE statement as a comment
		node = xml.createElement("comment");
		node.appendChild(xml.createTextNode(sql));
		tableNode.appendChild(node);

		node = xml.createElement("numTuples");
		node.appendChild(xml.createTextNode("1500"));
		tableNode.appendChild(node);

		// Add fields
		int position = 1;
		Iterator<IinqField> it = table.iterator();
		while (it.hasNext()) {
			IinqField field = it.next();
			Element fieldNode = xml.createElement("FIELD");

			node = xml.createElement("semanticFieldName");
			node.appendChild(xml.createTextNode(table.getTableName() + "." + field.getFieldName()));
			fieldNode.appendChild(node);

			node = xml.createElement("fieldName");
			node.appendChild(xml.createTextNode(field.getFieldName()));
			fieldNode.appendChild(node);

			node = xml.createElement("dataType");
			node.appendChild(xml.createTextNode(Integer.toString(field.getFieldType())));
			fieldNode.appendChild(node);

			node = xml.createElement("dataTypeName");
			node.appendChild(xml.createTextNode(field.getFieldTypeName()));
			fieldNode.appendChild(node);

			node = xml.createElement("fieldSize");
			node.appendChild(xml.createTextNode(Integer.toString(field.getFieldSize())));
			fieldNode.appendChild(node);

			node = xml.createElement("decimalDigits");
			node.appendChild(xml.createTextNode("0"));
			fieldNode.appendChild(node);

			node = xml.createElement("numberRadixPrecision");
			node.appendChild(xml.createTextNode("0"));
			fieldNode.appendChild(node);

			node = xml.createElement("remarks");
			fieldNode.appendChild(node);

			node = xml.createElement("defaultValue");
			fieldNode.appendChild(node);

			node = xml.createElement("characterOctetLength");
			node.appendChild(xml.createTextNode("0"));
			fieldNode.appendChild(node);

			node = xml.createElement("ordinalPosition");
			node.appendChild(xml.createTextNode(Integer.toString(position)));
			fieldNode.appendChild(node);

			node = xml.createElement("isNullable");
			if (position == table.getPrimaryKeyIndex()) {
				node.appendChild(xml.createTextNode("NO"));
			} else {
				node.appendChild(xml.createTextNode("YES"));
			}
			fieldNode.appendChild(node);

			node = xml.createElement("numDistinctValues");
			node.appendChild(xml.createTextNode("0"));
			fieldNode.appendChild(node);

			// Add field to table
			tableNode.appendChild(fieldNode);

			position++;
		}

		// Add primary key
		Element keyNode = xml.createElement("PRIMARYKEY");

		node = xml.createElement("keyScope");
		node.appendChild(xml.createTextNode("0"));
		keyNode.appendChild(node);

		node = xml.createElement("keyScopeName");
		keyNode.appendChild(node);

		node = xml.createElement("keyName");
		node.appendChild(xml.createTextNode("SYS_IDX_1"));
		keyNode.appendChild(node);

		node = xml.createElement("keyType");
		node.appendChild(xml.createTextNode("1"));
		keyNode.appendChild(node);

		// TODO: add support for composite keys
		node = xml.createElement("FIELDS");
		Element fieldNode = xml.createElement("fieldName");
		fieldNode.appendChild(xml.createTextNode(table.getPrimaryKey()));
		node.appendChild(fieldNode);
		keyNode.appendChild(node);

		tableNode.appendChild(keyNode);

		// write to sources XML file
		root.appendChild(tableNode);
		DOMSource dom = new DOMSource(xml);
		result = new StreamResult(new File(fullPath));
		trans.transform(dom, result);*/
	}

	public static void create_database(String path, String filename) throws Exception {
		File unityDB = new File(path + "/" + filename);

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		TransformerFactory transFactory = TransformerFactory.newInstance();
		Transformer trans = transFactory.newTransformer();
		DOMSource dom;
		StreamResult result;
		Element node;

		Document xml = builder.newDocument();

		Element root = xml.createElement("XSPEC");
		root.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
		root.setAttribute("xsi:noNamespaceSchemaLocation", "xspec.xsd");

		node = xml.createElement("semanticDatabaseName");
		root.appendChild(node);

		node = xml.createElement("databaseName");
		node.appendChild(xml.createTextNode("IinqDB"));
		root.appendChild(node);

		node = xml.createElement("databaseSystemName");
		root.appendChild(node);

		node = xml.createElement("databaseId");
		node.appendChild(xml.createTextNode("95020200"));
		root.appendChild(node);

		node = xml.createElement("databaseProductName");
		node.appendChild(xml.createTextNode("HSQL Database Engine"));
		root.appendChild(node);

		node = xml.createElement("databaseProductVersion");
		node.appendChild(xml.createTextNode("2.2.0"));
		root.appendChild(node);

		node = xml.createElement("urlJDBC");
		node.appendChild(xml.createTextNode("jdbc:hsqldb:mem:."));
		root.appendChild(node);

		node = xml.createElement("userid");
		node.appendChild(xml.createTextNode("sa"));
		root.appendChild(node);

		node = xml.createElement("driverName");
		node.appendChild(xml.createTextNode("HSQL Database Engine Driver"));
		root.appendChild(node);

		node = xml.createElement("delimitId");
		node.appendChild(xml.createTextNode("\""));
		root.appendChild(node);

		// write to sources XML file
		xml.appendChild(root);
		dom = new DOMSource(xml);
		result = new StreamResult(unityDB);
		trans.transform(dom, result);
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

	private static int
	ion_switch_key_type(
			String key_type
	) {
		key_type = key_type.toUpperCase();

		if (key_type.contains("VARCHAR")) {
			return Types.VARCHAR;
		}

		if (key_type.contains("CHAR")) {
			return Types.CHAR;
		}

		if (key_type.contains("INT")) {
			return Types.INTEGER;
		}

		if (key_type.contains("DECIMAL")) {
			return Types.DECIMAL;
		}

		return Types.NUMERIC; // TODO: When would we use this?
	}

	static String
	ion_switch_key_size(
			int key_type
	) {
		switch (key_type) {
			case Types.INTEGER: {
				return "sizeof(int)";
			}

			// TODO: change to appropriate string length
			case Types.CHAR:
			case Types.VARCHAR: {
				return "20";
			}

			case Types.DECIMAL: {
				return "sizeof(double)";
			}
		}

		return "20"; // TODO: Where did this value come from?
	}

	protected static String
	ion_get_value_size(String table_name, String field_type) {
		return ion_get_value_size(metadata.getTable("IinqDB", table_name), field_type);
	}

	private static String
	ion_get_value_size(
			SourceTable table, String field_name
	) {
		SourceField field = table.getSourceFields().get(field_name.toLowerCase());
		if (field.getDataType() == Types.CHAR) {
			return "sizeof(char) * " + field.getColumnSize();
		} else if (field.getDataType() == Types.VARCHAR) {
			return "sizeof(char) * " + field.getColumnSize();
		} else if (field.getDataType() == Types.BOOLEAN) {
			return "sizeof(ion_boolean_t)";
		} else if (field.getDataType() == Types.INTEGER) {
			return "sizeof(int)";
		} else if (field.getDataType() == Types.FLOAT || field.getDataType() == Types.DECIMAL) {
			return "sizeof(float)";
		}

		return "sizeof(int)";
	}


	private static void
	print_error(BufferedWriter out) throws IOException {
		out.newLine();
		out.newLine();

		out.write(iinq.functions.CommonCode.error_check());
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
		contents += "#include \"../../dictionary/dictionary_types.h\"\n" +
				"#include \"../../dictionary/dictionary.h\"\n" +
				"#include \"../iinq.h\"\n" + "#include \"iinq_functions.h\"\n\n";

		out.write(contents.getBytes());
	}

	private static void
	print_table(BufferedWriter out, String table_name) throws IOException, InvalidArgumentException, RelationNotFoundException, SQLFeatureNotSupportedException {

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

		for (int i = 0; i < function_headers.size(); i++) {
			contents += function_headers.get(i);
		}

		contents += iinqDatabase.getInsertHeaders();
		contents += iinqDatabase.getExecutionHeader();

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
		IinqInsertFields insert;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("INSERT") && !line.contains("/*") && !line.contains("//")) {
				contents += "/* " + line + " */\n";

				boolean prep = true;
				int index = line.indexOf("SQL_prepare");
				if (index == -1) {
					index = line.indexOf("SQL_execute");
					prep = false;
				}
				String sql = line.substring(index);
				sql = sql.substring(sql.toUpperCase().indexOf("INSERT"), sql.indexOf(";")).trim();
				StringFunc.verifyTerminator(sql);
				String temp = line.substring(0, index);

				insert = iinqDatabase.getInsert(count).getInsertParameters().insertFields;

				if (insert != null) {
					insert.sortFields();
					contents += "\t" + temp;
					if (!prep)
						contents += "execute(";
					contents += "insert_" + iinqDatabase.getInsert(count).getInsertParameters().name + "(";
					int field_count = 1;
					for (int j = 0; j < insert.fields.size(); j++) {
						if (insert.fields.get(j) != null) {
							if (j > 0) {
								contents += ", ";
							}
							while (insert.fields.get(j).field_num != field_count) {
								contents += "NULL_FIELD, ";
								field_count++;
							}

							if (prep && insert.fields.get(j).field.equals("(?)") || insert.fields.get(j).field.equals("?"))
								contents += "PREPARED_FIELD";
							else {
								if (insert.fields.get(j).field_type.equals("char")) {
									contents += insert.fields.get(j).field.replace("\'", "\"");
								} else {
									contents += insert.fields.get(j).field;
								}
							}
						}
						field_count++;
					}
					while (field_count <= insert.total_fields) {
						contents += ", NULL_FIELD";
						field_count++;
					}
				}
				if (!prep)
					contents += ")";
				contents += ");\n";
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
		iinq.delete_fields delete;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("DELETE") && line.contains("SQL_execute") && !line.contains("/*") && !line.contains("//")) {
				contents.append("/* " + line + " */\n");

				delete = delete_fields.get(count);

				if (delete != null) {
					contents.append("\tdelete_record(" + delete.table_id + ", \"" + delete.table_name + "\", "
							+ "print_table_" + delete.table_name + ", " + delete.ion_key + ", "
							+ delete.key_size + ", " + delete.value_size + ", " + delete.num_wheres);

					if (delete.num_wheres > 0) {
						contents.append(", IINQ_CONDITION_LIST(");
						for (int j = 0; j < delete.num_wheres; j++) {
							contents.append("IINQ_CONDITION(" + delete.fields.get(j) + ", " + delete.operators.get(j) + ", ");
							contents.append(delete.values.get(j) + "), ");
						}
						contents.setLength(contents.length()-2);
						contents.append(")");
					}

					contents.append(");\n");
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
			if ((line.toUpperCase()).contains("UPDATE") && !line.contains("/*") && !line.contains("//")) {
				contents.append("/* " + line + " */\n");

				update = IinqUpdate.get(count);
				int implicit_count = 0;

				if (update != null) {
					contents.append("\tupdate(" + update.table_id + ", \"" + update.table_name + "\", print_table_"
							+ update.table_name + ", " + update.ion_key + ", "
							+ update.key_size + ", " + update.value_size + ", " + update.num_wheres + ", "
							+ update.num_updates);

					if (update.num_wheres > 0) {
						contents.append(", IINQ_CONDITION_LIST(");
						for (int j = 0; j < update.num_wheres; j++) {
							contents.append("IINQ_CONDITION(" + update.where_fields.get(j) + ", " + update.where_operators.get(j) + ", ");
							contents.append(update.where_values.get(j) + "), ");
						}
						contents.setLength(contents.length()-2);
						contents.append(")");
					}

					contents.append(", IINQ_UPDATE_LIST(");
					for (int i = 0; i < update.num_updates; i++) {
						contents.append("IINQ_UPDATE(");
						if (!update.implicit.get(i)) {
							contents.append(update.update_fields.get(i) + ", 0, 0, ");

							if (update.update_field_types.get(i).contains("INT")) {
								contents.append(update.update_values.get(i));
							} else {
								contents.append("\"" + update.update_values.get(i) + "\"");
							}
						} else {
							contents.append(update.update_fields.get(i) + ", " + update.implicit_fields.get(implicit_count) + ", "
									+ update.update_operators.get(implicit_count) + ", ");

							if (update.update_field_types.get(i).contains("INT")) {
								contents.append(update.update_values.get(i));
							} else {
								contents.append("\"" + update.update_values.get(i) + "\"");
							}
							implicit_count++;
						}
						contents.append("), ");
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
	select_setup() throws IOException {

		/* Add new functions to be run to executable */
		String ex_path = user_file;
		BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

		StringBuilder contents = new StringBuilder();
		String line;
		iinq.select_fields select;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("SELECT") && !line.contains("/*") && !line.contains("//")) {
				contents.append("/* " + line + " */\n");

				select = select_fields.get(count);

				if (select != null) {
					contents.append("\t" + select.return_value + " = iinq_select(" + select.table_id + ", \"" + select.table_name + "\", " + select.key_type + ", " + select.key_size + ", "
							+ select.value_size + ", " + select.num_wheres + ", " + select.num_fields + ", ");

					if (select.num_wheres > 0) {
						contents.append(", IINQ_CONDITION_LIST(");
						for (int j = 0; j < select.num_wheres; j++) {
							contents.append("IINQ_CONDITION(");
							contents.append(select.where_fields.get(j) + ", " + select.where_operators.get(j) + ", ");

							if (select.where_field_types.get(j).contains("INT")) {
								contents.append(select.where_values.get(j));
							} else {
								contents.append("\"" + select.where_values.get(j) + "\"");
							}
							contents.append("), ");
						}
						contents.setLength(contents.length()-2);
						contents.append(")");
					}

					for (int i = 0; i < select.num_fields; i++) {
						contents.append(", " + select.fields.get(i));
					}

					contents.append(");\n");
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
		iinq.create_fields create;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("CREATE TABLE") && !line.contains("/*") && !line.contains("//")) {
				contents += "/* " + line + " */\n";

				create = create_fields.get(count);

				if (create != null) {
					contents += "\tcreate_table(\"" + create.table_name + "\", " + create.key_type + ", " + create.key_size + ", "
							+ create.value_size + ");\n";

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
		String table;
		int count = 0;

		while (null != (line = ex_file.readLine())) {
			if ((line.toUpperCase()).contains("DROP TABLE") && !line.contains("/*") && !line.contains("//")) {
				contents += "/* " + line + " */\n";

				table = drop_tables.get(count);

				if (table != null) {
					contents += "\tdrop_table(\"" + table + "\");\n";

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

	private static void getUnityConnection(String url) throws SQLException, ClassNotFoundException {
		Class.forName("unity.jdbc.UnityDriver");
		conUnity = DriverManager.getConnection(url);
		metadata = ((UnityConnection) conUnity).getGlobalSchema();
		ArrayList<SourceDatabase> databases = metadata.getAnnotatedDatabases();
		if (null == databases) {
			System.out.println("\nNo databases have been detected.");
		}
	}

	private static void getJavaConnection(String url) throws SQLException, ClassNotFoundException {
		Class.forName("org.hsqldb.jdbc.JDBCDriver");
		conJava = DriverManager.getConnection(url);
	}

	public static String
	getSchemaKeyword(String table_name, SchemaKeyword keyword) throws IOException, RelationNotFoundException, InvalidArgumentException, SQLFeatureNotSupportedException {
		return iinqDatabase.getSchemaValue(table_name, keyword, -1);
	}



	private static String get_field_size(SourceField field) throws SQLFeatureNotSupportedException {
		switch (field.getDataType()) {
			case 1: // CHAR
			case 12: // VARCHAR
				return String.format("sizeof(char) * %d", field.getColumnSize());
			case 3: // DECIMAL
				return "sizeof(double)";
			case 4: // INT
				return "sizeof(int)";
			default:
				throw new SQLFeatureNotSupportedException(String.format("Data type not supported: %s", field.getDataTypeName()));
		}
	}

	private static String[]
	get_fields(String statement, int num_fields) {

		String[] fields = new String[num_fields];

		int pos = 0;
		String field;

		for (int j = 0; j < num_fields; j++) {
			pos = statement.indexOf("AND");

			if (-1 != pos) {
				field = statement.substring(0, pos).trim();

				statement = statement.substring(pos + 3).trim();

				fields[j] = field;
			} else {
				fields[j] = statement;
			}
		}

		return fields;
	}

	private static void
	create_xml_source(String path, String filename) throws Exception {
		File unitySources = new File(path + "/" + filename);

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		TransformerFactory transFactory = TransformerFactory.newInstance();
		Transformer trans = transFactory.newTransformer();
		DOMSource dom;
		StreamResult result;
		Element node;

		Document xml = builder.newDocument();

		Element root = xml.createElement("SOURCES");
		Element database = xml.createElement("DATABASE");

		node = xml.createElement("URL");
		node.appendChild(xml.createTextNode("jdbc:hsqldb:mem:."));
		database.appendChild(node);

		node = xml.createElement("PASSWORD");
		database.appendChild(node);

		node = xml.createElement("DRIVER");
		node.appendChild(xml.createTextNode("org.hsqldb.jdbcDriver"));
		database.appendChild(node);

		node = xml.createElement("SCHEMA");
		node.appendChild(xml.createTextNode("iinq_database.xml"));
		database.appendChild(node);

		root.appendChild(database);

		// write to sources XML file
		xml.appendChild(root);
		dom = new DOMSource(xml);
		result = new StreamResult(unitySources);
		trans.transform(dom, result);
	}

	private static String
	create_schema_variable(String database_name, String table_name) throws RelationNotFoundException, IOException, InvalidArgumentException, SQLFeatureNotSupportedException {
		StringBuilder schema = new StringBuilder("(iinq_schema_t[]) {TABLE_SCHEMA(");
		int num_fields = Integer.parseInt(iinqDatabase.getSchemaValue(table_name, NUMBER_OF_FIELDS));

		schema.append(num_fields);
		schema.append(",\nTABLE_FIELD_TYPES(");
		for (int i = 0; i < num_fields; i++) {
			if (i > 0) {
				schema.append(", ");
			}
			switch (Integer.parseInt(iinqDatabase.getSchemaValue(table_name, FIELD_TYPE, i))) {
				case 1:
				case 12:
					schema.append("IINQ_STRING");
					break;
				case 3:
					schema.append("IINQ_DOUBLE");
					break;
				case 4:
					schema.append("IINQ_INT");
					break;
			}
		}
		schema.append("),\n TABLE_FIELD_SIZES(");
		for (int i = 0; i < num_fields; i++) {
			if (i > 0) {
				schema.append(", ");
			}
			schema.append(get_field_size(metadata.getTable(database_name, table_name).getSourceFieldsByPosition().get(i)));
		}
		schema.append("),\nTABLE_FIELD_NAMES(");
		for (int i = 0; i < num_fields; i++) {
			if (i > 0) {
				schema.append(", ");
			}
			schema.append(String.format("\"%s\"", iinqDatabase.getSchemaValue(table_name, FIELD_NAME, i)));
		}
		schema.append("))}");
		return schema.toString();
	}

	private static void
	create_table(String sql, BufferedWriter out) throws Exception {
		String table_name = null;
		sql = sql.substring(sql.toUpperCase().indexOf("CREATE"), sql.indexOf(";"));
		sql = StringFunc.verifyTerminator(sql);    // Make sure SQL is terminated by semi-colon properly

		// Create the table using HSQLDB to avoid string parsing
		IinqTable table = iinqDatabase.executeCreateTable(sql);

		/* Create CREATE TABLE method */
		if (!create_written) {
			IinqFunction function = new CreateTableFunction();
			out.write(function.getDefinition());

			function_headers.add(function.getHeader());
		}

		create_written = true;

		create_fields.add(new create_fields(table));
	}

	private static void
	insert(String sql, BufferedWriter out) throws IOException, InvalidArgumentException, RelationNotFoundException, SQLException {
		System.out.println("insert statement");

		sql = sql.substring(sql.toUpperCase().indexOf("INSERT"), sql.indexOf(";")).trim();
		sql = StringFunc.verifyTerminator(sql);    // Make sure SQL is terminated by semi-colon properly

		// Use UnityJDBC to parse the insert statement (metadata is required to verify fields)
		PreparedInsertFunction insert =  iinqDatabase.executeInsertStatement(sql);

		/* Create print table method if it doesn't already exist */
/*		if (!tables.get(table_name).isPrintFunctionWritten()) {
			print_table(out, table_name);
			tables.get(table_name).setPrintFunctionWritten(true);
		}*/
	}

	/* Concatenates information for additional tables onto already written INSERT functions */
	private static void
	params() throws IOException {
		String path = function_file;
		BufferedReader file = new BufferedReader(new FileReader(path));

		String contents = "";
		String line;
		boolean written;
		String table_name;
		ArrayList<String> params_written = new ArrayList<>();
		ArrayList<String> execute_written = new ArrayList<>();

		while (null != (line = file.readLine())) {
			if (line.contains("/* INSERT 4 */")) {
				int table_id;
				String key_type;

				for (int i = 0; i < inserts.size(); i++) {
					table_name = inserts.get(i).name;

					written = table_name.equals(written_table);

					if (!written) {
						for (int k = 0; k < execute_written.size(); k++) {
							if (table_name.equals(execute_written.get(k))) {
								written = true;
							}
						}
					}

					if (!written) {
						execute_written.add(table_name);
						table_id = inserts.get(i).id;
						key_type = inserts.get(i).key_type;
						table_name = inserts.get(i).name;

						contents += "\tif (*(int *) p.table == " + table_id + ") {\n";

						if (Integer.parseInt(key_type) == Types.INTEGER) {
							contents += "\t\tiinq_execute(\"" + table_name + "\", IONIZE(*(int *) p.key, int), p.value, iinq_insert_t);\n";
						} else {
							contents += "\t\tiinq_execute(\"" + table_name + "\", p.key, p.value, iinq_insert_t);\n";
						}
						contents += "\t}\n";
					}
				}
			}
			contents += line + '\n';
		}

		if (iinqDatabase.containsPreparedStatements()) {
			SetPreparedParametersFunction func = new SetPreparedParametersFunction();
			function_headers.add(func.getHeader());
			contents += func.getDefinition();
		}
		contents += iinqDatabase.getExecutionDefinition();

		for (int i = 0, n = iinqDatabase.getNumInserts(); i < n; i++) {
			PreparedInsertFunction insert = iinqDatabase.getInsert(i);
			if (!insert.isDuplicate()) {
				contents += insert.getDefinition();
			}
		}

		File ex_output_file = new File(path);
		FileOutputStream out = new FileOutputStream(ex_output_file, false);

		out.write(contents.getBytes());

		file.close();
		out.close();
	}

	private static void
	calculate_functions(BufferedWriter out) throws IOException {
		if (iinqDatabase.getTableCount() > 0) {
			String field_size_function = "";
			out.write("size_t calculateOffset(const unsigned char *table, int field_num) {\n\n");
			field_size_function += "iinq_field_t getFieldType(const unsigned char *table, int field_num) {\n\n";

			String offset_header = "size_t calculateOffset(const unsigned char *table, int field_num);\n";
			function_headers.add(offset_header);

			String size_header = "iinq_field_t getFieldType(const unsigned char *table, int field_num);\n";
			function_headers.add(size_header);

			out.write("\tswitch (*(int *) table) {\n");
			field_size_function += "\tswitch (*(int *) table) {\n";

			for (int i = 0, n = iinqDatabase.getTableCount(); i < n; i++) {
				IinqTable table = iinqDatabase.getIinqTableFromId(i);
				int table_id = table.getTableId();
				int num_fields = table.getNumFields();

				out.write("\t\tcase " + table_id + " : {\n");
				out.write("\t\t\tswitch (field_num) {\n");

				int int_count;
				boolean char_present = false;
				int char_multiplier;
				String data_type;

				for (int j = 1; j <= num_fields; j++) {
					char_multiplier = 0;
					int_count = 0;

					for (int k = 1; k <= j; k++) {
						data_type = table.getFieldTypeName(i).toLowerCase();

						if (data_type.contains("char")) {
							char_multiplier += table.getFieldSize(k);
							char_present = true;
						}

						if (data_type.contains("int")) {
							int_count++;
						}
					}

					out.write("\t\t\t\tcase " + j + " :\n");
					out.write("\t\t\t\t\treturn ");

					if (int_count > 0) {
						if (int_count > 1) {
							out.write("(sizeof(int) * " + int_count + ")");
						} else {
							out.write("sizeof(int)");
						}

						if (char_present) {
							out.write("+");
						} else {
							out.write(";\n");
						}
					}

					if (char_present) {
						out.write("(sizeof(char) * " + char_multiplier + ");\n");
					}
				}
				out.write("\t\t\t\tdefault:\n\t\t\t\t\treturn 0;\n");
				out.write("\t\t\t}\n\t\t}\n");
			}

			out.write("\t\tdefault:\n\t\t\treturn 0;\n");
			out.write("\t}\n}\n\n");

			out.write(field_size_function);
		}
	}

	private static void
	update(String sql, BufferedWriter out) throws IOException, InvalidArgumentException, RelationNotFoundException, SQLException {
		System.out.println("update statement");

		sql = sql.substring(sql.toUpperCase().indexOf("UPDATE"), sql.indexOf(";"));
		sql = StringFunc.verifyTerminator(sql);

		iinqDatabase.executeUpdateStatement(sql);

		/* Check if that table name already has an ID */
/*		for (int i = 0; i < table_names.size(); i++) {
			if (table_names.get(i).equalsIgnoreCase(table_name)) {
				table_id = i;
				new_table = false;
				table_found = true;
				break;
			}
		}

		if (!table_found) {
			table_names.add(table_name);
			table_id = table_id_count;
			table_id_count++;
			new_table = true;
		}*/

		/* Create print table method if it doesn't already exist */
/*		if (!tables.get(table_name).isPrintFunctionWritten()) {
			print_table(out, table_name);
			tables.get(table_name).setPrintFunctionWritten(true);
		}*/

/*		if (!update_written) {
			write_update_method(out);
		}*/

/*		update_written = true;*/










/*		if (new_table) {
			tableInfo table_info = new tableInfo(table_id, Integer.parseInt(iinqDatabase.getSchemaValue(table_name, NUMBER_OF_FIELDS)), new ArrayList<String>(Arrays.asList(where.getIinq_field_types())), field_sizes);

			calculateInfo.add(table_info);
			//tables_count++;
		}*/




	}

	private static void
	select(String sql, BufferedWriter out) throws IOException, SQLException, RelationNotFoundException, InvalidArgumentException {
		System.out.println("select statement");

		String return_val = sql.substring(0, sql.indexOf("=") - 1);

		sql = sql.substring(sql.indexOf("("));
		sql = sql.substring(sql.toUpperCase().indexOf("SELECT"), sql.indexOf(";")).trim();
		sql = StringFunc.verifyTerminator(sql);    // Make sure SQL is terminated by semi-colon properly

		// Parse semantic query string into a parse tree
		GlobalParser kingParser;
		GlobalQuery gq;
		if (null != metadata) {
			kingParser = new GlobalParser(false, true);
			gq = kingParser.parse(sql, metadata);
		} else {
			kingParser = new GlobalParser(false, false);
			gq = kingParser.parse(sql, new GlobalSchema());
		}
		gq.setQueryString(sql);

		// Optimize logical query tree before execution
		Optimizer opt = new Optimizer(gq, false, null);
		gq = opt.optimize();

		IinqBuilder builder = new IinqBuilder(gq.getLogicalQueryTree().getRoot());
		IinqQuery query = builder.toQuery();
		String table_name = query.getTableName();

		boolean table_found = false;
		int table_id = 0;

		/* Check if that table name already has an ID */
/*		for (int i = 0; i < table_names.size(); i++) {
			if (table_names.get(i).equals(table_name)) {
				table_id = i;
				new_table = false;
				table_found = true;
			}
		}

		if (!table_found) {
			table_names.add(table_name);
			table_id = table_id_count;
			table_id_count++;
			new_table = true;
		}*/

		SourceTable table = metadata.getTable("IinqDB", table_name);

		/* Create print table method if it doesn't already exist */
		if (!tables.get(table_name.toLowerCase()).isPrintFunctionWritten()) {
			print_table(out, table_name);
			tables.get(table_name.toLowerCase()).setPrintFunctionWritten(true);
		}

		if (!select_written) {
			write_select_method(out);
		}

		select_written = true;

		int pos = sql.toUpperCase().indexOf("WHERE");
		String where_condition = query.getParameter("filter");
		int num_conditions = 0;
		int i = -1;

		/* Get WHERE condition if it exists */
		if (-1 != pos) {
			//where_condition = sql.substring(pos + 6, sql.length() - 4);
			i = 0;
		}

		/* Calculate number of WHERE conditions in statement */

		while (-1 != i) {
			num_conditions++;
			i = where_condition.indexOf(",", i + 1);
		}

		String[] conditions;

		conditions = get_fields(where_condition, num_conditions);

		/* Get fields to select */
		String field_list;
		pos = sql.toUpperCase().indexOf("SELECT");
		field_list = query.getParameter("fields");

		int num_fields = 0;
		i = 0;

		/* Calculate number of fields to select in statement */
		while (-1 != i) {
			num_fields++;
			i = field_list.indexOf(",", i + 1);
		}

		ArrayList<Integer> where_field = new ArrayList<>(); /* Field value that is being used to update a field. */
		ArrayList<String> where_value = new ArrayList<>(); /* Value that is being added to another field value to update a field. */
		ArrayList<String> where_operator = new ArrayList<>(); /* Whether values are being updated through addition or subtraction. */
		ArrayList<String> iinq_field_types = new ArrayList<>();
		ArrayList<String> where_field_type = new ArrayList<>();

		int len = 0;
		String field = "";

		for (int j = 0; j < num_conditions; j++) {

			/* Set up field, operator, and condition for each WHERE clause */
			if (conditions[j].contains("!=")) {
				pos = conditions[j].indexOf("!=");
				len = 2;
				where_operator.add("iinq_not_equal");
			} else if (conditions[j].contains("<=")) {
				pos = conditions[j].indexOf("<=");
				len = 2;
				where_operator.add("iinq_less_than_equal_to");
			} else if (conditions[j].contains(">=")) {
				pos = conditions[j].indexOf(">=");
				len = 2;
				where_operator.add("iinq_greater_than_equal_to");
			} else if (conditions[j].contains("=")) {
				pos = conditions[j].indexOf("=");
				len = 1;
				where_operator.add("iinq_equal");
			} else if (conditions[j].contains("<")) {
				pos = conditions[j].indexOf("<");
				len = 1;
				where_operator.add("iinq_less_than");
			} else if (conditions[j].contains(">")) {
				pos = conditions[j].indexOf(">");
				len = 1;
				where_operator.add("iinq_greater_than");
			}

			field = conditions[j].substring(0, pos).trim();
			where_value.add(conditions[j].substring(pos + len).trim());
		}

		for (int n = 0; n < Integer.parseInt(iinqDatabase.getSchemaValue(table_name, NUMBER_OF_FIELDS)); n++) {

			String field_type = iinqDatabase.getSchemaValue(table_name, FIELD_TYPE, n);

			if (field_type.contains("CHAR")) {
				iinq_field_types.add("iinq_null_terminated_string");
			} else {
				iinq_field_types.add("iinq_int");
			}

			if (field.equals(iinqDatabase.getSchemaValue(table_name, FIELD_NAME, n))) {
				where_field.add(n + 1);
				where_field_type.add(field_type);
			}
		}

		ArrayList<Integer> select_field_nums = new ArrayList<>();
		ArrayList<String> field_sizes = new ArrayList<>();

		String[] fields;

		fields = get_fields(field_list, num_fields);

		for (int j = 0; j < num_fields; j++) {
			for (int n = 0; n < Integer.parseInt(iinqDatabase.getSchemaValue(table_name, NUMBER_OF_FIELDS)); n++) {
				String field_type = iinqDatabase.getSchemaValue(table_name, FIELD_TYPE, n);
				field_sizes.add(ion_get_value_size(table, table.getSourceFieldsByPosition().get(j).getColumnName()));

				if ((fields[j].trim()).equals(iinqDatabase.getSchemaValue(table_name, FIELD_NAME, n))) {
					select_field_nums.add(n + 1);
				}
			}
		}

		if (new_table) {
			tableInfo table_info = new tableInfo(table_id, Integer.parseInt(iinqDatabase.getSchemaValue(table_name, NUMBER_OF_FIELDS)), iinq_field_types, field_sizes);

			calculateInfo.add(table_info);
			//tables_count++;
		}

		String value_size = iinqDatabase.getSchemaValue(table_name, VALUE_SIZE);
		String key_size = iinqDatabase.getSchemaValue(table_name, PRIMARY_KEY_SIZE);
		String ion_key = iinqDatabase.getSchemaValue(table_name, ION_KEY_TYPE);

		select_fields.add(new select_fields(table_name, table_id, num_conditions, num_fields, where_field, where_operator,
				where_value, where_field_type, ion_key, key_size, value_size, select_field_nums, return_val));
	}

	private static void
	delete(String sql, BufferedWriter out) throws IOException, InvalidArgumentException, RelationNotFoundException, SQLException {
		System.out.println("delete statement");

		sql = sql.substring(sql.toUpperCase().indexOf("DELETE"), sql.indexOf(";"));
		sql = StringFunc.verifyTerminator(sql);

		// Use UnityJDBC to parse the drop table statement (metadata is required to verify table existence)
		GlobalParser kingParser;
		GlobalUpdate gu;
		if (null != metadata) {
			kingParser = new GlobalParser(false, true);
			gu = kingParser.parseUpdate(sql, metadata);
		} else {
			throw new SQLException("Metadata is required for dropping tables.");
		}
		LQDeleteNode delete = (LQDeleteNode) gu.getPlan().getLogicalQueryTree().getRoot();
		String table_name = delete.getSourceTable().getLocalName().toLowerCase();

		// TODO: update this after writing new buildCondition method for IinqBuilder
		LQCondNode conditionNode = delete.getCondition();
		ArrayList<String> conditions = null;
		int num_conditions = 0;
		if (conditionNode != null) {
			LQSelNode selNode = new LQSelNode();
			selNode.setCondition(conditionNode);
			IinqBuilder builder = new IinqBuilder(kingParser.parse("SELECT * FROM " + table_name + " WHERE " + conditionNode.generateSQL() + ";", metadata).getLogicalQueryTree().getRoot());
			IinqQuery query = builder.toQuery();
			Object filters = query.getParameterObject("filter");
			if (filters instanceof ArrayList) {
				conditions = (ArrayList) filters;
				num_conditions = conditions.size();
			} else if (filters instanceof String) {
				num_conditions = 1;
				conditions = new ArrayList<>();
				conditions.add((String) filters);
			}
		}

		boolean table_found = false;
		int table_id = 0;

		/* Check if that table name already has an ID */
/*		for (int i = 0; i < table_names.size(); i++) {
			if (table_names.get(i).equalsIgnoreCase(table_name)) {
				table_id = i;
				new_table = false;
				table_found = true;
			}
		}

		if (!table_found) {
			table_names.add(table_name);
			table_id = table_id_count;
			table_id_count++;
			new_table = true;
		}*/

		SourceTable table = metadata.getTable("IinqDB", table_name);
		IinqTable iinqTable = tables.get(table_name);
		if (null == iinqTable) {
			throw new SQLException("Delete attempted on non-existent table: " + table_name);
		}

		/* Create print table method if it doesn't already exist */
		if (!iinqTable.isPrintFunctionWritten()) {
			print_table(out, table_name);
			iinqTable.setPrintFunctionWritten(true);
		}

		/* Write function to file */
		String key_size = iinqDatabase.getSchemaValue(table_name, PRIMARY_KEY_SIZE);
		String value_size = iinqDatabase.getSchemaValue(table_name, VALUE_SIZE);

		if (!delete_written) {
			write_delete_method(out);
		}

		delete_written = true;


		String[] conditionFields = new String[num_conditions];

		for (int i = 0; i < num_conditions; i++) {
			conditionFields[i] = conditions.get(i);
		}

		IinqWhere iinqWhere = new IinqWhere(num_conditions);
		iinqWhere.generateWhere(conditionFields, iinqTable);

		String[] conditions1;
		String where_condition = sql.substring(sql.toUpperCase().indexOf("WHERE") + 5).trim();

		conditions1 = get_fields(where_condition, num_conditions);

		String field;

		if (new_table) {
			// TODO: update tableInfo constructor to take IinqWhere object a a parameter
			tableInfo table_info = new tableInfo(table_id, Integer.parseInt(iinqDatabase.getSchemaValue(table_name, NUMBER_OF_FIELDS)), new ArrayList(Arrays.asList(iinqWhere.getIinq_field_types())), new ArrayList(Arrays.asList(iinqWhere.getField_sizes())));

			calculateInfo.add(table_info);
			//tables_count++;
		}

		String ion_key = iinqDatabase.getSchemaValue(table_name, ION_KEY_TYPE);

		// TODO: update delete_fields to take an IinqWhere object as a parameter
		delete_fields.add(new delete_fields(table_name, table_id, num_conditions, new ArrayList<Integer>(Arrays.asList(iinqWhere.getWhere_field_nums())), new ArrayList<String>(Arrays.asList(iinqWhere.getWhere_operators())), new ArrayList<String>(Arrays.asList(iinqWhere.getWhere_values())), new ArrayList<String>(Arrays.asList(iinqWhere.getWhere_field_types())), key_size, value_size, ion_key));
	}

	private static void
	drop_table(String sql, BufferedWriter out) throws Exception {
		System.out.println("drop statement");

		sql = sql.substring(sql.toUpperCase().indexOf("DROP"));
		sql = sql.substring(0, sql.indexOf(";"));
		sql = StringFunc.verifyTerminator(sql);

		// Use UnityJDBC to parse the drop table statement (metadata is required to verify table existence)
		GlobalParser kingParser;
		GlobalUpdate gu;
		if (null != metadata) {
			kingParser = new GlobalParser(false, true);
			gu = kingParser.parseUpdate(sql, metadata);
		} else {
			throw new SQLException("Metadata is required for dropping tables.");
		}


		String table_name = ((LQDropNode) gu.getPlan().getLogicalQueryTree().getRoot()).getName().toLowerCase();
		IinqTable table = tables.get(table_name);
		if (table == null) {
			throw new SQLException("Attempt to drop non-existent table: " + table_name);
		}

		/* Delete from XML schema file */
		drop_table_from_database(directory, "iinq_database.xml", table_name);

		/* Refresh UnityJDBC */
		conUnity.close();
		getUnityConnection(urlUnity);

		/* Delete IinqTable reference */
		tables.remove(table_name);

		/* Drop table from in-memory database */
		Statement stmt = conJava.createStatement();
		stmt.execute(sql);

		/* Write function to file */
		if (!drop_written) {
			out.write("void drop_table(char *table_name) {\n\n");
			out.write("\tion_err_t error;\n\n");
			out.write("\terror = iinq_drop(table_name);");
			print_error(out);

			out.write("\tprintf(\"Table %s has been deleted." + "\\" + "n" + "\", table_name);");

			out.write("\n}\n\n");

			function_headers.add("void drop_table(char *table_name);\n");
		}

		drop_written = true;

		drop_tables.add(table_name);
	}

	public static void drop_table_from_database(String path, String filename, String table_name) throws Exception {
		String fullPath = path + "/" + filename;

		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		TransformerFactory transFactory = TransformerFactory.newInstance();
		Transformer trans = transFactory.newTransformer();
		Document xml = builder.parse(fullPath);
		StreamResult result;
		NodeList nodeList = xml.getElementsByTagName("TABLE");
		Element node;

		for (int i = 0, n = nodeList.getLength(); i < n; i++) {
			node = (Element) nodeList.item(i);
			if (node.getElementsByTagName("tableName").item(0).getTextContent().equalsIgnoreCase(table_name)) {
				node.getParentNode().removeChild(node);
				break;
			}
		}

		// write to sources XML file
		DOMSource dom = new DOMSource(xml);
		result = new StreamResult(new File(fullPath));
		trans.transform(dom, result);
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
