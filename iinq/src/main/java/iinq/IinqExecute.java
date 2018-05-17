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
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sun.javaws.exceptions.InvalidArgumentException;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import unity.annotation.*;
import unity.generic.query.QueryBuilder;
import unity.jdbc.UnityConnection;
import unity.jdbc.UnityPreparedStatement;
import unity.jdbc.UnityStatement;
import unity.parser.GlobalParser;
import unity.parser.PTreeBuilderValidater;
import unity.query.*;
import unity.util.StringFunc;

import javax.management.relation.RelationNotFoundException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class IinqExecute {

    private static int table_id_count = 0;
    private static int tables_count = 0;

    /* JVM options */
    private static String user_file; /**< Path to iinq_user.c source file. Mandatory for iinq to run. */
    private static String function_file; /**< Path to iinq_user_functions.c source file. Mandatory for iinq to run. */
    private static String function_header_file; /**< Path to iinq_user_functions.h source file. Mandatory for iinq to run. */
    private static String directory; /**< Path to directory to output UnityJDBC schema files. Mandatory for iinq to run. */
    private static boolean use_existing = false; /**< Optional JVM option to use pre-existing database files (i.e. Use tables generated from an earlier IinqExecute). */

    private static boolean param_written    = false;
    private static boolean delete_written   = false;
    private static boolean update_written   = false;
    private static boolean select_written   = false;
    private static boolean create_written   = false;
    private static boolean drop_written     = false;

    /* Variables for INSERT supported prepared statements on multiple tables */
    private static ArrayList<String>        table_names         = new ArrayList<>();
    private static ArrayList<insert>        inserts             = new ArrayList<>();
    private static ArrayList<insert_fields> insert_fields       = new ArrayList<>();
    private static ArrayList<String>        function_headers    = new ArrayList<>();
    private static ArrayList<tableInfo>     calculateInfo       = new ArrayList<>();
    private static ArrayList<delete_fields> delete_fields       = new ArrayList<>();
    private static ArrayList<update_fields> update_fields       = new ArrayList<>();
    private static ArrayList<select_fields> select_fields       = new ArrayList<>();
    private static ArrayList<create_fields> create_fields       = new ArrayList<>();
    private static ArrayList<String>        drop_tables         = new ArrayList<>();

    private static boolean new_table;
    private static String written_table;

	private static ArrayList<String> xml_schemas = new ArrayList<>();

	private static Connection conUnity = null; /**< Connection for UnityJDBC. */
	private static Connection conJava = null; /**< Connection for HSQLDB */
	private static GlobalSchema metadata = null; /**< Metadata object for Iinq tables */
	private static String urlUnity = null; /**< Url to use for UnityJDBC connection */
	private static String urlJava = null; /**< Url to use for HSQLDB */

	private static HashMap<String, IinqTable> tables = new HashMap<>();

    public static void main(String args[]) throws IOException, SQLException, RelationNotFoundException, InvalidArgumentException {

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
			if (!use_existing) {
				create_empty_database(directory);
			}

			getUnityConnection(urlUnity);
			getJavaConnection(urlJava);

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
            while (((sql = buff_in.readLine()) != null) && (!sql.contains("return 0;")))   {
                /* Verify file contents are as expected*/
                System.out.println (sql);

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
                }

                else if ((sql.toUpperCase()).contains("SELECT") && !sql.contains("/*") && !sql.contains("//")) {
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
		}
	}

	private static void write_delete_method(BufferedWriter out) throws IOException {
		out.write("void delete_record(int id, char *name, iinq_print_table_t print_function, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_fields, ...) {\n\n");
		out.write("\tva_list valist;\n");
		out.write("\tva_start(valist, num_fields);\n\n");
		out.write("\tunsigned char *table_id = malloc(sizeof(int));\n");
		out.write("\t*(int *) table_id = id;\n\n");
		out.write("\tchar *table_name = malloc(sizeof(char)*20);\n");
		out.write("\tmemcpy(table_name, name, sizeof(char)*20);\n\n");

		out.write("\tion_err_t                  error;\n");
		out.write("\tion_dictionary_t           dictionary;\n");
		out.write("\tion_dictionary_handler_t   handler;\n\n");

		out.write("\tdictionary.handler = &handler;\n\n");
		out.write("\terror              = iinq_open_source(table_name, &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");

		out.write("\tion_predicate_t predicate;\n");
		out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");

		out.write("\tion_dict_cursor_t *cursor = NULL;\n");
		out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n\n");
		out.write("\tion_record_t ion_record;\n");
		out.write("\tion_record.key     = malloc(key_size);\n");
		out.write("\tion_record.value   = malloc(value_size);\n\n");

		out.write("\tion_cursor_status_t status;\n\n");
		out.write("\terror = iinq_create_source(\"DEL.inq\", key_type, (ion_key_size_t) key_size, (ion_value_size_t) sizeof(int));\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tion_dictionary_t           dictionary_temp;\n");
		out.write("\tion_dictionary_handler_t   handler_temp;\n\n");

		out.write("\tdictionary_temp.handler = &handler_temp;\n\n");
		out.write("\terror              = iinq_open_source(\"DEL.inq\", &dictionary_temp, &handler_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tion_boolean_t condition_satisfied;\n\n");

		out.write("\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
		out.write("\t\tcondition_satisfied = where(table_id, &ion_record, num_fields, &valist);\n\n");
		out.write("\t\tif (!condition_satisfied || num_fields == 0) {\n");
		out.write("\t\t\terror = dictionary_insert(&dictionary_temp, ion_record.key, IONIZE(0, int)).error;\n\n");
		out.write("\t\t\tif (err_ok != error) {\n");
		out.write("\t\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n\t\t\t}\n");
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
		out.write("\t\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n\t\t}\n");
		out.write("\t}\n\n");

		out.write("\tcursor_temp->destroy(&cursor_temp);\n");
		out.write("\tprint_function(&dictionary);\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\terror = dictionary_delete_dictionary(&dictionary_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tfremove(\"DEL.inq\");\n");
		out.write("\tfree(table_id);\n");
		out.write("\tfree(table_name);\n");
		out.write("\tfree(ion_record.key);\n");
		out.write("\tfree(ion_record.value);\n");
		out.write("}\n\n");

		function_headers.add("void delete_record(int id, char *name, ion_key_type_t key_type, size_t key_size, size_t value_size, int fields, ...);\n");
	}

	private static void write_update_method(BufferedWriter out) throws IOException {
		out.write("void update(int id, char *name, iinq_print_table_t print_function, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_update, int num, ...) {\n\n");
		out.write("\tva_list valist;\n");
		out.write("\tva_start(valist, num);\n\n");
		out.write("\tunsigned char *table_id = malloc(sizeof(int));\n");
		out.write("\t*(int *) table_id = id;\n\n");
		out.write("\tchar *table_name = malloc(sizeof(char)*20);\n");
		out.write("\tmemcpy(table_name, name, sizeof(char)*20);\n\n");

		out.write("\tion_err_t                  error;\n");
		out.write("\tion_dictionary_t           dictionary;\n");
		out.write("\tion_dictionary_handler_t   handler;\n\n");

		out.write("\tdictionary.handler = &handler;\n\n");
		out.write("\terror              = iinq_open_source(table_name, &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");

		out.write("\tion_predicate_t predicate;\n");
		out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");

		out.write("\tion_dict_cursor_t *cursor = NULL;\n");
		out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n\n");
		out.write("\tion_record_t ion_record;\n");
		out.write("\tion_record.key     = malloc(key_size);\n");
		out.write("\tion_record.value   = malloc(value_size);\n\n");

		out.write("\tion_cursor_status_t status;\n\n");
		out.write("\terror = iinq_create_source(\"UPD.inq\", key_type, (ion_key_size_t) key_size, (ion_value_size_t) value_size);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tion_dictionary_t           dictionary_temp;\n");
		out.write("\tion_dictionary_handler_t   handler_temp;\n\n");

		out.write("\tdictionary_temp.handler = &handler_temp;\n\n");
		out.write("\terror              = iinq_open_source(\"UPD.inq\", &dictionary_temp, &handler_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tion_boolean_t condition_satisfied;\n\n");

		out.write("\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
		out.write("\t\tcondition_satisfied = where(table_id, &ion_record, num_wheres, &valist);\n\n");
		out.write("\t\tif (!condition_satisfied || num_wheres == 0) {\n");
		out.write("\t\t\terror = dictionary_insert(&dictionary_temp, ion_record.key, ion_record.value).error;\n\n");
		out.write("\t\t\tif (err_ok != error) {\n");
		out.write("\t\t\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n\t\t\t}\n");
		out.write("\t\t}\n\t}\n\n");

		out.write("\tcursor->destroy(&cursor);\n\n");

		out.write("\tint update_fields[num_update/4];\n");
		out.write("\tint implicit_fields[num_update/4];\n");
		out.write("\tiinq_math_operator_t operators[num_update/4];\n");
		out.write("\tvoid *field_values[num_update/4];\n");
		out.write("\tint i;\n\n");

		out.write("\tfor (i = 0; i < num_wheres; i++) {\n");
		out.write("\t\tva_arg(valist, void *);\n\t}\n\n");

		out.write("\tfor (i = 0; i < num_update/4; i++) {\n");
		out.write("\t\tupdate_fields[i]     = va_arg(valist, int);\n");
		out.write("\t\timplicit_fields[i]   = va_arg(valist, int);\n");
		out.write("\t\toperators[i]         = va_arg(valist, iinq_math_operator_t);\n");
		out.write("\t\tfield_values[i]      = va_arg(valist, void *);\n\t}\n\n");

		out.write("\tva_end(valist);\n\n");
		out.write("\tion_predicate_t predicate_temp;\n");
		out.write("\tdictionary_build_predicate(&predicate_temp, predicate_all_records);\n\n");
		out.write("\tion_dict_cursor_t *cursor_temp = NULL;\n");
		out.write("\tdictionary_find(&dictionary_temp, &predicate_temp, &cursor_temp);\n\n");

		out.write("\twhile ((status = iinq_next_record(cursor_temp, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
		out.write("\t\tfor (i = 0; i < num_update/4; i++) {\n");
		out.write("\t\t\tunsigned char *value;\n");
		out.write("\t\t\tif (implicit_fields[i] != 0) {\n");
		out.write("\t\t\t\tint new_value;\n");
		out.write("\t\t\t\tvalue = ion_record.value + calculateOffset(table_id, implicit_fields[i] - 1);\n\n");

		out.write("\t\t\t\tswitch (operators[i]) {\n");
		out.write("\t\t\t\t\tcase iinq_add :\n");
		out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) + (int) field_values[i]);\n");
		out.write("\t\t\t\t\t\tbreak;\n");
		out.write("\t\t\t\t\tcase iinq_subtract :\n");
		out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) - (int) field_values[i]);\n");
		out.write("\t\t\t\t\t\tbreak;\n");
		out.write("\t\t\t\t\tcase iinq_multiply :\n");
		out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) * (int) field_values[i]);\n");
		out.write("\t\t\t\t\t\tbreak;\n");
		out.write("\t\t\t\t\tcase iinq_divide :\n");
		out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) / (int) field_values[i]);\n");
		out.write("\t\t\t\t\t\tbreak;\n\t\t\t\t}\n");
		out.write("\t\t\t\tvalue = ion_record.value + calculateOffset(table_id, update_fields[i] - 1);\n");
		out.write("\t\t\t\t*(int *) value = new_value;\n\t\t\t}\n");

		out.write("\t\t\telse {\n");
		out.write("\t\t\t\tvalue = ion_record.value + calculateOffset(table_id, update_fields[i] - 1);\n\n");
		out.write("\t\t\t\tif (getFieldType(table_id, update_fields[i]) == iinq_int) {\n");
		out.write("\t\t\t\t\t*(int *) value = (int) field_values[i];\n\t\t\t\t}\n");
		out.write("\t\t\t\telse {\n");
		out.write("\t\t\t\t\tmemcpy(value, field_values[i], calculateOffset(table_id, update_fields[i]) - calculateOffset(table_id, update_fields[i - 1]));\n\t\t\t\t}\n\t\t\t}\n\t\t}\n\n");
		out.write("\t\terror = dictionary_update(&dictionary, ion_record.key, ion_record.value).error;\n\n");
		out.write("\t\tif (err_ok != error) {\n");
		out.write("\t\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t\t}\n\t}\n\n");
		out.write("\tcursor_temp->destroy(&cursor_temp);\n");
		out.write("\tprint_function(&dictionary);\n\n");
		out.write("\terror = dictionary_delete_dictionary(&dictionary_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");

		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");

		out.write("\tfremove(\"UPD.inq\");\n");
		out.write("\tfree(table_id);\n");
		out.write("\tfree(table_name);\n");
		out.write("\tfree(ion_record.key);\n");
		out.write("\tfree(ion_record.value);\n");
		out.write("}\n\n");

		function_headers.add("void update(int id, char *name, iinq_print_table_t print_function, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_update, int num, ...);\n");

	}

	private static void write_select_method(BufferedWriter out) throws IOException {
		out.write("iinq_result_set iinq_select(int id, char *name, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_fields, int num, ...) {\n\n");
		out.write("\tint i;\n");
		out.write("\tva_list valist, where_list;\n");
		out.write("\tva_start(valist, num);\n");
		out.write("\tva_copy(where_list, valist);\n\n");

		out.write("\tunsigned char *table_id = malloc(sizeof(int));\n");
		out.write("\t*(int *) table_id = id;\n\n");

		out.write("\tchar *table_name = malloc(sizeof(char)*20);\n");
		out.write("\tmemcpy(table_name, name, sizeof(char)*20);\n\n");

		out.write("\tion_err_t                  error;\n");
		out.write("\tion_dictionary_t           dictionary;\n");
		out.write("\tion_dictionary_handler_t   handler;\n\n");

		out.write("\tdictionary.handler = &handler;\n\n");
		out.write("\terror              = iinq_open_source(table_name, &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
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
		out.write("\terror = iinq_create_source(\"SEL.inq\", key_type, (ion_key_size_t) key_size, (ion_value_size_t) value_size);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tdictionary_temp.handler = &handler_temp;\n\n");
		out.write("\terror = iinq_open_source(\"SEL.inq\", &dictionary_temp, &handler_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
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
		out.write("\t\t\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t\t\t}\n\n");
		out.write("\t\t\tcount++;\n\t\t\tfree(fieldlist);\n\t\t}\n\t}\n\n");

		out.write("\tcursor->destroy(&cursor);\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary_temp);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
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
		out.write("\tion_err_t error = iinq_drop(\"SEL.inq\");\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
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
		out.write("\terror              = iinq_open_source(\"SEL.inq\", &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tdictionary_get(&dictionary, select->count, select->value);\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tfor (i = 0; i < *(int *) select->num_fields; i++) {\n");
		out.write("\t\tint field = *(int *) (select->fields + sizeof(int)*i);\n\n");
		out.write("\t\tif (getFieldType(select->table_id, field) == iinq_char) {\n");
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
		out.write("\terror              = iinq_open_source(\"SEL.inq\", &dictionary, &handler);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tdictionary_get(&dictionary, select->count, select->value);\n\n");
		out.write("\terror = ion_close_dictionary(&dictionary);\n\n");
		out.write("\tif (err_ok != error) {\n");
		out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
		out.write("\t}\n\n");
		out.write("\tfor (i = 0; i < *(int *) select->num_fields; i++) {\n");
		out.write("\t\tint field = *(int *) (select->fields + sizeof(int)*i);\n\n");
		out.write("\t\tif (getFieldType(select->table_id, field) == iinq_int) {\n");
		out.write("\t\t\tcount++;\n\t\t}\n\n");
		out.write("\t\tif (count == field_num) {\n");
		out.write("\t\t\treturn NEUTRALIZE(select->value + calculateOffset(select->table_id, field-1), int);\n");
		out.write("\t\t}\n\t}\n\n\treturn 0;\n}\n\n");

		function_headers.add("iinq_result_set iinq_select(int id, char *name, ion_key_type_t key_type, size_t key_size, size_t value_size, int num_wheres, int num_fields, int num, ...);\n");
		function_headers.add("ion_boolean_t next(iinq_result_set *select);\n");
		function_headers.add("char* getString(iinq_result_set *select, int field_num);\n");
		function_headers.add("int getInt(iinq_result_set *select, int field_num);\n");

	}

	private static void reload_tables() throws SQLException, ParserConfigurationException, IOException, SAXException {
		Statement stmt = conJava.createStatement();
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = docFactory.newDocumentBuilder();
		TransformerFactory transFactory = TransformerFactory.newInstance();
		Document xml = builder.parse(directory + "/iinq_database.xml");
		NodeList tableNodes = xml.getElementsByTagName("TABLE");

		for (int i = 0, n = tableNodes.getLength(); i < n; i++) {
			/* Create table statements are stored as a comment in the schema */
			String sql = ((Element) tableNodes.item(i)).getElementsByTagName("comment").item(0).getTextContent();
			stmt.execute(sql);
			IinqTable table = new IinqTable();
			String table_name = sql.substring(sql.toUpperCase().indexOf("TABLE")+5, sql.indexOf("(")).trim();
			table.setTableName(table_name);
			DatabaseMetaData newMetaData = conJava.getMetaData();
			ResultSet rst = newMetaData.getPrimaryKeys(null, null, table.getTableName().toUpperCase());
			rst.next();
			table.setPrimaryKey(rst.getString("COLUMN_NAME"));
			rst.close();

			// Get the remaining data
			rst = newMetaData.getColumns(null, null, table.getTableName().toUpperCase(), null);
			int num_fields = 0;
			while (rst.next()) {
				num_fields++;
				String field_name = rst.getString("COLUMN_NAME");
				int data_type = rst.getInt("DATA_TYPE");
				if (field_name.equals(table.getPrimaryKey())) {
					table.setPrimaryKeyIndex(num_fields);
					table.setPrimaryKeyType(data_type);
				}
				table.addField(field_name, data_type, rst.getString("TYPE_NAME"), rst.getInt("COLUMN_SIZE"));
			}
		}
	}

	public static void create_empty_database(String path) throws Exception {
    	create_xml_source(path, "iinq_sources.xml");
		create_database(path, "iinq_database.xml");
	}

	public static void add_table_to_database(String path, String filename, IinqTable table, String sql) throws Exception {
		String fullPath = path + "/" + filename;

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
		trans.transform(dom, result);
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
					|| line.contains("update") || line.contains("delete") || line.contains("iinq_select")
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

	private static String
	ion_switch_key_size(
			int key_type
	) {
		switch (key_type) {
			case Types.INTEGER:  {
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

    private static String
    ion_get_value_size(
        SourceTable table, String field_name
    ) {
		SourceField field = table.getSourceFields().get(field_name.toLowerCase());
        if (field.getDataType() == Types.CHAR) {
            return "sizeof(char) * "+field.getColumnSize();
        }
        else if (field.getDataType() == Types.VARCHAR) {
            return "sizeof(char) * "+field.getColumnSize();
        }
        else if (field.getDataType() == Types.BOOLEAN) {
            return "sizeof(ion_boolean_t)";
        }
        else if (field.getDataType() == Types.INTEGER) {
            return "sizeof(int)";
        }
        else if (field.getDataType() == Types.FLOAT || field.getDataType() == Types.DECIMAL) {
            return "sizeof(float)";
        }

        return "sizeof(int)";
    }

    private static void
    print_error (BufferedWriter out) throws IOException {
        out.newLine();
        out.newLine();

        out.write("\tif (err_ok != error) {\n");
        out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);\n");
        out.write("\t\treturn; \n");
        out.write("\t}\n\n");
    }

    private static void
    print_top_header (FileOutputStream out) throws IOException {
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
		out.write("void print_table_" + table_name.toLowerCase() + "(ion_dictionary_t *dictionary) {\n");
		out.write("\n\tion_predicate_t predicate;\n");
		out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");
		out.write("\tion_dict_cursor_t *cursor = NULL;\n");
		out.write("\tdictionary_find(dictionary, &predicate, &cursor);\n\n");
		out.write("\tion_record_t ion_record;\n");
		out.write("\tion_record.key		= malloc(" + get_schema_value(table_name, "PRIMARY KEY SIZE") + ");\n");
		out.write("\tion_record.value	= malloc(" + get_schema_value(table_name, "VALUE SIZE") + ");\n\n");
		out.write("\tprintf(\"Table: " + table_name + "\\" + "n" + "\");\n");

		for (int j = 0; j < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS")); j++) {
			out.write("\tprintf(\"" + get_schema_value(table_name, "FIELD" + j + " NAME") + "\t\");\n");
		}

		out.write("\tprintf(\"" + "\\" + "n" + "***************************************" + "\\" + "n" + "\");\n\n");

        out.write("\tion_cursor_status_t cursor_status;\n");
        out.write("\tunsigned char *value;\n\n");

        out.write("\twhile ((cursor_status = cursor->next(cursor, &ion_record)) == cs_cursor_active || " +
                "cursor_status == cs_cursor_initialized) {\n");
        out.write("\t\tvalue = ion_record.value;\n\n");

        String data_type;
        for (int j = 0; j < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS")); j++) {
            data_type = ion_get_value_size(metadata.getTable("IinqDB",table_name), metadata.getTable("IinqDB", table_name).getSourceFieldsByPosition().get(j).getColumnName());

            if (data_type.contains("char")) {
                out.write("\n\t\tprintf(\"%s\t\", (char *) value);\n");

                if (j < (Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS")) - 1)) {
                    out.write("\t\tvalue += " + data_type + ";\n");
                }
            }

            /* Implement for all data types - for now assume int if not char or varchar */
            else {
                out.write("\n\t\tprintf(\"%i\t\", NEUTRALIZE(value, int));\n");

                if (j < (Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS")) - 1)) {
                    out.write("\t\tvalue += " + data_type + ";\n");
                }
            }
        }

        out.write("\n\t\tprintf(\""+"\\"+"n"+"\");");
        out.write("\n\t}\n");
        out.write("\n\tprintf(\""+"\\"+"n"+"\");\n\n");

		out.write("\tcursor->destroy(&cursor);\n");
		out.write("\tfree(ion_record.key);\n");
		out.write("\tfree(ion_record.value);\n}\n\n");
	}

    private static void
    write_headers () throws IOException {
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

        contents += "\n#if defined(__cplusplus)\n" + "}\n" + "#endif\n" + "\n" + "#endif\n";

        header.write(contents.getBytes());

        header.close();
    }

    private static void
    insert_setup () throws IOException {

        /* Add new functions to be run to executable */
        String ex_path = user_file;
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        String contents = "";
        String line;
        iinq.insert_fields insert;
        int count = 0;

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains("INSERT") && !line.contains("/*") && !line.contains("//")) {
                contents += "/* "+line + " */\n";

                int index = line.indexOf("SQL_prepare");
                if (index == -1) {
                	index = line.indexOf("SQL_execute");
				}
				String sql = line.substring(index);
                sql = sql.substring(sql.toUpperCase().indexOf("INSERT"), sql.indexOf(";")).trim();
                StringFunc.verifyTerminator(sql);
                String temp = line.substring(0, index);

                insert = insert_fields.get(count);

                if (insert != null) {
                    contents += "\t" + temp + "insert_" + insert.table + "(";

                    for (int j = 0; j < insert.fields.size(); j++) {
                        if (insert.fields.get(j) != null) {

                            if (j > 0) {
                                contents += ", ";
                            }

                            if (insert.field_types.get(j).equals("char")) {
                                contents += "\"" + insert.fields.get(j) + "\"";
                            }

                            else {
                                if (insert.fields.get(j).equals("(?)") || insert.fields.get(j).equals("?")) {
                                    contents += "NULL";
                                }

                                else {
                                    contents += insert.fields.get(j);
                                }
                            }
                        }
                    }

                    contents += ");\n";
                    count++;
                }
            }

            else {
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
    delete_setup () throws IOException {

        /* Add new functions to be run to executable */
        String ex_path = user_file;
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        String contents = "";
        String line;
        iinq.delete_fields delete;
        int count = 0;

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains("DELETE") && !line.contains("/*") && !line.contains("//")) {
                contents += "/* "+line + " */\n";

                delete = delete_fields.get(count);

                if (delete != null) {
                    contents += "\tdelete_record("+delete.table_id+", \""+delete.table_name+"\", "+delete.ion_key+", "
                            + delete.key_size+", " +delete.value_size+", "+delete.num_wheres*3;

                    for (int j = 0; j < delete.num_wheres; j++) {
                        contents += ", " + delete.fields.get(j) + ", " + delete.operators.get(j) + ", ";

                        if (delete.field_types.get(j).contains("INT")) {
                            contents += delete.values.get(j);
                        }
                        else {
                            contents += "\"" + delete.values.get(j) + "\"";
                        }
                    }

                    contents += ");\n";
                    count++;
                }
            }

            else {
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
    update_setup () throws IOException {

        /* Add new functions to be run to executable */
        String ex_path = user_file;
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        String contents = "";
        String line;
        iinq.update_fields update;
        int count = 0;

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains("UPDATE") && !line.contains("/*") && !line.contains("//")) {
                contents += "/* "+line + " */\n";

                update = update_fields.get(count);
                int implicit_count = 0;

                if (update != null) {
                    contents += "\tupdate("+update.table_id+", \""+update.table_name+"\", "+update.ion_key+", "
                            +update.key_size+", " +update.value_size+", "+update.num_wheres*3+", "
                            +update.num_updates*4+", " +(update.num_wheres*3 + update.num_updates*4);

                    for (int j = 0; j < update.num_wheres; j++) {
                        contents += ", " + update.where_fields.get(j) + ", " + update.where_operators.get(j) + ", ";

                        if (update.where_field_types.get(j).contains("INT")) {
                            contents += update.where_values.get(j);
                        }
                        else {
                            contents += "\"" + update.where_values.get(j) + "\"";
                        }
                    }

                    for (int i = 0; i < update.num_updates; i++) {
                        if (!update.implicit.get(i)) {
                            contents += ", " + update.update_fields.get(i) + ", 0, 0, ";

                            if (update.update_field_types.get(i).contains("INT")) {
                                contents += update.update_values.get(i);
                            }
                            else {
                                contents += "\"" + update.update_values.get(i) + "\"";
                            }
                        }
                        else {
                            contents += ", " + update.update_fields.get(i) + ", " + update.implicit_fields.get(implicit_count) + ", "
                                    + update.update_operators.get(implicit_count) + ", ";

                            if (update.update_field_types.get(i).contains("INT")) {
                                contents += update.update_values.get(i);
                            }
                            else {
                                contents += "\"" + update.update_values.get(i) + "\"";
                            }
                            implicit_count++;
                        }
                    }

                    contents += ");\n";
                    count++;
                }
            }

            else {
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
    select_setup () throws IOException {

        /* Add new functions to be run to executable */
        String ex_path = user_file;
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        String contents = "";
        String line;
        iinq.select_fields select;
        int count = 0;

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains("SELECT") && !line.contains("/*") && !line.contains("//")) {
                contents += "/* "+line + " */\n";

                select = select_fields.get(count);

                if (select != null) {
                    contents += "\t"+select.return_value+" = iinq_select("+select.table_id+", \""+select.table_name+"\", "+select.key_type+", "+select.key_size+", "
                            +select.value_size+", "+select.num_wheres*3+", "+select.num_fields+", "
                            +(select.num_wheres*3 + select.num_fields);

                    for (int j = 0; j < select.num_wheres; j++) {
                        contents += ", " + select.where_fields.get(j) + ", " + select.where_operators.get(j) + ", ";

                        if (select.where_field_types.get(j).contains("INT")) {
                            contents += select.where_values.get(j);
                        }
                        else {
                            contents += "\"" + select.where_values.get(j) + "\"";
                        }
                    }

                    for (int i = 0; i < select.num_fields; i++) {
                        contents += ", " + select.fields.get(i);
                    }

                    contents += ");\n";
                    count++;
                }
            }

            else {
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
    create_setup () throws IOException {

        /* Add new functions to be run to executable */
        String ex_path = user_file;
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        String contents = "";
        String line;
        iinq.create_fields create;
        int count = 0;

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains("CREATE TABLE") && !line.contains("/*") && !line.contains("//")) {
                contents += "/* "+line + " */\n";

                create = create_fields.get(count);

                if (create != null) {
                    contents += "\tcreate_table(\""+create.table_name+"\", "+create.key_type+", "+create.key_size+", "
                            +create.value_size+");\n";

                    count++;
                }
            }

            else {
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
    drop_setup () throws IOException {

        /* Add new functions to be run to executable */
        String ex_path = user_file;
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        String contents = "";
        String line;
        String table;
        int count = 0;

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains("DROP TABLE") && !line.contains("/*") && !line.contains("//")) {
                contents += "/* "+line + " */\n";

                table = drop_tables.get(count);

                if (table != null) {
                    contents += "\tdrop_table(\""+table+"\");\n";

                    count++;
                }
            }

            else {
                contents += line + '\n';
            }
        }

		File ex_output_file = new File(ex_path);
		FileOutputStream ex_out = new FileOutputStream(ex_output_file, false);

		ex_out.write(contents.getBytes());

		ex_file.close();
		ex_out.close();
	}

	private static void getUnityConnection(String url) throws  SQLException, ClassNotFoundException{
		Class.forName("unity.jdbc.UnityDriver");
		conUnity = DriverManager.getConnection(url);
		metadata = ((UnityConnection) conUnity).getGlobalSchema();
		ArrayList<SourceDatabase> databases = metadata.getAnnotatedDatabases();
		if (null == databases) {
			System.out.println("\nNo databases have been detected.");
		}
	}

	private static void getJavaConnection(String url) throws SQLException, ClassNotFoundException{
		Class.forName("org.hsqldb.jdbc.JDBCDriver");
		conJava = DriverManager.getConnection(url);
	}

	// TODO: change keywords to enum
	protected static String
	get_schema_value(String table_name, String keyword) throws IOException, RelationNotFoundException, InvalidArgumentException, SQLFeatureNotSupportedException {
		String line;
		SourceTable table = metadata.getTable("IinqDB", table_name);

		switch (keyword) {
			case "PRIMARY KEY TYPE": {
				return Integer.toString(table.getPrimaryKey().getFields().get(0).getDataType());
			}
			case "PRIMARY KEY FIELD": {
				return Integer.toString(table.getField(table.getPrimaryKey().getFieldList()).getOrdinalPosition());
			}
			case "PRIMARY KEY SIZE": {
				switch (table.getPrimaryKey().getFields().get(0).getDataType()) {
					case 1: // CHAR
					case 12: // VARCHAR
						return String.format("sizeof(char) * %d",table.getField(table.getPrimaryKey().getFieldList()).getColumnSize());
					case 4: // int
						return "sizeof(int)";
				}
			}
			case "NUMBER OF RECORDS": {
				return Integer.toString(table.getNumTuples());
			}
			case "VALUE SIZE": {
				ArrayList<SourceField> fields = table.getSourceFieldsByPosition();
				int num_fields = table.getNumFields();
				StringBuilder returnValue = new StringBuilder();
				// Skip the first field which is part of the key
				for (int i = 1; i < num_fields; i++) {
					if (i > 0) {
						returnValue.append(" + ");
					}
					returnValue.append(get_field_size(fields.get(i)));
				}
				return returnValue.toString();
			}
			case "NUMBER OF FIELDS": {
				return Integer.toString(table.getNumFields());
			}
			case "ION KEY TYPE": {
				switch (table.getField(table.getPrimaryKey().getFieldList()).getDataType()){
					case Types.INTEGER:
						return "key_type_numeric_unsigned";
					case Types.CHAR:
					case Types.VARCHAR:
						return "key_type_char_array";
				}
				return null;
			}
			default: {
				if (keyword.matches("(FIELD)\\d*\\s(NAME)")) {
					int field_index = Integer.parseInt(keyword.substring(5, keyword.indexOf(" ")));
					return table.getSourceFieldsByPosition().get(field_index).getColumnName();
				} else if (keyword.matches("(FIELD)\\d*\\s(TYPE)")) {
					int field_index = Integer.parseInt(keyword.substring(5, keyword.indexOf(" ")));
					return table.getSourceFieldsByPosition().get(field_index).getDataTypeName();
				} else {
					throw new InvalidArgumentException(new String[]{String.format("%s is not a valid keyword.", keyword)});
				}
			}
		}

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
			pos = statement.indexOf(",");

			if (-1 != pos) {
				field = statement.substring(0, pos);

				statement = statement.substring(pos + 2);

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
		node.appendChild(xml.createTextNode("jdbc:hsqldb:hsql//localhost/tpch"));
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
		int num_fields = Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS"));

		schema.append(num_fields);
		schema.append(",\nTABLE_FIELD_TYPES(");
		for (int i = 0; i < num_fields; i++) {
			if (i > 0) {
				schema.append(", ");
			}
			switch (Integer.parseInt(get_schema_value(table_name, "FIELD" + i + " TYPE"))) {
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
			schema.append(String.format("\"%s\"", get_schema_value(table_name, "FIELD" + i + " NAME")));
		}
		schema.append("))}");
		return schema.toString();
	}

	private static void
	create_table(String sql, BufferedWriter out) throws Exception {
		String table_name = null;
		sql = sql.substring(sql.toUpperCase().indexOf("CREATE"), sql.indexOf(";"));
		sql = StringFunc.verifyTerminator(sql);    // Make sure SQL is terminated by semi-colon properly

		IinqTable table = new IinqTable();

		table_name = sql.substring(sql.toUpperCase().indexOf("TABLE") + 5, sql.indexOf("(")).trim().toLowerCase();
		table.setTableName(table_name);

		if (tables.get(table_name) != null) {
			throw new SQLException("Table already exists: " + table_name);
		}

		// Create the table using HSQLDB to avoid string parsing
		PreparedStatement stmt = conJava.prepareStatement(sql);
		stmt.execute();

		DatabaseMetaData newMetaData = conJava.getMetaData();

		// TODO: Add support for composite keys
		// Get primary key
		ResultSet rst = newMetaData.getPrimaryKeys(null, null, table.getTableName().toUpperCase());
		rst.next();
		table.setPrimaryKey(rst.getString("COLUMN_NAME"));
		rst.close();

		// Get the remaining data
		rst = newMetaData.getColumns(null, null, table.getTableName().toUpperCase(), null);
		int num_fields = 0;
		while (rst.next()) {
			num_fields++;
			String field_name = rst.getString("COLUMN_NAME");
			int data_type = rst.getInt("DATA_TYPE");
			if (field_name.equals(table.getPrimaryKey())) {
				table.setPrimaryKeyIndex(num_fields);
				table.setPrimaryKeyType(data_type);
			}
			table.addField(field_name, data_type, rst.getString("TYPE_NAME"), rst.getInt("COLUMN_SIZE"));
		}

		rst.close();

		String primary_key_size = ion_switch_key_size(table.getPrimaryKeyType());

		StringBuilder value_calculation = new StringBuilder();
		String value_size = "";
		int int_count = 0;
		boolean char_present = false;
		int char_multiplier = 0;

		for (int i = 0; i < num_fields - 1; i++) {
			if (table.getFieldTypeName(i).contains("CHAR")) {
				char_multiplier += table.getFieldSize(i);
				char_present = true;
			} else if (table.getFieldTypeName(i).contains("INT")) {
				int_count++;
			}
		}

		// TODO: add support for more data types
		if (int_count > 0) {
			value_calculation.append("(sizeof(int) * " + int_count + ")+");
		}
		if (char_present) {
			value_calculation.append("(sizeof(char) * " + char_multiplier + ")+");
		}
		value_calculation.setLength(value_calculation.length() - 1); // remove last "+"

		String ion_key = "";

		// TODO: add signed integer
		if (table.getPrimaryKeyType() == Types.INTEGER) {
			ion_key = "key_type_numeric_unsigned";
		} else if (table.getPrimaryKeyType() == Types.CHAR || table.getPrimaryKeyType() == Types.VARCHAR) {
			ion_key = "key_type_char_array";
		}

		String schema_name = table.getTableName().toLowerCase().concat(".xml");

		add_table_to_database(directory, "iinq_database.xml", table, sql);
		tables.put(table.getTableName().toLowerCase(), table);

		File schema = new File(directory + schema_name);
		FileOutputStream schema_out = new FileOutputStream(schema, false);

		//schema_out.write(contents.getBytes());

		schema_out.close();

		xml_schemas.add(schema_name);

		/* Create CREATE TABLE method - (Schema in .inq file) */
		/* out.write("void create_table" + create_count + "() {\n");
		out.write("\tprintf(\"%s" + "\\" + "n" + "\\" + "n" + "\", \"" + statement.substring(statement.indexOf("(") + 2, statement.length() - 4) + "\");\n");
		out.newLine();
		out.write("\tion_err_t error;");
		out.newLine();
		String schema_variable = create_schema_variable("createTable" + create_count, table_name);
		out.write("\n\terror = iinq_create_table(\"" + table_name + "\", " + primary_key_type + ", " + primary_key_size + ", " + value_size + ", " + schema_variable + ");");
		print_error(out, false, 0);
		out.write("\t");
		out.newLine();
		out.write("\tiinq_table_t table;");
		out.newLine();
		out.write("\terror = iinq_open_table(\"" + table_name + "\", &table);");
		print_error(out, false, 0);
		// TODO: fix all print_table_ function calls/definitions
		//out.write("\tprint_table_" + table_name.substring(0, table_name.length() - 4).toLowerCase() + "(&dictionary);\n");
		out.write("\terror = iinq_close_table(&table);");
		print_error(out, false, 0);

		out.write("}\n\n");

		System.out.println("schema " + schema_name);*/

		/* Create CREATE TABLE method */
		if (!create_written) {
			out.write("void create_table(char *table_name, ion_key_type_t key_type, ion_key_size_t key_size, ion_value_size_t value_size) {\n");
			out.write("\tion_err_t error = iinq_create_source(table_name, key_type, key_size, value_size);");

			print_error(out);

			out.write("}\n\n");

			function_headers.add("void create_table(char *table_name, ion_key_type_t key_type, ion_key_size_t key_size, ion_value_size_t value_size);\n");
		}

		create_written = true;

		create_fields.add(new create_fields(table.getTableName(), ion_key, primary_key_size, value_calculation.toString()));

		try {
			// Reload database with new tables
			if (conUnity != null)
				conUnity.close();
			getUnityConnection(urlUnity);

			/* Create print table method if it doesn't already exist */
			if (!tables.get(table_name).isWritten_table()) {
				if (drop_tables.contains(table_name)) {
					out.write("/* Table has the same name as a previously dropped table: " + table_name + ". Print table function is commented out. */\n/*");
					try {
						print_table(out, table_name);
						tables.get(table_name.toLowerCase()).setWritten_table(true);
					} catch (Exception e) {
						e.printStackTrace();
					}
					out.write("*/\n\n");
				} else {
					try {
						print_table(out, table_name);
						tables.get(table_name.toLowerCase()).setWritten_table(true);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}


		} catch (Exception e) {
			e.printStackTrace();
		}
    }

	private static void
	insert(String sql, BufferedWriter out) throws IOException, InvalidArgumentException, RelationNotFoundException, SQLException {
		System.out.println("insert statement");

        sql = sql.substring(sql.toUpperCase().indexOf("INSERT"),sql.indexOf(";")).trim();
		sql = StringFunc.verifyTerminator(sql);	// Make sure SQL is terminated by semi-colon properly

		// Use UnityJDBC to parse the insert statement (metadata is required to verify fields)
		GlobalParser kingParser;
		GlobalUpdate gu;
		if (null != metadata) {
			kingParser = new GlobalParser(false, true);
			gu = kingParser.parseUpdate(sql, metadata);
		}
		else {
			throw new SQLException("Metadata is required for inserts.");
		}

		LQInsertNode insert = (LQInsertNode) gu.getPlan().getLogicalQueryTree().getRoot();

		SourceTable table = insert.getSourceTable().getTable();
        String table_name = table.getTableName().toLowerCase();

        /* Create print table method if it doesn't already exist */
		if (!tables.get(table_name).isWritten_table()) {
			print_table(out, table_name);
			tables.get(table_name).setWritten_table(true);
		}

        /* Count number of fields */
        int count = insert.getInsertFields().size();

        boolean[] prep_fields = new boolean[count];

	    /* Check if the INSERT statement is a prepared statement */
		UnityPreparedStatement stmt = (UnityPreparedStatement) conUnity.prepareStatement(sql);
		boolean prep = stmt.getParameters().size() > 0;

        String[] fields = new String[count];
        String value = "";
        String field_value;

        String key_type = get_schema_value(table_name, "PRIMARY KEY TYPE");
        String key_field_num = get_schema_value(table_name, "PRIMARY KEY FIELD");

        boolean table_found = false;
        int table_id = 0;

        /* Check if that table name already has an ID */
        for (int i = 0; i < table_names.size(); i++) {
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
        }

        ArrayList<Integer> int_fields = new ArrayList<>();
        ArrayList<Integer> string_fields = new ArrayList<>();

        ArrayList<String> field_values = new ArrayList<>();
        ArrayList<String> field_types = new ArrayList<>();
        ArrayList<String> iinq_field_types = new ArrayList<>();
        ArrayList<String> field_sizes = new ArrayList<>();

        String prepare_function = "";

        if (new_table) {
            out.write("iinq_prepared_sql insert_" + table_name + "(");
            prepare_function += "iinq_prepared_sql insert_"+table_name+"(";
        }

		for (int j = 0; j < count; j++) {
        	fields[j] = ((LQExprNode) insert.getInsertValues().get(j)).getContent().toString();

            field_value = get_schema_value(table_name, "FIELD" + j + " TYPE");
            field_sizes.add(ion_get_value_size(table,table.getSourceFieldsByPosition().get(j).getColumnName()));

            /* To be added in function call */
            field_values.add(fields[j]);

            if (field_value.contains("CHAR")) {
                string_fields.add(j + 1);
            } else {
                int_fields.add(j + 1);
            }

            if (j > 0) {
                if (new_table) {
                    out.write(", ");
                    prepare_function += ", ";
                }
            }

            if (field_value.contains("CHAR")) {
                field_types.add("char");
                iinq_field_types.add("iinq_char");

                if (new_table) {
                    out.write("char *value_" + (j + 1));
                    prepare_function += "char *value_" + (j + 1);
                }
            } else {
                field_types.add("int");
                iinq_field_types.add("iinq_int");

                if (new_table) {
                    out.write("int value_" + (j + 1));
                    prepare_function += "int value_" + (j + 1);
                }
            }

            prep_fields[j] = fields[j].equals("?");
        }

        if (new_table) {
            prepare_function += ");\n";

            out.write(") {\n");
            out.write("\tiinq_prepared_sql p = {0};\n");

            out.write("\tp.table = malloc(sizeof(int));\n");
            out.write("\t*(int *) p.table = " + table_id + ";\n");
            out.write("\tp.value = malloc(" + get_schema_value(table_name, "VALUE SIZE") + ");\n");
            out.write("\tunsigned char\t*data = p.value;\n");
            out.write("\tp.key = malloc(" + ion_switch_key_size(Integer.parseInt(key_type)) + ");\n");

            if (key_type.equals("0") || key_type.equals("1")) {
                out.write("\t*(int *) p.key = value_" + (Integer.parseInt(key_field_num) + 1) + ";\n\n");
            } else {
                out.write("\tmemcpy(p.key, value_" + (Integer.parseInt(key_field_num) + 1) + ", " + key_type + ");\n\n");
            }

            for (int i = 0; i < fields.length; i++) {
                field_value = get_schema_value(table_name, "FIELD" + i + " TYPE");
                String value_size = ion_get_value_size(table,table.getSourceFieldsByPosition().get(i).getColumnName());

                if (field_value.contains("CHAR")) {
                    out.write("\tmemcpy(data, value_" + (i + 1) + ", " + value_size + ");\n");
                } else {
                    out.write("\t*(int *) data = value_" + (i + 1) + ";\n");
                }

                if (i < fields.length - 1) {
                    out.write("\tdata += " + value_size + ";\n\n");
                }
            }

            out.write("\n\treturn p;\n");
            out.write("}\n\n");

            tableInfo table_info = new tableInfo(table_id, count, iinq_field_types, field_sizes);

            calculateInfo.add(table_info);
            tables_count++;
        }

        String param_header = "";

        /* INSERT statement is a prepared statement */
        if (prep && !param_written) {
            written_table = table_name;

            out.write("void setParam(iinq_prepared_sql p, int field_num, void *val) {\n");
            out.write("\tunsigned char\t*data\t\t= p.value;\n\n");
            out.write("\tiinq_field_t type\t\t= getFieldType(p.table, field_num);\n");
            out.write("\tdata += calculateOffset(p.table, (field_num - 1));\n\n");
            out.write("\tif (type == iinq_int) {\n");
            out.write("\t\t*(int *) data = (int) val;\n\t}\n");
            out.write("\telse {\n");
            out.write("\t\tmemcpy(data, val, sizeof(val));\n\t}\n}\n\n");

            param_header = "void setParam(iinq_prepared_sql p, int field_num, void *val);\n";
        }

        if (!param_written) {
            /* Set-up execute() function */
            out.write("void execute(iinq_prepared_sql p) {\n");
            out.write("\tif (*(int *) p.table == "+table_id+") {\n");

            function_headers.add("void execute(iinq_prepared_sql p);\n");

            if (key_type.equals("0") || key_type.equals("1")) {
                out.write("\t\tiinq_execute(\"" + table_name + "\", IONIZE(*(int *) p.key, int), p.value, iinq_insert_t);\n");
            } else {
                out.write("\t\tiinq_execute(\"" + table_name + "\", p.key, p.value, iinq_insert_t);\n");
            }
            out.write("\t}\n");
            out.write("/* INSERT 4 */\n");

            out.write("\tfree(p.value);\n");
            out.write("\tfree(p.table);\n");
            out.write("\tfree(p.key);\n");
            out.write("}\n\n");
        }

        else {
            inserts.add(new insert(table_name, fields, int_fields, string_fields, count, sql, value, prep_fields, key_type, key_field_num, table_id));
        }

        if (!prepare_function.equals("")) {
            function_headers.add(prepare_function);
        }

        if (!param_header.equals("")) {
            function_headers.add(param_header);
        }

        param_written = true;
        insert_fields.add(new insert_fields(table_name, field_values, field_types));
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

                        if (key_type.equals("0") || key_type.equals("1")) {
                            contents += "\t\tiinq_execute(\"" + table_name + ".inq\", IONIZE(*(int *) p.key, int), p.value, iinq_insert_t);\n";
                        } else {
                            contents += "\t\tiinq_execute(\"" + table_name + ".inq\", p.key, p.value, iinq_insert_t);\n";
                        }
                        contents += "\t}\n";
                    }
                }
            }
            contents += line + '\n';
        }

        File ex_output_file = new File(path);
        FileOutputStream out = new FileOutputStream(ex_output_file, false);

        out.write(contents.getBytes());

        file.close();
        out.close();
    }

    private static void
    calculate_functions(BufferedWriter out) throws IOException {
        if (tables_count > 0) {
            String field_size_function = "";
            out.write("size_t calculateOffset(const unsigned char *table, int field_num) {\n\n");
            field_size_function += "iinq_field_t getFieldType(const unsigned char *table, int field_num) {\n\n";

            String offset_header = "size_t calculateOffset(const unsigned char *table, int field_num);\n";
            function_headers.add(offset_header);

            String size_header = "iinq_field_t getFieldType(const unsigned char *table, int field_num);\n";
            function_headers.add(size_header);

			out.write("\tswitch (*(int *) table) {\n");
			field_size_function += "\tswitch (*(int *) table) {\n";

            for (int i = 0; i < tables_count; i++) {
                int table_id = calculateInfo.get(i).tableId;
                int num_fields = calculateInfo.get(i).numFields;

                out.write("\t\tcase "+table_id+" : {\n");
                out.write("\t\t\tswitch (field_num) {\n");

                field_size_function += "\t\tcase "+table_id+" : {\n";
                field_size_function += "\t\t\tswitch (field_num) {\n";

                int int_count;
                boolean char_present = false;
                int char_multiplier;
                String value_size;

                for(int j = 0; j < num_fields; j++) {
                    char_multiplier = 0;
                    int_count = 0;

                    for (int k = 0; k <= j; k++) {
                        value_size = calculateInfo.get(i).field_sizes.get(k);

                        if (value_size.contains("char")) {
                            char_multiplier += Integer.parseInt(value_size.substring(value_size.indexOf("*") + 1).trim());
                            char_present = true;
                        }

                        if (value_size.contains("int")) {
                            int_count++;
                        }
                    }

                    out.write("\t\t\t\tcase "+(j+1)+" :\n");
                    out.write("\t\t\t\t\treturn ");

                    field_size_function += "\t\t\t\tcase "+(j+1)+" :\n";
                    field_size_function += "\t\t\t\t\treturn "+calculateInfo.get(i).field_types.get(j)+";\n";

                    if (int_count > 0) {
                        if (int_count > 1) {
                            out.write("(sizeof(int) * " + int_count + ")");
                        }

                        else {
                            out.write("sizeof(int)");
                        }

                        if (char_present) {
                            out.write("+");
                        }

                        else {
                            out.write(";\n");
                        }
                    }

                    if (char_present) {
                        out.write("(sizeof(char) * "+char_multiplier+");\n");
                    }
                }
				out.write("\t\t\t\tdefault:\n\t\t\t\t\treturn 0;\n");
				out.write("\t\t\t}\n\t\t}\n");
				field_size_function += "\t\t\t\tdefault:\n\t\t\t\t\treturn iinq_int;\n";
				field_size_function += "\t\t\t}\n\t\t}\n";
            }

			out.write("\t\tdefault:\n\t\t\treturn 0;\n");
			out.write("\t}\n}\n\n");

			field_size_function += "\t\tdefault:\n\t\t\treturn iinq_int;\n";
			field_size_function += "\t}\n}\n\n";

            out.write(field_size_function);
        }
    }

	private static void
	update(String sql, BufferedWriter out) throws IOException, InvalidArgumentException, RelationNotFoundException, SQLException {
		System.out.println("update statement");

        sql = sql.substring(sql.toUpperCase().indexOf("UPDATE"), sql.indexOf(";"));
        sql = StringFunc.verifyTerminator(sql);

		GlobalParser kingParser;
		GlobalUpdate gu;
		if (null != metadata) {
			kingParser = new GlobalParser(false, true);
			gu = kingParser.parseUpdate(sql, metadata);
		}
		else {
			throw new SQLException("Metadata is required for updating tables.");
		}

		LQUpdateNode updateNode = (LQUpdateNode) gu.getPlan().getLogicalQueryTree().getRoot();
        String table_name = updateNode.getTable().getLocalName().toLowerCase();

        boolean table_found = false;
        int table_id = 0;

        /* Check if that table name already has an ID */
        for (int i = 0; i < table_names.size(); i++) {
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
        }

        SourceTable table = metadata.getTable("IinqDB", table_name);
		IinqTable iinqTable = tables.get(table_name);
		if (null == iinqTable) {
			throw new SQLException("Update attempted on non-existent table: " + table_name);
		}


        /* Create print table method if it doesn't already exist */
		if (!tables.get(table_name).isWritten_table()) {
			print_table(out, table_name);
			tables.get(table_name).setWritten_table(true);
		}

        if (!update_written) {
			write_update_method(out);
 		}

        update_written = true;

		LQCondNode conditionNode =  updateNode.getCondition();
		String where_condition = "";
		ArrayList<String> conditions = null;
		int num_conditions = 0;
		if (conditionNode != null) {
			LQSelNode selNode = new LQSelNode();
			selNode.setCondition(conditionNode);
			// TODO: Is there a better way to do this?
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

		String[] conditionFields = new String[num_conditions];

		for (int i = 0; i < num_conditions; i++) {
			conditionFields[i] = conditions.get(i);
		}

        /* Get fields to update */
		String update;

		/* Calculate number of fields to update in statement */
		int num_fields = updateNode.getNumFields();

        IinqWhere where = new IinqWhere(num_conditions);
        where.generateWhere(conditionFields,table_name);

        ArrayList<Integer> update_field_nums    = new ArrayList<>();
        ArrayList<Boolean> implicit             = new ArrayList<>();
        ArrayList<Integer> implicit_fields      = new ArrayList<>();
        ArrayList<String> update_operators      = new ArrayList<>();
        ArrayList<String> update_values         = new ArrayList<>();
        ArrayList<String> update_field_types    = new ArrayList<>();
        ArrayList<String>   field_sizes         = new ArrayList<>();

        String[] fields = new String[updateNode.getNumFields()];
		LQExprNode[] fieldValues = new LQExprNode[updateNode.getNumFields()];
        for (int i = 0; i < fields.length; i++) {
			LQExprNode node = ((LQExprNode) updateNode.getField(i));
        	fields[i] = node.getContent().toString();
        	fieldValues[i] = (LQExprNode)updateNode.getValue(i);
		}

        String set_string;
        String update_field;
        String implicit_field = "";
        boolean is_implicit;
        String update_value = null;
        int implicit_count = 0;

        for (int j = 0; j < num_fields; j++) {
            is_implicit = false;
            update_field = fields[j].trim();

            /* Check if update value contains an operator */
            if (fieldValues[j].getContent().equals("+")) {
                update_operators.add("iinq_add");
                implicit_field = fieldValues[j].getChild(0).getContent().toString();
                update_value = fieldValues[j].getChild(1).getContent().toString();
                is_implicit = true;
            } else if (fieldValues[j].getContent().equals("-")) {
                update_operators.add("iinq_subtract");
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
                is_implicit = true;
            } else if (fieldValues[j].getContent().equals("*")) {
                update_operators.add("iinq_multiply");
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
                is_implicit = true;
            } else if (fieldValues[j].getContent().equals("/")) {
                update_operators.add("iinq_divide");
				implicit_field = fieldValues[j].getChild(0).getContent().toString();
				update_value = fieldValues[j].getChild(1).getContent().toString();
                is_implicit = true;
            }

            update_values.add(update_value);
            implicit.add(is_implicit);

            if (is_implicit) {
                implicit_count++;
            }

            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS")); n++) {
                String field_type = get_schema_value(table_name, "FIELD"+n+" TYPE");
                field_sizes.add(ion_get_value_size(table,table.getSourceFieldsByPosition().get(n).getColumnName()));

                if (update_field.equalsIgnoreCase(get_schema_value(table_name, "FIELD" + n + " NAME"))) {
                    update_field_nums.add(n+1);
                    update_field_types.add(field_type);
                }
                if (implicit_field.equalsIgnoreCase(get_schema_value(table_name, "FIELD"+n+" NAME"))) {
                    implicit_fields.add(n+1);
                }
            }
        }

        if (new_table) {
            tableInfo table_info = new tableInfo(table_id, Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS")), new ArrayList<String>(Arrays.asList(where.getIinq_field_types())), field_sizes);

            calculateInfo.add(table_info);
            tables_count++;
        }

        String key_size = get_schema_value(table_name, "PRIMARY KEY SIZE");
        String value_size = get_schema_value(table_name, "VALUE SIZE");
        String ion_key = get_schema_value(table_name, "ION KEY TYPE");

        // TODO revise update_fields to use IinqWhere object
        update_fields.add(new update_fields(table_name, table_id, num_conditions, num_fields, new ArrayList<Integer>(Arrays.asList(where.getWhere_field_nums())), new ArrayList<String>(Arrays.asList(where.getWhere_operators())),
                new ArrayList<String>(Arrays.asList(where.getWhere_values())), new ArrayList<String>(Arrays.asList(where.getWhere_field_types())), key_size, value_size, ion_key, update_field_nums, implicit, implicit_fields, update_operators,
                update_values, update_field_types, implicit_count));
    }

    private static void
    select(String sql, BufferedWriter out) throws IOException, SQLException, RelationNotFoundException, InvalidArgumentException {
        System.out.println("select statement");

		String return_val = sql.substring(0, sql.indexOf("=") - 1);

        sql = sql.substring(sql.indexOf("("));
        sql = sql.substring(sql.toUpperCase().indexOf("SELECT"),sql.indexOf(";")).trim();
		sql = StringFunc.verifyTerminator(sql);	// Make sure SQL is terminated by semi-colon properly

		// Parse semantic query string into a parse tree
		GlobalParser kingParser;
		GlobalQuery gq;
		if (null != metadata) {
			kingParser = new GlobalParser(false, true);
			gq = kingParser.parse(sql, metadata);
		}
		else {
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

        int pos = table_name.indexOf(" ");

		String table_name_sub = table_name;
        table_name = table_name+".inq";

        boolean table_found = false;
        int table_id = 0;

        /* Check if that table name already has an ID */
        for (int i = 0; i < table_names.size(); i++) {
            if (table_names.get(i).equals(table_name_sub)) {
                table_id = i;
                new_table = false;
                table_found = true;
            }
        }

        if (!table_found) {
            table_names.add(table_name_sub);
            table_id = table_id_count;
            table_id_count++;
            new_table = true;
        }

        SourceTable table = metadata.getTable("IinqDB",table_name_sub);

        /* Create print table method if it doesn't already exist */
        if (!tables.get(table_name_sub.toLowerCase()).isWritten_table()) {
            print_table(out, table_name_sub);
			tables.get(table_name_sub.toLowerCase()).setWritten_table(true);
        }

        if (!select_written) {
        	write_select_method(out);
        }

        select_written = true;

        pos = sql.toUpperCase().indexOf("WHERE");
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

        ArrayList<Integer> where_field      = new ArrayList<>(); /* Field value that is being used to update a field. */
        ArrayList<String> where_value       = new ArrayList<>(); /* Value that is being added to another field value to update a field. */
        ArrayList<String> where_operator    = new ArrayList<>(); /* Whether values are being updated through addition or subtraction. */
        ArrayList<String> iinq_field_types  = new ArrayList<>();
        ArrayList<String> where_field_type  = new ArrayList<>();

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

        for (int n = 0; n < Integer.parseInt(get_schema_value(table_name_sub, "NUMBER OF FIELDS")); n++) {

            String field_type = get_schema_value(table_name_sub, "FIELD"+n+" TYPE");

            if (field_type.contains("CHAR")) {
                iinq_field_types.add("iinq_char");
            }
            else {
                iinq_field_types.add("iinq_int");
            }

            if (field.equals(get_schema_value(table_name_sub, "FIELD"+n+" NAME"))) {
                where_field.add(n+1);
                where_field_type.add(field_type);
            }
        }

        ArrayList<Integer> select_field_nums    = new ArrayList<>();
        ArrayList<String>   field_sizes         = new ArrayList<>();

        String[] fields;

        fields = get_fields(field_list, num_fields);

        for (int j = 0; j < num_fields; j++) {
            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name_sub, "NUMBER OF FIELDS")); n++) {
                String field_type = get_schema_value(table_name_sub, "FIELD"+n+" TYPE");
                field_sizes.add(ion_get_value_size(table, table.getSourceFieldsByPosition().get(j).getColumnName()));

                if ((fields[j].trim()).equals(get_schema_value(table_name_sub, "FIELD" + n + " NAME"))) {
                    select_field_nums.add(n+1);
                }
            }
        }

        if (new_table) {
            tableInfo table_info = new tableInfo(table_id, Integer.parseInt(get_schema_value(table_name_sub, "NUMBER OF FIELDS")), iinq_field_types, field_sizes);

            calculateInfo.add(table_info);
            tables_count++;
        }

        String value_size = get_schema_value(table_name_sub, "VALUE SIZE");
        String key_size = get_schema_value(table_name_sub, "PRIMARY KEY SIZE");
        String ion_key = get_schema_value(table_name_sub, "ION KEY TYPE");

        select_fields.add(new select_fields(table_name_sub, table_id, num_conditions, num_fields, where_field, where_operator,
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
		}
		else {
			throw new SQLException("Metadata is required for dropping tables.");
		}
		LQDeleteNode delete = (LQDeleteNode) gu.getPlan().getLogicalQueryTree().getRoot();
        String table_name = delete.getSourceTable().getLocalName().toLowerCase();

        boolean table_found = false;
        int table_id = 0;

        /* Check if that table name already has an ID */
        for (int i = 0; i < table_names.size(); i++) {
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
        }

        SourceTable table = metadata.getTable("IinqDB", table_name);

		sql = sql.substring(table_name.length() - 3);

        /* Create print table method if it doesn't already exist */
		if (!tables.get(table_name).isWritten_table()) {
			print_table(out, table_name);
			tables.get(table_name).setWritten_table(true);
		}

        /* Write function to file */
        String key_size = get_schema_value(table_name, "PRIMARY KEY SIZE");
        String value_size = get_schema_value(table_name, "VALUE SIZE");

        if (!delete_written) {
        	write_delete_method(out);
        }

        delete_written = true;

        int pos = sql.indexOf("WHERE");
        String where_condition = "";
        int num_conditions = 0;
        int i = -1;

        /* Get WHERE condition if it exists */
        if (-1 != pos) {
            where_condition = sql.substring(pos + 6, sql.length() - 4);
            i = 0;
        }

        /* Calculate number of WHERE conditions in statement */
        while (-1 != i) {
            num_conditions++;
            i = where_condition.indexOf(",", i + 1);
        }

		String[] conditions;

		conditions = get_fields(where_condition, num_conditions);

        ArrayList<Integer>  fields           = new ArrayList<>();
        ArrayList<String>   operators         = new ArrayList<>();
        ArrayList<String>   values            = new ArrayList<>();
        ArrayList<String>   field_types       = new ArrayList<>();
        ArrayList<String>   iinq_field_types  = new ArrayList<>();
        ArrayList<String>   field_sizes       = new ArrayList<>();
        int len = 0;
        String field;

        for (int j = 0; j < num_conditions; j++) {

            /* Set up field, operator, and condition for each WHERE clause */
            if (conditions[j].contains("!=")) {
                pos = conditions[j].indexOf("!=");
                len = 2;
                operators.add("iinq_not_equal");
            }
            else if (conditions[j].contains("<=")) {
                pos = conditions[j].indexOf("<=");
                len = 2;
                operators.add("iinq_less_than_equal_to");
            }
            else if (conditions[j].contains(">=")) {
                pos = conditions[j].indexOf(">=");
                len = 2;
                operators.add("iinq_greater_than_equal_to");
            }
            else if (conditions[j].contains("=")) {
                pos = conditions[j].indexOf("=");
                len = 1;
                operators.add("iinq_equal");
            }
            else if (conditions[j].contains("<")) {
                pos = conditions[j].indexOf("<");
                len = 1;
                operators.add("iinq_less_than");
            }
            else if (conditions[j].contains(">")) {
                pos = conditions[j].indexOf(">");
                len = 1;
                operators.add("iinq_greater than");
            }

            field = conditions[j].substring(0, pos).trim();
            values.add(conditions[j].substring(pos + len).trim());

            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS")); n++) {

                String field_type = get_schema_value(table_name, "FIELD"+n+" TYPE");
                String field_name = get_schema_value(table_name, "FIELD"+n+" NAME");
                field_sizes.add(ion_get_value_size(table, field_name));

                if (field_type.contains("CHAR")) {
                    iinq_field_types.add("iinq_char");
                }
                else {
                    iinq_field_types.add("iinq_int");
                }

                if (field.equalsIgnoreCase(get_schema_value(table_name, "FIELD"+n+" NAME"))) {
                    fields.add(n+1);
                    field_types.add(field_type);
                }
            }
        }

        if (new_table) {
            tableInfo table_info = new tableInfo(table_id, Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS")), iinq_field_types, field_sizes);

            calculateInfo.add(table_info);
            tables_count++;
        }

        String ion_key = get_schema_value(table_name, "ION KEY TYPE");

        delete_fields.add(new delete_fields(table_name, table_id, num_conditions, fields, operators, values, field_types, key_size, value_size, ion_key));
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
		}
		else {
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

        while(null != (line = file.readLine())) {
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
