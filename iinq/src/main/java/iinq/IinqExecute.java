/******************************************************************************/
/**
 @file		    IinqExecute.java
 @author		Dana Klamut
 @see		For more information, refer to dictionary.h.
 @copyright	Copyright 2017
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
import java.io.*;

public class IinqExecute {

    private static int create_count = 1;
    private static int insert_count = 1;
    private static int update_count = 1;
    private static int delete_count = 1;
    private static int drop_count = 1;

    public static void main(String args[]) throws IOException {
        FileInputStream in = null;
        FileOutputStream out = null;
        FileOutputStream header = null;

        try {
            in = new FileInputStream("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c");

            /* Create output file */
            File output_file = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user_functions.c");
            output_file.createNewFile();
            out = new FileOutputStream(output_file, false);

            /* Create output header file */
            File header_file = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user_functions.h");
            header_file.createNewFile();
            header = new FileOutputStream(header_file, false);

            BufferedReader buff_in = new BufferedReader(new InputStreamReader(in));
            BufferedWriter buff_out = new BufferedWriter(new OutputStreamWriter(out));
            BufferedWriter buff_header = new BufferedWriter(new OutputStreamWriter(header));

            String sql;

            /* File is read line by line */
            while ((sql = buff_in.readLine()) != null)   {
                /* Verify file contents are as expected*/
                System.out.println (sql);

                /* CREATE TABLE statements exists in code that is not a comment */
                if ((sql.toUpperCase()).contains("CREATE TABLE") && !sql.contains("/*")) {
                    create_table(sql, buff_out, buff_header);
                }

                /* INSERT statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("INSERT INTO") && !sql.contains("/*")) {
                    insert(sql, buff_out, buff_header);
                }

                /* UPDATE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("UPDATE") && !sql.contains("/*")) {
                    update(sql, buff_out, buff_header);
                }

                /* DELETE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("DELETE FROM") && !sql.contains("/*")) {
                    delete(sql, buff_out, buff_header);
                }

                /* DROP TABLE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("DROP TABLE") && !sql.contains("/*")) {
                    drop_table(sql, buff_out, buff_header);
                }
            }

            buff_in.close();
            buff_out.close();
            buff_header.close();
        }

        finally {
            if (null != in) {
                in.close();
            }
            if (null != out) {
                out.close();
            }

            if (null != header) {
                header.close();
            }
        }
    }

    private static int
    ion_switch_key_type(
        String key_type
    ) {
        if (key_type.equals("CHAR")) {
            return 2;
        }

        if (key_type.equals("VARCHAR")) {
            return 3;
        }

        if (key_type.equals("INT")) {
            return 0;
        }

        return 2;
    }

    private static String
    ion_switch_key_size(
            int key_type
    ) {
        switch (key_type) {
            case 0 : {
                return "sizeof(int)";
            }

            case 1 : {
                return "sizeof(int)";
            }

            case 2 : {
                return "20";
            }

            case 3 : {
                return "20";
            }
        }

        return "20";
    }

    private static void
    print_error (BufferedWriter out, boolean status) throws IOException {
        out.newLine();
        out.newLine();

        if (!status) {
            out.write("\tif (err_ok != error) {");
        }

        else {
            out.write("\tif (err_ok != status.error) {");
        }

        out.newLine();
        out.write("\t\tprintf(\"Error occurred. Error code: %i\", error);");
        out.newLine();
        out.write("\t\treturn; \n\t}");
        out.newLine();
        out.newLine();
    }

    private static void
    create_table(String sql, BufferedWriter out, BufferedWriter header) throws IOException {
        System.out.println("create statement");

        sql = sql.trim();
        sql = sql.substring(34);

        String table_name = (sql.substring(0, sql.indexOf(" ")))+".inq";
        System.out.println(table_name);

        sql = sql.substring(sql.indexOf(" ") + 2);

        int num_fields = 0;
        int i = 0;

        /* Calculate number of fields in table */
        while (-1 != i) {
            num_fields++;
            i = sql.indexOf(",", i + 1);
        }

        System.out.println(num_fields);
        System.out.println(sql);

        int key_type, pos;
        String field;
        String field_name;
        String field_type;

        String[] field_names = new String[num_fields];
        int[] field_types = new int[num_fields];

	    /* Set up attribute names and types */
        for (int j = 0; j < num_fields - 1; j++) {
            pos = sql.indexOf(",");

            field = sql.substring(0, pos);

            sql = sql.substring(pos + 2);

            pos = field.indexOf(" ");

            field_name = field.substring(0, pos);
            field_type = field.substring(pos + 1, field.length());

            key_type										= ion_switch_key_type(field_type);

            field_names[j] = field_name;
            System.out.println(field_name);
            field_types[j] = key_type;
            System.out.println(key_type);
        }

        /* Table set-up */

        pos = sql.indexOf("(");
        int pos2 = sql.indexOf(")");

        String primary_key;

        primary_key = sql.substring(pos + 1, pos2);
        System.out.println("primary key "+primary_key);

	    /* Set up table for primary key */

        String	primary_key_size;
        int primary_key_field_num = -1;
        int	    primary_key_type = 0;

        for (int j = 0; j < num_fields - 1; j++) {
		/* Primary key attribute information found */
		    if (primary_key.equals(field_names[j])) {
		        primary_key_type = field_types[j];
		        primary_key_field_num = j;
		        System.out.println("pkt"+primary_key_type);
		        break;
            }
        }

        primary_key_size = ion_switch_key_size(primary_key_type);

        String value_size = "";

        for (int j = 0; j < num_fields - 1; j++) {
            if (j > 0) {
                value_size = value_size.concat("+");
            }
            value_size = value_size.concat(ion_switch_key_size(field_types[j]));
        }

        /* Create header of file */
        out.write("#include \"iinq_user_functions.h\"\n\n");

        /* Create print table method */
        out.write("void print_table"+create_count+"(ion_dictionary_t *dictionary) {\n");
        out.write("\n\tion_predicate_t predicate;\n");
        out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");
        out.write("\tion_dict_cursor_t *cursor = NULL;\n");
        out.write("\tdictionary_find(dictionary, &predicate, &cursor);\n\n");
        out.write("\tion_record_t ion_record;\n");
        out.write("\tion_record.key		= malloc("+primary_key_size+");\n");
        out.write("\tion_record.value	= malloc("+value_size+");\n\n");
        out.write("\tprintf(\"Table: "+table_name.substring(0, table_name.length() - 4)+"\");\n");

        for (int j = 0; j < num_fields - 1; j++) {
            out.write("\tprintf(\""+field_names[j]+"\t\");\n");
        }

        out.write("\tprintf(\"***************************************\");\n\n");

        out.write("\tion_cursor_status_t cursor_status;\n\n");

        out.write("\twhile ((cursor_status = cursor->next(cursor, &ion_record)) == cs_cursor_active || " +
                "cursor_status == cs_cursor_initialized) {\n");
        out.write("\t\tprintf(\"%s\", (char *) ion_record.value); \n\t}\n");

        out.write("\tcursor->destroy(&cursor);\n");
        out.write("\tfree(ion_record.key);\n");
        out.write("\tfree(ion_record.value);\n}\n\n");

        /* Create CREATE TABLE method */
        out.write("void create_table" + create_count +"() {\n");
        out.newLine();
        out.write("\tion_err_t error;");
        out.newLine();
        out.write("\tion_dictionary_t dictionary;");
        out.newLine();
        out.write("\tion_dictionary_handler_t handler;");
        out.newLine();
        out.write("\n\terror = iinq_create_source(\""+table_name+"\", "+primary_key_type+", "+primary_key_size+", "+value_size+");");
        print_error(out, false);
        out.write("\tdictionary.handler = &handler;");
        out.newLine();
        out.write("\terror = iinq_open_source(\""+table_name+"\", &dictionary, &handler);");
        print_error(out, false);

        out.write("\tprint_table"+create_count+"(&dictionary);\n");
        out.write("\terror = ion_close_dictionary(&dictionary);");
        print_error(out, false);

        String schema_name = table_name.substring(0, table_name.length() - 4).concat(".sch");

        /* Create schema table */
        out.write("\tion_dictionary_t schema_dictionary;");
        out.newLine();
        out.write("\tion_dictionary_handler_t schema_handler;");
        out.newLine();
        out.write("\terror = iinq_create_source(\""+schema_name+"\", "+0+", sizeof(int), 20);");
        print_error(out, false);
        out.write("\tschema_dictionary.handler = &schema_handler;");
        out.newLine();
        out.write("\terror = iinq_open_source(\""+schema_name+"\", &schema_dictionary, &schema_handler);");
        print_error(out, false);
        out.newLine();
        out.write("\tion_status_t status = dictionary_insert(&schema_dictionary, IONIZE(1, int), \""+table_name+"\");");
        print_error(out, true);
        out.write("\tstatus = dictionary_insert(&schema_dictionary, IONIZE(2, int), IONIZE("+primary_key_type+", int));");
        print_error(out, true);
        out.write("\tstatus = dictionary_insert(&schema_dictionary, IONIZE(3, int),  IONIZE("+primary_key_size+", int));");
        print_error(out, true);
        out.write("\tstatus = dictionary_insert(&schema_dictionary, IONIZE(4, int), IONIZE("+value_size+", int));");
        print_error(out, true);
        out.write("\tstatus = dictionary_insert(&schema_dictionary, IONIZE(5, int), IONIZE("+num_fields+", int));");
        print_error(out, true);
        out.write("\tstatus = dictionary_insert(&schema_dictionary, IONIZE(6,int), IONIZE("+0+", int));");
        print_error(out, true);
        out.write("\tstatus = dictionary_insert(&schema_dictionary, IONIZE(7, int), IONIZE("+(8+primary_key_field_num)+", int));");
        print_error(out, true);

        int curr_key = 8;

        for (int j = 0; j < num_fields; j++) {
            out.write("\tstatus = dictionary_insert(&schema_dictionary, IONIZE("+curr_key+", int), \""+field_names[j]+"\");");
            print_error(out, true);
            curr_key++;
            out.write("\tstatus = dictionary_insert(&schema_dictionary, IONIZE("+curr_key+", int), IONIZE("+field_types[j]+", int));");
            print_error(out, true);
            curr_key++;
        }

        out.write("\terror = ion_close_dictionary(&schema_dictionary);");
        print_error(out, false);

        out.write("}\n\n");

        /* Create schema table header file */
        header.write("#if !defined(IINQ_USER_FUNCTIONS_H_)\n" + "#define IINQ_USER_FUNCTIONS_H_\n\n");
        header.write("#if defined(__cplusplus)\n" + "extern \"C\" {\n" + "#endif\n\n");
        /* Include other headers*/
        header.write("#include \"../../dictionary/dictionary_types.h\"\n" +
                "#include \"../../dictionary/dictionary.h\"\n" +
                "#include \"../iinq.h\"\n" + "#include \"iinq_execute.h\"\n\n");
        header.write("void create_table" + create_count +"();\n\n");
        header.write("#if defined(__cplusplus)\n" + "}\n" + "#endif\n" + "\n" + "#endif\n");

        /* Add to file to executable includes */
        String include_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.h";
        BufferedReader file = new BufferedReader(new FileReader(include_path));

        String line = "";
        String contents = "";
        int count = 0;

        while(null != (line = file.readLine())) {
            contents += line + '\n';

            if (line.contains("#include") && 0 == count && !file.readLine().equals("#include \"iinq_user_function.c\"")) {
                contents += "#include \"iinq_user_functions.h\"\n";
                count++;
            }
        }

        File output_file = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.h");
        FileOutputStream header_out = new FileOutputStream(output_file, false);

        header_out.write(contents.getBytes());

        file.close();
        out.close();
        header_out.close();

        /* Add new functions to be run to executable */
        String ex_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c";
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        contents = "";

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains("CREATE TABLE") && !line.contains("/*")) {
                contents += "/* "+line + " */\n";
                contents += "\tcreate_table" + create_count +"();\n";
            }

            else {
                contents += line + '\n';
            }
        }

        File ex_output_file = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c");
        FileOutputStream ex_out = new FileOutputStream(ex_output_file, false);

        ex_out.write(contents.getBytes());

        ex_file.close();
        ex_out.close();

        System.out.println("schema "+schema_name);
    }

    private static void
    insert(String sql, BufferedWriter out, BufferedWriter header) throws IOException {
        System.out.println("insert statement");
    }

    private static void
    update(String sql, BufferedWriter out, BufferedWriter header) throws IOException {
        System.out.println("update statement");
    }

    private static void
    delete(String sql, BufferedWriter out, BufferedWriter header) throws IOException {
        System.out.println("delete statement");
    }

    private static void
    drop_table(String sql, BufferedWriter out, BufferedWriter header) throws IOException {
        System.out.println("drop statement");
    }
}
