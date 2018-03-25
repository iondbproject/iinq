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
import java.util.ArrayList;

public class IinqExecute {

    private static int table_id_count = 0;
    private static int tables_count = 0;

    private static boolean print_written    = false;
    private static boolean param_written    = false;
    private static boolean delete_written   = false;
    private static boolean update_written   = false;
    private static boolean select_written   = false;
    private static boolean create_written   = false;
    private static boolean drop_written     = false;

    /* Variables for INSERT supported prepared statements on multiple tables */
    private static ArrayList<String> table_names = new ArrayList<>();
    private static boolean new_table;
    private static ArrayList<insert> inserts = new ArrayList<>();
    private static ArrayList<insert_fields> insert_fields = new ArrayList<>();
    private static String written_table;
    private static ArrayList<String> function_headers = new ArrayList<>();
    private static ArrayList<tableInfo> calculateInfo = new ArrayList<>();
    private static ArrayList<delete_fields> delete_fields = new ArrayList<>();
    private static ArrayList<update_fields> update_fields = new ArrayList<>();
    private static ArrayList<select_fields> select_fields = new ArrayList<>();
    private static ArrayList<create_fields> create_fields = new ArrayList<>();
    private static ArrayList<String> drop_tables = new ArrayList<>();

    public static void main(String args[]) throws IOException {

        FileInputStream in = null;
        FileOutputStream out = null;

        try {
            in = new FileInputStream("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c");

            /* Create output file */
            File output_file = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user_functions.c");
            output_file.createNewFile();
            out = new FileOutputStream(output_file, false);

            BufferedReader buff_in = new BufferedReader(new InputStreamReader(in));
            BufferedWriter buff_out = new BufferedWriter(new OutputStreamWriter(out));

            String sql;
            buff_out.write("#include \"iinq_user_functions.h\"\n\n");

            main_setup();

            /* File is read line by line */
            while (((sql = buff_in.readLine()) != null) && (!sql.contains("return")))   {
                /* Verify file contents are as expected*/
                System.out.println (sql);

                /* CREATE TABLE statements exists in code that is not a comment */
                if ((sql.toUpperCase()).contains("CREATE TABLE") && !sql.contains("/*")) {
                    create_table(sql, buff_out);
                }

                /* INSERT statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("INSERT INTO") && !sql.contains("/*")) {
                    insert(sql, buff_out);
                }

                /* UPDATE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("UPDATE") && !sql.contains("/*")) {
                    update(sql, buff_out);
                }

                /* DELETE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("DELETE FROM") && !sql.contains("/*")) {
                    delete(sql, buff_out);
                }

                /* DROP TABLE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("DROP TABLE") && !sql.contains("/*")) {
                    drop_table(sql, buff_out);
                }

                else if ((sql.toUpperCase()).contains("SELECT") && !sql.contains("/*")) {
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
        }

        finally {
            if (null != in) {
                in.close();
            }
            if (null != out) {
                out.close();
            }
        }
    }

    private static void
    main_setup() throws IOException {
        /* Comment out old IINQ functions */
        String path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c";
        BufferedReader file = new BufferedReader(new FileReader(path));

        String contents = "";
        String line;

        while(null != (line = file.readLine())) {
            if ((line.contains("create_table") || line.contains("insert")
                    || line.contains("update") || line.contains("delete")
                    || line.contains("drop_table")) && !line.contains("/*")) {
                contents += "/* "+line + " */\n";
            }

            else {
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

        if (key_type.contains("CHAR")) {
            return 2;
        }

        if (key_type.contains("VARCHAR")) {
            return 3;
        }

        if (key_type.contains("INT")) {
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

    private static String
    ion_switch_value_size(
        String value_type
    ) {
        if (value_type.contains("CHAR") && !value_type.contains("VARCHAR")) {
            value_type = value_type.substring(value_type.indexOf("[") + 1, value_type.indexOf("]"));
            return "sizeof(char) * "+value_type;
        }
        else if (value_type.contains("VARCHAR")) {
            value_type = value_type.substring(value_type.indexOf("[") + 1, value_type.indexOf("]"));
            return "sizeof(char) * "+value_type;
        }
        else if (value_type.contains("BOOLEAN")) {
            return "sizeof(ion_boolean_t)";
        }
        else if (value_type.contains("INT")) {
            return "sizeof(int)";
        }
        else if (value_type.contains("FLOAT")) {
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
    print_table (BufferedWriter out, String table_name) throws IOException {
        out.write("void print_table_"+table_name.substring(0, table_name.length() - 4).toLowerCase()+"(ion_dictionary_t *dictionary) {\n");
        out.write("\n\tion_predicate_t predicate;\n");
        out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");
        out.write("\tion_dict_cursor_t *cursor = NULL;\n");
        out.write("\tdictionary_find(dictionary, &predicate, &cursor);\n\n");
        out.write("\tion_record_t ion_record;\n");
        out.write("\tion_record.key		= malloc("+get_schema_value(table_name, "PRIMARY KEY SIZE: ")+");\n");
        out.write("\tion_record.value	= malloc("+get_schema_value(table_name, "VALUE SIZE: ")+");\n\n");
        out.write("\tprintf(\"Table: "+table_name.substring(0, table_name.length() - 4)+"\\"+"n"+"\");\n");

        for (int j = 0; j < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); j++) {
            out.write("\tprintf(\""+get_schema_value(table_name, "FIELD"+j+" NAME: ")+"\t\");\n");
        }

        out.write("\tprintf(\""+"\\"+"n"+"***************************************"+"\\"+"n"+"\");\n\n");

        out.write("\tion_cursor_status_t cursor_status;\n");
        out.write("\tunsigned char *value;\n\n");

        out.write("\twhile ((cursor_status = cursor->next(cursor, &ion_record)) == cs_cursor_active || " +
                "cursor_status == cs_cursor_initialized) {\n");
        out.write("\t\tvalue = ion_record.value;\n\n");

        String data_type;
        for (int j = 0; j < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); j++) {
            data_type = ion_switch_value_size(get_schema_value(table_name, "FIELD"+j+" TYPE: "));

            if (data_type.contains("char")) {
                out.write("\n\t\tprintf(\"%s\t\", (char *) value);\n");

                if (j < (Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")) - 1)) {
                    out.write("\t\tvalue += " + data_type + ";\n");
                }
            }

            /* Implement for all data types - for now assume int if not char or varchar */
            else {
                out.write("\n\t\tprintf(\"%i\t\", NEUTRALIZE(value, int));\n");

                if (j < (Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")) - 1)) {
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
        String header_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user_functions.h";

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
        String ex_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c";
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        String contents = "";
        String line;
        iinq.insert_fields insert;
        int count = 0;

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains("INSERT") && !line.contains("/*") && !line.contains("//")) {
                contents += "/* "+line + " */\n";

                String temp = line.substring(0, line.indexOf("SQL_prepare"));

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
                                if (insert.fields.get(j).contains("(?)")) {
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
        String ex_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c";
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
                    contents += "\tdelete("+delete.table_id+", \""+delete.table_name+"\", "+delete.key_size+", "
                            +delete.value_size+", "+delete.num_wheres*3;

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
        String ex_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c";
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        String contents = "";
        String line;
        iinq.update_fields update;
        int count = 0;

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains("UPDATE") && !line.contains("/*") && !line.contains("//")) {
                contents += "/* "+line + " */\n";

                update = update_fields.get(count);

                if (update != null) {
                    contents += "\tupdate("+update.table_id+", \""+update.table_name+"\", "+update.key_size+", "
                            +update.value_size+", "+update.num_wheres*3+", "+update.num_updates*4+", "
                            +(update.num_wheres*3 + update.num_updates*4);

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
                            contents += ", " + update.update_fields.get(i) + ", " + update.implicit_fields.get(i) + ", " + update.update_operators.get(i) + ", ";

                            if (update.update_field_types.get(i).contains("INT")) {
                                contents += update.update_values.get(i);
                            }
                            else {
                                contents += "\"" + update.update_values.get(i) + "\"";
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
    select_setup () throws IOException {

        /* Add new functions to be run to executable */
        String ex_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c";
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
                    contents += "\tiinq_select("+select.table_id+", \""+select.table_name+"\", "+select.key_size+", "
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
        String ex_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c";
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
        String ex_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c";
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

    private static String
    get_schema_value (String table_name, String keyword) throws IOException {
        String line;

        String path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/"+
                table_name.substring(0, table_name.length() - 4).toLowerCase()+".xml";
        BufferedReader file = new BufferedReader(new FileReader(path));

        while (null != (line = file.readLine())) {
            if (line.contains(keyword)) {
                line = line.substring(keyword.length());
                break;
            }
        }

        file.close();

        if (null != line) {
            if (line.contains(":")) {
                line = line.substring(line.indexOf(":") + 2);
            }
        }

        return line;
    }

    private static void
    increment_num_records (String table_name, boolean increment) throws IOException {
        String path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/"+
                table_name.substring(0, table_name.length() - 4).toLowerCase()+".xml";
        BufferedReader file = new BufferedReader(new FileReader(path));

        String contents = "";
        String line;
        int num;

        while (null != (line = file.readLine())) {
            if (line.contains("NUMBER OF RECORDS: ")) {
                if (line.contains(":")) {
                    line = line.substring(line.indexOf(":") + 2);
                }

                num = Integer.parseInt(line);

                if (increment) {
                    contents += "NUMBER OF RECORDS: " + (num + 1) + "\n";
                }

                else {
                    contents += "NUMBER OF RECORDS: " + (num - 1) + "\n";
                }
            }

            else {
                contents += line + '\n';
            }
        }

        File output_file = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/"+
                table_name.substring(0, table_name.length() - 4).toLowerCase()+".xml");
        FileOutputStream out = new FileOutputStream(output_file, false);

        out.write(contents.getBytes());

        file.close();
        out.close();
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
            }

            else {
                fields[j] = statement;
            }
        }

        return fields;
    }

    private static void
    create_table(String sql, BufferedWriter out) throws IOException {
        System.out.println("create statement");

        sql = sql.trim();
        sql = sql.substring(26);

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

        int pos;
        String field;
        int key_type = 2;
        String field_name;
        String field_type;

        String[] field_names = new String[num_fields];
        String[] field_types = new String[num_fields];

	    /* Set up attribute names and types */
        for (int j = 0; j < num_fields - 1; j++) {
            pos = sql.indexOf(",");

            field = sql.substring(0, pos);

            sql = sql.substring(pos + 2);

            pos = field.indexOf(" ");

            field_name = field.substring(0, pos);
            field_type = field.substring(pos + 1, field.length());

            key_type = ion_switch_key_type(field_type);

            field_names[j] = field_name;
            field_types[j] = field_type;
        }

        /* Table set-up */

        pos = sql.indexOf("(");
        int pos2 = sql.indexOf(")");

        String primary_key;

        primary_key = sql.substring(pos + 1, pos2);

	    /* Set up table for primary key */

        String	primary_key_size;
        int primary_key_field_num = -1;
        int	    primary_key_type = 0;

        for (int j = 0; j < num_fields - 1; j++) {
		/* Primary key attribute information found */
		    if (primary_key.equals(field_names[j])) {
		        primary_key_type = ion_switch_key_type(field_types[j]);
		        primary_key_field_num = j;
		        break;
            }
        }

        primary_key_size = ion_switch_key_size(primary_key_type);

        String value_calculation = "";
        String value_size = "";
        int int_count = 0;
        boolean char_present = false;
        int char_multiplier = 0;

        for (int j = 0; j < num_fields - 1; j++) {
            value_size = ion_switch_value_size(field_types[j]);
            if (value_size.contains("char")) {
                char_multiplier += Integer.parseInt(value_size.substring(value_size.indexOf("*") + 1).trim());
                char_present = true;
            }

            if (value_size.contains("int")) {
                int_count++;
            }
        }

        if (int_count > 0) {
            if (int_count > 1) {
                value_calculation += "(sizeof(int) * " + int_count + ")";
                System.out.println("Int mult: "+int_count);
            }

            else {
                value_calculation += "sizeof(int)";
            }

            if (char_present) {
                value_calculation += "+(sizeof(char) * "+char_multiplier+")";
                System.out.println("Char mult: "+char_multiplier);
            }
        }

        String schema_name = table_name.substring(0, table_name.length() - 4).toLowerCase().concat(".xml");

        /* Set up schema XML file */
        String contents = "";

        contents += "<?xml version=\"1.0\" ?>";
        contents += "\n<schema>";
        contents += "\n\t<table_name>";
        contents += "\n\t\tTABLE NAME: "+table_name;
        contents += "\n\t</table_name>";
        contents += "\n\t<primary_key_type>";
        contents += "\n\t\tPRIMARY KEY TYPE: "+primary_key_type;
        contents += "\n\t</primary_key_type>";
        contents += "\n\t<primary_key_size>";
        contents += "\n\t\tPRIMARY KEY SIZE: "+primary_key_size;
        contents += "\n\t</primary_key_size>";
        contents += "\n\t<value_size>";
        contents += "\n\t\tVALUE SIZE: "+value_calculation;
        contents += "\n\t</value_size>";
        contents += "\n\t<num_fields>";
        contents += "\n\t\tNUMBER OF FIELDS: "+(num_fields - 1);
        contents += "\n\t</num_fields>";
        contents += "\n\t<num_records>";
        contents += "\n\t\t\t\tNUMBER OF RECORDS: 0";
        contents += "\n\t</num_records>";
        contents += "\n\t<primary_key_field>";
        contents += "\n\t\tPRIMARY KEY FIELD: "+primary_key_field_num;
        contents += "\n\t</primary_key_field>";
        contents += "\n\t<fields>";

        for (int j = 0; j < num_fields - 1; j++) {
            contents += "\n\t\t<field>";
            contents += "\n\t\t\t<field_name>";
            contents += "\n\t\t\t\tFIELD"+j+" NAME: "+field_names[j];
            contents += "\n\t\t\t</field_name>";
            contents += "\n\t\t\t<field_type>";
            contents += "\n\t\t\t\tFIELD"+j+" TYPE: "+field_types[j];
            contents += "\n\t\t\t</field_type>";
            contents += "\n\t\t</field>";
        }

        contents += "\n\t</fields>";
        contents += "\n</schema>";

        File schema = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/"+schema_name);
        FileOutputStream schema_out = new FileOutputStream(schema, false);

        schema_out.write(contents.getBytes());

        schema_out.close();

        /* Create print table method if it doesn't already exist */
        if (!print_written) {
            print_table(out, table_name);
        }

        print_written = true;

        /* Create CREATE TABLE method */
        if (!create_written) {
            out.write("void create_table(char *table_name, ion_key_type_t key_type, ion_key_size_t key_size, ion_value_size_t value_size) {\n");
            out.write("\tion_err_t error;\n\n");
            out.write("\terror = iinq_create_source(table_name, key_type, key_size, value_size);");

            print_error(out);

            out.write("}\n\n");

            function_headers.add("void create_table(char *table_name, ion_key_type_t key_type, ion_key_size_t key_size, ion_value_size_t value_size);\n");
        }

        create_written = true;

        String ion_key = "";

        if (key_type == 0) {
            ion_key = "key_type_numeric_unsigned";
        }
        else if (key_type == 2) {
            ion_key = "key_type_char_array";
        }

        create_fields.add(new create_fields(table_name, ion_key, primary_key_size, value_calculation));
    }

    private static void
    insert(String sql, BufferedWriter out) throws IOException {
        System.out.println("insert statement");

        sql = sql.trim();

        sql = sql.substring((sql.toUpperCase()).indexOf("INTO ")+5);

        String table_name = (sql.substring(0, sql.indexOf(" "))) + ".inq";
        System.out.println(table_name);

        /* Create print table method if it doesn't already exist */
        if (!print_written) {
            print_table(out, table_name);
        }

        print_written = true;

        /* Write function to file */

        int pos = sql.indexOf("(");

        sql = sql.substring(pos + 1);

        /* INSERT statement */
        String value = sql.substring(0, sql.length() - 5);
        System.out.println(value + "\n");

	    /* Get key value from record to be inserted */
        int count = 1;

        /* Count number of fields */
        for (int i = 0; i < sql.length(); i++) {
            if (sql.charAt(i) == ',') {
                count++;
            }
        }

        boolean[] prep_fields = new boolean[count];

	    /* Check if the INSERT statement is a prepared statement */
        boolean prep = sql.contains("(?)");
        System.out.println("PREP: "+prep);

        String[] fields = new String[count];
        value = "";
        String field_value;

        String key_type = get_schema_value(table_name, "PRIMARY KEY TYPE: ");
        String key_field_num = get_schema_value(table_name, "PRIMARY KEY FIELD: ");
        String table_name_sub = table_name.substring(0, table_name.length()-4);

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

        System.out.println("NEW TABLE?: "+new_table);
        System.out.println("TABLE ID: "+table_id);

        ArrayList<Integer> int_fields = new ArrayList<>();
        ArrayList<Integer> string_fields = new ArrayList<>();

        ArrayList<String> field_values = new ArrayList<>();
        ArrayList<String> field_types = new ArrayList<>();
        ArrayList<String> iinq_field_types = new ArrayList<>();
        ArrayList<String> field_sizes = new ArrayList<>();

        String prepare_function = "";

        if (new_table) {
            out.write("iinq_prepared_sql insert_" + table_name_sub + "(");
            prepare_function += "iinq_prepared_sql insert_"+table_name_sub+"(";
        }

        for (int j = 0; j < count; j++) {
            pos = sql.indexOf(",");

            if (-1 == pos) {
                String temp = sql.substring(0, sql.lastIndexOf(")"));
                pos = temp.lastIndexOf(")");
            }

            if (-1 == pos) {
                System.out.println("Error parsing values to be inserted, please check that a value has been listed for each column in table.");
                return;
            }

            fields[j] = sql.substring(0, pos);

            field_value = get_schema_value(table_name, "FIELD" + j + " TYPE: ");
            field_sizes.add(ion_switch_value_size(field_value));

            /* To be added in function call */
            field_values.add(fields[j]);

            sql = sql.substring(pos + 2);

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

            prep_fields[j] = fields[j].contains("(?)");

            if (new_table) {
                System.out.println("FIELD: " + fields[j]);
                System.out.println("PREP FIELD: " + prep_fields[j]);
            }
        }

        if (new_table) {
            prepare_function += ");\n";

            out.write(") {\n");
            out.write("\tiinq_prepared_sql p = {0};\n");

            out.write("\tp.table = malloc(sizeof(int));\n");
            out.write("\t*(int *) p.table = " + table_id + ";\n");
            out.write("\tp.value = malloc(" + get_schema_value(table_name, "VALUE SIZE: ") + ");\n");
            out.write("\tunsigned char\t*data = p.value;\n");
            out.write("\tp.key = malloc(" + ion_switch_key_size(Integer.parseInt(key_type)) + ");\n");

            if (key_type.equals("0") || key_type.equals("1")) {
                out.write("\t*(int *) p.key = value_" + (Integer.parseInt(key_field_num) + 1) + ";\n\n");
            } else {
                out.write("\tmemcpy(p.key, value_" + (Integer.parseInt(key_field_num) + 1) + ", " + key_type + ");\n\n");
            }

            for (int i = 0; i < fields.length; i++) {
                field_value = get_schema_value(table_name, "FIELD" + i + " TYPE: ");
                String value_size = ion_switch_value_size(field_value);

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

            tableInfo table = new tableInfo(table_id, count, iinq_field_types, field_sizes);

            calculateInfo.add(table);
            tables_count++;
        }

        String param_header = "";

        /* INSERT statement is a prepared statement */
        if (prep && !param_written) {
            written_table = table_name_sub;

            out.write("void setParam(iinq_prepared_sql p, int field_num, void *val) {\n");
            out.write("\tunsigned char\t*data\t\t= p.value;\n\n");
            out.write("\tiinq_field_t type\t\t= getFieldType(p.table, field_num);");
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
            out.write("}\n\n");
        }

        else {
            inserts.add(new insert(table_name_sub, fields, int_fields, string_fields, count, sql, pos, value, prep_fields, key_type, key_field_num, table_id));
        }

        if (!prepare_function.equals("")) {
            function_headers.add(prepare_function);
        }

        if (!param_header.equals("")) {
            function_headers.add(param_header);
        }

        param_written = true;
        insert_fields.add(new insert_fields(table_name_sub, field_values, field_types));
    }

    /* Concatenates information for additional tables onto already written INSERT functions */
    private static void
    params() throws IOException {
        System.out.println("HELLOOOOO");
        String path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user_functions.c";
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
                    System.out.println("written: "+written);

                    if (!written) {
                        System.out.println("in here hello, size: "+execute_written.size());
                        for (int k = 0; k < execute_written.size(); k++) {
                            System.out.println("Table name: "+table_name+", offset written: "+execute_written.get(k));
                            if (table_name.equals(execute_written.get(k))) {
                                written = true;
                            }
                        }
                    }
                    System.out.println("written: "+written);

                    if (!written) {
                        execute_written.add(table_name);
                        table_id = inserts.get(i).id;
                        key_type = inserts.get(i).key_type;
                        table_name = inserts.get(i).name;

                        System.out.println("in here 4");
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

            for (int i = 0; i < tables_count; i++) {
                int table_id = calculateInfo.get(i).tableId;
                int num_fields = calculateInfo.get(i).numFields;

                out.write("\tswitch (*(int *) table) {\n");
                out.write("\t\tcase "+table_id+" : {\n");
                out.write("\t\t\tswitch (field_num) {\n");

                field_size_function += "\tswitch (*(int *) table) {\n";
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
                out.write("\t\tdefault:\n\t\t\treturn 0;\n");
                out.write("\t}\n}\n\n");

                field_size_function += "\t\t\t\tdefault:\n\t\t\t\t\treturn iinq_int;\n";
                field_size_function += "\t\t\t}\n\t\t}\n";
                field_size_function += "\t\tdefault:\n\t\t\treturn iinq_int;\n";
                field_size_function += "\t}\n}\n\n";
            }

            out.write(field_size_function);
        }
    }

    private static void
    update(String sql, BufferedWriter out) throws IOException {
        System.out.println("update statement");

        sql = sql.trim();
        sql = sql.substring(20);

        String table_name = (sql.substring(0, sql.indexOf(" ")))+".inq";
        String table_name_sub = table_name.substring(0, table_name.length()-4);
        System.out.println(table_name);

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

        sql = sql.substring(table_name.length() + 1);

        /* Create print table method if it doesn't already exist */
        if (!print_written) {
            print_table(out, table_name);
        }

        print_written = true;

        if (!update_written) {
            out.write("void update(int id, char *name, size_t key_size, size_t value_size, int num_wheres, int num_update, int num, ...) {\n\n");
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
            out.write("\terror              = iinq_open_source(table_name, &dictionary, &handler);");
            print_error(out);

            out.write("\tion_predicate_t predicate;\n");
            out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");

            out.write("\tion_dict_cursor_t *cursor = NULL;\n");
            out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n\n");
            out.write("\tion_record_t ion_record;\n");
            out.write("\tion_record.key     = malloc(key_size);\n");
            out.write("\tion_record.value   = malloc(value_size);\n\n");

            out.write("\tion_cursor_status_t status;\n\n");
            out.write("\tint count = 0;\n");
            out.write("\tion_key_t keys[100];\n");
            out.write("\tion_value_t values[100];\n");
            out.write("\tion_boolean_t condition_satisfied;\n\n");

            out.write("\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
            out.write("\t\tcondition_satisfied = where(table_id, &ion_record, num_wheres, &valist);\n\n");
            out.write("\t\tif (condition_satisfied || num_wheres == 0) {\n");
            out.write("\t\t\tkeys[count]    = malloc(key_size);\n");
            out.write("\t\t\tvalues[count]  = malloc(value_size);\n");
            out.write("\t\t\tmemcpy(keys[count], ion_record.key, key_size);\n");
            out.write("\t\t\tmemcpy(values[count], ion_record.value, value_size);\n");
            out.write("\t\t\tcount++;\n\t\t}\n\t}\n\n");

            out.write("\tcursor->destroy(&cursor);\n\n");

            out.write("\tint update_fields[num_update/4];\n");
            out.write("\tint implicit_fields[num_update/4];\n");
            out.write("\tiinq_math_operator_t operators[num_update/4];\n");
            out.write("\tvoid *field_values[num_update/4];\n\n");

            out.write("\tfor (int i = 0; i < num_wheres; i++) {\n");
            out.write("\t\tva_arg(valist, void *);\n\t}\n\n");

            out.write("\tfor (int i = 0; i < num_update/4; i++) {\n");
            out.write("\t\tupdate_fields[i]     = va_arg(valist, int);\n");
            out.write("\t\timplicit_fields[i]   = va_arg(valist, int);\n");
            out.write("\t\toperators[i]         = va_arg(valist, iinq_math_operator_t);\n");
            out.write("\t\tfield_values[i]      = va_arg(valist, void *);\n\t}\n\n");

            out.write("\tva_end(valist);\n\n");
            out.write("\tfor (int i = 0; i < count; i++) {\n");
            out.write("\t\tfor (int j = 0; j < num_update/4; j++) {\n");
            out.write("\t\t\tunsigned char *value;\n");
            out.write("\t\t\tif (implicit_fields[j] != 0) {\n");
            out.write("\t\t\t\tint new_value;\n");
            out.write("\t\t\t\tvalue = values[i] + calculateOffset(table_id, implicit_fields[j] - 1);\n\n");

            out.write("\t\t\t\tswitch (operators[j]) {\n");
            out.write("\t\t\t\t\tcase iinq_add :\n");
            out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) + (int) field_values[j]);\n");
            out.write("\t\t\t\t\t\tbreak;\n");
            out.write("\t\t\t\t\tcase iinq_subtract :\n");
            out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) - (int) field_values[j]);\n");
            out.write("\t\t\t\t\t\tbreak;\n");
            out.write("\t\t\t\t\tcase iinq_multiply :\n");
            out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) * (int) field_values[j]);\n");
            out.write("\t\t\t\t\t\tbreak;\n");
            out.write("\t\t\t\t\tcase iinq_divide :\n");
            out.write("\t\t\t\t\t\tnew_value = (NEUTRALIZE(value, int) / (int) field_values[j]);\n");
            out.write("\t\t\t\t\t\tbreak;\n\t\t\t\t}\n");
            out.write("\t\t\t\tvalue = values[i] + calculateOffset(table_id, update_fields[j] - 1);\n");
            out.write("\t\t\t\t*(int *) value = new_value;\n\t\t\t}\n");

            out.write("\t\t\telse {\n");
            out.write("\t\t\t\tvalue = values[i] + calculateOffset(table_id, update_fields[j] - 1);\n\n");
            out.write("\t\t\t\tif (getFieldType(table_id, update_fields[j]) == iinq_int) {\n");
            out.write("\t\t\t\t\t*(int *) value = (int) field_values[j];\n\t\t\t\t}\n");
            out.write("\t\t\t\telse {\n");
            out.write("\t\t\t\t\tmemcpy(value, field_values[j], sizeof(field_values[j]));\n\t\t\t\t}\n\t\t\t}\n\n");
            out.write("\t\t\tiinq_execute(table_name, keys[i], values[i], iinq_update_t);\n");
            out.write("\t\t}\n\t}\n}\n\n");

            function_headers.add("void update(int id, char *name, size_t key_size, size_t value_size, int num_wheres, int num_update, int num, ...);\n");
        }

        update_written = true;

        int pos = sql.toUpperCase().indexOf("WHERE");
        String where_condition = "";
        int num_conditions = 0;
        int i = -1;

        /* Get WHERE condition if it exists */
        if (-1 != pos) {
            where_condition = sql.substring(pos + 6, sql.length() - 4);
            i = 0;
        }

        System.out.println(where_condition+"\n");

        /* Calculate number of WHERE conditions in statement */

        while (-1 != i) {
            num_conditions++;
            i = where_condition.indexOf(",", i + 1);
        }

        String[] conditions;

        conditions = get_fields(where_condition, num_conditions);

        /* Get fields to update */
        String update;

        if (-1 != pos) {
            update = sql.substring(0, pos);
        }

        else {
            update = sql.substring(0, sql.length() - 4);
        }

        int num_fields = 0;
        i = 0;

        System.out.println(update+"\n");

        /* Calculate number of fields to update in statement */
        while (-1 != i) {
            num_fields++;
            i = update.indexOf(",", i + 1);
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

        for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {

            String field_type = get_schema_value(table_name, "FIELD"+n+" TYPE: ");

            if (field_type.contains("CHAR")) {
                iinq_field_types.add("iinq_char");
            }
            else {
                iinq_field_types.add("iinq_int");
            }

            if (field.equals(get_schema_value(table_name, "FIELD"+n+" NAME: "))) {
                where_field.add(n+1);
                where_field_type.add(field_type);
            }
        }

        ArrayList<Integer> update_field_nums    = new ArrayList<>();
        ArrayList<Boolean> implicit             = new ArrayList<>();
        ArrayList<Integer> implicit_fields      = new ArrayList<>();
        ArrayList<String> update_operators      = new ArrayList<>();
        ArrayList<String> update_values         = new ArrayList<>();
        ArrayList<String> update_field_types    = new ArrayList<>();
        ArrayList<String>   field_sizes         = new ArrayList<>();

        String[] fields;

        fields = get_fields(update, num_fields);

        String set_string;
        String update_field;
        String implicit_field = "";
        boolean is_implicit = false;
        String update_value;

        for (int j = 0; j < num_fields; j++) {
            pos = fields[j].indexOf("=");
            update_field = fields[j].substring(0, pos).trim();
            set_string = fields[j].substring(pos + 1).trim();
            update_value = set_string;

            /* Check if update value contains an operator */
            if (set_string.contains("+")) {
                update_operators.add("iinq_add");
                pos = set_string.indexOf("+");
                implicit_field = set_string.substring(0, pos).trim();
                update_value = set_string.substring(pos + 1).trim();
                is_implicit = true;
            } else if (set_string.contains("-")) {
                update_operators.add("iinq_subtract");
                pos = set_string.indexOf("-");
                implicit_field = set_string.substring(0, pos).trim();
                update_value = set_string.substring(pos + 1).trim();
                is_implicit = true;
            } else if (set_string.contains("*")) {
                update_operators.add("iinq_multiply");
                pos = set_string.indexOf("*");
                implicit_field = set_string.substring(0, pos).trim();
                update_value = set_string.substring(pos + 1).trim();
                is_implicit = true;
            } else if (set_string.contains("/")) {
                update_operators.add("iinq_divide");
                pos = set_string.indexOf("/");
                implicit_field = set_string.substring(0, pos).trim();
                update_value = set_string.substring(pos + 1).trim();
                is_implicit = true;
            }

            update_values.add(update_value);
            implicit.add(is_implicit);

            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {
                String field_type = get_schema_value(table_name, "FIELD"+n+" TYPE: ");
                field_sizes.add(ion_switch_value_size(field_type));

                if (update_field.equals(get_schema_value(table_name, "FIELD" + n + " NAME: "))) {
                    update_field_nums.add(n+1);
                    update_field_types.add(field_type);
                }
                if (implicit_field.equals(get_schema_value(table_name, "FIELD"+n+" NAME: "))) {
                    implicit_fields.add(n+1);
                }
            }
        }

        if (new_table) {
            tableInfo table = new tableInfo(table_id, Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")), iinq_field_types, field_sizes);

            calculateInfo.add(table);
            tables_count++;
        }

        String key_size = get_schema_value(table_name, "PRIMARY KEY SIZE: ");
        String value_size = get_schema_value(table_name, "VALUE SIZE: ");

        update_fields.add(new update_fields(table_name, table_id, num_conditions, num_fields, where_field, where_operator,
                where_value, where_field_type, key_size, value_size, update_field_nums, implicit, implicit_fields, update_operators,
                update_values, update_field_types));
    }

    private static void
    select(String sql, BufferedWriter out) throws IOException {
        System.out.println("select statement");

        sql = sql.trim();

        String table_name = sql.substring(sql.indexOf("FROM") + 5);
        int pos = table_name.indexOf(" ");

        if (pos == -1) {
            pos = table_name.indexOf(";");
        }

        table_name = (table_name.substring(0, pos))+".inq";
        String table_name_sub = table_name.substring(0, table_name.length()-4);
        System.out.println(table_name);

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

        /* Create print table method if it doesn't already exist */
        if (!print_written) {
            print_table(out, table_name);
        }

        print_written = true;

        if (!select_written) {
            out.write("void iinq_select(int id, char *name, size_t key_size, size_t value_size, int num_wheres, int num_fields, int num, ...) {\n\n");
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
            out.write("\terror              = iinq_open_source(table_name, &dictionary, &handler);");
            print_error(out);

            out.write("\tion_predicate_t predicate;\n");
            out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");

            out.write("\tion_dict_cursor_t *cursor = NULL;\n");
            out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n\n");
            out.write("\tion_record_t ion_record;\n");
            out.write("\tion_record.key     = malloc(key_size);\n");
            out.write("\tion_record.value   = malloc(value_size);\n\n");

            out.write("\tion_cursor_status_t status;\n\n");
            out.write("\tint count = 0;\n");
            out.write("\tion_value_t values[100];\n");
            out.write("\tion_boolean_t condition_satisfied;\n\n");

            out.write("\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
            out.write("\t\tcondition_satisfied = where(table_id, &ion_record, num_wheres, &valist);\n\n");
            out.write("\t\tif (condition_satisfied || num_wheres == 0) {\n");
            out.write("\t\t\tvalues[count]  = malloc(value_size);\n");
            out.write("\t\t\tmemcpy(values[count], ion_record.value, value_size);\n");
            out.write("\t\t\tcount++;\n\t\t}\n\t}\n\n");

            out.write("\tcursor->destroy(&cursor);\n\n");

            out.write("\tint fields[num_fields];\n\n");

            out.write("\tfor (int i = 0; i < num_wheres; i++) {\n");
            out.write("\t\tva_arg(valist, void *);\n\t}\n\n");

            out.write("\tfor (int i = 0; i < num_fields; i++) {\n");
            out.write("\t\tfields[i]     = va_arg(valist, int);\n\t}\n\n");

            out.write("\tva_end(valist);\n\n");
            out.write("\tfor (int i = 0; i < count; i++) {\n");
            out.write("\t\tfor (int j = 0; j < num_fields; j++) {\n");
            out.write("\t\t\tif (getFieldType(table_id, fields[j]) == iinq_int) {\n");
            out.write("\t\t\t\tprintf(\"%i\", NEUTRALIZE(values[i] = values[i] + calculateOffset(table_id, fields[j] - 1), int));\n");
            out.write("\t\t\t}\n");
            out.write("\t\t\telse {\n");
            out.write("\t\t\t\tprintf(\"%s\", (char *) (values[i] = values[i] + calculateOffset(table_id, fields[j] - 1)));\n");
            out.write("\t\t\t}\n");
            out.write("\t\t\tprintf(\""+"\\"+"t\");\n");
            out.write("\t\t}\n");
            out.write("\t\tprintf(\""+"\\"+"n\");\n\t}\n}\n\n");

            function_headers.add("void iinq_select(int id, char *name, size_t key_size, size_t value_size, int num_wheres, int num_fields, int num, ...);\n");
        }

        select_written = true;

        pos = sql.toUpperCase().indexOf("WHERE");
        String where_condition = "";
        int num_conditions = 0;
        int i = -1;

        /* Get WHERE condition if it exists */
        if (-1 != pos) {
            where_condition = sql.substring(pos + 6, sql.length() - 4);
            i = 0;
        }

        System.out.println(where_condition+"\n");

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
        field_list = sql.substring(pos + 7, sql.toUpperCase().indexOf("FROM") - 1);

        int num_fields = 0;
        i = 0;

        System.out.println(field_list+"\n");

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

        for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {

            String field_type = get_schema_value(table_name, "FIELD"+n+" TYPE: ");

            if (field_type.contains("CHAR")) {
                iinq_field_types.add("iinq_char");
            }
            else {
                iinq_field_types.add("iinq_int");
            }

            if (field.equals(get_schema_value(table_name, "FIELD"+n+" NAME: "))) {
                where_field.add(n+1);
                where_field_type.add(field_type);
            }
        }

        ArrayList<Integer> select_field_nums    = new ArrayList<>();
        ArrayList<String>   field_sizes         = new ArrayList<>();

        String[] fields;

        fields = get_fields(field_list, num_fields);

        for (int j = 0; j < num_fields; j++) {
            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {
                String field_type = get_schema_value(table_name, "FIELD"+n+" TYPE: ");
                field_sizes.add(ion_switch_value_size(field_type));

                if ((fields[j].trim()).equals(get_schema_value(table_name, "FIELD" + n + " NAME: "))) {
                    select_field_nums.add(n+1);
                }
            }
        }

        if (new_table) {
            tableInfo table = new tableInfo(table_id, Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")), iinq_field_types, field_sizes);

            calculateInfo.add(table);
            tables_count++;
        }

        String value_size = get_schema_value(table_name, "VALUE SIZE: ");
        String key_size = get_schema_value(table_name, "PRIMARY KEY SIZE: ");

        select_fields.add(new select_fields(table_name, table_id, num_conditions, num_fields, where_field, where_operator,
                        where_value, where_field_type, key_size, value_size, select_field_nums));
    }

    private static void
    delete(String sql, BufferedWriter out) throws IOException {
        System.out.println("delete statement");

        sql = sql.trim();
        sql = sql.substring(25);

        String table_name = (sql.substring(0, sql.indexOf(" ")))+".inq";
        String table_name_sub = table_name.substring(0, table_name.length()-4);
        System.out.println(table_name);

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

        sql = sql.substring(table_name.length() - 3);

        /* Create print table method if it doesn't already exist */
        if (!print_written) {
            print_table(out, table_name);
        }

        print_written = true;

        /* Write function to file */
        String key_size = get_schema_value(table_name, "PRIMARY KEY SIZE: ");
        String value_size = get_schema_value(table_name, "VALUE SIZE: ");

        if (!delete_written) {
            out.write("void delete(int id, char *name, size_t key_size, size_t value_size, int num_fields, ...) {\n\n");
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
            out.write("\terror              = iinq_open_source(table_name, &dictionary, &handler);");
            print_error(out);

            out.write("\tion_predicate_t predicate;\n");
            out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n\n");

            out.write("\tion_dict_cursor_t *cursor = NULL;\n");
            out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n\n");
            out.write("\tion_record_t ion_record;\n");
            out.write("\tion_record.key     = malloc(key_size);\n");
            out.write("\tion_record.value   = malloc(value_size);\n\n");

            out.write("\tion_cursor_status_t status;\n\n");
            out.write("\tint count = 0;\n");
            out.write("\tion_key_t keys[100];\n");
            out.write("\tion_boolean_t condition_satisfied;\n\n");

            out.write("\twhile ((status = iinq_next_record(cursor, &ion_record)) == cs_cursor_initialized || status == cs_cursor_active) {\n");
            out.write("\t\tcondition_satisfied = where(table_id, &ion_record, num_fields, &valist);\n\n");
            out.write("\t\tif (condition_satisfied) {\n");
            out.write("\t\t\tkeys[count] = malloc(key_size);\n");
            out.write("\t\t\tmemcpy(keys[count], ion_record.key, key_size);\n");
            out.write("\t\t\tcount++;\n\t\t}\n\t}\n\n");

            out.write("\tva_end(valist);\n\n");
            out.write("\tfor (int i = 0; i < count; i++) {\n");
            out.write("\t\tiinq_execute(table_name, keys[i], NULL, iinq_delete_t);\n\t}\n\n");
            out.write("\tcursor->destroy(&cursor);\n}\n\n");

            function_headers.add("void delete(int id, char *name, size_t key_size, size_t value_size, int fields, ...);\n");
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

        System.out.println("WHERE: "+where_condition+"\n");

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
            System.out.println(conditions[j]);

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

            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {

                String field_type = get_schema_value(table_name, "FIELD"+n+" TYPE: ");
                field_sizes.add(ion_switch_value_size(field_type));

                if (field_type.contains("CHAR")) {
                    iinq_field_types.add("iinq_char");
                }
                else {
                    iinq_field_types.add("iinq_int");
                }

                if (field.equals(get_schema_value(table_name, "FIELD"+n+" NAME: "))) {
                    fields.add(n+1);
                    field_types.add(field_type);
                }
            }
        }

        if (new_table) {
            tableInfo table = new tableInfo(table_id, Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")), iinq_field_types, field_sizes);

            calculateInfo.add(table);
            tables_count++;
        }

        delete_fields.add(new delete_fields(table_name, table_id, num_conditions, fields, operators, values, field_types, key_size, value_size));
    }

    private static void
    drop_table(String sql, BufferedWriter out) throws IOException {
        System.out.println("drop statement");

        sql = sql.trim();

        sql = sql.substring(24);

        String table_name = (sql.substring(0, sql.indexOf(";"))) + ".inq";
        System.out.println(table_name);

        /* Write function to file */
        if (!drop_written) {
            out.write("void drop_table(char *table_name) {\n\n");
            out.write("\tion_err_t error;\n\n");
            out.write("\terror = iinq_drop(table_name);");
            print_error(out);

            File file = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/" +
                    table_name.substring(0, table_name.length() - 4).toLowerCase() + ".xml");

            if (!file.delete()) {
                out.write("\tprintf(\"Error occurred deleting table." + "\\" + "n" + "\");");
            }

            out.write("\tprintf(\"Table %s has been deleted." + "\\" + "n" + "\", table_name);");

            out.write("\n}\n\n");

            function_headers.add("void drop_table(char *table_name);");
        }

        drop_written = true;

        drop_tables.add(table_name);
    }

    private static void
    function_close() throws IOException {
        /* Closes insert functions because there do not exist any more commands to be read */
        String path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user_functions.c";
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
