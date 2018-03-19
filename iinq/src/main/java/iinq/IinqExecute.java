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

    private static int create_count = 1;
    private static int insert_count = 1;
    private static int update_count = 1;
    private static int drop_count = 1;
    private static int tables_count = 0;

    private static boolean header_written = false;
    private static boolean print_written = false;
    private static boolean param_written = false;
    private static boolean delete_written = false;

    /* Variables for INSERT supported prepared statements on multiple tables */
    private static ArrayList<String> table_names = new ArrayList<>();
    private static boolean new_table;
    private static ArrayList<insert> inserts = new ArrayList<>();
    private static ArrayList<insert_fields> insert_fields = new ArrayList<>();
    private static String written_table;
    private static ArrayList<String> function_headers = new ArrayList<>();
    private static ArrayList<tableInfo> calculateInfo = new ArrayList<>();
    private static ArrayList<delete_fields> delete_fields = new ArrayList<>();

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
                    create_count++;
                }

                /* INSERT statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("INSERT INTO") && !sql.contains("/*")) {
                    insert(sql, buff_out);
                    insert_count++;
                }

                /* UPDATE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("UPDATE") && !sql.contains("/*")) {
                    update(sql, buff_out);
                    update_count++;
                }

                /* DELETE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("DELETE FROM") && !sql.contains("/*")) {
                    delete(sql, buff_out);
                }

                /* DROP TABLE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("DROP TABLE") && !sql.contains("/*")) {
                    drop_table(sql, buff_out);
                    drop_count++;
                }
            }

            calculate_functions(buff_out);

            buff_in.close();
            buff_out.close();

            params();
            write_headers();
            insert_setup();
            delete_setup();
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
    print_error (BufferedWriter out, boolean status, int num_tabs) throws IOException {
        out.newLine();
        out.newLine();

        if (!status) {
            if (0 != num_tabs) {
                for (int i = 0; i < num_tabs; i++) {
                    out.write("\t");
                }
            }
            out.write("\tif (err_ok != error) {");
        }

        else {
            if (0 != num_tabs) {
                for (int i = 0; i < num_tabs; i++) {
                    out.write("\t");
                }
            }
            out.write("\tif (err_ok != status.error) {");
        }

        out.newLine();
        if (0 != num_tabs) {
            for (int i = 0; i < num_tabs; i++) {
                out.write("\t");
            }
        }
        out.write("\t\tprintf(\"Error occurred. Error code: %i"+"\\"+"n"+"\", error);");
        out.newLine();
        if (0 != num_tabs) {
            for (int i = 0; i < num_tabs; i++) {
                out.write("\t");
            }
        }
        out.write("\t\treturn; \n");

        if (0 != num_tabs) {
            for (int i = 0; i < num_tabs; i++) {
                out.write("\t");
            }
        }
        out.write("\t}");
        out.newLine();
        out.newLine();
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
    file_setup (boolean header_written, String function, String keyword) throws IOException {
        /* Write header file */
        String header_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user_functions.h";
        BufferedReader header_file = null;

        if (header_written) {
            header_file = new BufferedReader(new FileReader(header_path));
        }

        String contents = "";
        String line = "";
        int count = 0;

        if(header_written) {
            while (null != (line = header_file.readLine())) {
                contents += line + '\n';

                if (line.contains("void") && 0 == count) {
                    if ((insert_count == 1) || (!keyword.contains("INSERT"))) {
                        contents += "\nvoid " + function + "();\n";
                    }
                    count++;
                }
            }
            header_file.close();
        }

        File output_file = new File(header_path);

        /* Create header file if it does not previously exist*/
        if (!output_file.exists()) {
            output_file.createNewFile();
        }

        FileOutputStream header = new FileOutputStream(output_file, false);

        if (header_written) {
            header.write(contents.getBytes());
        }

        /* Create schema table header file */
        else {
            contents = "";
            print_top_header(header);

            if ((insert_count == 1) || (!keyword.contains("INSERT"))) {
                contents += "void " + function + "();\n\n";
            }
            contents += "#if defined(__cplusplus)\n" + "}\n" + "#endif\n" + "\n" + "#endif\n";

            header.write(contents.getBytes());
        }

        header.close();

        /* Add new functions to be run to executable */
        String ex_path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.c";
        BufferedReader ex_file = new BufferedReader(new FileReader(ex_path));

        contents = "";
        boolean found = false;

        while(null != (line = ex_file.readLine())) {
            if ((line.toUpperCase()).contains(keyword) && !line.contains("/*") && !found) {
                contents += "/* "+line + " */\n";

                if ((insert_count == 1) || (!keyword.contains("INSERT"))) {
                    contents += "\t" + function + "();\n";
                }
                found = true;
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

        String statement = sql;

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
        String field, key_type;
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

            key_type										= ion_switch_value_size(field_type);

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

        String value_size = "";

        for (int j = 0; j < num_fields - 1; j++) {
            if (j > 0) {
                value_size = value_size.concat("+");
            }
            value_size = value_size.concat(ion_switch_value_size(field_types[j]));
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
        contents += "\n\t\tVALUE SIZE: "+value_size;
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
        out.write("void create_table" + create_count +"() {\n");
        out.write("\tprintf(\"%s"+"\\"+"n"+"\\"+"n"+"\", \""+statement.substring(statement.indexOf("(") + 2, statement.length() - 4)+"\");\n");
        out.newLine();
        out.write("\tion_err_t error;");
        out.newLine();
        out.write("\tion_dictionary_t dictionary;");
        out.newLine();
        out.write("\tion_dictionary_handler_t handler;");
        out.newLine();
        out.write("\n\terror = iinq_create_source(\""+table_name+"\", "+primary_key_type+", "+primary_key_size+", "+value_size+");");
        print_error(out, false, 0);
        out.write("\tdictionary.handler = &handler;");
        out.newLine();
        out.write("\terror = iinq_open_source(\""+table_name+"\", &dictionary, &handler);");
        print_error(out, false, 0);

        out.write("\tprint_table_"+table_name.substring(0, table_name.length() - 4).toLowerCase()+"(&dictionary);\n");
        out.write("\terror = ion_close_dictionary(&dictionary);");
        print_error(out, false, 0);

        out.write("}\n\n");

        System.out.println("schema "+schema_name);

        file_setup(header_written,"create_table"+create_count, "CREATE TABLE");
        function_headers.add("void create_table"+create_count+"();\n");
        header_written = true;
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
            table_id = insert_count - 1;
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
            out.write("\tunsigned char\t*data = p.value;\n\n");

            param_header = "void setParam(iinq_prepared_sql p, int field_num, void *val);\n";

            for (int j = 0; j < count; j++) {

                if (sql.length() > pos + 2) {
                    sql = sql.substring(pos + 2);
                }

                value += fields[j] + ",\t";
            }

            out.write("\tif (*(int *) p.table == "+table_id+") {\n");
            boolean first = true;

            if (int_fields.get(0) != null) {
                out.write("\t\tif (field_num ==");

                for (int i = 0; i < int_fields.size(); i++) {
                    if (int_fields.get(i) != null) {
                        System.out.println("int field: "+int_fields.get(i));
                        if (first) {
                            System.out.println("FIRST FIELD: "+int_fields.get(i));
                            out.write(" "+int_fields.get(i));
                            first = false;
                        } else {
                            out.write(" || field_num == " + int_fields.get(i));
                        }
                    }
                }

                out.write(") {\n");
                out.write("\t\t\tdata += calculateOffset(p.table, (field_num - 1));\n");
                out.write("\t\t\t*(int *) data = (int) val;\n\t\t}\n");
            }

            if (string_fields.get(0) != null) {
                out.write("\t\tif (field_num ==");
                first = true;

                for (int i = 0; i < string_fields.size(); i++) {
                    if (string_fields.get(i) != null) {
                        System.out.println("string field: "+string_fields.get(i));
                        if (first) {
                            out.write(" "+string_fields.get(i));
                            first = false;
                        } else {
                            out.write(" || field_num == " + string_fields.get(i));
                        }
                    }
                }

                out.write(") {\n");
                out.write("\t\t\tdata += calculateOffset(p.table, (field_num - 1));\n");
                out.write("\t\t\tmemcpy(data, val, sizeof(val));\n\t\t}\n");

                out.write("\t}\n\n");
            }

            out.write("/* INSERT 3 */\n");
            out.write("}\n\n");
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
            if (line.contains("/* INSERT 3 */")) {
                int count;
                String sql;
                int pos;
                String[] fields;
                ArrayList<Integer> int_fields;
                ArrayList<Integer> string_fields;
                int table_id;

                for (int i = 0; i < inserts.size(); i++) {
                    table_name = inserts.get(i).name;
                    written = table_name.equals(written_table);

                    if (!written) {
                        for (int l = 0; l < params_written.size(); l++) {
                            if (table_name.equals(params_written.get(l))) {
                                written = true;
                            }
                        }
                    }

                    if (!written) {
                        params_written.add(table_name);

                        count = inserts.get(i).count;
                        sql = inserts.get(i).sql;
                        pos = inserts.get(i).pos;
                        fields = inserts.get(i).fields;
                        int_fields = inserts.get(i).int_fields;
                        string_fields = inserts.get(i).string_fields;
                        table_id = inserts.get(i).id;

                        System.out.println("in here 3");
                        for (int j = 0; j < count; j++) {

                            if (sql.length() > pos + 2) {
                                sql = sql.substring(pos + 2);
                            }
                        }

                        contents += "\tif (*(int *) p.table == "+table_id+") {\n";

                        if (int_fields.get(0) != null) {
                            contents += "\t\tif (field_num == ";

                            for (int k = 0; k < int_fields.size(); k++) {
                                if (int_fields.get(k) != null) {
                                    System.out.println("int field: "+int_fields.get(k));
                                    if (k == 0) {
                                        contents += int_fields.get(k);
                                    } else {
                                        contents += " || field_num == " + int_fields.get(k);
                                    }
                                }
                            }

                            contents += ") {\n";
                            contents += "\t\t\tdata += calculateOffset(p.table, (field_num - 1));\n";
                            contents += "\t\t\t*(int *) data = (int) val;\n\t\t}\n";
                        }

                        if (string_fields.get(0) != null) {
                            contents += "\t\tif (field_num == ";

                            for (int k = 0; k < string_fields.size(); k++) {
                                if (string_fields.get(k) != null) {
                                    System.out.println("string field: "+string_fields.get(k));
                                    if (k == 0) {
                                        contents += string_fields.get(k);
                                    } else {
                                        contents += " || field_num == " + string_fields.get(k);
                                    }
                                }
                            }

                            contents += ") {\n";
                            contents += "\t\t\tdata += calculateOffset(p.table, (field_num - 1));\n";
                            contents += "\t\t\tmemcpy(data, val, sizeof(val));\n\t\t}\n";
                        }

                        contents += "\t}\n";
                    }
                }
            }
            else if (line.contains("/* INSERT 4 */")) {
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
                String value_size = "";

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

        String statement = sql;

        sql = sql.trim();
        sql = sql.substring(20);

        String table_name = (sql.substring(0, sql.indexOf(" ")))+".inq";
        System.out.println(table_name);

        sql = sql.substring(table_name.length() + 1);

        /* Create print table method if it doesn't already exist */
        if (!print_written) {
            print_table(out, table_name);
        }

        print_written = true;

        /* Write function to file */

        out.write("void update"+update_count+"() {\n\n");
        out.write("\tprintf(\"%s"+"\\"+"n"+"\\"+"n"+"\", \""+statement.substring(statement.indexOf("(") + 2, statement.length() - 4)+"\");\n");
        out.write("\tion_err_t error;\n" + "\tion_dictionary_t dictionary;\n" + "\tion_dictionary_handler_t handler;\n");
        out.write("\tdictionary.handler = &handler;\n" + "\n\terror = iinq_open_source(\""+table_name+"\", &dictionary, &handler);");
        print_error(out, false, 0);

        int pos = sql.indexOf("WHERE");
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

        String[] fields;

        fields = get_fields(update, num_fields);

        out.write("\tion_predicate_t predicate;\n");
        out.write("\tion_cursor_status_t cursor_status;\n");
        out.write("\tion_status_t status;\n");
        out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n");
        out.write("\tion_dict_cursor_t *cursor = NULL;\n");
        out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n\n");

        out.write("\tint count = 0;\n");

        String num_records = get_schema_value(table_name, "NUMBER OF RECORDS: ");
        String key_type = ion_switch_value_size(get_schema_value(table_name, "PRIMARY KEY TYPE: "));

        int primary_key_field = Integer.parseInt(get_schema_value(table_name, "PRIMARY KEY FIELD: "));
        boolean update_key = false; /* Whether the key of a record is being updated */

        for (int j = 0; j < num_fields; j++) {
            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {
                if (fields[j].substring(0, fields[j].indexOf("=")).trim().equals(get_schema_value(table_name, "FIELD"+n+" NAME: "))) {
                    if (n == primary_key_field) {
                        update_key = true;
                    }
                }
            }
        }

        if (key_type.contains("char")) {
            out.write("\tchar *old_keys["+num_records+"];\n");

            if (update_key) {
                out.write("\tchar *new_keys[" + num_records + "];\n\n");
            }
        }

        /* Update later for all data types */
        else {
            out.write("\tint old_keys["+num_records+"];\n");

            if (update_key) {
                out.write("\tint new_keys[" + num_records + "];\n\n");
            }
        }

        out.write("\tunsigned char *new_records["+num_records+"];\n\n");

        out.write("\tfor (int i = 0; i < "+num_records+"; i++) {\n" +
                "\t\tnew_records[i]\t= malloc("+get_schema_value(table_name, "VALUE SIZE: ")+");\n" +
                "\t}\n\n");
        out.write("\tion_record_t ion_record;\n");
        out.write("\tion_record.key = malloc("+ion_switch_value_size(get_schema_value(table_name, "PRIMARY KEY TYPE: "))+");\n");
        out.write("\tion_record.value = malloc("+get_schema_value(table_name, "VALUE SIZE: ")+");\n\n");
        out.write("\tion_boolean_t condition_satisfied;\n");
        out.write("\n\tunsigned char *record_data;\n");
        out.write("\tunsigned char *old_data;\n\n");

        String[] update_value_field = new String[num_fields]; /* Field value that is being used to update a field. */
        String[] update_value_value = new String[num_fields]; /* Value that is being added to another field value to update a field. */
        String[] update_operator = new String[num_fields]; /* Whether values are being updated through addition or subtraction. */
        boolean[] implicit_update_field = new boolean[num_fields];

        for (int j = 0; j < num_fields; j++) {
            if (fields[j].contains("+") || fields[j].contains("-") || fields[j].contains("*") || fields[j].contains("/")) {
                    implicit_update_field[j] = true;
            }
        }

        out.write("\twhile ((cursor_status = cursor->next(cursor, &ion_record)) == cs_cursor_active || cursor_status == cs_cursor_initialized) {\n\n");

        out.write("\t\tcondition_satisfied = boolean_true;\n");

        String[] field = new String[num_conditions];
        String[] operator = new String[num_conditions];
        String[] condition = new String[num_conditions];
        int[] field_num = new int[num_conditions]; /* Fields which have a WHERE condition corresponding to them */
        int len = 0;
        int field_type;

        out.write("\n\t\twhile (boolean_true == condition_satisfied) {\n");
        out.write("\t\t\trecord_data = new_records[count];\n");
        out.write("\t\t\told_data = ion_record.value;\n\n");

        /* Copy old key regardless if the key itself is being updated or not */
        if (key_type.contains("char")) {
            out.write("\t\t\tstrcpy(old_keys[count], ion_record.key);\n\n");
        }

        /* Update for all data types later */
        else {
            out.write("\t\t\told_keys[count] = NEUTRALIZE(ion_record.key, int);\n\n");
        }

        for (int j = 0; j < num_conditions; j++) {

            /* Set up field, operator, and condition for each WHERE clause */
            if (conditions[j].contains("!=")) {
                pos = conditions[j].indexOf("!=");
                len = 2;
                operator[j] = conditions[j].substring(pos, pos + len);
            } else if (conditions[j].contains("<=")) {
                pos = conditions[j].indexOf("<=");
                len = 2;
                operator[j] = conditions[j].substring(pos, pos + len);
            } else if (conditions[j].contains(">=")) {
                pos = conditions[j].indexOf(">=");
                len = 2;
                operator[j] = conditions[j].substring(pos, pos + len);
            } else if (conditions[j].contains("=")) {
                pos = conditions[j].indexOf("=");
                len = 1;
                operator[j] = "==";
            } else if (conditions[j].contains("<")) {
                pos = conditions[j].indexOf("<");
                len = 1;
                operator[j] = conditions[j].substring(pos, pos + len);
            } else if (conditions[j].contains(">")) {
                pos = conditions[j].indexOf(">");
                len = 1;
                operator[j] = conditions[j].substring(pos, pos + len);
            }

            if (0 < num_conditions) {
                field[j] = conditions[j].substring(0, pos).trim();
                condition[j] = conditions[j].substring(pos + len).trim();
            }
        }

        for (int m = 0; m < num_conditions; m++) {
            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {
                if (field[m].equals(get_schema_value(table_name, "FIELD" + n + " NAME: "))) {
                    field_num[m] = n;
                }
            }
        }

        String[] update_field = new String[num_fields];
        int[] update_field_num = new int[num_fields];
        String[] update_value = new String[num_fields];

        for (int j = 0; j < num_fields; j++) {
            pos = fields[j].indexOf("=");
            update_field[j] = fields[j].substring(0, pos).trim();
            update_value[j] = (fields[j].substring(pos + 1)).trim();
            update_operator[j] = "=";

            /* Check if update value contains an operator */
            if (update_value[j].contains("+")) {
                update_operator[j] = "+";
                pos = update_value[j].indexOf("+");
                update_operator[j] = update_value[j].substring(pos, pos + 1);
                update_value_field[j] = update_value[j].substring(0, pos).trim();
                update_value_value[j] = update_value[j].substring(pos + 1).trim();
            } else if (update_value[j].contains("-")) {
                update_operator[j] = "-";
                pos = update_value[j].indexOf("-");
                update_operator[j] = update_value[j].substring(pos, pos + 1);
                update_value_field[j] = update_value[j].substring(0, pos).trim();
                update_value_value[j] = update_value[j].substring(pos + 1).trim();
            } else if (update_value[j].contains("*")) {
                update_operator[j] = "*";
                pos = update_value[j].indexOf("*");
                update_operator[j] = update_value[j].substring(pos, pos + 1);
                update_value_field[j] = update_value[j].substring(0, pos).trim();
                update_value_value[j] = update_value[j].substring(pos + 1).trim();
            } else if (update_value[j].contains("/")) {
                update_operator[j] = "/";
                pos = update_value[j].indexOf("/");
                update_operator[j] = update_value[j].substring(pos, pos + 1);
                update_value_field[j] = update_value[j].substring(0, pos).trim();
                update_value_value[j] = update_value[j].substring(pos + 1).trim();
            }

            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {
                if (update_field[j].equals(get_schema_value(table_name, "FIELD" + n + " NAME: "))) {
                    update_field_num[j] = n;
                }
            }
        }

        String val_type;
        boolean where;
        boolean update_col;
        boolean implicit_update;

        for (int k = 0; k < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); k++) {
            val_type = ion_switch_value_size(get_schema_value(table_name, "FIELD" + k + " TYPE: "));
            update_col = false;
            where = false;
            implicit_update = true;

            /* Loop through WHERE conditions */
            for (int m = 0; m < num_conditions; m++) {
                if (k == field_num[m]) {
                    where = true;
                    if (val_type.contains("char")) {
                        out.write("\t\t\tif (0 " + operator[m] + " strncmp((char *) old_data, \"" + condition[m] + "\", " + val_type + ")) {\n");
                    }
                        /* Update to include all data types */
                    else {
                        out.write("\t\t\tif (NEUTRALIZE(old_data, int) " + operator[m] + " " + condition[m] + ") {\n");
                    }
                }
            }

            /* Loop through fields to UPDATE */
            for (int n = 0; n < num_fields; n++) {
                /* Field being examined is a field to update */
                if (k == update_field_num[n]) {
                    update_col = true;

                    if (update_operator[n].equals("=")) {
                        implicit_update = false;
                    }

                    if (val_type.contains("char")) {
                        if (where) {
                            if (k == primary_key_field) {
                                out.write("\t\t\t\tstrcpy(new_keys[count], " + update_value[n] + ", " + val_type + ");\n");
                            }
                            out.write("\t\t\t\tmemcpy(record_data, " + update_value[n] + ", " + val_type + ");\n");
                            out.write("\t\t\t\trecord_data += " + val_type + ";\n");
                            out.write("\t\t\t\told_data += " + val_type + ";\n\t\t\t}\n");
                            out.write("\t\t\telse {\n");
                            out.write("\t\t\t\told_keys[count] = -999;\n");
                            out.write("\t\t\t\tnew_keys[count] = -999;\n\t\t\t\tbreak;\n\t\t\t}\n");
                        } else {
                            if (k == primary_key_field) {
                                out.write("\t\t\t\tstrcpy(new_keys[count], " + update_value[n] + ", " + val_type + ");\n");
                            }
                            out.write("\t\t\tmemcpy(record_data, " + update_value_value[n] + ", " + val_type + ");\n");
                            out.write("\t\t\trecord_data += " + val_type + ";\n");
                            out.write("\t\t\told_data += " + val_type + ";\n");
                        }
                    }
                    /* Update to include all data types */
                    else {
                        if (where && implicit_update) {
                            if (k == primary_key_field) {
                                out.write("\t\t\t\tnew_keys[count] = (NEUTRALIZE(old_data, int) " + update_operator[n] + " " + update_value_value[n] + ");\n");
                            }
                            out.write("\t\t\t\t*(int *) record_data = (NEUTRALIZE(old_data, int) " + update_operator[n] + " " + update_value_value[n] + ");\n");
                            out.write("\t\t\t\trecord_data += " + val_type + ";\n");
                            out.write("\t\t\t\told_data += " + val_type + ";\n\t\t\t}\n");
                            out.write("\t\t\telse {\n");
                            out.write("\t\t\t\told_keys[count] = -999;\n");
                            out.write("\t\t\t\tnew_keys[count] = -999;\n\t\t\t\tbreak;\n\t\t\t}\n\n");
                        }
                        else if (where && !implicit_update) {
                            if (k == primary_key_field) {
                                out.write("\t\t\t\tnew_keys[count] = " + update_value[n] + ";\n");
                            }
                            out.write("\t\t\t\t*(int *) record_data = " + update_value[n] + ";\n");
                            out.write("\t\t\t\trecord_data += " + val_type + ";\n");
                            out.write("\t\t\t\told_data += " + val_type + ";\n\t\t\t}\n");
                            out.write("\t\t\telse {\n");
                            out.write("\t\t\t\told_keys[count] = -999;\n");
                            out.write("\t\t\t\tnew_keys[count] = -999;\n\t\t\t\tbreak;\n\t\t\t}\n\n");
                        }
                        else if (!where && implicit_update) {
                            if (k == primary_key_field) {
                                out.write("\t\t\tnew_keys[count] = (NEUTRALIZE(old_data, int) " + update_operator[n] + " " + update_value_value[n] + ");\n");
                            }
                            out.write("\t\t\t*(int *) record_data = (NEUTRALIZE(old_data, int) " + update_operator[n] + " " + update_value_value[n] + ");\n");
                            out.write("\t\t\trecord_data += " + val_type + ";\n");
                            out.write("\t\t\told_data += " + val_type + ";\n\n");
                        }
                        else {
                            if (k == primary_key_field) {
                                out.write("\t\t\tnew_keys[count] = " + update_value[n] + ";\n");
                            }
                            out.write("\t\t\t*(int *) record_data = " + update_value[n] + ";\n");
                            out.write("\t\t\trecord_data += " + val_type + ";\n");
                            out.write("\t\t\told_data += " + val_type + ";\n\n");
                        }
                    }
                }
            }

            if (!update_col && where) {
                if (val_type.contains("char")) {
                    out.write("\t\t\t\tmemcpy(record_data, old_data, "+val_type+");\n");
                    out.write("\t\t\t\trecord_data += "+val_type+";\n");
                    out.write("\t\t\t\told_data += "+val_type+";\n\t\t\t}\n");
                    out.write("\t\t\telse {\n");
                    out.write("\t\t\t\told_keys[count] = -999;\n");
                    out.write("\t\t\t\tnew_keys[count] = -999;\n\t\t\t\tbreak;\n\t\t\t}\n\n");
                }

                /* Update later for all data types */
                else {
                    out.write("\t\t\t\t*(int *) record_data = (NEUTRALIZE(old_data, int));\n");
                    out.write("\t\t\t\trecord_data += "+val_type+";\n");
                    out.write("\t\t\t\told_data += "+val_type+";\n\t\t\t}\n");
                    out.write("\t\t\telse {\n");
                    out.write("\t\t\t\told_keys[count] = -999;\n");
                    out.write("\t\t\t\tnew_keys[count] = -999;\n\t\t\t\tbreak;\n\t\t\t}\n\n");
                }
            }

            if (!where && !update_col) {
                if (val_type.contains("char")) {
                    out.write("\t\t\tmemcpy(record_data, old_data, "+val_type+");\n");
                    out.write("\t\t\trecord_data += "+val_type+";\n");
                    out.write("\t\t\told_data += "+val_type+";\n\n");
                }

                /* Update later for all data types */
                else {
                    out.write("\t\t\t*(int *) record_data = (NEUTRALIZE(old_data, int));\n");
                    out.write("\t\t\trecord_data += "+val_type+";\n");
                    out.write("\t\t\told_data += "+val_type+";\n\n");
                }
            }

            /* End loop for last column value */
            if (k == Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")) - 1) {
                out.write("\t\t\tbreak;\n");
                out.write("\t\t}\n\n");
            }
        }

        out.write("\t\tcount++;\n\t}\n\n");

        out.write("\tfor (int i = 0; i < "+get_schema_value(table_name, "NUMBER OF RECORDS: ")+"; i++) {\n" +
                "\t\tif (-999 != old_keys[i]) {\n\n");
        if (update_key) {

            if (ion_switch_value_size(get_schema_value(table_name, "PRIMARY KEY TYPE: ")).contains("char")) {
                out.write("\t\t\tstatus = dictionary_delete(&dictionary, old_keys[i]);");
                print_error(out, true, 2);
                out.write("\t\t\tstatus = dictionary_insert(&dictionary, new_keys[i], new_records[i]);\n");
            }

            /* Update for all data types later */
            else {
                out.write("\t\t\tstatus = dictionary_delete(&dictionary, IONIZE(old_keys[i], int));");
                print_error(out, true, 2);
                out.write("\t\t\tstatus = dictionary_insert(&dictionary, IONIZE(new_keys[i], int), new_records[i]);");
            }
        }

        else {
            if (ion_switch_value_size(get_schema_value(table_name, "PRIMARY KEY TYPE: ")).contains("char")) {
                out.write("\t\t\tstatus = dictionary_update(&dictionary, old_keys[i], new_records[i]);");
            }

            else {
                out.write("\t\t\tstatus = dictionary_update(&dictionary, IONIZE(old_keys[i], int), new_records[i]);");
            }
        }

        print_error(out, true, 2);

        out.write("\t\t}\n\t\tfree(new_records[i]);\n\t}\n\n");

        out.write("\tcursor->destroy(&cursor);\n");
        out.write("\tprint_table_"+table_name.substring(0, table_name.length() - 4).toLowerCase()+"(&dictionary);\n");

        out.write("\terror = ion_close_dictionary(&dictionary);");
        print_error(out, false, 0);

        out.write("\tfree(ion_record.key);\n\tfree(ion_record.value);\n");

        out.write("}\n\n");

        file_setup(header_written, "update"+update_count, "UPDATE");
        header_written = true;
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
            table_id = insert_count - 1;
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
            print_error(out, false, 0);

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
        String field = "";

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
                operators.add("greater than");
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

        /* Write function to file */

        out.write("void drop_table"+drop_count+"() {\n\n");
        out.write("\tprintf(\"%s"+"\\"+"n"+"\\"+"n"+"\", \""+sql.substring(sql.indexOf("(") + 2, sql.length() - 4)+"\");\n");

        sql = sql.substring(24);

        String table_name = (sql.substring(0, sql.indexOf(";")))+".inq";
        System.out.println(table_name);

        out.write("\tion_err_t error;\n\n");
        out.write("\terror = iinq_drop(\""+table_name+"\");");
        print_error(out, false, 0);

        File file = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/"+
                table_name.substring(0, table_name.length() - 4).toLowerCase()+".xml");

        if (!file.delete()) {
            out.write("\tprintf(\"Error occurred deleting table."+"\\"+"n"+"\");");
        }

        out.write("\tprintf(\"Table "+table_name.substring(0, table_name.length() - 4)+
                " has been deleted."+"\\"+"n"+"\");");

        out.write("\n}\n\n");

        file_setup(header_written,"drop_table"+drop_count, "DROP TABLE");
        header_written = true;
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
