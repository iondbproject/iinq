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

    private static boolean header_written = false;
    private static boolean first_function = true;
    private static boolean print_written = false;

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

            print_header();
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
                    delete_count++;
                }

                /* DROP TABLE statements exists in code that is not a comment */
                else if ((sql.toUpperCase()).contains("DROP TABLE") && !sql.contains("/*")) {
                    drop_table(sql, buff_out);
                    drop_count++;
                }
            }

            buff_in.close();
            buff_out.close();
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
    print_header() throws IOException {

        String contents = "";

        contents += "/******************************************************************************/\n" +
                "/**\n" +
                "@file\t\tiinq_user.h\n" +
                "@author\t\tDana Klamut\n" +
                "@brief\t\tThis code contains definitions for iinq user functions\n" +
                "@copyright\tCopyright 2017\n" +
                "\t\t\tThe University of British Columbia,\n" +
                "\t\t\tIonDB Project Contributors (see AUTHORS.md)\n" +
                "@par Redistribution and use in source and binary forms, with or without\n" +
                "\tmodification, are permitted provided that the following conditions are met:\n" +
                "\n" +
                "@par 1.Redistributions of source code must retain the above copyright notice,\n" +
                "\tthis list of conditions and the following disclaimer.\n" +
                "\n" +
                "@par 2.Redistributions in binary form must reproduce the above copyright notice,\n" +
                "\tthis list of conditions and the following disclaimer in the documentation\n" +
                "\tand/or other materials provided with the distribution.\n" +
                "\n" +
                "@par 3.Neither the name of the copyright holder nor the names of its contributors\n" +
                "\tmay be used to endorse or promote products derived from this software without\n" +
                "\tspecific prior written permission.\n" +
                "\n" +
                "@par THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS \"AS IS\"\n" +
                "\tAND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE\n" +
                "\tIMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE\n" +
                "\tARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE\n" +
                "\tLIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR\n" +
                "\tCONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF\n" +
                "\tSUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS\n" +
                "\tINTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN\n" +
                "\tCONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)\n" +
                "\tARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE\n" +
                "\tPOSSIBILITY OF SUCH DAMAGE.\n" +
                "*/\n" +
                "/******************************************************************************/\n" +
                "\n" +
                "#if !defined(IINQ_USER_H_)\n" +
                "#define IINQ_USER_H_\n" +
                "\n" +
                "#if defined(__cplusplus)\n" +
                "extern \"C\" {\n" +
                "#endif\n" +
                "\n" +
                "#include \"../../key_value/kv_system.h\"\n" +
                "#include \"iinq_user_functions.h\"\n" +
                "#include \"iinq_execute.h\"\n" +
                "\n" +
                "#if defined(__cplusplus)\n" +
                "}\n" +
                "#endif\n" +
                "\n" +
                "#endif\n";

        File file = new File("/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/iinq_user.h");
        FileOutputStream out = new FileOutputStream(file, false);

        out.write(contents.getBytes());

        out.close();
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
        contents += "#if !defined(IINQ_USER_FUNCTIONS_H_)\n" + "#define IINQ_USER_FUNCTIONS_H_\n\n";
        contents += "#if defined(__cplusplus)\n" + "extern \"C\" {\n" + "#endif\n\n";

        /* Include other headers*/
        contents += "#include \"../../dictionary/dictionary_types.h\"\n" +
                "#include \"../../dictionary/dictionary.h\"\n" +
                "#include \"../iinq.h\"\n" + "#include \"iinq_execute.h\"\n\n";


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

        out.write("\tion_cursor_status_t cursor_status;\n\n");

        out.write("\twhile ((cursor_status = cursor->next(cursor, &ion_record)) == cs_cursor_active || " +
                "cursor_status == cs_cursor_initialized) {\n");
        out.write("\t\tprintf(\"%s"+"\\"+"n"+"\", (char *) ion_record.value); \n\t}\n");
        out.write("\n\tprintf(\""+"\\"+"n"+"\");\n\n");

        out.write("\tcursor->destroy(&cursor);\n");
        out.write("\tfree(ion_record.key);\n");
        out.write("\tfree(ion_record.value);\n}\n\n");
    }

    private static void
    file_setup (boolean header_written, boolean first_function, String function, String keyword) throws IOException {
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
                    contents += "\nvoid " + function + "();\n";
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

            contents += "void " + function + "();\n\n";
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
                contents += "\t" + function +"();\n";
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
                table_name.substring(0, table_name.length() - 4).toLowerCase()+".sch";
        BufferedReader file = new BufferedReader(new FileReader(path));

        while (null != (line = file.readLine())) {
            if (line.contains(keyword)) {
                line = line.substring(keyword.length());
                break;
            }
        }

        file.close();

        System.out.println(line);
        return line;
    }

    private static void
    increment_num_records (String table_name, boolean increment) throws IOException {
        String path = "/Users/danaklamut/ClionProjects/iondb/src/iinq/iinq_interface/"+
                table_name.substring(0, table_name.length() - 4).toLowerCase()+".sch";
        BufferedReader file = new BufferedReader(new FileReader(path));

        String contents = "";
        String line;
        int num;

        while (null != (line = file.readLine())) {
            if (line.contains("NUMBER OF RECORDS: ")) {
                num = Integer.parseInt(line.substring(19));

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
                table_name.substring(0, table_name.length() - 4).toLowerCase()+".sch");
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
            field_types[j] = key_type;
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
		        primary_key_type = field_types[j];
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
            value_size = value_size.concat(ion_switch_key_size(field_types[j]));
        }

        String schema_name = table_name.substring(0, table_name.length() - 4).toLowerCase().concat(".sch");

        /* Set up schema header in file */
        String contents = "";

        contents += "TABLE NAME: "+table_name;
        contents += "\nPRIMARY KEY TYPE: "+primary_key_type;
        contents += "\nPRIMARY KEY SIZE: "+primary_key_size;
        contents += "\nVALUE SIZE: "+value_size;
        contents += "\nNUMBER OF FIELDS: "+(num_fields - 1);
        contents += "\nNUMBER OF RECORDS: 0";
        contents += "\nPRIMARY KEY FIELD: "+primary_key_field_num;

        for (int j = 0; j < num_fields - 1; j++) {
            contents += "\nFIELD"+j+" NAME: "+field_names[j];
            contents += "\nFIELD"+j+" TYPE: "+field_types[j];
        }

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

        file_setup(header_written, first_function, "create_table"+create_count, "CREATE TABLE");
        first_function = false;
        header_written = true;
    }

    private static void
    insert(String sql, BufferedWriter out) throws IOException {
        System.out.println("insert statement");

        String statement = sql;

        sql = sql.trim();
        sql = sql.substring(25);

        String table_name = (sql.substring(0, sql.indexOf(" ")))+".inq";
        System.out.println(table_name);

        /* Create print table method if it doesn't already exist */
        if (!print_written) {
            print_table(out, table_name);
        }

        print_written = true;

        /* Write function to file */

        out.write("void insert"+insert_count+"() {\n\n");
        out.write("\tprintf(\"%s"+"\\"+"n"+"\\"+"n"+"\", \""+statement.substring(statement.indexOf("(") + 2, statement.length() - 4)+"\");\n");
        out.write("\tion_err_t error;\n" + "\tion_dictionary_t dictionary;\n" + "\tion_dictionary_handler_t handler;\n");
        out.write("\tdictionary.handler = &handler;\n" + "\n\terror = iinq_open_source(\""+table_name+"\", &dictionary, &handler);");
        print_error(out, false, 0);

        int pos = sql.indexOf("(");

        sql = sql.substring(pos + 1);

        String value = sql.substring(0, sql.length() - 5);
        System.out.println(value+"\n");

	    /* Get key value from record to be inserted */
        int count = 1;

        /* Count number of fields */
	    for (int i = 0; i < sql.length(); i ++) {
            if(sql.charAt(i) == ',') {
                count++;
            }
        }

        String[] fields = new String[count];
        value = "";

        for (int j = 0; j < count; j++) {
            pos = sql.indexOf(",");

            if (-1 == pos) {
                pos = sql.indexOf(")");
            }

            if (-1 == pos) {
                out.write("printf(\"Error occurred inserting values, please check that a value has been listed for each column in table.\");\n");
                return;
            }

            fields[j] = sql.substring(0, pos);

            sql = sql.substring(pos + 2);
            value += fields[j]+",\t";
        }

        value = value.substring(0, value.length() - 2);

        String key_type = get_schema_value(table_name, "PRIMARY KEY TYPE: ");
        String key_field_num = get_schema_value(table_name, "PRIMARY KEY FIELD: ");

        out.write("\tion_status_t status;\n");

        if (key_type.equals("0") || key_type.equals("1")) {
            out.write("\tstatus = dictionary_insert(&dictionary, IONIZE("+fields[Integer.parseInt(key_field_num)]+
                    ", int), \""+value+"\");");
        }

        else {
            out.write("\tstatus = dictionary_insert(&dictionary, \""+fields[Integer.parseInt(key_field_num)]+
                    "\", \""+value+"\");");
        }

        print_error(out, true, 0);

        out.write("\tprintf(\"Record inserted: "+value+"\\"+"n"+"\\"+"n"+"\");");

        increment_num_records(table_name, true);

        out.write("\tprint_table_"+table_name.substring(0, table_name.length() - 4).toLowerCase()+"(&dictionary);\n");
        out.write("\terror = ion_close_dictionary(&dictionary);");
        print_error(out, false, 0);

        out.write("}\n\n");

        file_setup(header_written, first_function,  "insert"+insert_count, "INSERT INTO");
        first_function = false;
        header_written = true;
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
        int i = 0;

        /* Get WHERE condition if it exists */
        if (-1 != pos) {
            where_condition = sql.substring(pos + 6, sql.length() - 4);
        }

        System.out.println(where_condition+"\n");

        /* Calculate number of WHERE conditions in statement */
        while (-1 != i) {
            num_conditions++;
            i = where_condition.indexOf(",", i + 1);
        }

        String[] conditions = new String[num_conditions];

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

        String[] fields = new String[num_fields];

        fields = get_fields(update, num_fields);

        out.write("\tion_predicate_t predicate;\n");
        out.write("\tion_cursor_status_t cursor_status;\n");
        out.write("\tion_status_t status;\n");
        out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n");
        out.write("\tion_dict_cursor_t *cursor = NULL;\n");
        out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n");
        out.write("\n\tion_record_t ion_record;\n");
        out.write("\tion_record.key = malloc("+get_schema_value(table_name, "PRIMARY KEY SIZE: ")+");\n");
        out.write("\tion_record.value	= malloc("+get_schema_value(table_name, "VALUE SIZE: ")+");\n\n");

        out.write("\tint num, pos;\n");
        out.write("\tion_key_t key;\n");
        out.write("\tion_boolean_t condition_satisfied = boolean_true;\n");
        out.write("\tchar *old_record, *record, *substring, *pointer, *value, *print_record;\n");
        out.write("\n\twhile ((cursor_status = cursor->next(cursor, &ion_record)) == cs_cursor_active || cursor_status == cs_cursor_initialized) {\n");

        out.write("\t\tsubstring = malloc(strlen(ion_record.value));\n");

        String field = "";
        String operator = "";
        String condition = "";
        String update_value = "";
        int field_num = 0;
        int len = 0;
        String field_type;

        for (int j = 0; j < num_conditions; j++) {

            /* Set up field, operator, and condition for each WHERE clause */
            if (conditions[j].contains("!=")) {
                pos = conditions[j].indexOf("!=");
                len = 2;
                operator = conditions[j].substring(pos, pos + len);
            }
            else if (conditions[j].contains("<=")) {
                pos = conditions[j].indexOf("<=");
                len = 2;
                operator = conditions[j].substring(pos, pos + len);
            }
            else if (conditions[j].contains(">=")) {
                pos = conditions[j].indexOf(">=");
                len = 2;
                operator = conditions[j].substring(pos, pos + len);
            }
            else if (conditions[j].contains("=")) {
                pos = conditions[j].indexOf("=");
                len = 1;
                operator = "==";
            }
            else if (conditions[j].contains("<")) {
                pos = conditions[j].indexOf("<");
                len = 1;
                operator = conditions[j].substring(pos, pos + len);
            }
            else if (conditions[j].contains(">")) {
                pos = conditions[j].indexOf(">");
                len = 1;
                operator = conditions[j].substring(pos, pos + len);
            }

            field = conditions[j].substring(0, pos).trim();
            condition = conditions[j].substring(pos + len).trim();

            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {
                if (field.equals(get_schema_value(table_name, "FIELD"+n+" NAME: "))) {
                    field_num = n;
                }
            }

            out.write("\t\tif (boolean_true == condition_satisfied) {\n");
            out.write("\t\t\t\tstrcpy(substring, ion_record.value);\n\n");
            out.write("\t\t\tfor (int i = 0; i <= "+field_num+"; i++) {\n");
            out.write("\t\t\t\tpointer = strstr(substring, \",\");\n\n");
            out.write("\t\t\t\tif (NULL == pointer) {\n" + "\t\t\t\t\tchar col_val[strlen(substring)];\n\n");
            out.write("\t\t\t\t\tmemcpy(col_val, substring, strlen(substring) + 1);\n" +
                    "\t\t\t\t\tcol_val[strlen(substring)]\t= '\\0';\n\n");
            out.write("\t\t\t\t\tvalue = malloc(strlen(col_val));\n" + "\t\t\t\t\tstrcpy(value, col_val);\n" + "\t\t\t\t}\n");
            out.write("\t\t\t\telse {\n" + "\t\t\t\t\tpos = (int) (pointer - substring);\n" + "\n" +
                    "\t\t\t\t\tchar col_val[pos + 1];\n");
            out.write("\n" + "\t\t\t\t\tmemcpy(col_val, substring, pos);\n" + "\t\t\t\t\tcol_val[pos]\t= '\\0';\n" + "\n");
            out.write("\t\t\t\t\tsubstring = pointer + 2;\n" + "\n" + "\t\t\t\t\tvalue = malloc(strlen(col_val));\n");
            out.write("\t\t\t\t\tstrcpy(value, col_val);\n" + "\t\t\t\t}\n" + "\t\t\t}\n");

            field_type = get_schema_value(table_name, "FIELD"+field_num+" TYPE: ");

            if (field_type.equals("0") || field_type.equals("1")) {
                out.write("\n\t\t\tnum = atoi(value);\n\n");
                out.write("\t\t\tif (num "+operator+" "+condition+") {\n"+"\t\t\t\tcondition_satisfied = boolean_true;\n\t\t\t}\n");
                out.write("\t\t\telse {\n\t\t\t\tcondition_satisfied = boolean_false;\n\t\t\t}\n\t\t}\n\n");
            }

            else {
                out.write("\n\t\t\tif (0 "+operator+" strncmp(value, \""+condition+"\", strlen(value))) {\n");
                out.write("\t\t\t\tcondition_satisfied = boolean_true;\n\t\t\t}\n");
                out.write("\t\t\telse {\n\t\t\t\tcondition_satisfied = boolean_false;\n\t\t\t}\n\t\t}\n\n");
            }
        }

        /* If boolean_true, record has passed all conditions */
        out.write("\t\tif (boolean_true == condition_satisfied) {\n\n");
        out.write("\t\t\tchar	old_value[strlen(ion_record.value) + 1];\n");
        out.write("\t\t\tchar	*field_value;\n\n");
        out.write("\t\t\tchar new_record["+get_schema_value(table_name, "VALUE SIZE: ")+"] = \"\";\n\n");
        out.write("\t\t\tmemcpy(old_value, ion_record.value, strlen(ion_record.value));\n");
        out.write("\t\t\told_value[strlen(ion_record.value)] = '\\0';\n\n");
        out.write("\t\t\trecord = malloc(strlen(old_value));\n\t\t\tstrcpy(record, old_value);\n\n");
        out.write("\t\t\told_record = malloc(strlen(record));\n\t\t\tstrcpy(old_record, record);\n\n");

        out.write("\t\t\tfor (int i = 0; i < "+get_schema_value(table_name, "NUMBER OF FIELDS: ")+"; i++) {\n\n");
        out.write("\t\t\t\tpointer = strstr(record, \",\");\n\n");

        for (int j = 0; j < num_fields; j++) {
            pos = fields[j].indexOf("=");
            field = fields[j].substring(0, pos).trim();
            update_value = fields[j].substring(pos + 1).trim();

            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {
                if (field.equals(get_schema_value(table_name, "FIELD"+n+" NAME: "))) {
                    field_num = n;
                }
            }

            if (0 == j) {
                out.write("\t\t\t\tif (" + field_num + " == i) {\n");
            }

            else {
                out.write("\t\t\t\telse if (" + field_num + " == i) {\n");
            }
            out.write("\t\t\t\t\tstrcat(new_record, \"" + update_value + "\");\n\n");
            out.write("\t\t\t\t\tif (NULL != pointer) {\n" + "\t\t\t\t\t\trecord = pointer + 2;\n" +
                    "\t\t\t\t\t\tstrcat(new_record, \", \");\n"+ "\t\t\t\t\t}\n" + "\t\t\t\t}\n");
        }

        out.write("\t\t\t\telse if (NULL == pointer) {\n\t\t\t\t\tchar col_val[strlen(record)];\n\n");
        out.write("\t\t\t\t\tmemcpy(col_val, record, strlen(record) + 1);\n" +
                "\t\t\t\t\tcol_val[strlen(record)] = '\\0';\n\n");
        out.write("\t\t\t\t\tstrcat(new_record, col_val);\n" +
                "\t\t\t\t}\n\t\t\t\telse {\n");
        out.write("\t\t\t\t\tpos = (int) (pointer - record);\n\n" +
                "\t\t\t\t\tchar col_val[pos + 1];\n\n\t\t\t\t\tmemcpy(col_val, record, pos);\n");
        out.write("\t\t\t\t\tcol_val[pos]	= '\\0';\n\n" +
                "\t\t\t\t\trecord = pointer + 2;\n\t\t\t\t\tfield_value = malloc(strlen(col_val));\n");
        out.write("\t\t\t\t\tstrcat(new_record, col_val);\n\t\t\t\t\tstrcat(new_record, \", \");\n\t\t\t\t}\n\t\t\t}\n\n");

        boolean update_key = false;

        if (field_num == Integer.parseInt(get_schema_value(table_name, "PRIMARY KEY FIELD: "))) {
            field_type = get_schema_value(table_name, "FIELD"+field_num+" TYPE: ");

            if (field_type.equals("0") || field_type.equals("1")) {

                out.write("\t\t\tstatus = dictionary_delete(&dictionary, ion_record.key);");
                print_error(out, true, 3);

                update_key = true;

                out.write("\t\t\tint num = "+update_value+";\n");
                out.write("\t\t\tkey = IONIZE(num, int);\n");
            }

            else {
                out.write("\t\t\tstatus = dictionary_delete(&dictionary, ion_record.key);");
                print_error(out, true, 3);

                update_key = true;

                out.write("\t\t\tkey = malloc(strlen(\""+update_value+"\"));\n");
                out.write("\t\t\tstrcpy(key, \""+update_value+"\");\n");
            }
        }

        else {
            out.write("\t\t\tkey = ion_record.key;\n\n");
        }

        if (update_key) {
            out.write("\t\t\tstatus = dictionary_insert(&dictionary, key, new_record);");
        }

        else {
            out.write("\t\t\tstatus = dictionary_update(&dictionary, key, new_record);");
        }

        print_error(out, true, 2);
        out.write("\t\t\tprint_record = malloc(strlen(new_record));\n\t\t\tstrcpy(print_record, new_record);\n\n");

        out.write("\t\t\tprintf(\"Updated record: %s"+"\\"+"n"+"\\"+"n"+"\", print_record);\n\n");
        out.write("\t\t}\n\t}\n\n");

        out.write("\tcursor->destroy(&cursor);\n");
        out.write("\tfree(ion_record.key);\n\tfree(ion_record.value);\n\n");
        out.write("\tprint_table_"+table_name.substring(0, table_name.length() - 4).toLowerCase()+"(&dictionary);\n");

        out.write("\terror = ion_close_dictionary(&dictionary);");
        print_error(out, false, 0);

        out.write("}\n\n");

        file_setup(header_written, first_function,  "update"+update_count, "UPDATE");
        first_function = false;
        header_written = true;
    }

    private static void
    delete(String sql, BufferedWriter out) throws IOException {
        System.out.println("delete statement");

        String statement = sql;

        sql = sql.trim();
        sql = sql.substring(25);

        String table_name = (sql.substring(0, sql.indexOf(" ")))+".inq";
        System.out.println(table_name);

        sql = sql.substring(table_name.length() - 3);

        /* Create print table method if it doesn't already exist */
        if (!print_written) {
            print_table(out, table_name);
        }

        print_written = true;

        /* Write function to file */

        out.write("void delete"+delete_count+"() {\n\n");
        out.write("\tprintf(\"%s"+"\\"+"n"+"\\"+"n"+"\", \""+statement.substring(statement.indexOf("(") + 2, statement.length() - 4)+"\");\n");
        out.write("\tion_err_t error;\n" + "\tion_dictionary_t dictionary;\n" + "\tion_dictionary_handler_t handler;\n");
        out.write("\tdictionary.handler = &handler;\n" + "\n\terror = iinq_open_source(\""+table_name+"\", &dictionary, &handler);");
        print_error(out, false, 0);

        int pos = sql.indexOf("WHERE");
        String where_condition = "";
        int num_conditions = 0;
        int i = 0;

        /* Get WHERE condition if it exists */
        if (-1 != pos) {
            where_condition = sql.substring(pos + 6, sql.length() - 4);
        }

        System.out.println("WHERE: "+where_condition+"\n");

        /* Calculate number of WHERE conditions in statement */
        while (-1 != i) {
            if (!where_condition.equals("")) {
                num_conditions++;
            }

            i = where_condition.indexOf(",", i + 1);
        }

        String[] conditions;

        conditions = get_fields(where_condition, num_conditions);

        out.write("\tion_predicate_t predicate;\n");
        out.write("\tion_cursor_status_t cursor_status;\n");
        out.write("\tion_status_t status;\n");
        out.write("\tdictionary_build_predicate(&predicate, predicate_all_records);\n");
        out.write("\tion_dict_cursor_t *cursor = NULL;\n");
        out.write("\tdictionary_find(&dictionary, &predicate, &cursor);\n");

        out.write("\tint num, pos;\n");
        out.write("\tion_boolean_t condition_satisfied = boolean_true;\n");
        out.write("\tchar *substring, *pointer, *value;\n");
        out.write("\tint count = 0;\n" +
                "\n" +
                "\tion_record_t deleted_records["+get_schema_value(table_name, "NUMBER OF RECORDS: ")+"];\n" +
                "\n" +
                "\tfor (int i = 0; i < "+get_schema_value(table_name, "NUMBER OF RECORDS: ")+"; i++) {\n" +
                "\t\tdeleted_records[i].key\t\t= malloc("+get_schema_value(table_name, "PRIMARY KEY SIZE: ")+");\n" +
                "\t\tdeleted_records[i].value\t= malloc("+get_schema_value(table_name, "VALUE SIZE: ")+");\n" +
                "\t}\n");
        out.write("\n\twhile ((cursor_status = cursor->next(cursor, &deleted_records[count])) == cs_cursor_active || cursor_status == cs_cursor_initialized) {\n");

        out.write("\n\t\tsubstring = malloc(strlen(deleted_records[count].value));\n");
        out.write("\t\tcondition_satisfied = boolean_true;\n\n");

        String field = "";
        String operator = "";
        String condition = "";
        String update_value = "";
        int field_num = 0;
        int len = 0;
        String field_type;

        for (int j = 0; j < num_conditions; j++) {
            System.out.println(conditions[j]);

            /* Set up field, operator, and condition for each WHERE clause */
            if (conditions[j].contains("!=")) {
                pos = conditions[j].indexOf("!=");
                len = 2;
                operator = conditions[j].substring(pos, pos + len);
            }
            else if (conditions[j].contains("<=")) {
                pos = conditions[j].indexOf("<=");
                len = 2;
                operator = conditions[j].substring(pos, pos + len);
            }
            else if (conditions[j].contains(">=")) {
                pos = conditions[j].indexOf(">=");
                len = 2;
                operator = conditions[j].substring(pos, pos + len);
            }
            else if (conditions[j].contains("=")) {
                pos = conditions[j].indexOf("=");
                len = 1;
                operator = "==";
            }
            else if (conditions[j].contains("<")) {
                pos = conditions[j].indexOf("<");
                len = 1;
                operator = conditions[j].substring(pos, pos + len);
            }
            else if (conditions[j].contains(">")) {
                pos = conditions[j].indexOf(">");
                len = 1;
                operator = conditions[j].substring(pos, pos + len);
            }

            field = conditions[j].substring(0, pos).trim();
            condition = conditions[j].substring(pos + len).trim();

            for (int n = 0; n < Integer.parseInt(get_schema_value(table_name, "NUMBER OF FIELDS: ")); n++) {
                if (field.equals(get_schema_value(table_name, "FIELD"+n+" NAME: "))) {
                    field_num = n;
                }
            }

            out.write("\t\tif (boolean_true == condition_satisfied) {\n");
            out.write("\t\t\tstrcpy(substring, deleted_records[count].value);\n\n");
            out.write("\t\t\tfor (int i = 0; i <= "+field_num+"; i++) {\n");
            out.write("\t\t\t\tpointer = strstr(substring, \",\");\n\n");
            out.write("\t\t\t\tif (NULL == pointer) {\n" + "\t\t\t\t\tchar col_val[strlen(substring)];\n\n");
            out.write("\t\t\t\t\tmemcpy(col_val, substring, strlen(substring) + 1);\n" +
                    "\t\t\t\t\tcol_val[strlen(substring)]\t= '\\0';\n\n");
            out.write("\t\t\t\t\tvalue = malloc(strlen(col_val));\n" + "\t\t\t\t\tstrcpy(value, col_val);\n" + "\t\t\t\t}\n");
            out.write("\t\t\t\telse {\n" + "\t\t\t\t\tpos = (int) (pointer - substring);\n" + "\n" +
                    "\t\t\t\t\tchar col_val[pos + 1];\n");
            out.write("\n" + "\t\t\t\t\tmemcpy(col_val, substring, pos);\n" + "\t\t\t\t\tcol_val[pos]\t= '\\0';\n" + "\n");
            out.write("\t\t\t\t\tsubstring = pointer + 2;\n" + "\n" + "\t\t\t\t\tvalue = malloc(strlen(col_val));\n");
            out.write("\t\t\t\t\tstrcpy(value, col_val);\n" + "\t\t\t\t}\n" + "\t\t\t}\n");

            field_type = get_schema_value(table_name, "FIELD"+field_num+" TYPE: ");

            if (field_type.equals("0") || field_type.equals("1")) {
                out.write("\n\t\t\tnum = atoi(value);\n\n");
                out.write("\t\t\tif (num "+operator+" "+condition+") {\n"+"\t\t\t\tcondition_satisfied = boolean_true;\n\t\t\t}\n");
                out.write("\t\t\telse {\n\t\t\t\tcondition_satisfied = boolean_false;\n\t\t\t}\n\t\t}\n\n");
            }

            else {
                out.write("\n\t\t\tif (0 "+operator+" strncmp(value, \""+condition+"\", strlen(value))) {\n");
                out.write("\t\t\t\tcondition_satisfied = boolean_true;\n\t\t\t}\n");
                out.write("\t\t\telse {\n\t\t\t\tcondition_satisfied = boolean_false;\n\t\t\t}\n\t\t}\n\n");
            }
        }

        /* If boolean_true, record has passed all conditions */
        out.write("\t\tif (boolean_false == condition_satisfied) {\n");

        out.write("\t\t\tdeleted_records[count].key = NULL;\n" +
                "\t\t\tdeleted_records[count].value = NULL;\n");

        out.write("\t\t}\n\n\t\tcount++;\n\t}\n\n");

        out.write("\tfor (int i = 0; i < count; i++) {\n" +
                "\t\tif (NULL != deleted_records[i].key) {\n" +
                "\t\t\tstatus = dictionary_delete(&dictionary, deleted_records[i].key);\n" +
                "\n" +
                "\t\t\tif (err_ok != status.error) {\n" +
                "\t\t\t\tprintf(\"Error occurred deleting record from table. Error code: %i\\n\", status.error);\n" +
                "\t\t\t\treturn;\n" +
                "\t\t\t}\n" +
                "\n" +
                "\t\t\tprintf(\"Record deleted: %s"+"\\"+"n"+"\\"+"n"+"\", (char *) deleted_records[i].value);\n"+
                "\t\t\tfree(deleted_records[i].key);\n" + "\t\t\tfree(deleted_records[i].value);\n" +
                "\t\t}\n" +
                "\t}\n");

        increment_num_records(table_name, false);

        out.write("\n\tcursor->destroy(&cursor);\n");
        out.write("\tprint_table_"+table_name.substring(0, table_name.length() - 4).toLowerCase()+"(&dictionary);\n");

        out.write("\terror = ion_close_dictionary(&dictionary);");
        print_error(out, false, 0);

        out.write("}\n\n");

        file_setup(header_written, first_function,  "delete"+delete_count, "DELETE");
        first_function = false;
        header_written = true;
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

        out.write("\tfremove(\""+table_name+"\");\n");

        out.write("\tprintf(\"Table "+table_name.substring(0, table_name.length() - 4)+
                " has been deleted."+"\\"+"n"+"\");");

        out.write("\n}\n\n");

        file_setup(header_written, first_function,  "drop_table"+drop_count, "DROP TABLE");
        first_function = false;
        header_written = true;
    }
}
