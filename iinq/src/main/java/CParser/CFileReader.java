package CParser;

import iinq.IinqBuilder;
import iinq.IinqQuery;
import unity.annotation.GlobalSchema;
import unity.parser.GlobalParser;
import unity.query.GlobalQuery;
import unity.query.Optimizer;

import java.io.*;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * Reads a C source file to find SQL queries. Looks for the function call SQLExecute(query)
 */
public class CFileReader {
    private String inSourceFile;
    private String outSourceFile;
    private String outHeaderFile;
    private String outputString;
    private LinkedList<CFunction> funcList = new LinkedList<>();

    public CFileReader(String inSourceFile, String outSourceFile, String outHeaderFile) {
        this.inSourceFile = inSourceFile;
        this.outSourceFile = outSourceFile;
        this.outHeaderFile = outHeaderFile;
    }

    public void readFile() throws IOException, SQLException {
        BufferedReader in = null;
        StringBuilder output = new StringBuilder();
        String line;
        int funcCount = 0;

        in = new BufferedReader(new FileReader(inSourceFile));

        while (in.ready()) {
            line = in.readLine();

            if (line != null && line.contains("executeSQL(")) {
                /* check that the line is not the start of a comment */
                String firstTwoChar = line.trim().substring(0, 2);
                /* line comment */
                if (firstTwoChar.equals("//")) {
                    continue;
                }
                /* block comment */
                else if (firstTwoChar.equals("/*")) {
                    boolean isComment = true;
                    while (isComment) {
                        /* skip all lines until the comment is closed */
                        if (line.contains("*/")) {
                            isComment = false;
                        }
                        line = in.readLine();
                    }
                    continue;
                }
                String SQL = parseSQL(line);
                if (SQL != null) {
                    GlobalParser kingParser = new GlobalParser(false, false);
                    GlobalQuery gq = kingParser.parse(SQL, new GlobalSchema());
                    gq.setQueryString(SQL);
                    Optimizer opt = new Optimizer(gq, false, null);
                    gq = opt.optimize();
                    IinqBuilder builder = new IinqBuilder(gq.getLogicalQueryTree().getRoot());
                    IinqQuery query = builder.toQuery();
                    String body = query.generateCode();
                    CFunction func = new CFunction(++funcCount, body);
                    output.append("\t" + func.getName() + "();\n");
                    this.funcList.add(func);
                }

            } else {
                output.append(line + "\n");
            }
        }
        in.close();
        for (CFunction s : this.funcList) {
            output.append(s.getName());
            output.append("() {\n");
            output.append(s.getBody());
            output.append("\n}\n");
        }
        outputString = output.toString();

    }

    public void writeFile() throws IOException {
        PrintWriter out = new PrintWriter(new FileOutputStream(outSourceFile));
        out.print(outputString.toString());
        out.close();

        out = new PrintWriter(new FileOutputStream(outHeaderFile));
        for (CFunction s : this.funcList) {
            out.print("void\n" + s.getName() + "();\n\n");
        }
        out.close();
    }


    /**
     * Reads a line of C code and extracts the SQL query.
     *
     * @param line Line of C code to read.
     * @return SQL query as a String. Returns null if the function call was commented out.
     */
    public static String parseSQL(String line) {
        /* get the SQL query from the functin call */
        String statement = line.substring(line.indexOf("executeSQL(\"") + 12, line.lastIndexOf(";\")") + 1);
        return statement;
    }

    public String getInSourceFile() {
        return inSourceFile;
    }

    public void setInSourceFile(String inSourceFile) {
        this.inSourceFile = inSourceFile;
    }

    public String getOutSourceFile() {
        return outSourceFile;
    }

    public void setOutSourceFile(String outSourceFile) {
        this.outSourceFile = outSourceFile;
    }

    public String getOutHeaderFile() {
        return outHeaderFile;
    }

    public void setOutHeaderFile(String outHeaderFile) {
        this.outHeaderFile = outHeaderFile;
    }

    public String getOutputString() {
        return outputString;
    }

    public void setOutputString(String outputString) {
        this.outputString = outputString;
    }

    public LinkedList<CFunction> getFuncList() {
        return funcList;
    }

}
