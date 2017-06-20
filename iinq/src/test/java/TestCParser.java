import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import java.sql.SQLException;

import CParser.CFileReader;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test method for reading C and writing C source files.
 */
public class TestCParser {
    public static String inputSourceFile = "src/test/CTestFiles/main.c";
    public static String outputSourceFile = "src/test/CTestFiles/output.c";
    public static String outputHeaderFile = "src/test/CTestFiles/output.h";

    /**
     * Test reading/parsing the input source file and writing the output header and source file
     */
    @Test
    public void testReadWrite()
    {
        String answer = "";

        CFileReader test = new CFileReader(inputSourceFile, outputSourceFile, outputHeaderFile);
        Assert.assertSame(inputSourceFile, test.getInSourceFile());
        Assert.assertSame(outputHeaderFile, test.getOutHeaderFile());
        Assert.assertSame(outputSourceFile, test.getOutSourceFile());

        try {
            /* Try reading */
            test.readFile();
            Assert.assertFalse(test.getFuncList() == null);
            Assert.assertTrue(test.getFuncList().size() == 1);
            Assert.assertFalse(test.getOutputString() == null);

            /* Try writing */
            test.writeFile();
            Assert.assertTrue(new File(outputSourceFile).exists());
            Assert.assertTrue(new File(outputHeaderFile).exists());


        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
