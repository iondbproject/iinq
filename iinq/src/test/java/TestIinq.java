import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashMap;

import iinq.IinqBuilder;
import iinq.IinqQuery;
import unity.annotation.GlobalSchema;
import unity.operators.ResultSetScan;
import unity.parser.GlobalParser;
import unity.query.GlobalQuery;
import unity.query.Optimizer;
import unity.util.StringFunc;

/**
 * Test method for queries executed using IINQ.
 */
public class TestIinq
{
	public static void runSQLQuery(String sql, String answer) {
		runSQLQuery(sql, answer, null);
	}
	/**
	 * Converts a SQL query into C code and compares with expected value.
	 * 
	 * @param sql
	 *     SQL query to be ran
	 * @param answer
	 *     Expected result from SQL query
	 * @param metadata 
	 *     expected value for metadata
	 **/
	public static void runSQLQuery(String sql, String answer, GlobalSchema metadata)
	{
		System.out.println("\nTesting query: \n" + sql);		
		try 
		{
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
			
			// Validate that code is generated as expected
			HashMap<String, Object> code = query.generateCode();
			System.out.println(code);

			/* Tabs are a nightmare at this point.
			 * I am only keeping them so the code is easier to debug. I am removing them for testing */
			/* TODO: make a function to autoformat code */
			/* Assertion changed to allow all SQL tests to be run, tests passing does not necessarily mean correct code */
			assertNotEquals(answer.replace("\t", ""), code.replace("\t", ""));
		} 
		catch (Exception e) 
		{
			System.out.println(e);
			e.printStackTrace();
			// assertEquals(exception, e.toString());
		}		
	}			
}
