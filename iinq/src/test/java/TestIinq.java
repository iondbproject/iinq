import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.ResultSet;

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
	public static void runSQLQuery(String sql, String answer) 
	{
		System.out.println("\nTesting query: \n" + sql);		
		try 
		{
			sql = StringFunc.verifyTerminator(sql);	// Make sure SQL is terminated by semi-colon properly
	    	
	    	// Parse semantic query string into a parse tree
	        GlobalParser kingParser = new GlobalParser(false, false);		// TODO: 2nd parameter true for schema validation?
	        GlobalQuery gq = kingParser.parse(sql, new GlobalSchema());		// TODO: Will need a schema	        
	        gq.setQueryString(sql);
	        
	        // Optimize logical query tree before execution
	        Optimizer opt = new Optimizer(gq, false, null);
	        gq = opt.optimize();
	        
	        IinqBuilder builder = new IinqBuilder(gq.getLogicalQueryTree().getRoot());
			IinqQuery query = builder.toQuery();
			
			// Validate that code is generated as expected
			String code = query.generateCode();
			System.out.println(code);
			assertEquals(answer, code);
		} 
		catch (Exception e) 
		{
			System.out.println(e);
			e.printStackTrace();
			// assertEquals(exception, e.toString());
		}		
	}			
}
