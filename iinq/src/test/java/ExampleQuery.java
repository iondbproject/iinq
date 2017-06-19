import static org.junit.Assert.fail;

import java.sql.*;

import unity.jdbc.UnityStatement;
import unity.operators.ResultSetScan;
import unity.query.GlobalQuery;
import unity.util.StringFunc;

/**
 * An example of parsing and executing a query using UnityJDBC virtualization engine.
 */
@SuppressWarnings({"nls"})
public class ExampleQuery 
{
	// URL for sources.xml file specifying what databases to integrate.  This file must be locally accessible or available over the Internet.
	private static String url="jdbc:unity://data/xspec/UnityDemo.xml";
	

	/**
	 * Main method
	 * 
	 * @param args
	 * 			no args required
	 */
	public static void main(String [] args)
	{
		Connection con = null;
		UnityStatement stmt = null;
		
		try 
		{
			// Create new instance of UnityDriver and make connection
			System.out.println("\nRegistering driver.");
			Class.forName("unity.jdbc.UnityDriver");
			
			System.out.println("\nGetting connection:  "+url);
			con = DriverManager.getConnection(url);
			System.out.println("\nConnection successful for "+ url);

			System.out.println("\nCreating statement.");
			stmt = (UnityStatement) con.createStatement();
			
			String sql = "SELECT PartDB.Part.P_NAME, OrderDB.LineItem.L_QUANTITY, OrderDB.Customer.C_Name, PartDB.Supplier.s_name " +
					 " FROM OrderDB.CUSTOMER, OrderDB.LINEITEM, OrderDB.ORDERS, PartDB.PART, PartDB.Supplier " +
					 " WHERE OrderDB.LINEITEM.L_PARTKEY = PartDB.PART.P_PARTKEY AND OrderDB.LINEITEM.L_ORDERKEY = OrderDB.ORDERS.O_ORDERKEY " +
					 " AND OrderDB.ORDERS.O_CUSTKEY = OrderDB.CUSTOMER.C_CUSTKEY and PartDB.supplier.s_suppkey = OrderDB.lineitem.l_suppkey " +
					 " AND OrderDB.Customer.C_Name = 'Customer#000000025';";
			
			String answer = "";
			
			ExampleQuery.runSQLQuery(stmt, sql, answer, null, false);        						
					
			System.out.println("\nOPERATION COMPLETED SUCCESSFULLY!"); 
		}
		catch (Exception ex)
		{	System.out.println("Exception: " + ex);
		}
		finally
		{			
			if (stmt != null)
				try
				{	// Close the statement
					stmt.close();				
				}
				catch (SQLException ex)
				{	System.out.println("SQLException: " + ex);
				}	
			if (con != null)
			{	try
				{	// Close the connection
					con.close();				
				}
				catch (SQLException ex)
				{	System.out.println("SQLException: " + ex);
				}
			}
		} 
	}
	
	
	 /**
     * Runs an SQL query and compares answer to expected answer.  Checks to see if GlobalQuery contains only one subquery.
     * This is useful for detecting if a function or expression was pushed down to the database or was executed by UnityJDBC.
     * 
     * @param stmt
     * 		statement
     * @param sql
     * 		SQL query
     * @param answer
     * 		verify that output contains expected answer
     * @param exception
     * 		exception string if an exception is expected
     * @param hasOneSubquery 
     * 		true if should only have one subquery, false if more than one
     * @return 
     *      GlobalQuery object produced
     */
    public static GlobalQuery runSQLQuery(UnityStatement stmt, String sql, String answer, String exception, boolean hasOneSubquery)
    {
    	 System.out.println("\nTesting query: \n"+sql);  
    	 long currentTime = System.currentTimeMillis();
         try
         {
        	 GlobalQuery gq = stmt.parseQuery(sql);
     			if (!(gq.getExecutionTree() instanceof ResultSetScan) && hasOneSubquery)
     				fail("Query should have nothing executed at UnityJDBC level.");
     			else if (!hasOneSubquery && gq.getExecutionTree() instanceof ResultSetScan)
     				fail("Query should have more than one subquery.");

     		 ResultSet rst = stmt.executeQuery(gq);
     		 
             // Validate that execution result is as expected
             String result = StringFunc.resultSetToString(rst);     
             System.out.println(result);
             if (!result.contains(answer))
            	 fail("Incorrect result");                      
             
             // If exception was the expected outcome, indicate a failure
             if (exception != null)
             	fail("Query was expected to generate an exception.");
             System.out.println("Query execution time: "+(System.currentTimeMillis()-currentTime));
             
             rst.close();
             
             return gq;
         }            
         catch (SQLException e)
         {
             System.out.println(e);
        	 if (!e.toString().contains(exception))
            	 fail("Incorrect exception: "+e);
         }       
         return null;
    }
    
}
