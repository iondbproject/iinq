import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import unity.annotation.GlobalSchema;
import unity.annotation.SourceDatabase;
import unity.annotation.SourceField;
import unity.annotation.SourceTable;
import unity.jdbc.UnityConnection;
import unity.jdbc.UnityStatement;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;


/**
 * Tests support for IINQ queries.
 */
@SuppressWarnings("nls")
public class TestIinqQuery 
{
    private static GlobalSchema metadata = null;
    private static Connection con = null;
    private static String url="jdbc:unity://data/xspec/IonDBDemo.xml";

	/**
	 * Initializes a IINQ JDBC connection.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@BeforeClass
	public static void init() throws Exception {

        System.out.println("\nRegistering driver.");
        Class.forName("unity.jdbc.UnityDriver");

        System.out.println("\nGetting connection:  " + url);
        con = DriverManager.getConnection(url);
        System.out.println("\nConnection successful for " + url);
        System.out.println("\nGetting metadata.");
        metadata = ((UnityConnection )con).getGlobalSchema();
		ArrayList<SourceDatabase> databases = metadata.getAnnotatedDatabases();
		if (null == databases) {
			System.out.println("\nNo databases have been detected.");
		}
		else {
			System.out.println("\nThe following schema have been detected: \n");
			for (SourceDatabase db : databases) {
				System.out.println(db.getDatabaseName());
				ArrayList<SourceTable> tables = db.getSourceTablesByName();
				if (null == tables) {
					System.out.println("\tNo tables detected.");
				}
				for (SourceTable table : tables) {
					System.out.print("\t");
					System.out.println(table.getTableName());
					ArrayList<SourceField> fields = table.getSourceFieldList();
					for (SourceField field: fields) {
						System.out.print("\t\t");
						if (field.getDataTypeName().contains("CHAR")) {
							System.out.printf("%s[%d]\n", field.toShortString(), field.getColumnSize());
						} else {
							System.out.println(field.toShortString());
						}
					}
				}
				System.out.println();
			}
		}

    }


        /**
         * Closes the IINQ JDBC connection.
         *
         * @throws Exception
         *             if an error occurs
         */
	@AfterClass
	public static void end() throws Exception 
	{
	    if (null != con) {
	        con.close();
        }
	
	}

	/**
	 * Test SELECT * FROM Table
	 */
	@Test
	public void testSelectAll()
	{
        String answer =
                "";

        TestIinq.runSQLQuery("SELECT * FROM test1;", answer, metadata);
        TestIinq.runSQLQuery("SELECT * FROM test1;", answer);
	}

	/**
	 * Test SELECT fieldList FROM Table
	 */
	@Test
	public void testSelectFieldList() 
	{		
		String answer = "Al 'Adliyah, 498.75, 21.0"
		                +"\nTotal results: 3458";
		
		TestIinq.runSQLQuery("SELECT col1, col2, col3 FROM test1;", answer);
	}

	/**
     * Test SELECT constant expression
     */
    @Test
    public void testSelectConstant() 
    {     
        String answer = "Total columns: 1"
                        +"\nField1"
                        +"\n1"
                        +"\nTotal results: 1";
    
        // SELECT 1
        TestIinq.runSQLQuery("SELECT 1;", answer);     

        // SELECT expression (promoted to Unity)
        answer = "Total columns: 1"
                +"\nval"
                +"\n8"
                +"\nTotal results: 1";
        TestIinq.runSQLQuery("SELECT 3+5 as val;", answer);     
        
        
        // SELECT expression with other field 
        answer = "Total columns: 2"
                +"\nval, City"
                +"\n8, Al 'Adliyah"
                +"\n8, Albuquerque"
                +"\nTotal results: 2";
                
        TestIinq.runSQLQuery("SELECT 4*2 as val, col1 FROM test1 LIMIT 2;", answer);
        
        // SELECT string expression with other field 
        answer = "Total columns: 2"
                +"\nval, City"
                +"\nX, Al 'Adliyah"
                +"\nX, Albuquerque"
                +"\nTotal results: 2";
                
        TestIinq.runSQLQuery("SELECT 'X' as val, col1 FROM test1 LIMIT 2;", answer);
        
        /*
        // TODO: Does not behave as would expect (only returns one row)
        // SELECT 1 FROM table
        TestIinq.runSQLQuery("SELECT 1 FROM test1;", answer,
                "http://209.136.222.21/IINQ10/cgi-bin/IINQisapi.dll/test1 Parameters: {fields=[Store Type, Planned Gross Sales, Planned Profit, Profit, Quantity, Sales Cost, Sales Date, Sales per transaction, Unit Price, City, Department, Product Line, Product Type, Discount, Gross Sales, Items per transaction, Manufacturing Cost, Country, No of customers, State]}", null);
        */     
    }
    
	/**
     * Test SELECT fieldList FROM Table with no results (but should be able to produce a schema).
     */
    @Test
    public void testSelectFieldListNoResults() 
    {
        fail("Not implemented");
    }
    
	/**
     * Test SELECT fieldList FROM Table where field does not exist.
     */
    @Test
    public void testSelectFieldListFieldDNE() 
    {        
        String answer = "";        
        TestIinq.runSQLQuery("SELECT cityDNE, col1, col2 FROM test1;", answer);
    }
    
    /**
     * Test SELECT fieldList FROM Table where table does not exist.
     */
    @Test
    public void testSelectFieldListTableDNE() 
    {        
        String answer = "";
        TestIinq.runSQLQuery("SELECT field1, field2 FROM no_table", answer);                
    }
    	        
    
	/**
	 * Test SELECT fields a field alias (e.g. field AS X).
	 */
	@Test
	public void testSelectAlias() 
	{
	    String answer = "Total columns: 3"
	                    +"\nC, grossSales, Unit Price";
        
        TestIinq.runSQLQuery("SELECT col1 as C, col2 AS grossSales, col3 FROM test1;", answer);
	}

	/**
     * Test SELECT fields a field alias (e.g. field AS X).
     */
    @Test
    public void testSelectAliasMultipleFields() 
    {
        String answer = "Total columns: 3"
                +"\nC, grossSales, unitPrice";
        
        TestIinq.runSQLQuery("SELECT col1 as C, col2 AS grossSales, col3 unitPrice FROM  test1;", answer);
    }
    
	/**
     * Test WHERE equality filter.
     */
    @Test
    public void testWhereEqualityFilter() 
    {
        String answer = "Total results: 292";

        // String filter
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col2 = 'United Kingdom';", answer);
        
        // Number filter
        answer = "Total results: 1";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col1 = 31.02;", answer);
    }
        
    /**
     * Test WHERE filter >, >=, <. <=, and != with String comparisons.
     */
    @Test
    public void testWhereFilterString() 
    {
        String answer = "Total results: 1724";
        
        // >
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col2 > 'United Kingdom';", answer);
 
        // >=
        answer = "Total results: 2016";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col2 >= 'United Kingdom';", answer);
 
        // <
        answer = "Total results: 1442";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col2 < 'United Kingdom';", answer);
 
        // <=
        answer = "Total results: 1734";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col2 <= 'United Kingdom';", answer);
 
        // !=
        answer = "Total results: 3166";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col2 <> 'United Kingdom';", answer);
        
        answer = "Total results: 3166";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col2 != 'United Kingdom';", answer);
    }

    /**
     * Test WHERE filter >, >=, <. <=, and != with number (real) comparisons.
     */
    @Test
    public void testWhereFilterReal() 
    {
        String answer;
        
        // >
        answer = "Total columns: 4"
                +"\nCity, Country, Gross Sales, Unit Price"                
                +"\nLafayette, United States, 44157.33, 6783.0"
                +"\nSandy Springs, United States, 29055.38, 881.0"                
                +"\nTotal results: 2";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col3 > 27300;", answer);
 
        // >=
        answer = "Total columns: 4"
                +"\nCity, Country, Gross Sales, Unit Price"
                +"\nLafayette, United States, 44157.33, 6783.0"
                +"\nLausanne, Switzerland, 27300.0, 600.0"
                +"\nSandy Springs, United States, 29055.38, 881.0"
                +"\nTotal results: 3";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col3 >= 27300;", answer);
 
        // <
        answer = "Total columns: 4"
                +"\nCity, Country, Gross Sales, Unit Price"
                +"\nCoconut Grove, United States, 1.9, 2.0"
                +"\nMorges, Switzerland, 0.99, 1.0"
                +"\nTotal results: 2";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col1 < 2.1;", answer);
 
        // <=        
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col1 <= 1.9;", answer);

		// !=
        answer = "Total results: 3457";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col1 <> 4;", answer);
                
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col1 != 4;", answer);
    }
    
	/**
	 * Test ORDER BY.
	 */
	@Test
	public void testOrderBySingleAttr() 
	{
	    String answer;
		// ASC
        answer = "Total columns: 4"
                +"\nCity, Country, Gross Sales, Unit Price"                
                +"\nVesenaz, Switzerland, 26496.0, 600.0"
                +"\nCheltenham, United Kingdom, 26600.0, 3500.0"
                +"\nRed Deer, Canada, 26880.0, 3500.0"
                +"\nLausanne, Switzerland, 27300.0, 600.0"
                +"\nSandy Springs, United States, 29055.38, 881.0"
                +"\nLafayette, United States, 44157.33, 6783.0"
                +"\nTotal results: 6";
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col3 > 26000 ORDER BY col3 ASC;", answer);
       
		// DESC
        answer = "Total columns: 4"
                 +"\nCity, Country, Gross Sales, Unit Price"
                 +"\nVesenaz, Switzerland, 26496.0, 600.0"
                 +"\nSandy Springs, United States, 29055.38, 881.0"
                 +"\nRed Deer, Canada, 26880.0, 3500.0"
                 +"\nLausanne, Switzerland, 27300.0, 600.0"
                 +"\nLafayette, United States, 44157.33, 6783.0"
                 +"\nCheltenham, United Kingdom, 26600.0, 3500.0"
                 +"\nTotal results: 6";
        
        TestIinq.runSQLQuery("SELECT col1, col2, col3, col4 FROM test1 WHERE col3 > 26000 ORDER BY col4 DESC;", answer);
	}


    /**
     * Test ORDER BY with multiple attributes.
     */
    @Test
    public void testOrderByMultipleAttr() 
    {
        String answer;
        // ASC
        answer = "Total columns: 4"
                +"\nCity, Country, Gross Sales, Unit Price"                
                +"\nRed Deer, Canada, 26880.0, 3500.0"
                +"\nVesenaz, Switzerland, 26496.0, 600.0"
                +"\nLausanne, Switzerland, 27300.0, 600.0"
                +"\nCheltenham, United Kingdom, 26600.0, 3500.0"
                +"\nSandy Springs, United States, 29055.38, 881.0"
                +"\nLafayette, United States, 44157.33, 6783.0"
                +"\nTotal results: 6";
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE col3 > 26000 ORDER BY col4 ASC, col3 ASC;", answer);
         
        // DESC
        answer = "Total columns: 4"
                +"\nCity, Country, Gross Sales, Unit Price"                
                +"\nLafayette, United States, 44157.33, 6783.0"
                +"\nSandy Springs, United States, 29055.38, 881.0"
                +"\nCheltenham, United Kingdom, 26600.0, 3500.0"
                +"\nLausanne, Switzerland, 27300.0, 600.0"
                +"\nVesenaz, Switzerland, 26496.0, 600.0"
                +"\nRed Deer, Canada, 26880.0, 3500.0"
                +"\nTotal results: 6";
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE col3 > 26000 ORDER BY col4 DESC, col3 DESC;", answer);
    }
    
    /**
     * Test COUNT aggregate function with GROUP BY that is sorted on.
     */
    @Test
    public void testGroupByAggCountOrderBy() 
    {
        String answer = "Navan, 28"
                        +"\nTotal results: 161";
        
        TestIinq.runSQLQuery("SELECT col2, COUNT(col3) from test1 GROUP BY col2 ORDER BY COUNT(col3) DESC", answer);
    }
    
	/**
	 * Test LIMIT.
	 */
	@Test
	public void testLimit() 
	{
	    String answer;
        // ASC
        answer = "Total columns: 4"
                +"\nCity, Country, Gross Sales, Unit Price"                
                +"\nVesenaz, Switzerland, 26496.0, 600.0"
                +"\nCheltenham, United Kingdom, 26600.0, 3500.0"
                +"\nRed Deer, Canada, 26880.0, 3500.0"
                +"\nTotal results: 3";
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE col3 > 26000 ORDER BY col3 ASC LIMIT 3;", answer);

	}
		
	/**
     * Test LIMIT/OFFSET.
     */
    @Test
    public void testLimitOffset() 
    {
        String answer;
        // ASC
        answer = "Total columns: 4"
                +"\nCity, Country, Gross Sales, Unit Price"                
                +"\nRed Deer, Canada, 26880.0, 3500.0"
                +"\nLausanne, Switzerland, 27300.0, 600.0"
                +"\nSandy Springs, United States, 29055.38, 881.0"
                +"\nTotal results: 3";
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE col3 > 26000 ORDER BY col3 ASC LIMIT 3 OFFSET 2;", answer);

    }
    
	/**
	 * Test date comparisons.
	 */
	@Test
	public void testDateComparisons() 
	{
	    /*
	    String answer = "Total results: 57";
	
	    // DateTime constant (no cast)
        TestIinq.runSQLQuery("SELECT date_hour, _time FROM TestSearch where _time >= '2014-04-05 17:08:00';", answer,
	//    TestIinq.runSQLQuery("SELECT date_hour, _time FROM TestSearch where _time < '2014-04-02T01:00:00'", answer,
                "https://192.168.159.133:8089/services/admin/saved/searches/TestSearch Parameters: {full_search=search error AND _time<\"2014-06-10 00:00:00\" | fields date_hour, _time, filter=_time<\"2014-06-10 00:00:00\", fields=date_hour, _time}", null);	    
        */
        fail("Not implemented");
    }
	
	/**
	 * Test handling and filtering out unnecessary clause 1=1 or any other equivalent.
	 */
	@Test
	public void testRemoveUnnecessaryWhere() 
	{	    
	    String answer = "";
                        	    
        TestIinq.runSQLQuery("SELECT * from test1 WHERE 1 = 1", answer);
    }
	
	/**
     * Test AND.
     */
    @Test
    public void testAnd() 
    {
        String answer = "London, United Kingdom, 177.65, 11.0"
                        +"\nLondon, United Kingdom, 209.52, 6.0"
                        +"\nTotal results: 18";
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE col4 = 'United Kingdom' and col2 = 'London';", answer);
    }
       
    
    /**
     * Test OR.
     */
    @Test
    public void testOr() 
    {
        String answer = "Total results: 2016";
    
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE col4 = 'United Kingdom' OR col4 = 'United States';", answer);

        answer = "Total results: 318";
        
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE col4 = 'United Kingdom' OR col2 = 'New York';", answer);
    }
    
    /**
     * Test AND and OR.
     */
    @Test
    public void testAndOr() 
    {
        String answer;
        
        answer = "Total results: 48";
        
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE col4 = 'United States' and (col2 = 'Albuquerque' OR col2 = 'New York');", answer);
    }
    
    /**
     * Test NOT.
     */
    @Test
    public void testNot() 
    {
        String answer;
        
        answer = "Total results: 1734";
        
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE NOT col4 = 'United States';", answer);
    }
    
    /**
     * Test NOT, AND and OR.
     */
    @Test
    public void testNotAndOr() 
    {
        String answer;
        
        answer = "Total results: 322";
        
        TestIinq.runSQLQuery("SELECT col2, col4, col3, col1 FROM test1 WHERE col4 = 'United States' and (col2 = 'Albuquerque' OR col2 = 'New York') or (col4 = 'United Kingdom' and NOT col2 = 'London');", answer);
     }      
            
    /**
     * Test WHERE filter IS NULL and IS NOT NULL.
     */
    @Test
    public void testIsNull() 
    {
        String answer = "Total columns: 1"
                +"\nGrossSales"
                +"\n3454"
                +"\nTotal results: 1";
        
        // IS NULL
        TestIinq.runSQLQuery("SELECT COUNT(col3) as GrossSales from test1 WHERE col2 IS NULL", answer);
                        
        // IS NOT NULL
        TestIinq.runSQLQuery("SELECT COUNT(col3) as GrossSales from test1 WHERE col2 IS NOT NULL", answer);
    }
    
    /**
     * Test subquery in FROM. Promoted to Unity.
     */
    @Test
    public void testSubQueryInFrom()
    {
        String answer = "Total columns: 1"
                +"\nGrossSales"
                +"\n3454"
                +"\nTotal results: 1";
        
        TestIinq.runSQLQuery("SELECT COUNT(col3) as GrossSales from (SELECT col4, col2, col3 FROM test1) S", answer);
    }
        
    /**
     * Test COUNT(*) with no group by.
     */
    @Test
    public void testCountStar() 
    {   
        String answer = "";
        
        TestIinq.runSQLQuery("SELECT COUNT(*) from test1", answer);
    }
    
    /**
     * Test COUNT aggregate function.
     */
    @Test
    public void testAggCount() 
    {
        String answer = "Total columns: 1"
                        +"\nExpr0"
                        +"\n3458"
                        +"\nTotal results: 1";
        
        TestIinq.runSQLQuery("SELECT COUNT(col3) from test1", answer);
    }
    
    /**
     * Test SUM aggregate function.
     */
    @Test
    public void testAggSum() 
    {
        String answer = "Total columns: 1"
                        +"\nExpr0"
                        +"\n6109678.72"
                        +"\nTotal results: 1";
        
        TestIinq.runSQLQuery("SELECT SUM(col3) from test1", answer);
    }
    
    /**
     * Test AVG aggregate function.
     */
    @Test
    public void testAggAvg() 
    {
        String answer = "Total columns: 1"
                        +"\nExpr0"
                        +"\n1766.82438404"
                        +"\nTotal results: 1";
        
        TestIinq.runSQLQuery("SELECT AVG(col3) from test1", answer);
    }
    
    /**
     * Test MIN/MAX aggregate functions.
     */
    @Test
    public void testAggMinMax() 
    {
        String answer = "Total columns: 1"
                        +"\nExpr0"
                        +"\n44157.33"
                        +"\nTotal results: 1";
       
        TestIinq.runSQLQuery("SELECT MAX(col3) from test1", answer);
        
        answer = "Total columns: 1"
                +"\nExpr0"
                +"\n0.99"
                +"\nTotal results: 1";
        
        TestIinq.runSQLQuery("SELECT MIN(col3) from test1", answer);

        // MIN and MAX together
        answer = "Total columns: 2"
                +"\nExpr0, Expr1"
                +"\n0.99, 44157.33"
                +"\nTotal results: 1";
        TestIinq.runSQLQuery("SELECT MIN(col3),  MAX(col3) from test1", answer);
    }
       
    
    /**
     * Test MEDIAN aggregate functions.
     */
    @Test
    public void testAggMedian() 
    {
        String answer = "Total columns: 1"
                        +"\nExpr0"
                        +"\n425.46"
                        +"\nTotal results: 1";
        
        TestIinq.runSQLQuery("SELECT MEDIAN(col3) from test1", answer);
    }
    
    /**
     * Test COUNT(*) with GROUP BY.
     */
    @Test
    public void testGroupByCountStar() 
    {   
        String answer = "";
        
        TestIinq.runSQLQuery("SELECT col2, col4, COUNT(*) from test1 GROUP BY col2, col4", answer);
    }
    
    /**
     * Test COUNT aggregate function with GROUP BY.
     */
    @Test
    public void testGroupByAggCount() 
    {
        String answer = "Baton Rouge, 20"
                        +"\nTotal results: 161";
        
        TestIinq.runSQLQuery("SELECT col2, COUNT(col3) from test1 GROUP BY col2", answer);
    }
    
    /**
     * Test SUM aggregate function with GROUP BY.
     */
    @Test
    public void testGroupByAggSum() 
    {
        String answer = "Alliston, Canada, 17834.29"
                        +"\nBelleville, Canada, 39576.35"
                        +"\nTotal results: 161";
        
        TestIinq.runSQLQuery("SELECT col2, col4, SUM(col3) from test1 GROUP BY col2, col4", answer);
    }
    
    /**
     * Test AVG aggregate function with GROUP BY.
     */
    @Test
    public void testGroupByAggAvg() 
    {
        String answer = "India, 1265.82227273"
                        +"\nIreland, 1744.69856115"
                        +"\nTotal results: 24";
        
        TestIinq.runSQLQuery("SELECT col4, AVG(col3) from test1 GROUP BY col4", answer);
    }
    
    /**
     * Test MIN/MAX aggregate functions with GROUP BY.
     */
    @Test
    public void testGroupByAggMinMax() 
    {
        String answer = "India, 7261.44"
                        +"\nIreland, 16412.76"
                        +"\nTotal results: 24";
       
        TestIinq.runSQLQuery("SELECT col4, MAX(col3) from test1 GROUP BY col4", answer);

        answer = "India, 22.75"
                 +"\nIreland, 7.44"
                 +"\nTotal results: 24";
        
        TestIinq.runSQLQuery("SELECT col4, MIN(col3) from test1 GROUP BY col4", answer);
          
        // MIN and MAX together
        answer = "Alliston, Canada, 58.24, 6349.7"
                 +"\nBelleville, Canada, 14.4, 18000.0"
                 +"\nTotal results: 161";
        
       TestIinq.runSQLQuery("SELECT col2, col4, MIN(col3),  MAX(col3) from test1 GROUP BY col2, col4", answer);
    }
    
    /**
     * Test GROUP BY where GROUP BY field is not in the output.
     */
    @Test
    public void testGroupByNotInOutput() 
    {
        String answer = "Total columns: 1"
                        +"\nmaxGrossSales"
                        +"\n23681.28"
                        +"\n8905.26"
                        +"\n6485.08"
                        +"\n26880.0"
                        +"\n13710.06"
                        +"\n6897.24"
                        +"\n16422.0"
                        +"\n14535.0"
                        +"\n7261.44"
                        +"\n16412.76"
                        +"\nTotal results: 24";
       
        TestIinq.runSQLQuery("SELECT MAX(col3) as maxGrossSales from test1 GROUP BY col4", answer);
    }
    
    /**
     * Test MEDIAN aggregate functions with GROUP BY.
     */
    @Test
    public void testGroupByAggMedian() 
    {
        String answer = "India, 255.36"
                        +"\nIreland, 478.8"
                        +"\nTotal results: 24";
        
        TestIinq.runSQLQuery("SELECT col4, MEDIAN(col3) from test1 GROUP BY col4", answer);
    }
                        
    /**
     * Tests MIN, MAX, AVG, SUM aggregate functions.
     */
    @Test
    public void testAggMinMaxSumAvg() 
    {
        String answer = "Total columns: 5"
                        +"\nCountry, maxGrossSales, minQuantity, avgProfit, sumUnitPrice"
                        +"\nAustralia, 23681.28, 1.0, 2098.26589027, 8676.0"
                        +"\nBahrain, 8905.26, 4.0, 1960.12953333, 1757.0"
                        +"\nBelgium, 6485.08, 1.0, 1478.52304762, 3915.0"
                        +"\nTotal results: 3";
        TestIinq.runSQLQuery("SELECT col4, MAX(col3) as maxGrossSales, MIN(col3) as minQuantity, AVG(profit) as avgProfit, SUM(col1) as sumUnitPrice from test1 GROUP BY col4 LIMIT 3", answer);
    }       
    
    
    /**
     * Test HAVING. Currently promoted to Unity.
     */
    @Test
    public void testHaving() 
    {
        String answer = "Total columns: 4"
                +"\nmaxGrossSales, minQuantity, avgProfit, sumUnitPrice"
                +"\n26880.0, 1.0, 1797.91784485, 26477.0"
                +"\n26600.0, 1.0, 1953.88268767, 25422.0"
                +"\n44157.33, 1.0, 1616.71726009, 143596.0"
                +"\nTotal results: 3";

        TestIinq.runSQLQuery("SELECT MAX(col3) as maxGrossSales, MIN(col3) as minQuantity, AVG(profit) as avgProfit, SUM(col1) as sumUnitPrice from test1 GROUP BY col4 HAVING sumUnitPrice > 20000", answer);
    }
       
    /**
     * Test IN in WHERE clause.
     */
    @Test
    public void testIn() 
    {
        String answer = "Total columns: 1"
                        +"\nCountry"
                        +"\nCanada"
                        +"\nUnited Kingdom"
                        +"\nUnited States"
                        +"\nTotal results: 3";
        
        TestIinq.runSQLQuery("SELECT col4 FROM test1 WHERE col4 IN ('Canada', 'United Kingdom', 'United States');", answer);
    }
    
    /*
     * Expression tests
     */
    /**
     * Test SELECT fieldList FROM Table with expressions
     */
    @Test
    public void testSelectFieldListExpressions() 
    {
        String answer = "Al 'AdliyahA, 498.75, 121.0"
                        +"\nTotal results: 3458";
     
        TestIinq.runSQLQuery("SELECT col1+100  as UnitPricePlus100 FROM test1;", answer);
    
        
        TestIinq.runSQLQuery("SELECT col2+'A', col3, col1 +100 as UnitPricePlus100 FROM test1;", answer);

    }
    
    /**
     * Test expressions with aggregate functions in SELECT. Example: SELECT Avg(expr) FROM Table.
     */
    @Test
    public void testSelectAggExpressions() 
    {
        String answer = "Total columns: 2"
                        +"\navgSales, UnitPricePlus100"
                        +"\n3603.143310549774, 621676.0"
                        +"\nTotal results: 1";
        
        TestIinq.runSQLQuery("SELECT AVG(col3*2) as avgSales, SUM(col1 +100) as UnitPricePlus100 FROM test1;", answer);
    }
    
    /**
     * Test expressions within WHERE filter.
     */
    @Test
    public void testWhereExpressions() 
    {
        String answer = "Total columns: 2"
                        +"\nGross Sales, Unit Price"
                        +"\n0.99, 1.0"
                        +"\n1.9, 2.0"
                        +"\n2.7, 1.0"
                        +"\nTotal results: 3";
        
        // Expression with math operators
        TestIinq.runSQLQuery("SELECT col3, col1 FROM test1 WHERE col3 * 2 - 100 < 1000 LIMIT 3;", answer);
        
        // Expression with math operators with multiple attributes
        TestIinq.runSQLQuery("SELECT col3, col1 FROM test1 WHERE col3 * col1 - 100 < 1000 LIMIT 3;", answer);
        
        // Expression with functions (number)
        TestIinq.runSQLQuery("SELECT col3, col1 FROM test1 WHERE abs(col3) * col1 - 100 < 1000 LIMIT 3;", answer);
       
        answer = "Total columns: 3"
                 +"\nCity, Gross Sales, Unit Price"
                 +"\nFarmington, 22.56, 6.0"
                 +"\nFarmington, 39.6, 4.0"
                 +"\nFarmington, 95.04, 12.0"
                 +"\nTotal results: 3";
        
        // Expression with functions (string)
        TestIinq.runSQLQuery("SELECT col2, col3, col1 FROM test1 WHERE left(col2,3) > 'F' LIMIT 3;", answer);
    }
}
