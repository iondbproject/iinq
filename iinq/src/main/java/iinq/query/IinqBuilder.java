package iinq.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import iinq.metadata.IinqDatabase;
import iinq.metadata.IinqTable;
import unity.annotation.SourceField;
import unity.engine.Attribute;
import unity.engine.Relation;
import unity.generic.jdbc.Parameter;
import unity.generic.query.QueryBuilder;
import unity.generic.query.WebQuery;
import unity.query.GQFieldRef;
import unity.query.GQTableRef;
import unity.query.LQCondNode;
import unity.query.LQDupElimNode;
import unity.query.LQExprNode;
import unity.query.LQGroupByNode;
import unity.query.LQLimitNode;
import unity.query.LQNode;
import unity.query.LQOrderByNode;
import unity.query.LQProjNode;
import unity.query.LQSelNode;
import unity.query.LQTreeConstants;
import unity.util.StringFunc;

/**
 * Convert a SQL query into C code.
 */
@SuppressWarnings("nls")
public class IinqBuilder extends QueryBuilder
{
	private IinqDatabase database;

	/**
	 * Constructor that takes a logical query tree as input.
	 * 	
	 * @param startNode
	 * 		root node of logical query tree
	 */
	public IinqBuilder(LQNode startNode, IinqDatabase database)
	{
	    super("", startNode);	     //$NON-NLS-1$
		this.database = database;
	}
	
	/**
	 * Constructs a C code query from logical query tree.
	 * 
	 * @return
	 * 		WebQuery
	 * @throws SQLException
	 * 		if an error occurs	 
	 */
	public IinqQuery toQuery() throws SQLException
	{
		LQNode currentNode = this.startNode;
		IinqQuery query = new IinqQuery("", database); //$NON-NLS-1$
	
	    // Traverse tree to build query
		processNode(query, currentNode);
		return query;
	}
		
	/**
	 * Converts a Projection node.
	 * 
	 * @param node
	 *     projection node
	 * @param query
	 *     query being built
	 * @throws SQLException
	 *     if an error occurs  
	 */	
	@Override
	protected void buildProjection(LQProjNode node, WebQuery query) throws SQLException 
	{	    
	    if (node != this.firstProj)
	        return;
	    
	    // Set the query's projection node
		query.setProjection(node);
		
		boolean selectAll = node.isSelectAll();
		Attribute[] attr = null;
		//StringBuilder fields = new StringBuilder();
		ArrayList<LQExprNode> expList = node.getExpressions();
		Iterator<LQExprNode> it = expList.iterator();
		ArrayList<String> fieldList = new ArrayList<>();
		ArrayList<Integer> fieldListNums = new ArrayList<>();
		while (it.hasNext()) {
			LQExprNode expNode = it.next();
			fieldList.add(expNode.getFieldReference().getName());
			fieldListNums.add(expNode.getFieldReference().getField().getOrdinalPosition());
		}
		query.setParameter("fieldList", fieldList);
		query.setParameter("fieldListNums", fieldListNums);
		
/*		if (selectAll)
		{   // SELECT * handled differently as do not know what attributes will be returned
		    node.setSelectAll(true);
		    fields.append("*"); //$NON-NLS-1$
		}
		else
		{
    		// Validate the expressions to see if can be process.
    		// Cannot process functions or expressions only base fields.
    		ArrayList<LQExprNode> expressions = node.getExpressions();
    				     
            attr = new Attribute[expressions.size()];
                    
            int pos = 0;                    
    		for (LQExprNode field : expressions) 
    		{			
    		    LQExprNode fld = field;
    			if (field.getType() == LQTreeConstants.AS_IDENTIFIER)
    				field = (LQExprNode) field.getChild(0);	    // Keep the base field from the AS expression (first child)
    
    			// Try to build expression if can.  Throws an exception if a failure.
    			String expr = buildExpression(field, query);
    			
    			// Only add fields to request sent to IINQ.  Other operators are done at Unity level.
    			if (field.getType() != LQTreeConstants.IDENTIFIER)			
    			{
    			    if (fld.toString().contains("COUNT(*)") && fld == field) // Does not have an alias
    			    {
    			        String name = query.getParameter("countStar");
    			        LQExprNode en = new LQExprNode();
    			        en.setType(LQTreeConstants.IDENTIFIER);
    			        en.setContent(name);
    			        expressions.set(0, en);
    			    }    			      
    			    continue;
    			}
    			String[] components = StringFunc.divideId(expr);
    			String name = components[0];
    			if (components.length > 1)
    				name = components[components.length-1];		// Get only field name
    			    			
			    // Create attribute    			
			    GQFieldRef fref = (GQFieldRef) field.getContent(); 
			    SourceField sf = fref.getField();
			    // Remove quotes from name
			    name = StringFunc.undelimitName(name, '"');
			    attr[pos++] = new Attribute(name, sf.getDataType(), sf.getColumnSize(), fref);
    			    			
    			// Add field to field parameter list
    			if (fields.length() > 0)
    			    fields.append(", "); //$NON-NLS-1$
    			fields.append(name);			
    		}
		}*/
		
		// Create output relation
		// Default is that no attributes have been defined for relation.  Need to wait until get fields from output when query is run.
		// Otherwise attribute array is built while traversing through the expression list.
		Relation r = new Relation(attr);
        query.setTableRelation(r);
        r.setProperty("name", this.table.getTableName()); //$NON-NLS-1$
        query.setRelation(r);
	    
        // Set output relation of input to projection node (most likely table)
        node.getChild(0).setOutputRelation(r);
                
        // Set the query parameter for field filter        
       // query.setParameter("fields", fields.toString());                      //$NON-NLS-1$
	}
	

	/**
	 * Builds a condition node.  
	 * 
	 * @param condition
	 *     condition node
	 * @throws SQLException
	 *     if an error occurs 
	 */	
	protected String buildCondition(LQCondNode condition, WebQuery in) throws SQLException 
	{		
	    int type = condition.getType();
	    LQCondNode left, right;
	    String leftCondition, rightCondition;
	    
	    switch(type) 
	    { 
            case LQTreeConstants.XOR:
                throw new SQLException("WHERE filters with XOR are not supported."); //$NON-NLS-1$
                
            case LQTreeConstants.NOT:
                left = (LQCondNode) condition.getChild(0);                
                leftCondition = buildCondition(left, in);                
                return "NOT (" + leftCondition +")"; //$NON-NLS-1$ //$NON-NLS-2$
    
            case LQTreeConstants.OR:
                left = (LQCondNode) condition.getChild(0);
                right = (LQCondNode) condition.getChild(1);
                leftCondition = buildCondition(left, in);
                rightCondition = buildCondition(right, in);
                return '(' + leftCondition +" OR " + rightCondition + ')';   //$NON-NLS-1$
                
            case LQTreeConstants.AND: 
                left = (LQCondNode) condition.getChild(0);
                right = (LQCondNode) condition.getChild(1);
                leftCondition = buildCondition(left, in);
                rightCondition = buildCondition(right, in);
                return leftCondition +" AND " + rightCondition;                 //$NON-NLS-1$
                    
            default:
                return this.buildComparison(condition, in);  
        }                	    			
	}		
	
	/**
     * Builds a selection operator.
     * 
     * @param node
     *     selection node
     * @param query
     *     query being built
     * @throws SQLException
     *     if an error occurs
     */
    protected void buildHaving(LQSelNode node, WebQuery query) throws SQLException 
    {
        LQCondNode condition = node.getCondition();     
       
        String havingFilter = this.buildCondition(condition, query);
               
		String currentHavingFilter = query.getParameter("having");
        if (currentHavingFilter == null)
            query.setParameter("having", havingFilter);
        else
            query.setParameter("having", currentHavingFilter+" "+havingFilter);     // Default is AND.  Parsing seems to cause problems so leave it out.
    }   
    
    /**
     * Builds a selection operator.
     * 
     * @param node
     *     selection node
     * @param query
     *     query being built
     * @throws SQLException
     *     if an error occurs
     */
    @SuppressWarnings("rawtypes")
	@Override
	protected void buildSelection(LQSelNode node, WebQuery query) throws SQLException 
    {
        if (node.bHavingCondition)
        {   // Handle a HAVING condition
            buildHaving(node, query);
            return;
        }
        
        LQCondNode condition = node.getCondition();     
       
        ArrayList<String> whereFilter = new ArrayList<>();
        whereFilter.add(this.buildCondition(condition, query));
        
        Object currentWhereFilter = query.getParameterObject("filter");
        if (currentWhereFilter == null)
            query.setParameter("filter", whereFilter);
        else
        {
            if (currentWhereFilter instanceof ArrayList)
                ((ArrayList) currentWhereFilter).addAll(whereFilter);
            /*else
            {
                ArrayList<String> filters = new ArrayList<String>();
                filters.add(currentWhereFilter.toString());
                filters.add(whereFilter);
                query.setParameter("filter", filters);
            }*/
        }
//            query.setParameter("filter", currentWhereFilter+" AND "+whereFilter);     // Add another filter using AND if a filter already exists
            // query.setParameter("filter", currentWhereFilter+" "+whereFilter);     // Default is AND.  Parsing seems to cause problems so leave it out.
        
        /*
        String currentWhereFilter = query.getParameter("filter");
        if (currentWhereFilter == null)
            query.setParameter("filter", whereFilter);
        else
//            query.setParameter("filter", currentWhereFilter+" AND "+whereFilter);     // Add another filter using AND if a filter already exists
            query.setParameter("filter", currentWhereFilter+" "+whereFilter);     // Default is AND.  Parsing seems to cause problems so leave it out.
        */
    }   
    
    
	/**
	 * Builds an expression.  In IINQ, constants do not have to be quoted, but we use double quotes in all cases to handle potential issues.
	 * 
	 * @param node
	 *      expression node being converted    
	 * @param query 
	 *      IINQ query being built
	 * @param condition
	 * 		filter condition
	 * @return 
	 *      String representation of expression
	 * @throws SQLException 
	 * 		if an error occurs
	 */		
	protected String buildExpression(LQExprNode node, WebQuery query) throws SQLException 
	{
	    int type = node.getType();
        
        if (type == LQTreeConstants.IDENTIFIER || type == LQTreeConstants.EXPRESSION)
        {   // Identifier or expression.  Remove any double-quotes if it was a delimited identifier.
            return StringFunc.undelimitName(getFieldName(node.getContent()), '"');
            //return StringFunc.undelimitName(node.getContent().toString(), '"');
        }      
        else if (type == LQTreeConstants.CONSTANTEXPRESSION || type == LQTreeConstants.STRING)
        {   // String constant expression.  Remove single quotes surrounding string constant.
            return '"'+StringFunc.removeQuotes(node.getContent().toString())+'"';
        }
        else if (type == LQTreeConstants.INTEGER || type == LQTreeConstants.REAL || type == LQTreeConstants.BOOLEAN)
        {   // Number constant expression
            return node.getContent().toString();
        }
        else if (type == LQTreeConstants.DATE || type == LQTreeConstants.TIME || type == LQTreeConstants.TIMESTAMP)
        {   // DATE, TIME, or TIMESTAMP constant expression
            Object dateVal = node.getContent();
            if (dateVal instanceof String)
                return '"'+StringFunc.removeQuotes((String) node.getContent())+'"';
           //     SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
           // SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy:HH:mm:ss");  // This was setting before
         //   SimpleDateFormat df = new SimpleDateFormat("yyyy/MM/dd");
         //   return "\"strptime(\""+df.format(dateVal)+"\",\"%Y/%m/%d\")\"";
            // return '"'+df.format(dateVal)+'"';
            // return "1459224511";
            if (dateVal instanceof Date)
                return ""+((Date) dateVal).getTime()/1000;
            return dateVal.toString();
        }
        else if (type == LQTreeConstants.ARITHMETICFUNCTION)
        {   // Math expressions are supported in IINQ but not yet in driver
            throw new SQLException("Expressions are not supported.");                                 
        }
        else if (type == LQTreeConstants.AGGREGATEFUNCTION)
        {   // Most aggregate functions are supported by the driver
            StringBuilder buf = new StringBuilder();
            buf.append(node.getContent().toString());
            
            // TODO: Only supports one parameter
            String val = buildExpression((LQExprNode) node.getChild(0), query);
            if (!val.equals("\"*\""))
            {   buf.append("(");
                buf.append(val);
                buf.append(")");
            }            
            return buf.toString();
        }
        else if (type == LQTreeConstants.FUNCTION || type == LQTreeConstants.DISTINCTAGGREGATEFUNCTION)
        {   // Some functions are supported by IINQ but not yet by driver.
            throw new SQLException("Expressions are not supported.");                      
        }
        else if (type == LQTreeConstants.ISCOMPARE)
        {   // IS NULL or IS NOT NULL
            return node.getContent().toString();
        }
        else if (type == LQTreeConstants.PARAMETER)                
        {   // Parameters will return their base data type (int, string, etc.)
            Object content = node.getContent();
            Object obj = Parameter.retrieveParameterValue(content);         
            
            if (obj instanceof String)
                return '"'+obj.toString()+'"';
			return obj.toString();                
        }          
        else if (type == LQTreeConstants.EXPRLIST)
        {   // Expression lists occur with IN (1, 3).  The list of expressions are in an ArrayList.
            throw new SQLException("Expressions are not supported.");                       
        }      
        else if (type == LQTreeConstants.SUBQUERY)
        {   // Subqueries are not supported
            throw new SQLException("Expressions are not supported.");
        }
        
        throw new SQLException("Expressions are not supported.");   
	}
		
	private String getAliasName(String defaultName, LQExprNode en)
	{
	    String aliasName = defaultName;        
        LQExprNode projfld = this.firstProj.findExpression(en);
        if (projfld != null)
            aliasName = projfld.getChild(1).generateSQL();
        return aliasName;
	}
	
	/**
     * Builds a GROUP BY operator.  Not supported.
     * 
     * @param node
     *     grouping node
     * @param query
     *     query being built
     * @throws SQLException
     *     if an error occurs
     */
	@SuppressWarnings("null")
	@Override
	protected void buildGroupBy(LQGroupByNode node, WebQuery query) throws SQLException 
	{	  	   
	    ArrayList<LQExprNode> list = node.getFunctionList();
	    
	    // Only support currently for COUNT(*) with no GROUP BY
	    if (node.isEmptyGrouping() && node.isOnlyCountStar())
	    {   // Separate support for COUNT(*) by itself (no GROUP BY).  TODO: May consider merging with more general GROUP BY code below.  	        
	        query.setParameter("countStar", getAliasName("count", list.get(0)));
	        return;
	    }
	    
	    // Build a stats expression for GROUP BY
	    StringBuilder buf = new StringBuilder();
	    buf.append("stats ");

	    // Add aggregate functions	  
	    if (list != null && list.size() >= 1)
	    {
	        String expr = this.buildExpression(list.get(0), query);	        	        
            buf.append(expr);            
            buf.append(" as ");            
            buf.append(getAliasName("expr0", list.get(0)));
	    }
	    for (int i=1; i < list.size(); i++)
	    {
	        buf.append(", ");
	        String expr = this.buildExpression(list.get(i), query);
            buf.append(expr);	        
            buf.append(" as ");            
            buf.append(getAliasName("expr"+i, list.get(i)));
	    }
	    
	    // Add GROUP BY fields
	    list = node.getExpressions();
        if (list != null && list.size() >= 1)
        {
            buf.append(" by ");
            String expr = this.buildExpression(list.get(0), query);
            buf.append(expr);
        }
        for (int i=1; i < list.size(); i++)
        {
            buf.append(", ");
            String expr = this.buildExpression(list.get(i), query);
            buf.append(expr);                       
        }
        if (!buf.toString().equals("stats "))
        {
            // System.out.println(buf.toString());
            query.setParameter("stats",buf.toString());
        }
	   //  throw new SQLException("GROUP BY is not supported.");
	}
	
	/**
	 * Handles an ORDER BY node.  Supported supports sort on multiple attributes with no expressions.
	 *  
	 * @param node
	 *     order by node
	 * @param query
	 *     query being built
	 * @throws SQLException
     *     if an error occurs
	 */
	@Override
	protected void buildOrderBy(LQOrderByNode node, WebQuery query) throws SQLException
	{
	    StringBuilder sort = new StringBuilder("sort 0");
		int limit = 0;
		IinqSort iinqSort = new IinqSort(limit);
		IinqTable table = this.database.getIinqTable(query.getParameter("source"));
        // Note: IINQ has support for sort mode.  Using auto for now.  Reference: http://docs.IINQ.com/Documentation/IINQ/latest/SearchReference/Sort
	    // 0 means no limit on elements to sort
	    
        for (int i=0; i < node.getOrderNumChildren(); i++)
        {
            LQNode orderByExpr = node.getOrderChild(i);
            if (orderByExpr.getType() != LQTreeConstants.IDENTIFIER)
                throw new SQLException("ORDER BY only supports sorting on one field with no expressions.");
            
            // Gets the name of the field.
            String fieldName = getFieldName(orderByExpr.getContent());     
            fieldName = StringFunc.undelimitName(fieldName, '"');
            String sqlDirection = node.getDirection(i);
            
            sort.append(' ');
			IinqSort.DIRECTION direction;
            if (sqlDirection.equals("ASC")) {
				sort.append("+");
				direction = IinqSort.DIRECTION.ASC;
			} else {
				sort.append("-");
				direction = IinqSort.DIRECTION.DESC;
			}
            sort.append(fieldName);
            iinqSort.addSortElement(direction, table.getTableId(), table.getFieldPosition(fieldName));
        }
    
        // Set sort expression
        query.setParameter("iinqSort", iinqSort);
        query.setParameter("sort", sort.toString());
	}
		
	/**
	 * Builds a FROM node.  Only supports a single table. 
	 * 
	 * @param node
	 *     FROM or identifier node
	 * @param query
	 *     query being built
	 * @throws SQLException	
	 *     if an error occurs
	 */
	@Override
	protected void buildFrom(LQNode node, WebQuery query) throws SQLException 
	{
/*		if (this.fromSeen) // join exception.
			throw new SQLException("Two tables were received. The driver only supports single table queries.");
		this.fromSeen = true;*/
	
		GQTableRef tref = (GQTableRef) node.getContent();
		String tableName = tref.getTable().getTableName();		
		tableName = StringFunc.undelimitName(tableName, '"');

		query.setParameter("source", tableName);

		// Set the table
		this.table = tref.getTable();
	}


	/**
	 * Builds LIMIT clause.
	 *
	 * @param node
	 *     LIMIT node
	 * @param query
	 *     query being built
	 * @throws SQLException
     *     if an error occurs
	 */
	@Override
	protected void buildLimit(LQLimitNode node, WebQuery query) throws SQLException
	{
	    // IINQ supports LIMIT/OFFSET using count and offset parameters
	    if(node.hasOffset())
        {   // Offset works for Jobs exported as XML/JSON but not for CSV it seems.  Not sure why.  So, force OFFSET to be promoted.
	        int start = node.getStart();
	        int count = node.getCount();
	        if (start > 0)
	            throw new SQLException("OFFSET is not supported");
	        
	        query.setParameter("count", ""+count);
	        // This would be the code to use for IINQ if using JSON Job output and supporting offsets
	        /*	        
	        query.setParameter("offset", ""+start);	        
	        */	        
        }
	    else
	    {
	        query.setParameter("count", ""+node.getCount());
	    }		
	}
	
	/**
     * DISTINCT clause is not supported.
     * 
     * @param node
     *     DISTINCT node
     * @param in
     *     query being built
     * @throws SQLException 
     *     if an error occurs
     */
	@Override
	protected void buildDistinct(LQDupElimNode node, WebQuery in) throws SQLException
    {
        throw new SQLException("DISTINCT is not supported.");       
    }
	
	/**
	 * Builds a comparison (e.g. A = 5).
	 * 
	 * @param condition
	 *      condition node converted
	 * @param query
	 *      IINQ query being built
	 * @return
	 *      String representation of condition
	 * @throws SQLException
	 *      if a conversion error occurs
	 */
	private String buildComparison(LQCondNode condition, WebQuery query) throws SQLException
    {        
        String op = condition.getContent().toString();                    

        // Supports comparison operators.  Add a filter.
        int type = condition.getType();
        if (type == LQTreeConstants.COMPARISON_OP)
        {
            LQNode left = condition.getChild(0);
            LQNode right = condition.getChild(1);                                 
            
            if (op.equals("=") ||
                    op.equals(">") || op.equals("<") || op.equals("!=") || op.equals("<>")|| op.equals("<>") || op.equals("<=") || op.equals(">="))
            {
                String leftExpr = buildExpression((LQExprNode) left, query);
                String rightExpr = buildExpression((LQExprNode) right, query);
                if (right.getType() == LQTreeConstants.IDENTIFIER)
                    throw new SQLException("Attribute to attribute comparisons not currently supported.");
                                
                return leftExpr+op+rightExpr;
            }
            else if (op.equals("IS"))
            {
                String leftExpr = buildExpression((LQExprNode) left, query);
                String rightExpr = buildExpression((LQExprNode) right, query);
                if (rightExpr.equals("NOT NULL"))
                    return leftExpr+"=\"*\"";
				return "NOT "+leftExpr+"=\"*\"";                
            }
        }
       
        throw new SQLException("Comparison operator not supported: "+op);    
    }		
    
	/**
	 * Converts a QuandlQuery into a string.
	 * 
	 * @return
	 * 		query string
	 * @throws SQLException
	 * 		if an error occurs
	 */
	public String toQueryString() throws SQLException 
	{
		return this.toQuery().toString();
	}
	
	@Override
	public String toString() 
	{
		try 
		{
			return this.toQueryString();
		} 
		catch (Exception e) 
		{	
			e.printStackTrace();
		}
		return "";
	}
}
