package unity.generic.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import unity.annotation.SourceField;
import unity.annotation.SourceTable;
import unity.engine.Attribute;
import unity.engine.Relation;
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
 * An abstract class for converting a logical query tree into a URL query (REST API) for a web service.  Subclasses override the methods that convert each relational operator.
 * 
 */
@SuppressWarnings("nls")
public abstract class QueryBuilder 
{
    /**
     * Root node of logical query tree
     */
	protected LQNode startNode;
	
	/**
	 * Table queried (only one allowed at a time)
	 */
	protected SourceTable table;
	
	/**
	 * Stores the first (closest to the root) projection as that is the projection converted
	 */
	protected LQProjNode firstProj = null;
	
	/**
	 * Flag to indicate if have previously seen a table identifier in FROM (only one table is allowed).
	 */
	protected boolean fromSeen;
	
	/**
	 * Base URL of data source
	 */
	protected String serverURL;
	
	
	/**
	 * Constructor that takes a logical query tree as input.
	 * 
	 * @param serverURL 
	 * 		URL location of server/data source
	 * @param startNode
	 * 		root node of logical query tree
	 */
	public QueryBuilder(String serverURL, LQNode startNode) 
	{
		this.startNode = startNode;	
		this.serverURL = serverURL;
	}
		
	
	/**
	 * Process tree in post-order fashion (bottom-up).
	 * 
	 * @param query
	 *      generated query
	 * @param currentNode
	 *      current logical query tree node being processed (recursively)
	 * @throws SQLException 
	 *      if an error occurs
	 */
	protected void processNode(WebQuery query, LQNode currentNode) throws SQLException
	{
		if (currentNode != null) 
		{	
		    // Even though post-order, need to identify first projection node first
		    if (currentNode.getType() == LQTreeConstants.PROJECTION && this.firstProj == null)
		        this.firstProj = (LQProjNode) currentNode;
		    
		    // Process children first
		    if (currentNode.getNumChildren() > 1) 
                throw new SQLException("Too many children found at node " + currentNode.toString());
            
		    if (currentNode.getNumChildren() > 0)
		        processNode(query, currentNode.getChild());
		    
			switch (currentNode.getType()) 
			{			
				case LQTreeConstants.PROJECTION:
					buildProjection((LQProjNode) currentNode, query);
					break;
				
				case LQTreeConstants.SELECTION:
					buildSelection((LQSelNode) currentNode, query);
					break;
				
				case LQTreeConstants.TABLE: 
				case LQTreeConstants.IDENTIFIER:
					buildFrom(currentNode, query);
					break;
				
				case LQTreeConstants.LIMIT:
					buildLimit((LQLimitNode) currentNode, query);
					break;
				
				case LQTreeConstants.ORDERBY:
					buildOrderBy((LQOrderByNode) currentNode, query);
					break;
				
				case LQTreeConstants.GROUPBY:
					buildGroupBy((LQGroupByNode) currentNode, query);
					break;
				
				case LQTreeConstants.DUPLICATE_ELIMINATION: 
					buildDistinct((LQDupElimNode) currentNode, query);
					break;
				
				default:
					throw new SQLException("Unable to process query operation.  Operation not supported by driver: " + currentNode);
			}		
		}			
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
	protected void buildProjection(LQProjNode node, WebQuery query) throws SQLException 
	{	    
	    if (node != this.firstProj)
	        return;
	    
	    // Set the query's projection node
		query.setProjection(node);
		
		// Validate the expressions to see if can be process.
		// Cannot process functions or expressions only base fields.
		ArrayList<LQExprNode> expressions = node.getExpressions();
		HashMap<String, LQExprNode> fields = new HashMap<String, LQExprNode>();
		for (LQExprNode field : expressions) 
		{								
			if (field.getType() == LQTreeConstants.AS_IDENTIFIER)
				field = (LQExprNode) field.getChild(0);	// throw away it's second child, the alias (for now?).

			if (field.getType() == LQTreeConstants.ARITHMETICFUNCTION)
				throw new SQLException("Expressions in projection are not supported.");
			
			if (field.getType() == LQTreeConstants.EXPRESSION) 
			{
				field = ((LQExprNode) field.getContent());
				throw new SQLException("Expressions in projection are not supported.");			
			}
			
			String[] components = StringFunc.divideId(field.toString());
			String name = components[0];
			if (components.length > 1)
				name = components[components.length-1];		// Get only field table_id
			
			if (name.equals("*"))
			{
				// no projections required, everything is included.
			    node.setSelectAll(true);
				break;
			}

			fields.put(name, field);
		}
		
		// Create a relation from all the fields in this table		
        Iterator<SourceField> it = this.table.fieldIterator();
        Attribute[] attr = new Attribute[expressions.size()];
        int pos = 0;

        
        while (it.hasNext())
        {   SourceField sf = it.next();
            
        	// Check if field is in list
            String fieldName = sf.getColumnName();
            LQExprNode field = fields.get(fieldName);
            if (field == null)
                field = fields.get(StringFunc.delimitName(fieldName, '"')); // Test a delimited field as well
            if (field == null)
            {  
            	// Field is not in output
                continue;
            }
            GQFieldRef fref = (GQFieldRef) field.getContent(); 
            attr[pos++] = new Attribute(fieldName, sf.getDataType(), sf.getColumnDisplaySize(), fref);            
        }
        Relation r = new Relation(attr);
        query.setTableRelation(r);
        r.setProperty("table_id", this.table.getTableName());
        query.setRelation(r);
        
        // Set output relation of input to projection node (most likely table)
        node.getChild(0).setOutputRelation(r);
	}
	
	
	
	/**
	 * Retrieves base field table_id to use in query rather than an alias in the field reference.
	 * 
	 * @param obj
	 *     object that may be a GQFieldRef 
	 * @return 
	 *     correct field table_id for querying
	 */
	@SuppressWarnings("null")
	protected String getFieldName(Object obj)
	{	    
        if (obj != null && obj instanceof GQFieldRef)
        {   // Retrieve base table_id for field not the GQFieldRef
            SourceField sf = ((GQFieldRef) obj).getField();
            return sf.getColumnName();
        }
        return obj.toString();
	}
	
	/**
	 * Builds an expression.  Only supports equality comparisons between an attribute and a constant.
	 * 
	 * @param condition
	 * 		filter condition
	 * @param query 
     *     query being built
	 * @throws SQLException 
	 * 		if an error occurs
	 */		
	protected void buildExpression(LQCondNode condition, WebQuery query) throws SQLException 
	{
	    throw new SQLException("Expression not supported: "+condition.toString());
	    
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
	protected void buildSelection(LQSelNode node, WebQuery query) throws SQLException 
	{
		// LQCondNode condition = node.getCondition();		
		// this.buildCondition(condition, query);		
	}	
	
	/**
     * Builds a GROUP BY operator.  
     * 
     * @param node
     *     grouping node
     * @param query
     *     query being built
     * @throws SQLException
     *     if an error occurs
     */
	protected void buildGroupBy(LQGroupByNode node, WebQuery query) throws SQLException 
	{
	    throw new SQLException("GROUP BY is not supported.");
	}
	
	/**
	 * Handles an ORDER BY node.  Supported for single attribute with no expressions.
	 *  
	 * @param node
	 *     order by node
	 * @param query
	 *     query being built
	 * @throws SQLException
     *     if an error occurs
	 */
	protected void buildOrderBy(LQOrderByNode node, WebQuery query) throws SQLException
	{
	    throw new SQLException("ORDER BY is not supported.");					
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
	protected void buildFrom(LQNode node, WebQuery query) throws SQLException 
	{
		if(this.fromSeen) // join exception.
			throw new SQLException("Two tables were received. The driver only supports single table queries.");
		
		this.fromSeen = true;
	
		GQTableRef tref = (GQTableRef) node.getContent();
		@SuppressWarnings("unused")
        String tableName = tref.getTable().getTableName();			
		
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
	protected void buildLimit(LQLimitNode node, WebQuery query) throws SQLException
	{
	    throw new SQLException("LIMIT/OFFSET is not supported.");				
	}
	
	/**
     * DISTINCT clause is not supported.
     * 
     * @param node
     *     DISTINCT node
     * @param query
     *     query being built
     * @throws SQLException 
     *     if an error occurs
     */
	protected void buildDistinct(LQDupElimNode node, WebQuery query) throws SQLException
    {
        throw new SQLException("DISTINCT is not supported.");       
    }		
}
