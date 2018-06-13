package unity.generic.query;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.TreeMap;

import unity.engine.Relation;
import unity.operators.ArrayListScan;
import unity.operators.Operator;
import unity.query.GQFieldRef;
import unity.query.GlobalQuery;
import unity.query.LQExprNode;
import unity.query.LQProjNode;
import unity.query.LQTreeConstants;


/**
 * A base class for performing REST API (web queries) with parameters for execution.
 * Subclasses will override methods for query execution.
 */
public class WebQuery 
{    
	/**
	 * Query URL
	 */
	protected String url;
	
	/**
	 * Query parameters 
	 */
	protected HashMap<String, Object> parameters;
	
	/**
	 * Query output relation
	 */
	protected Relation relation;
	
	/**
     * Input table relation
     */
	protected Relation tableRelation;
    
	/**
	 * Projection node describing fields
	 */
	protected LQProjNode proj;
	
	/**
	 * Connection properties (used for user id and password)
	 */
	protected Properties prop;
	
	
	/**
	 * Constructs a query.
	 * 
	 * @param url
	 * 		query URL 
	 */
	public WebQuery(String url)
	{
		this.url = url;		
		this.parameters = new HashMap<String, Object>();		
	}

	/**
	 * Sets the connection properties.
	 * 
	 * @param properties
	 *      connection properties
	 */
	public void setProperties(Properties properties)
	{
	    this.prop = properties;
	}
	
	/**
	 * Sets the query URL.
	 * 
	 * @param url
	 * 		query URL 
	 */
	public void setURL(String url)
	{
		this.url = url;			
	}
	
	/**
     * Gets the query URL.
     * 
     * @return
     *      query URL 
     */
    public String getURL()
    {
        return this.url;         
    }
    
    
	/**
     * Sets the query parameters.
     * 
     * @param parameters
     *      query parameters
     */
    public void setParameters(HashMap<String, Object> parameters)
    {
        this.parameters = parameters;         
    }
    
    /**
     * Sets a query parameter.
     * 
     * @param key
     *      key
     * @param value
     *      value
     */
    public void setParameter(String key, Object value)
    {
        this.parameters.put(key, value);         
    }
    
    /**
     * Gets a query parameter.
     * 
     * @param key
     *      key
     * @return
     *      value
     */
    public String getParameter(String key)
    {
        Object val = this.parameters.get(key);
        if (val != null)
            return val.toString();
        return null;
    }
    
    /**
     * Gets a query parameter.
     * 
     * @param key
     *      key
     * @return
     *      value
     */
    public Object getParameterObject(String key)
    {
        return this.parameters.get(key);        
    }
    
	/**
	 * Runs the query by ...
	 * 	
	 * @return
	 * 		results 
	 * @throws SQLException
	 * 		if an error occurs
	 */	
	public Operator run() throws SQLException 
	{	
	    // This code is connection specific?
	    /*
	    String tableName = this.relation.getProperty("name");
	    	    	   
	    ArrayList<Tuple> results = ServerConnection.getRecords(this.url, tableName, this.parameters, this.tableRelation, this.prop);	   
	    
	    if (results == null)
	        throw new SQLException("ERROR: No response returned from server to query.");
	    */
	    // Operator op = new ArrayListScan(results, this.tableRelation);
	    	    	    
	    Operator op = new ArrayListScan(null, this.tableRelation);
	    // This code needs to be kept.
	    
		// Now perform a projection (if any)	    
		if (this.proj != null)
		{
			if (this.proj.isSelectAll())
			{	// Do nothing.  Return tuples AS IS.
			}
			else
			{	// Build a projection operator			    
				this.proj.getChild(0).setOutputRelation(op.getOutputRelation());
				// Fix up the output relation so that build projection will find fields even if have aliases
				ArrayList<LQExprNode> expressions = this.proj.getExpressions();
		        for (LQExprNode field : expressions) 
		        {       		            		            
		            if (field.getType() == LQTreeConstants.AS_IDENTIFIER)
		            {    field = (LQExprNode) field.getChild(0);
		                 
		                 if (field.getType() == LQTreeConstants.IDENTIFIER)
		                 {    
		                     GQFieldRef fref = (GQFieldRef) field.getContent();
		                     String fieldName = fref.getField().getColumnName();
		                     // Find field in relation
		                     int fieldIndex = this.relation.getAttributeIndexByName(fieldName);
		                     if (fieldIndex >= 0)
		                         this.relation.getAttribute(fieldIndex).setReference(fref);
		                 }
		            } 
		        }
		        
				op = this.proj.buildOperator(new Operator[]{op}, new GlobalQuery(), null);				
			}
		}
		
		this.setRelation(this.proj.getOutputRelation());		
		
		return op;
	}
	
	/**
	 * Returns the query string.
	 * 
	 * @return
	 * 		query string
	 */
	public String getQueryString()
	{	return this.url;	
	}
	
	@Override
	public String toString()
	{
	    TreeMap<String, Object> params = new  TreeMap<String, Object>();
	    params.putAll(this.parameters);
		return this.url+" Parameters: "+params; //$NON-NLS-1$
	}
	
	/**
	 * Sets the output relation.
	 * 
	 * @param relation
	 * 		output relation for query
	 */
	public void setRelation(Relation relation) 
	{
		this.relation = relation;
	}

	/**
	 * Gets the output relation.
	 * 
	 * @return
	 * 		output relation for query
	 */
	public Relation getRelation()					
	{	
		return this.relation;
	}
	
	/**
     * Sets the projection node.
     * 
     * @param proj
     *      projection node
     */
    public void setProjection(LQProjNode proj) 
    {
        this.proj = proj;
    }
    
    /**
     * Sets the relation of the input table.  This may differ from the output relation if renaming is performed.  
     * 
     * @param relation
     *      relation of the input table
     */
    public void setTableRelation(Relation relation) 
    {
        this.tableRelation = relation;
    }
}