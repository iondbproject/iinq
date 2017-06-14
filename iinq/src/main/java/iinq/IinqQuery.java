package iinq;

import unity.generic.query.WebQuery;


/**
 * Stores a representation of an IINQ query.
 */
public class IinqQuery extends WebQuery
{    		
	/**
	 * Constructs an IINQ query.
	 * 
	 * @param url
	 * 		query URL 
	 */
	public IinqQuery(String url)
	{
	    super(url);			
	}    
	
	/**
	 * Generates the C code for this SQL query.
	 * 
	 * @return
	 * 		code to execute query
	 */
	public String generateCode()
	{
		return "CODE!!!";
	}
}