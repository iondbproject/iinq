package iinq;

/**
 * Exception thrown when a clause requires metadata that was not given
 */
class RequiresSchemaException extends Exception
{
	// Parameterless Constructor
	public RequiresSchemaException() {}

	// Constructor that accepts a message
	public RequiresSchemaException(String message)
	{
		super(message);
	}
}
