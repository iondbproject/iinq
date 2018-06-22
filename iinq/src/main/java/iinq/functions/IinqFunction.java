package iinq.functions;

public abstract class IinqFunction {
	private String name;
	private String header;
	private String definition;

	protected IinqFunction() {
		setName(null);
		setHeader(null);
		setDefinition(null);
	}

	protected IinqFunction(String name, String header, String definition) {
		setName(name);
		setHeader(header);
		setDefinition(definition);
	}

	@Override
	public String toString() {
		return String.format("Name: %s, " +
				"Header: %s, " +
				"Definition: %s", this.name, this.header, this.definition);
	}

	public String getName() {
		return name;
	}

	void setName(String name) {
		this.name = name;
	}

	public String getHeader() {
		return header;
	}

	void setHeader(String header) {
		this.header = header;
	}

	public String getDefinition() {
		return definition;
	}

	protected void setDefinition(String definition) {
		this.definition = definition;
	}

}
