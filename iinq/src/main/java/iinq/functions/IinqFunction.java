package iinq.functions;

public abstract class IinqFunction {
	private String name;
	private String header;
	private String definition;

	public IinqFunction() {
		setName(null);
		setHeader(null);
		setDefinition(null);
	}

	public IinqFunction(String name, String header, String definition) {
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

	public void setName(String name) {
		this.name = name;
	}

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getDefinition() {
		return definition;
	}

	public void setDefinition(String definition) {
		this.definition = definition;
	}

}
