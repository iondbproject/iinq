package iinq;

public class IinqFunction {
	private String name;
	private String definition;

	public IinqFunction(String name, String definition) {
		setName(name);
		setDefinition(definition);
	}

	@Override
	public String toString() {
		return String.format("Name: %s, " +
				"Definition: %s", this.name, this.definition);
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDefinition() {
		return definition;
	}

	public void setDefinition(String definition) {
		this.definition = definition;
	}


}
