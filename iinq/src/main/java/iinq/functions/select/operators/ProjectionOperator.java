package iinq.functions.select.operators;

import iinq.callable.IinqProjection;
import iinq.functions.select.operators.destroy.ProjectionDestroyFunction;
import iinq.functions.select.operators.init.ProjectionInitFunction;
import iinq.functions.select.operators.next.ProjectionNextFunction;
import iinq.functions.select.operators.struct.ProjectionStruct;

public class ProjectionOperator extends IinqOperator {
	protected IinqProjection projection;

	public ProjectionOperator(IinqProjection projection, IinqOperator inputOperator) {
		super("projection", new ProjectionInitFunction(), new ProjectionNextFunction(), new ProjectionDestroyFunction());
		this.projection = projection;
		projection.setOperator(this);
		this.inputOperators.add(inputOperator);
		this.struct = new ProjectionStruct();
	}

	public String generateInitFunctionCall() {
		return String.format("%s(%s, %d, %s)", getInitFunction().getName(), getInputOperators().get(0).generateInitFunctionCall(), projection.getNumFields(), projection.getIinqProjectionList());
	}
}
