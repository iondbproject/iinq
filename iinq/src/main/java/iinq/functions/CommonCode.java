package iinq.functions;

public final class CommonCode {
	public static enum ReturnType {
		EMPTY_STRING,
		EMPTY_NUMERIC,
		ERROR_VALUE,
		EMPTY_RETURN
	}

	public static String
	error_check(int extraIndents) {
		StringBuilder errorCheck = new StringBuilder();
		for (int i = 0; i < extraIndents; i++)
			errorCheck.append("\t");
		errorCheck.append("\tif (err_ok != error) {\n");
		for (int i = 0; i < extraIndents; i++)
			errorCheck.append("\t");
		errorCheck.append("\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n");
		for (int i = 0; i < extraIndents; i++)
			errorCheck.append("\t");
		errorCheck.append("\t\treturn; \n");
		for (int i = 0; i < extraIndents; i++)
			errorCheck.append("\t");
		errorCheck.append("\t}\n\n");
		return errorCheck.toString();
	}
	public static String
	error_check() {
		return error_check(0);
	}

	public static String
	errorCheckResultSet(String varName) {
		return errorCheckResultSet(varName, false);
	}

	public static String
	errorCheckResultSet(String varName, boolean pointer) {
		if (pointer)
			return "\tif (err_ok != " + varName + "->status.error)\n" +
					"\t\treturn " + varName + ";\n\n";
		else
			return "\tif (err_ok != " + varName + ".status.error)\n" +
				"\t\treturn " + varName + ";\n\n";
	}

	public static String
	errorCheckWithReturn() {
		return errorCheckWithReturn("error", ReturnType.ERROR_VALUE);
	}

	public static String
	errorCheckWithEmptyReturn(String errorVar) {
		return errorCheckWithReturn(errorVar, ReturnType.EMPTY_RETURN);
	}

	public static String
	errorCheckWithReturn(String errorVar, ReturnType returnType) {
		switch (returnType) {
			case EMPTY_STRING:
				return "\tif (err_ok != " + errorVar + ")\n" +
						"\t\treturn \"\";\n\n";
			case EMPTY_NUMERIC:
				return "\tif (err_ok != " + errorVar + ")\n" +
						"\t\treturn -1;\n\n";
			case ERROR_VALUE:
				return "\tif (err_ok != " + errorVar + ")\n" +
						"\t\treturn " + errorVar + ";\n\n";
			case EMPTY_RETURN:
			default:
				return 	"\tif (err_ok != " + errorVar + ")\n" +
						"\t\treturn;\n\n";
		}

	}

	public static String wrapInExecuteFunction(String codeFragment) {
		return "execute(" + codeFragment + ")";
	}
}
