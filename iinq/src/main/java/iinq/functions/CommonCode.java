package iinq.functions;

public final class CommonCode {
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

	public static String wrapInExecuteFunction(String codeFragment) {
		return "execute(" + codeFragment + ")";
	}
}
