package iinq.functions;

public final class CommonCode {
	public static String
	error_check() {
		return "\tif (err_ok != error) {\n" +
				"\t\tprintf(\"Error occurred. Error code: %i" + "\\" + "n" + "\", error);\n" +
				"\t\treturn; \n" +
				"\t}\n\n";
	}

	public static String wrapInExecuteFunction(String codeFragment) {
		return "execute(" + codeFragment + ")";
	}
}
