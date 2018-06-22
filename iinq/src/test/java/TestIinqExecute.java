import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.IinqExecute;
import org.junit.Test;

import javax.management.relation.RelationNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;

import java.nio.file.Path;
import java.nio.file.Files;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class TestIinqExecute {

	public static final String interfaceDir = "../../iondb/src/iinq/iinq_interface/";
	public static final String interfaceUserFile = "../../iondb/src/iinq/iinq_interface/iinq_user.c";
	public static final String interfaceUserFunctions = "../../iondb/src/iinq/iinq_interface/iinq_user_functions.c";
	public static final String interfaceUserHeader = "../../iondb/src/iinq/iinq_interface/iinq_user_functions.h";

	public static final String testingDir = "../../iondb/src/iinq/iinq_interface/";
	public static final String testingUserFile = "../../iondb/src/iinq/iinq_interface/iinq_user.c";
	public static final String testingUserFunctions = "../../iondb/src/iinq/iinq_interface/iinq_user_functions.c";
	public static final String testingUserHeader = "../../iondb/src/iinq/iinq_interface/iinq_user_functions.h";

	public static final String useExisting = "false";
	
	public static void setSystemPropertiesForIinqInterface() {
		setSystemProperties(interfaceUserFile, interfaceUserFunctions, interfaceUserHeader, interfaceDir, useExisting);
	}

	public static void setSystemPropertiesForPlanckUnit() {
		setSystemProperties(testingUserFile, testingUserFunctions, testingUserHeader, testingDir, useExisting);
	}

	public static void setSystemProperties(String userFile, String userFunctions, String userHeader, String testingDir, String useExisting) {
		System.setProperty("USE_EXISTING", useExisting);
		System.setProperty("USER_FILE", testingUserFile);
		System.setProperty("FUNCTION_FILE", testingUserFunctions);
		System.setProperty("FUNCTION_HEADER_FILE", testingUserHeader);
		System.setProperty("DIRECTORY", testingDir);
	}

	@Test
	public void testFull() throws SQLException, InvalidArgumentException, RelationNotFoundException, IOException {
		setSystemPropertiesForIinqInterface();
		Path sourceFile = Paths.get("src/test/c/IinqFullTestCode.c");
		Path destFile = Paths.get(interfaceUserFile);

		Files.copy(sourceFile, destFile, REPLACE_EXISTING);

		IinqExecute.main(null);
	}

	@Test
	public void testInsertAndSelectSingleTable() throws IOException {
		setSystemPropertiesForIinqInterface();
		Path sourceFile = Paths.get("src/test/c/IinqInsertAndSelectSingleTableTestCode.c");
		Path destFile = Paths.get(interfaceUserFile);

		Files.copy(sourceFile, destFile, REPLACE_EXISTING);

		IinqExecute.main(null);
	}

	@Test
	public void testInsertAndSelectSingleTablePlanckUnit() throws IOException {
		setSystemPropertiesForPlanckUnit();

	}

}
