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

	private static final String interfaceDir = "../../iondb/src/iinq/iinq_interface/";

	private static final String interfaceUserFile = interfaceDir + "iinq_user.c";
	private static final String interfaceOutputDirectory = interfaceDir;
	private static final String interfaceOutputName = "iinq_user_functions";

	private static final String testingDirectory = "../../iondb/src/iinq/iinq_testing/";
	private static final String testingUserFile = testingDirectory + "test_iinq_device.c";
	private static final String testingOutputDirectory = testingDirectory + "/generated";
	private static final String testingOutputName = "iinq_testing_functions";

	private static final String useExisting = "false";
	
	private static void setSystemPropertiesForIinqInterface() {
		setSystemProperties(interfaceUserFile, interfaceOutputDirectory, interfaceOutputName, interfaceDir, useExisting);
	}

	private static void setSystemPropertiesForPlanckUnit() {
		setSystemProperties(testingUserFile, testingOutputDirectory, testingOutputName, interfaceDir, useExisting);
	}

	private static void setSystemProperties(String userFile, String outputDirectory, String outputName, String interfaceDirectory, String useExisting) {
		System.setProperty("USE_EXISTING", useExisting);
		System.setProperty("USER_FILE", userFile);
		System.setProperty("OUTPUT_DIRECTORY", outputDirectory);
		System.setProperty("OUTPUT_NAME", outputName);
		System.setProperty("INTERFACE_DIRECTORY", interfaceDirectory);
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
		System.setProperty("COMMENT_OUT_EXISTING_FUNCTIONS", "false");

		Path sourceFile = Paths.get("src/test/c/IinqInsertAndSelectSingleTablePlanckUnitTestCode.c");
		Path destFile = Paths.get(testingUserFile);

		Files.copy(sourceFile, destFile, REPLACE_EXISTING);

		IinqExecute.main(null);
	}

}
