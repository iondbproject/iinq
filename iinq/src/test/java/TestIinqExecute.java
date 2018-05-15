import com.sun.javaws.exceptions.InvalidArgumentException;
import iinq.IinqExecute;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.management.relation.RelationNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.SQLException;

import java.nio.file.Path;
import java.nio.file.Files;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class TestIinqExecute {

	public static final String directory = "../../iondb/src/iinq/iinq_interface/";
	public static final String user_file = "../../iondb/src/iinq/iinq_interface/iinq_user.c";
	public static final String function_file = "../../iondb/src/iinq/iinq_interface/iinq_user_functions.c";
	public static final String function_header_file = "../../iondb/src/iinq/iinq_interface/iinq_user_functions.h";
	public static final String use_existing = "false";

	@BeforeClass
	public static void setSystemProperties() {
		System.setProperty("USE_EXISTING", use_existing);
		System.setProperty("USER_FILE", user_file);
		System.setProperty("FUNCTION_FILE", function_file);
		System.setProperty("FUNCTION_HEADER_FILE", function_header_file);
		System.setProperty("DIRECTORY", directory);
	}

	@Test
	public void testFull() throws SQLException, InvalidArgumentException, RelationNotFoundException, IOException {
		Path sourceFile = Paths.get("src/test/c/IinqTestCode.c");
		Path destFile = Paths.get(user_file);

		Files.copy(sourceFile, destFile, REPLACE_EXISTING);

		IinqExecute.main(null);
	}

}
