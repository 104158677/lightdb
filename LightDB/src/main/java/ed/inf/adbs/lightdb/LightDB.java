package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.stream.Collectors;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Lightweight in-memory database system
 *
 */
public class LightDB {

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("Usage: LightDB database_dir input_file output_file");
			return;
		}

		String databaseDir = args[0];
		String inputFile = args[1];
		String outputFile = args[2];

		// Just for demonstration, replace this function call with your logic
		executeQuery(databaseDir, inputFile, outputFile);
	}

	/**
	 * Example method for getting started with JSQLParser. Reads SQL statement from
	 * a file and prints it to screen; then extracts SelectBody from the query and
	 * prints it to screen.
	 */

	public static void executeQuery(String databaseDir, String inputFile, String outputFile) {
		try {
			Statement statement = CCJSqlParserUtil.parse(new FileReader(inputFile));
			DatabaseCatalog.getInstance(databaseDir);
			if (statement != null) {
				System.out.println("Read statement: " + statement);
				QueryPlanner queryPlanner = new QueryPlanner(statement);
				ArrayList<Tuple> results = queryPlanner.dump();
				File theDir = new File("samples/output");
				if (!theDir.exists()){ //create the output dir if not exist
					theDir.mkdirs();
				}
				FileWriter fileWriter = new FileWriter(outputFile);
				if (results != null && !results.isEmpty()){
					System.out.println(results.get(0).getSchema());
					for (Tuple tuple: results){
						String output = tuple.getValues().stream().map(Object::toString).collect(Collectors.joining(","));
						fileWriter.append(output);
						fileWriter.append("\n");
						System.out.println(output);
					}
				}
				else {System.out.println("Null or Empty");}
				fileWriter.flush();
				fileWriter.close();
			}
		} catch (Exception e) {
			System.err.println("Exception occurred");
			e.printStackTrace();
		}
	}
}
