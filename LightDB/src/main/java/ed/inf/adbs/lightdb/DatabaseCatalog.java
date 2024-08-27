package ed.inf.adbs.lightdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/*
database catalog class, using a singleton pattern for the catalog keeping track of information of table
paths and schemas. Incl. basic constructor and getInstance function.
 */
public class DatabaseCatalog {

    private static HashMap<String, String> paths = new HashMap<>(); //store the paths
    private static HashMap<String, ArrayList<String>> schemas = new HashMap<>();//store the schemas
    String fileSeparator = File.separator;

    /*
    Constructor use a scanner to visit the schema file, saves the path to different csv files in the
    paths hashmap and saves the schemas of different csv files in the schemas hashmap.
    @params databaseDir the path to where databases are stored
     */
    DatabaseCatalog(String databaseDir) {
        try {
            String filepath = databaseDir+ fileSeparator + "schema.txt";
            Scanner scanner = new Scanner(new File(filepath));
            while(scanner.hasNextLine()){
                String schemaIns = scanner.nextLine();
                System.out.println(schemaIns);
                String[] info = schemaIns.split(" ", 2);
                paths.put(info[0], databaseDir+ fileSeparator + "data" + fileSeparator + info[0] + ".csv");
                ArrayList<String> cols = new ArrayList<>(Arrays.asList(info[1].split(" ")));
                schemas.put(info[0], cols);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /*
    getInstance of the singleton class
    @params String databaseDir, path to the database
    @return an instance of DatabaseCatalog
     */
    public static DatabaseCatalog getInstance(String databaseDir){
        return new DatabaseCatalog(databaseDir);
    }

    /*
    get the path of a table from the paths hashmap
    @params String table: table name.
    @return string of the path to the specific table.
     */
    public static String getPath(String table){
        return paths.get(table);
    }

    /*
    get the schema of a table from the schemas hashmap
    @params String table: table name.
    @return ArrayList<String> of the schemas of the specific table.
    */
    public static ArrayList<String> getSchema(String table){
        return schemas.get(table);
    }

}
