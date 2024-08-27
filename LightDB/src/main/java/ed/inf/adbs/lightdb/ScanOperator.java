package ed.inf.adbs.lightdb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/*
scan all tuples from the target table
 */
public class ScanOperator extends Operator{
    private BufferedReader reader; //bufferedreader to scan the target file.
    private String table; //table name
    private String path; //path to the table file
    private ArrayList<String> newSchema = new ArrayList<>();

    /*
    constructor, create a DatabaseCatalog instance to read from the target table using bufferedreader.
    @params table: table name, alias or not
     */
    public ScanOperator(String table) {
        try {
            this.table = table;
            this.path = DatabaseCatalog.getPath(Alias.getTableAlias().get(table));
            reader = new BufferedReader(new FileReader(path));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        ArrayList<String> tbSchema = DatabaseCatalog.getSchema(Alias.getTableAlias().get(table));
        for (String string: tbSchema)
            newSchema.add(table+"#"+string); //format the table name, either using alias or not
    }

    /*
    get the next tuple from the table
    @return next tuple
     */
    public Tuple getNextTuple() {
        try {
            String line = reader.readLine();
            if (line == null) return null;
            ArrayList<Integer> values = new ArrayList<>();
            String[] raw = line.split(", ");
            for (String s : raw) {
                values.add(Integer.parseInt(s));
            }
            return new Tuple(newSchema, values);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //create a new bufferedreader to read from the start of the file
    @Override
    public void reset() {
        try {
            reader.close();
            reader = new BufferedReader(new FileReader(path));
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public ArrayList<Tuple> dump() {
        return super.dump();
    }
}