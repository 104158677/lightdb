package ed.inf.adbs.lightdb;

import java.util.ArrayList;

/**
 * The tuple class, store schema and values of tuples.
 */
public class Tuple {

    private final ArrayList<String> schema;
    private final ArrayList<Integer> values; //we assume that all values are integers.

    //tuple constructor
    public Tuple(ArrayList<String> schema, ArrayList<Integer> values) {
        this.schema = schema;
        this.values = values;
    }

    public ArrayList<Integer> getValues() {
        return values;
    }

    public ArrayList<String> getSchema() {
        return schema;
    }

}