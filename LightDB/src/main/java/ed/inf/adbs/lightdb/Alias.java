package ed.inf.adbs.lightdb;

import java.util.HashMap;

/**
 * A singleton class to save aliases of tables. The mapping from table aliases to the real table names are
 * stored in the hashmap tableAlias. If there is no alias, the aliases and real table names are the same(i.e.
 * hashmap stores A = A).
 */
public class Alias {
    private static HashMap<String, String> tableAlias = new HashMap<>();//store the mapping

    //basic constructor
    private Alias(HashMap<String, String> tableAlias){
        Alias.tableAlias = tableAlias;
    }

    //getInstance method of the singleton class
    //@params HashMap<String, String> tableAlias, the hashmap storing the mapping from alias to tables.
    //@return return an Alias instance.
    public static Alias getInstance(HashMap<String, String> tableAlias){
        return new Alias(tableAlias);
    }

    //@return HashMap<String, String> return the mapping from aliases to tables.
    public static HashMap<String, String> getTableAlias() {
        return tableAlias;
    }
}