package ed.inf.adbs.lightdb;

import java.util.*;
import java.util.stream.Collectors;

public class SumOperator extends Operator {
    private final Operator childOperator;
    private final String columnName;
    private final List<String> groupByColumns;
    private final List<Tuple> results = new ArrayList<>();
    private ArrayList<String> newSchema = new ArrayList<>();


    public SumOperator(Operator child, String columnName, List<String> groupByColumns) {
        this.childOperator = child;
        this.columnName = columnName;
        this.groupByColumns = new ArrayList<>(groupByColumns);
    }





    @Override
    public Tuple getNextTuple() {
        return  null;
    }

    @Override
    public void reset() {
    }

    public ArrayList<Tuple> dump() {
        return super.dump();
    }

}