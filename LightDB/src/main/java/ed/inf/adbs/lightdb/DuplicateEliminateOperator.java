package ed.inf.adbs.lightdb;

import java.util.ArrayList;

/*
The DuplicateEliminateOperator class, it is used to implement the distinct keyword. My implementation is
sorting-based, after sorting the potential output by some particular order, the getNextTuple decides
whether the tuple is valid to be output.
@params op, the operator
@params pastaPle, the previous tuple used as a benchmark.
 */
public class DuplicateEliminateOperator extends Operator{
    private Operator op;
    private Tuple pastaPle;

    //constructor
    public DuplicateEliminateOperator(Operator operator) {
        this.op = operator;
    }

    @Override
    /*
    It decides the output. if the next tuple is null(the end of the file), then return null,
    if the previous tuple is null(meaning the tuple is at the beginning of the file), then replace
    it to pastaPle. Finally check if the current tuple equals to the previous one, if so, ignore
    it and go to the next tuple. If not identical, then the tuple passes and return it.
    @return distinct tuple after check.
     */
    public Tuple getNextTuple() {
        Tuple tuple = op.getNextTuple();
        if (tuple == null)
            return null;
        if (pastaPle == null){
            pastaPle = tuple;
            return tuple;
        }
        if (pastaPle.getValues().equals(tuple.getValues()))
            return getNextTuple();
        pastaPle = tuple;
        return tuple;
    }

    @Override
    public void reset() {
        op.reset();
    }

    @Override
    public ArrayList<Tuple> dump() {
        return super.dump();
    }
}
