package ed.inf.adbs.lightdb;

import java.util.ArrayList;

/*
abstract class operator
 */
public abstract class Operator {

    //get the next tuple from the operator
    public abstract Tuple getNextTuple();

    //clean execution and reset the operator
    public abstract void reset();

    /*
    get all valid tuples from the operator
    @return ArrayList<Tuple> array list of tuples
     */
    public ArrayList<Tuple> dump() {
        ArrayList<Tuple> tuples = new ArrayList<>();
        while (true){
            Tuple tuple = getNextTuple();
            if (tuple == null)
                break;
            tuples.add(tuple);
        }
        return tuples;
    }

}