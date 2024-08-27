package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import net.sf.jsqlparser.expression.Expression;

public class SelectOperator extends Operator {

    private Expression expression;//target expression
    private ScanOperator scanOperator; //child operator

    /*
    constructor
    @params Expression expression: target expression
    @params ScanOperator scanOperator: child operator
     */
    public SelectOperator(Expression expression, ScanOperator scanOperator) {
        this.scanOperator = scanOperator;
        this.expression = expression;
    }

    /*
    use the SelectExprDePaser to check if the tuple meet the requirements
    @return tuple with new schema required by the select phase
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple = scanOperator.getNextTuple();
        while (tuple != null){
            SelectExprDePaser selectExprDePaser = new SelectExprDePaser(tuple); //create a new instance
            expression.accept(selectExprDePaser);
            if (selectExprDePaser.check()) {//check
                return tuple;
            }
            tuple = scanOperator.getNextTuple();//go next
        }
        return null;
    }

    @Override
    public void reset() {
        scanOperator.reset();
    }

    @Override
    public ArrayList<Tuple> dump() {
        return super.dump();
    }
}