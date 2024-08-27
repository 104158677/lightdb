package ed.inf.adbs.lightdb;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
public class JoinOperator extends Operator{

    private Expression expression;//expression to be processed
    private Operator leftOp; //the left child operator
    private Operator rightOp; //the right child operator
    private Tuple current; //current tuple from the left child operator


    /*
    constructor
    @params expression
    @params leftOp
    @params rightOp
     */

    public JoinOperator(Expression expression, Operator leftOp, Operator rightOp) {
        this.expression = expression;
        this.leftOp = leftOp;
        this.rightOp = rightOp;
        this.current = leftOp.getNextTuple();
    }

    /*
    override the getNextTuple method to implement a join. Basically traverse two operators
    and use the SelectExprDePaser to check if the tuple formed by two child operators meet
    the join condition.
     */
    @Override
    public Tuple getNextTuple() {
        //the last tuple of the left(outer) child operator, return null
        if (current == null) {
            return null;
        }
        Tuple rightTuple = rightOp.getNextTuple();
        /* traverse both operators and check if the new tuple formed by both child operators
        meet the required expression.
         */
        while (rightTuple != null){
            // construct a new tuple from the two operators
            ArrayList<Integer> newValue = new ArrayList<>();
            newValue.addAll(current.getValues());
            newValue.addAll(rightTuple.getValues());
            ArrayList<String> newSchema = new ArrayList<>();
            newSchema.addAll(current.getSchema());
            newSchema.addAll(rightTuple.getSchema());
            Tuple tuple = new Tuple(newSchema, newValue);
            //if the expression is null, then we conduct a cross product.
            if (expression == null) {
                return tuple;
            }
            //check if the new tuple meets the expression
            SelectExprDePaser selectExprDePaser = new SelectExprDePaser(tuple);
            expression.accept(selectExprDePaser);
            if (selectExprDePaser.check()) {
                return tuple;
            }
            rightTuple = rightOp.getNextTuple();//go to the next tuple of the right operator.
        }
        current = leftOp.getNextTuple(); //go to the next tuple of the left operator.
        if (current != null){ //reset the right operator to check the next tuple from the left operator.
            rightOp.reset();
            return getNextTuple();
        }
        return null;
    }

    /*
    reset both child operators and set the current tuple to be null.
     */
    @Override
    public void reset() {
        leftOp.reset();
        rightOp.reset();
        current = null;
    }

    @Override
    public ArrayList<Tuple> dump() {
        return super.dump();
    }
}
