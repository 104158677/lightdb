package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.Stack;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

/*
The class to check if a tuple can pass certain expressions. It extends the ExpressionDePaser.
If the tuple meets the conditions, the indicator variable pass will be set to true, otherwise false.
Either the value of the corresponding column or the long value itself will be used to check the
expression. The visitor classes resolve the references.
 */
public class SelectExprDePaser extends ExpressionDeParser {
    private boolean pass = true; //indicator of pass or not, default true
    private Stack<Integer> stack = new Stack<>();//used to compare two values with the target operator
    private ArrayList<String> schema;
    private ArrayList<Integer> values;

    /*
    constructor
    @params tuple
     */
    public SelectExprDePaser(Tuple tuple) {
        schema = tuple.getSchema();
        values = tuple.getValues();
    }

    //empty constructor
    public SelectExprDePaser(){}

    /*
    return the boolean value of pass
    @return Boolean pass
     */
    public Boolean check(){
        return pass;
    }

    public void visit(AndExpression andExpression) {
        super.visit(andExpression);
    }

    @Override
    public void visit(LongValue longValue) {//push the value of the long value to the stack
        super.visit(longValue);
        stack.push((int)longValue.getValue());
    }

    @Override
    public void visit(Column column) {//push the value of the column to the stack
        String table = column.getTable().getName();
        super.visit(column);
        stack.push(values.get(schema.indexOf(table + "#" + column.getColumnName())));
    }

    @Override
    public void visit(EqualsTo equalsTo) {//check the condition of EqualsTo, set pass to false if not
        super.visit(equalsTo);
        Integer right = stack.pop();
        Integer left = stack.pop();
        if (left.compareTo(right) != 0)
            pass = false;
    }

    @Override
    public void visit(NotEqualsTo notEqualsTo) {//check the condition of notEqualsTo
        super.visit(notEqualsTo);
        Integer right = stack.pop();
        Integer left = stack.pop();
        if (left.compareTo(right) == 0)
            pass = false;
    }

    @Override
    public void visit(MinorThan minorThan) {//check the condition of minorThan
        super.visit(minorThan);
        Integer right = stack.pop();
        Integer left = stack.pop();
        if (left.compareTo(right) != -1)
            pass = false;
    }

    @Override
    public void visit(MinorThanEquals minorThanEquals) {//check the condition of minorThanEquals
        super.visit(minorThanEquals);
        Integer right = stack.pop();
        Integer left = stack.pop();
        if (left.compareTo(right) == 1)
            pass = false;
    }

    @Override
    public void visit(GreaterThan greaterThan) {//check the condition of greaterThan
        super.visit(greaterThan);
        Integer right = stack.pop();
        Integer left = stack.pop();
        if (left.compareTo(right) != 1)
            pass = false;
    }

    @Override
    public void visit(GreaterThanEquals greaterThanEquals) {//check the condition of greaterThanEquals
        super.visit(greaterThanEquals);
        Integer right = stack.pop();
        Integer left = stack.pop();
        if (left.compareTo(right) == -1)
            pass = false;
    }



}
