package ed.inf.adbs.lightdb;

import java.util.*;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;


public class SortOperator extends Operator {

    private Integer pos = -1; //current position of the tuple in the sorted list
    private boolean sorted = false; //indicate whether the tuples are sorted or not
    private boolean orderAll; //indicate whether we order tuples using all the attributes
    private Operator op; //child operator
    private List<OrderByElement> orderByElementList; //order requirements
    private ArrayList<Tuple> tuples; //tuples to be sorted


    //constructor of the sortOperator
    public SortOperator(List<OrderByElement> orderByElementList, Operator op, Boolean orderAll) {
        this.orderByElementList = orderByElementList;
        this.op = op;
        this.orderAll = orderAll;
    }

    @Override
    public void reset() {
        pos = -1;
    }


    /*
    get the next tuple from the sorted list
    @return tuple
     */
    @Override
    public Tuple getNextTuple() {
        if (!sorted) { //if the tuples are not sorted, call the sortTuples method to sort them
            sortTuples();
        }
        if (pos >= tuples.size() - 1) { //reach the end of the tuples list
            return null;
        }
        pos += 1;
        return tuples.get(pos); //return tuple
    }

    /*
    sort the target tuples list
     */
    public void sortTuples(){
        tuples = op.dump(); //get all the tuples from the child operator
        ArrayList<String> currentSchema = tuples.get(0).getSchema();
        List<Integer> orderList = new ArrayList<>(); //store the indices of the sort attributes
        if (orderByElementList != null){
            for (OrderByElement orderByElement: orderByElementList){
                Expression expression = orderByElement.getExpression();
                String table = ((Column) expression).getTable().getName();
                String colName = ((Column) expression).getColumnName();
                orderList.add(currentSchema.indexOf(table+"#"+colName));
            }
        }
        if (orderAll){ //if we need to order all the columns, add the rest attributes
            for (String attr: currentSchema){
                Integer attrIdx = currentSchema.indexOf(attr);
                if (!orderList.contains(attrIdx)) {
                    orderList.add(attrIdx);
                }
            }
        }
        tuples.sort((tuple1, tuple2) -> { //sort the target tuple list
            int indicator = 0;
            for (Integer idx : orderList) {
                Integer tuple1Value = tuple1.getValues().get(idx);
                Integer tuple2Value = tuple2.getValues().get(idx);
                indicator = tuple1Value.compareTo(tuple2Value);
                if (indicator != 0)
                    break;
            }
            return indicator;
        });
        sorted = true; //set the indicator to true
    }

    @Override
    public ArrayList<Tuple> dump() {
        sortTuples();//sort the tuples first the return
        return tuples;
    }
}