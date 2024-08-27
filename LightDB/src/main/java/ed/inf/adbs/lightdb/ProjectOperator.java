package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.util.ArrayList;
import java.util.List;

public class ProjectOperator extends Operator{

    private Operator op;
    private ArrayList<String> newSchema = new ArrayList<>();

    /*
    constructor get the table name and create a new schema according to the selectItem list.
    @params List<SelectItem<?>> selectItemList: the attributes to be selected
    @params Operator operator: child operator
     */
    public ProjectOperator(List<SelectItem<?>> selectItemList, Operator operator) {
        this.op = operator;
        for (SelectItem selectItem: selectItemList){
            Expression expression = selectItem.getExpression();
            if (expression instanceof Column){
                String table;
                String colName = ((Column) expression).getColumnName();
                table = ((Column) expression).getTable().getName();
                newSchema.add(table+"#"+colName);
            }
        }
    }

    /*
    format the tuple output according to the new schema got from above
    @return tuple meet the select condition.
     */
    @Override
    public Tuple getNextTuple() {
        Tuple tuple = op.getNextTuple();
        if (tuple == null) {return null;}
        ArrayList<Integer> values = new ArrayList<>();
        for (String attr: newSchema){ //fill the tuple with the new schema
            values.add(tuple.getValues().get(tuple.getSchema().indexOf(attr)));}
        return new Tuple(newSchema, values);
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