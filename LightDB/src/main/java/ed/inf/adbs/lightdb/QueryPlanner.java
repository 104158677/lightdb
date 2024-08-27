package ed.inf.adbs.lightdb;

import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import java.util.*;

public class QueryPlanner {
    private Distinct distinct;
    private List<SelectItem<?>> selectItem;
    private FromItem fromItem;
    private List<Join> joinList;
    private Expression where;
    private List<OrderByElement> orderByElementList;

    private GroupByElement groupBy;

    private List<String> groupByColNames = new ArrayList<>();

    private String sumColName = null;

    private List<String> joinTables = new ArrayList<>();

    private Boolean alreayEmpty = false;
    private Map<Set<String>, Expression> whereForJoin = new HashMap<>();
    private Map<String, Expression> whereForSelect = new HashMap<>();
    private String mainAlias;
    private String baseTable;
    private HashMap<String, String> alias = new HashMap<>();
    public QueryPlanner(Statement statement){
        Select select = (Select) statement;
        PlainSelect body = (PlainSelect) select.getSelectBody();
        distinct = body.getDistinct();
        selectItem = body.getSelectItems();
        fromItem = body.getFromItem();
        joinList = body.getJoins();
        where = body.getWhere();
        orderByElementList = body.getOrderByElements();
        groupBy = body.getGroupBy();

        parseFrom();
        if (where != null) parseWhere();
        checkSum(body);
        List<String> groupByColumnName = findGroupByColumnNames(body);
    }

    //try to implement the sum operator
    public void checkSum(PlainSelect plainSelect){
        for (SelectItem item : plainSelect.getSelectItems()) {
            String itemStr = item.toString();
            if (itemStr.toUpperCase().startsWith("SUM(")) {
                sumColName = itemStr.substring(itemStr.indexOf('(') + 1, itemStr.lastIndexOf(')'));
            }
        }

    }

    //try to implement the sum operator
    private static List<String> findGroupByColumnNames(PlainSelect plainSelect) {
        List<String> groupByColumnNames = new ArrayList<>();
        GroupByElement groupBy = plainSelect.getGroupBy();
        if (groupBy != null) {
            List<Expression> groupByExpressions = groupBy.getGroupByExpressions();
            if (groupByExpressions != null && !groupByExpressions.isEmpty()) {
                for (Expression groupByExpr : groupByExpressions) {
                    if (groupByExpr instanceof Column) {
                        String columnName = ((Column) groupByExpr).getFullyQualifiedName();
                        groupByColumnNames.add(columnName);
                    }
                }
            }
        }
        return groupByColumnNames; // Return the list of GROUP BY column names
    }

    /*
    Get alias of tables from the formItem and store it using a class alias;
     */
    public void parseFrom(){
        //check the possible alias of the main table, and store it to the alias arraylist.
        String[] split = fromItem.toString().split("\\s");
        mainAlias = (split.length==1)?split[0]:split[1];
        alias.put(mainAlias, split[0]);
        baseTable = mainAlias;
        Alias.getInstance(alias);
        if (joinList != null){
            for (Join join: joinList){
                split = join.getRightItem().toString().split("\\s");
                mainAlias = (split.length==1)?split[0]:split[1];
                alias.put(mainAlias,split[0]);
                joinTables.add(mainAlias);
            }
        }
    }

    public void processWhere(Expression expression){
        if (expression instanceof ComparisonOperator){
            Expression expressionLeft = ((ComparisonOperator) expression).getLeftExpression();
            Expression expressionRight = ((ComparisonOperator) expression).getRightExpression();
            if (expressionLeft instanceof LongValue && expressionRight instanceof LongValue){
                //check if the equation between the two longvalues holds, if not, set alreadyEmpty to null then
                //finally return null
                SelectExprDePaser selectExprDePaser = new SelectExprDePaser();
                expression.accept(selectExprDePaser);
                if (!selectExprDePaser.check()){
                    alreayEmpty = true;
                }
            }
            //join cases. Process join conditions, save them in a mapping. Key is a set of tables
            //concatenate the rest expressions with the existing value using ANDExpression
            //
            else if (expressionLeft instanceof Column && expressionRight instanceof Column){
                Set<String> table = new HashSet<>();
                table.add(((Column) expressionLeft).getTable().getName());
                table.add(((Column) expressionRight).getTable().getName());
                if (whereForJoin.get(table) == null){
                    whereForJoin.put(table, expression);
                } else {
                    AndExpression andExpression1 = new AndExpression();
                    andExpression1.withRightExpression(whereForJoin.get(table));
                    andExpression1.withLeftExpression(expression);
                    whereForJoin.put(table, andExpression1); //save in the whereForJoin
                }
            }
            //selection condition, save them in another mapping. keys are tables names
            else {
                String table = null;
                if (expressionLeft instanceof Column)
                    table = ((Column) expressionLeft).getTable().getName();
                if (expressionRight instanceof Column)
                    table = ((Column) expressionRight).getTable().getName();
                if (whereForSelect.get(table) == null)
                    whereForSelect.put(table, expression);
                else{
                    AndExpression andExpression1 = new AndExpression();
                    andExpression1.withRightExpression(whereForSelect.get(table));
                    andExpression1.withLeftExpression(expression);
                    whereForSelect.put(table, andExpression1);
                }
            }
        }

    }
    //process the where clause
    public void parseWhere(){

        //another class that extends ExpressionDeParser to process the where clause.
        ExpressionDeParser expressionDeParser = new ExpressionDeParser(){
            @Override
            public void visit(AndExpression andExpression) {
                super.visit(andExpression);
                Expression leftExpression = andExpression.getLeftExpression();
                Expression rightExpression = andExpression.getRightExpression();

                processWhere(leftExpression); //process the left expression
                processWhere(rightExpression); //process the last right expression
            }
        };

        if (where instanceof AndExpression) {//call the above implementation (where is an andexpression)
            where.accept(expressionDeParser);
        }
        else //where is a comparison expression
        {
            processWhere(where);
        }
    }


    public ArrayList<Tuple> dump(){
        if (alreayEmpty) {//no need to do further process
            return null;
        }

        Set<String> currentTableSet = new HashSet<>();//save tables that have been joined
        ScanOperator scanOperator = new ScanOperator(baseTable);
        Expression expression = whereForSelect.get(baseTable);
        Operator operatorLeft = (expression != null)? new SelectOperator(expression, scanOperator) : scanOperator;
        currentTableSet.add(baseTable);

        for (String joinTable: joinTables){
            ScanOperator scanOperator1 = new ScanOperator(joinTable);
            Expression expression1 = whereForSelect.get(joinTable);
            Operator operatorRight = (expression1 != null)? new SelectOperator(expression1, scanOperator1) : scanOperator1;
            currentTableSet.add(joinTable);
            List<Expression> expressions = new ArrayList<>();
            List<Set<String>> usedJoin = new ArrayList<>();
            for (Set<String> stringSet: whereForJoin.keySet()){//find all conditions for one join
                if (currentTableSet.containsAll(stringSet)){
                    expressions.add(whereForJoin.get(stringSet));
                    usedJoin.add(stringSet);
                }
            }
            for (Set<String> stringSet: usedJoin){//remove used join conditions
                whereForJoin.remove(stringSet);
            }
            if (expressions.size() == 0){//cross product
                operatorLeft = new JoinOperator(null, operatorLeft, operatorRight);
            } else if (expressions.size() == 1){//one join condition
                operatorLeft = new JoinOperator(expressions.get(0), operatorLeft, operatorRight);
            } else {//construct a final expression on several expression
                AndExpression andExpression = new AndExpression();
                andExpression.withLeftExpression(expressions.get(0));
                andExpression.withRightExpression(expressions.get(1));
                for (Expression expression2: expressions.subList(2,expressions.size())){
                    AndExpression andExpression1 = new AndExpression();
                    andExpression1.withLeftExpression(andExpression);
                    andExpression1.withRightExpression(expression2);
                    andExpression = andExpression1;
                }
                operatorLeft = new JoinOperator(andExpression, operatorLeft, operatorRight);
            }
        }

//        if (sumColName != null || !groupByColNames.isEmpty()){
//            operatorLeft = new SumOperator(operatorLeft, sumColName, groupByColNames);
//        }

        if (!(Objects.equals(selectItem.get(0).toString(), "*")))//not all columns, need projection
            operatorLeft = new ProjectOperator(selectItem, operatorLeft);

        if (distinct != null){//distinct exists.
            {
                operatorLeft = new SortOperator(orderByElementList, operatorLeft, true);
                operatorLeft = new DuplicateEliminateOperator(operatorLeft);
            }
        }else if (orderByElementList != null){//sort on the given list
            operatorLeft = new SortOperator(orderByElementList, operatorLeft, false);
        }

        return operatorLeft.dump();
    }


}
