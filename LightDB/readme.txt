There are 3 possible situations in the where clause.
1. Comparing two LongValues. 
I used an indicator alreadyEmpty to check if there are statements like "2=1". If exists, the output will be null.

2. Comparing a column value to a longvalue.
This is a selection condition. I stored these conditions in a hashmap whereForSelect mapping from a table to an expression.

3. Comparing two column values.
This is a join condition. I saved these conditions in a hashmap whereForJoin mapping from a set storing both tables of the joins to an expression.

All these three possibilities are processed in the method parseWhere. In the query planner every scan operator other than the base one, it associates with a join operator, which checks the join condition saved in the whereForJoin hashmap. After a selection or join, related conditions will be removed from them.

My query plan pushes down selection to filter as early as possible. By implementing selection before joins, this can make the intermediate inputs to joins smaller. In the join implementation, possible join conditions are extracted from the where clause and evaluated as part of joins, to avoid expensive cartesian product. The parameter alreadyEmpty also helps to stop early when dealing with always wrong statements like "1=2".

Files in the dir:
Alias: an singleton class dealing with the aliases of tables.
Tuple: implement the tuple object.
DatabaseCatalog: read the csv files.
SelectExprDePaser: extends ExpressionDeParser to check if a tuple can pass the selection conditions.