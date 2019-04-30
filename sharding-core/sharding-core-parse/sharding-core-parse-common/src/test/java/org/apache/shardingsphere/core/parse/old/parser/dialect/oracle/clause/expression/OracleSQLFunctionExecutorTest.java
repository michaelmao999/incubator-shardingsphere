package org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.clause.expression;

import org.apache.shardingsphere.core.parse.old.parser.context.table.Tables;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLFunctionExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLIdentifierExpression;
import org.apache.shardingsphere.core.parse.old.parser.expression.SQLTextExpression;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class OracleSQLFunctionExecutorTest {

    @Test
    public void computeTodate() throws  Exception{
        OracleSQLFunctionExecutor executor = new OracleSQLFunctionExecutor();
        DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        Date expectedData = isoDateFormat.parse("2019-05-01");

        List<SQLExpression> parameters = new ArrayList<>();
        SQLExpression data = new SQLIdentifierExpression("05-01-2019");
        SQLExpression format = new SQLTextExpression("MM-DD-YYYY");
        parameters.add(data);
        parameters.add(format);

        SQLFunctionExpression functionExpression1 = new SQLFunctionExpression("to_date", parameters);
        Object result1 = executor.compute(functionExpression1, null);

        assertTrue(expectedData.equals(result1));

        parameters.clear();
        data = new SQLIdentifierExpression("2019/05/01");
        format = new SQLTextExpression("yyyy/mm/dd");
        parameters.add(data);
        parameters.add(format);
        SQLFunctionExpression functionExpression2 = new SQLFunctionExpression("to_date", parameters);
        Date result2 = (Date)executor.compute(functionExpression1, null);

        assertTrue(expectedData.equals(result2));

        parameters.clear();
        data = new SQLIdentifierExpression("20190501");
        format = new SQLTextExpression("yyyymmdd");
        parameters.add(data);
        parameters.add(format);
        SQLFunctionExpression functionExpression3 = new SQLFunctionExpression("to_date", parameters);
        Date result3 = (Date)executor.compute(functionExpression1, null);

        assertTrue(expectedData.equals(result3));
    }

}
