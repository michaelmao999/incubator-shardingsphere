package org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.clause.expression;

import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.old.parser.expression.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OracleSQLFunctionExecutor extends AbstractSQLFunctionExecutor {

    public OracleSQLFunctionExecutor(ShardingTableMetaData shardingTableMetaData) {
        super(shardingTableMetaData);
    }

    @Override
    public Object compute(SQLFunctionExpression functionExpression) {
        String function = functionExpression.getFunction();
        Object result = null;
        List<SQLExpression> parameters = functionExpression.getParameters();
        if ("to_date".equals(function)) {
            Date dateObj = converToDate(parameters.get(0), parameters.get(1));
            functionExpression.setValue(dateObj);
            return dateObj;
        }
        return null;
    }


    private static Date converToDate(SQLExpression date1, SQLExpression dateFormat) {
        String date1String = null;
        if (date1 instanceof SQLIdentifierExpression) {
            date1String = ((SQLIdentifierExpression)date1).getName();
        } else if (date1 instanceof SQLTextExpression) {
            date1String = ((SQLTextExpression)date1).getText();
        }
        String formatText = null;
        if (dateFormat instanceof SQLIdentifierExpression) {
            formatText = ((SQLIdentifierExpression)dateFormat).getName();
        } else if (dateFormat instanceof SQLTextExpression) {
            formatText = ((SQLTextExpression)dateFormat).getText();
        }
        if (date1String == null || formatText == null) {
            return null;
        }
        try {
            String dateValue = convertJavaDate(formatText, date1String);
            if (dateValue != null) {
                DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd");
                return isoDateFormat.parse(dateValue);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String convertJavaDate(String format, String date) {
        Map<String, String> ymd = new HashMap<String, String>();
        if (format.indexOf('-') > 0) {
            String[] formatArray = format.split("-");
            String[] dateArray = date.split("-");

            int len = formatArray.length;
            for (int index = 0; index < len; index++) {
                if (formatArray[index].toLowerCase().indexOf('y') >= 0) {
                    ymd.put("year", dateArray[index]);
                } else if (formatArray[index].toLowerCase().indexOf('m') >= 0) {
                    ymd.put("month", dateArray[index]);
                }else if (formatArray[index].toLowerCase().indexOf('d') >= 0) {
                    ymd.put("day", dateArray[index]);
                }
            }
        }
        if (ymd.size() == 3) {
            return ymd.get("year") + "-" + ymd.get("month") + "-" + ymd.get("day");
        }
        return null;

    }

    public static void main(String[] argc) throws  Exception {
        String date = "05-01-2019";
        String format = "MM-DD-YYYY";

        SimpleDateFormat formatObj = new SimpleDateFormat(format);
        System.out.println(formatObj.format(new Date()));
        System.out.println(formatObj.parse(date));

        DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println(isoDateFormat.format(new Date()));
        System.out.println(isoDateFormat.parse("2019-05-01"));

    }

}
