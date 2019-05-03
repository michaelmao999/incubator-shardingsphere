package org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.clause.expression;

import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.old.parser.expression.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OracleSQLFunctionExecutor implements SQLFunctionExector {


    @Override
    public Object compute(SQLFunctionExpression functionExpression, final List<Object> sqlparameters) {
        String function = functionExpression.getFunction();
        Object result = null;
        List<SQLExpression> parameters = functionExpression.getParameters();
        if ("to_date".equals(function)) {
            Date dateObj = converToDate(parameters.get(0), parameters.get(1), sqlparameters);
            functionExpression.setValue(dateObj);
            return dateObj;
        }
        return null;
    }


    private static Date converToDate(SQLExpression date1, SQLExpression dateFormat, final List<Object> sqlparameters) {
        String date1String = null;
        if (date1 instanceof SQLIdentifierExpression) {
            date1String = ((SQLIdentifierExpression)date1).getName();
        } else if (date1 instanceof SQLTextExpression) {
            date1String = ((SQLTextExpression)date1).getText();
        } else if (date1 instanceof SQLParameterMarkerExpression) {
            date1String = (String) sqlparameters.get(((SQLParameterMarkerExpression) date1).getIndex());
        }
        String formatText = null;
        if (dateFormat instanceof SQLIdentifierExpression) {
            formatText = ((SQLIdentifierExpression)dateFormat).getName();
        } else if (dateFormat instanceof SQLTextExpression) {
            formatText = ((SQLTextExpression)dateFormat).getText();
        } else if (dateFormat instanceof SQLParameterMarkerExpression) {
            formatText = (String) sqlparameters.get(((SQLParameterMarkerExpression) dateFormat).getIndex());
        }
        if (date1String == null || formatText == null) {
            return null;
        }
        Date result = null;
        try {
            if (formatText.indexOf('-') > 0) {
                result = convertJavaDate(formatText, date1String, "-");
            } else if (formatText.indexOf('/') > 0) {
                result = convertJavaDate(formatText, date1String, "/");
            } else {
                result = convertJavaDate(formatText, date1String);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private static Date convertJavaDate(String format, String date, String splitChar) throws java.text.ParseException {
        Map<String, String> ymd = new HashMap<String, String>();
        if (format.indexOf(splitChar) > 0) {
            String[] formatArray = format.split(splitChar);
            String[] dateArray = date.split(splitChar);

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
            DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            return isoDateFormat.parse(ymd.get("year") + "-" + ymd.get("month") + "-" + ymd.get("day"));
        }
        return null;

    }

    private static Date convertJavaDate(String format, String date) throws java.text.ParseException {
        Map<String, StringBuilder> ymd = new HashMap<String, StringBuilder>();
        int len = format.length();
        for (int index = 0; index < len; index++) {
            char data = format.charAt(index);
            if (data == 'y' || data == 'Y') {
                appendChar(date.charAt(index), ymd, "year");
            }else if (data == 'm' || data == 'M') {
                appendChar(date.charAt(index), ymd, "month");
            }else if (data == 'd' || data == 'D') {
                appendChar(date.charAt(index), ymd, "day");
            }
        }
        if (ymd.size() == 3) {
            DateFormat isoDateFormat = new SimpleDateFormat("yyyy-MM-dd");
            return isoDateFormat.parse(ymd.get("year") + "-" + ymd.get("month") + "-" + ymd.get("day"));
        }
        return null;

    }

    private static void appendChar(char data, Map<String, StringBuilder> ymd, String key) {
        StringBuilder builder = ymd.get(key);
        if (builder == null) {
            builder = new StringBuilder();
            ymd.put(key, builder);
        }
        builder.append(data);
    }

}
