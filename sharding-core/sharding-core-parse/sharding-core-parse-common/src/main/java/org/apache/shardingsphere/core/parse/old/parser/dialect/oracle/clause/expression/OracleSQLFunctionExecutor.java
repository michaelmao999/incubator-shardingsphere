package org.apache.shardingsphere.core.parse.old.parser.dialect.oracle.clause.expression;

import org.apache.shardingsphere.core.metadata.table.ShardingTableMetaData;
import org.apache.shardingsphere.core.parse.old.parser.expression.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

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
        } else if ("trunc".equals(function)) {
            if (parameters != null && parameters.size() == 1) {
                // trunc(sysdate -1 ) trunc(column)
            } else if (parameters != null && parameters.size() == 2) {
                //trunc(sysdate - 1, 'dd')
                if (sqlExpressionToString(parameters.get(0)).indexOf("sysdate") >= 0) {
                    Date dateObj = convertSysdate(parameters.get(0), parameters.get(1), sqlparameters);
                    functionExpression.setValue(dateObj);
                    return dateObj;
                }
            }
        } else if (function == null || function.length() == 0) {
            //pure function
            String functionSQL = functionExpression.toFunctionSQL();
            if (!parameters.isEmpty() && functionSQL != null && functionSQL.toLowerCase().indexOf("sysdate") >= 0) {
                Date dateObj = convertSysdate(functionExpression, sqlparameters);
                functionExpression.setValue(dateObj);
                return dateObj;
            }

        }
        return null;
    }



    private String sqlExpressionToString(SQLExpression expression) {
        if (expression instanceof SQLIdentifierExpression) {
            return ((SQLIdentifierExpression)expression).getName().toLowerCase();
        } else if (expression instanceof SQLFunctionExpression) {
            return expression.toString();
        }
        return expression.toString();
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

    private static Date convertSysdate(SQLExpression date1, SQLExpression dateFormat, final List<Object> sqlparameters) {
        Calendar sysdate = Calendar.getInstance();
        if (date1 instanceof SQLFunctionExpression) {
            SQLFunctionExpression sysdateFunction = (SQLFunctionExpression) date1;
            List<SQLExpression> sysdateExpressionList = sysdateFunction.getParameters();
            int len = sysdateExpressionList.size();
            //1: plus   2 : subtract
            //sysdate - 1
            int isPlus = 0;
            Integer value = null;
            for (int index = 1; index < len; index++) {
                SQLExpression sqlExpression = sysdateExpressionList.get(index);
                if (sqlExpression instanceof  SQLIdentifierExpression) {
                    if ("-".equals(((SQLIdentifierExpression)sqlExpression).getName())) {
                        isPlus = 1;
                    } else if ("+".equals(((SQLIdentifierExpression)sqlExpression).getName())) {
                        isPlus = 2;
                    } else {
                        value = Integer.valueOf(((SQLIdentifierExpression)sqlExpression).getName());
                    }
                } else if (sqlExpression instanceof SQLTextExpression) {
                    value = Integer.valueOf(((SQLTextExpression)sqlExpression).getText());
                } else if (sqlExpression instanceof SQLParameterMarkerExpression) {
                    Object valueObj =  sqlparameters.get(((SQLParameterMarkerExpression) sqlExpression).getIndex());
                    value = Integer.valueOf(valueObj.toString());
                } else if (sqlExpression instanceof  SQLNumberExpression) {
                    value = Integer.valueOf(((SQLNumberExpression) sqlExpression).getNumber() +  "");
                    //sysdate -1
                    if (isPlus == 0) {
                        isPlus = 2;
                    }
                }
                if (value != null) {
                    if (isPlus == 1) {
                        sysdate.add(Calendar.DATE, -1* value);
                    } else if (isPlus == 2) {
                        sysdate.add(Calendar.DATE, value);
                    }
                    isPlus = 0;
                    value = null;
                }
            }
        }
        String formatText = null;
        if (dateFormat instanceof SQLIdentifierExpression) {
            formatText = ((SQLIdentifierExpression)dateFormat).getName();
        } else if (dateFormat instanceof SQLTextExpression) {
            formatText = ((SQLTextExpression)dateFormat).getText();
        } else if (dateFormat instanceof SQLParameterMarkerExpression) {
            formatText = (String) sqlparameters.get(((SQLParameterMarkerExpression) dateFormat).getIndex());
        }
        if (sysdate == null || formatText == null) {
            return null;
        }
        Date result = null;
        try {
            //trunc(SYSDATE , 'DD')
            if (formatText.indexOf("DD") >= 0 || formatText.indexOf("J") >= 0) {
                result = sysdate.getTime();
                //trunc(SYSDATE , 'RM')
            } else  if (formatText.indexOf("MM") >= 0 || formatText.indexOf("MO") >= 0 || formatText.indexOf("RM") >= 0) {
                sysdate.set(Calendar.DATE, 1);
                result = sysdate.getTime();
            } else  if (formatText.indexOf("SYYYY") >= 0 || formatText.indexOf("YYYY") >= 0 || formatText.indexOf("YEAR") >= 0) {
                sysdate.set(Calendar.DATE, 1);
                sysdate.set(Calendar.MONTH, 1);
                result = sysdate.getTime();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    private static Date convertSysdate(SQLExpression date1, final List<Object> sqlparameters) {
        Date result = null;
        if (date1 instanceof SQLFunctionExpression) {
            Calendar sysdate = Calendar.getInstance();
            SQLFunctionExpression sysdateFunction = (SQLFunctionExpression) date1;
            List<SQLExpression> sysdateExpressionList = sysdateFunction.getParameters();
            int len = sysdateExpressionList.size();
            //1: plus   2 : subtract
            //sysdate - 1
            int isPlus = 0;
            Integer value = null;
            for (int index = 1; index < len; index++) {
                SQLExpression sqlExpression = sysdateExpressionList.get(index);
                if (sqlExpression instanceof  SQLIdentifierExpression) {
                    if ("-".equals(((SQLIdentifierExpression)sqlExpression).getName())) {
                        isPlus = 1;
                    } else if ("+".equals(((SQLIdentifierExpression)sqlExpression).getName())) {
                        isPlus = 2;
                    } else {
                        value = Integer.valueOf(((SQLIdentifierExpression)sqlExpression).getName());
                    }
                } else if (sqlExpression instanceof SQLTextExpression) {
                    value = Integer.valueOf(((SQLTextExpression)sqlExpression).getText());
                } else if (sqlExpression instanceof SQLParameterMarkerExpression) {
                    Object valueObj =  sqlparameters.get(((SQLParameterMarkerExpression) sqlExpression).getIndex());
                    value = Integer.valueOf(valueObj.toString());
                } else if (sqlExpression instanceof  SQLNumberExpression) {
                    value = Integer.valueOf(((SQLNumberExpression) sqlExpression).getNumber() +  "");
                    if (isPlus == 0) {
                        isPlus = 2;
                    }
                }
                if (value != null) {
                    if (isPlus == 1) {
                        sysdate.add(Calendar.DATE, -1* value);
                    } else if (isPlus == 2) {
                        sysdate.add(Calendar.DATE, value);
                    }
                    isPlus = 0;
                    value = null;
                }
            }
            result = sysdate.getTime();
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
                }else if (formatArray[index].toLowerCase().indexOf('d') >= 0) {
                    ymd.put("day", dateArray[index]);
                } else if (formatArray[index].toLowerCase().indexOf('m') >= 0) {
                    ymd.put("month", dateArray[index]);
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
