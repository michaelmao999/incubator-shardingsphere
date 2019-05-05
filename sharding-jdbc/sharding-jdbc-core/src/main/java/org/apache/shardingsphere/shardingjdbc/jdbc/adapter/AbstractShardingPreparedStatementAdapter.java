/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.shardingjdbc.jdbc.adapter;

import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.shardingsphere.shardingjdbc.jdbc.adapter.invocation.SetParameterMethodInvocation;
import org.apache.shardingsphere.shardingjdbc.jdbc.unsupported.AbstractUnsupportedOperationPreparedStatement;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

/**
 * Sharding adapter for {@code PreparedStatement}.
 *
 * @author zhangliang
 * @author maxiaoguang
 */
public abstract class AbstractShardingPreparedStatementAdapter extends AbstractUnsupportedOperationPreparedStatement {
    
    private final List<SetParameterMethodInvocation> setParameterMethodInvocations = new LinkedList<>();
    
    @Getter
    private final List<Object> parameters = new ArrayList<>();
    
    @Override
    public void setNull(final int parameterIndex, final int sqlType) throws SQLException {
        setParameter(parameterIndex, null);
    }
    
    @Override
    public void setNull(final int parameterIndex, final int sqlType, final String typeName) throws SQLException {
        setParameter(parameterIndex, null);
    }
    
    @Override
    public void setBoolean(final int parameterIndex, final boolean x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setByte(final int parameterIndex, final byte x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setShort(final int parameterIndex, final short x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setInt(final int parameterIndex, final int x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setLong(final int parameterIndex, final long x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setFloat(final int parameterIndex, final float x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setDouble(final int parameterIndex, final double x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setString(final int parameterIndex, final String x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setBigDecimal(final int parameterIndex, final BigDecimal x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setDate(final int parameterIndex, final Date x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setDate(final int parameterIndex, final Date x, final Calendar cal) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setTime(final int parameterIndex, final Time x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setTime(final int parameterIndex, final Time x, final Calendar cal) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setTimestamp(final int parameterIndex, final Timestamp x, final Calendar cal) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setBytes(final int parameterIndex, final byte[] x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setBlob(final int parameterIndex, final Blob x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setBlob(final int parameterIndex, final InputStream x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setBlob(final int parameterIndex, final InputStream x, final long length) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setClob(final int parameterIndex, final Clob x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setClob(final int parameterIndex, final Reader x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setClob(final int parameterIndex, final Reader x, final long length) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final int length)  throws SQLException{
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setAsciiStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @SuppressWarnings("deprecation")
    @Override
    public void setUnicodeStream(final int parameterIndex, final InputStream x, final int length)  throws SQLException{
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x) throws SQLException{
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final int length) throws SQLException{
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setBinaryStream(final int parameterIndex, final InputStream x, final long length) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setCharacterStream(final int parameterIndex, final Reader x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setCharacterStream(final int parameterIndex, final Reader x, final int length) throws SQLException{
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setCharacterStream(final int parameterIndex, final Reader x, final long length) throws SQLException{
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setURL(final int parameterIndex, final URL x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setSQLXML(final int parameterIndex, final SQLXML x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setObject(final int parameterIndex, final Object x) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    @Override
    public void setObject(final int parameterIndex, final Object x, final int targetSqlType, final int scaleOrLength) throws SQLException {
        setParameter(parameterIndex, x);
    }
    
    private void setParameter(final int parameterIndex, final Object value) {
        if (parameters.size() == parameterIndex - 1) {
            parameters.add(value);
            return;
        }
        for (int i = parameters.size(); i <= parameterIndex - 1; i++) {
            parameters.add(null);
        }
        parameters.set(parameterIndex - 1, value);
    }
    
    protected final void replaySetParameter(final PreparedStatement preparedStatement, final List<Object> parameters) {
        setParameterMethodInvocations.clear();
        addParameters(parameters);
        for (SetParameterMethodInvocation each : setParameterMethodInvocations) {
            each.invoke(preparedStatement);
        }
    }
    
    private void addParameters(final List<Object> parameters) {
        int i = 0;
        for (Object each : parameters) {
            setParameter(new Class[]{int.class, Object.class}, i++ + 1, each);
        }
    }
    
    @SneakyThrows
    private void setParameter(final Class[] argumentTypes, final Object... arguments) {
        setParameterMethodInvocations.add(new SetParameterMethodInvocation(PreparedStatement.class.getMethod("setObject", argumentTypes), arguments, arguments[1]));
    }
    
    @Override
    public void clearParameters() {
        parameters.clear();
        setParameterMethodInvocations.clear();
    }
}
