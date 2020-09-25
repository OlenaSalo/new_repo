package com.equifax.apireporting.pipelines.utils;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@RunWith(MockitoJUnitRunner.class)
public class JdbcConvertersTests {

    private static final String NAME_KEY = "message";
    private static final String NAME_VALUE = "Fake";
    private static final String AGE_KEY = "age";
    private static final int AGE_VALUE = 28;
    private static TableRow expectedTableRow;

    @Mock
    private ResultSet resultSet;

    @Mock private ResultSetMetaData resultSetMetaData;

    @Before
    public void setUp() throws Exception {

        Mockito.when(resultSet.getObject(1)).thenReturn(NAME_VALUE);
        Mockito.when(resultSet.getObject(2)).thenReturn(AGE_VALUE);
        Mockito.when(resultSet.getMetaData()).thenReturn(resultSetMetaData);

        Mockito.when(resultSetMetaData.getColumnCount()).thenReturn(2);

        Mockito.when(resultSetMetaData.getColumnName(1)).thenReturn(NAME_KEY);

        Mockito.when(resultSetMetaData.getColumnName(2)).thenReturn(AGE_KEY);

        expectedTableRow = new TableRow();
        expectedTableRow.set(NAME_KEY, NAME_VALUE);
        expectedTableRow.set(AGE_KEY, AGE_VALUE);
    }

    @Test
    public void testRowMapper() throws Exception {

        JdbcIO.RowMapper<TableRow> resultSetConverters = JdbcConverters.getResultSetToTableRow();
        TableRow actualTableRow = resultSetConverters.mapRow(resultSet);

        assertThat(expectedTableRow.size(), equalTo(actualTableRow.size()));
        assertThat(actualTableRow, equalTo(expectedTableRow));
    }
}
