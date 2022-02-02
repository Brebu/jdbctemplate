package com.brebu.jdbcresttemplate.util;

import com.brebu.jdbcresttemplate.model.ColumnType;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnTypeMapper implements RowMapper<ColumnType> {

    @Override
    public ColumnType mapRow(ResultSet resultSet, int i) throws SQLException {
        ColumnType columnType = new ColumnType();
        columnType.setName(resultSet.getString("COLUMN_NAME"));
        columnType.setType(resultSet.getString("DATA_TYPE"));
        return columnType;
    }
}
