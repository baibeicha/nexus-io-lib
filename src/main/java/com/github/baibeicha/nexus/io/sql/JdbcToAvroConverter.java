package com.github.baibeicha.nexus.io.sql;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

public class JdbcToAvroConverter {

    public Object convert(ResultSet rs, int columnIndex, int sqlType) throws SQLException {
        Object value = rs.getObject(columnIndex);

        if (value == null) {
            return null;
        }

        switch (sqlType) {
            case Types.INTEGER, Types.SMALLINT, Types.TINYINT -> {
                if (value instanceof Number n) return n.intValue();
                return Integer.parseInt(value.toString());
            }
            case Types.BIGINT -> {
                if (value instanceof Number n) return n.longValue();
                return Long.parseLong(value.toString());
            }
            case Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL -> {
                if (value instanceof Number n) return n.doubleValue();
                try { return Double.parseDouble(value.toString()); } catch (NumberFormatException e) { return 0.0; }
            }
            case Types.BIT, Types.BOOLEAN -> {
                if (value instanceof Boolean b) return b;
                if (value instanceof Number n) return n.intValue() == 1;
                return Boolean.parseBoolean(value.toString());
            }
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> {
                if (value instanceof Timestamp ts) return ts.getTime();
                if (value instanceof LocalDateTime ldt) return Timestamp.valueOf(ldt).getTime();
                return 0L;
            }
            case Types.DATE -> {
                if (value instanceof java.sql.Date date) return (int) date.toLocalDate().toEpochDay();
                if (value instanceof LocalDate ld) return (int) ld.toEpochDay();
                return 0;
            }
            case Types.ARRAY -> {
                Array sqlArray = rs.getArray(columnIndex);
                if (sqlArray == null) {
                    return null;
                }
                try {
                    Object[] arrayData = (Object[]) sqlArray.getArray();

                    return Arrays.stream(arrayData)
                            .map(obj -> obj == null ? null : obj.toString())
                            .collect(Collectors.toList());
                } finally {
                    sqlArray.free();
                }
            }
            case Types.OTHER, Types.STRUCT, Types.JAVA_OBJECT -> {
                return value.toString();
            }
            default -> {
                if (value instanceof UUID) return value.toString();
                if (value instanceof java.sql.Clob clob) return clob.getSubString(1, (int) clob.length());
                return value.toString();
            }
        }
    }
}
