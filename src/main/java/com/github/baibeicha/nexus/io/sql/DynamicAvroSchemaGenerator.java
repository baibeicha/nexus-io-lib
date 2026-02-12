package com.github.baibeicha.nexus.io.sql;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class DynamicAvroSchemaGenerator {

    public Schema generate(ResultSetMetaData metaData) throws SQLException {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("NexusData")
                .namespace("by.nexus.data")
                .fields();

        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            String safeName = normalizeName(columnName);

            int sqlType = metaData.getColumnType(i);

            switch (sqlType) {
                case Types.INTEGER, Types.SMALLINT, Types.TINYINT ->
                        fields.name(safeName).type().nullable().intType().noDefault();
                case Types.BIGINT ->
                        fields.name(safeName).type().nullable().longType().noDefault();
                case Types.FLOAT, Types.REAL, Types.DOUBLE, Types.NUMERIC, Types.DECIMAL ->
                        fields.name(safeName).type().nullable().doubleType().noDefault();
                case Types.BOOLEAN, Types.BIT ->
                        fields.name(safeName).type().nullable().booleanType().noDefault();
                case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE ->
                        fields.name(safeName).type().nullable().longBuilder()
                                .prop("logicalType", "timestamp-millis").endLong().noDefault();
                case Types.DATE ->
                        fields.name(safeName).type().nullable().intBuilder()
                                .prop("logicalType", "date").endInt().noDefault();
                case Types.ARRAY ->
                        fields.name(safeName).type().nullable().array()
                                .items().nullable().stringType()
                                .noDefault();
                case Types.OTHER, Types.STRUCT, Types.JAVA_OBJECT, Types.SQLXML ->
                        fields.name(safeName).type().nullable().stringType().noDefault();
                default ->
                        fields.name(safeName).type().nullable().stringType().noDefault();
            }
        }

        return fields.endRecord();
    }

    public static String normalizeName(String rawName) {
        String safe = rawName.toLowerCase().replaceAll("[^a-z0-9_]", "_");
        if (safe.isEmpty() || Character.isDigit(safe.charAt(0))) {
            return "_" + safe;
        }
        return safe;
    }
}
