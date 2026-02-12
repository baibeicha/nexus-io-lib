package com.github.baibeicha.nexus.io.format.nxdt;

import com.github.baibeicha.nexus.io.parquet.ParquetIo;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

public class NxdtReader implements Closeable, Iterable<Map<String, Object>> {

    private final ParquetReader<GenericRecord> reader;

    public NxdtReader(Path sourceFile) throws IOException {
        InputFile inputFile = new ParquetIo.NioInputFile(sourceFile);

        this.reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                .withDataModel(GenericData.get())
                .withConf(new Configuration())
                .build();
    }

    /**
     * Читает следующую запись и конвертирует её в Map.
     * Возвращает null, если файл закончился.
     */
    public Map<String, Object> readNext() throws IOException {
        GenericRecord record = reader.read();
        if (record == null) return null;

        return convertToMap(record);
    }

    private Map<String, Object> convertToMap(GenericRecord record) {
        Map<String, Object> map = new LinkedHashMap<>();
        for (Schema.Field field : record.getSchema().getFields()) {
            String key = field.name();
            Object value = record.get(key);

            if (value instanceof CharSequence) {
                value = value.toString();
            }
            map.put(key, value);
        }
        return map;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    public Iterator<Map<String, Object>> iterator() {
        return new Iterator<>() {
            private Map<String, Object> nextRecord = null;
            private boolean done = false;

            private void advance() {
                if (nextRecord == null && !done) {
                    try {
                        nextRecord = readNext();
                        if (nextRecord == null) done = true;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                advance();
                return !done;
            }

            @Override
            public Map<String, Object> next() {
                advance();
                if (done) throw new NoSuchElementException();
                Map<String, Object> result = nextRecord;
                nextRecord = null;
                return result;
            }
        };
    }
}
