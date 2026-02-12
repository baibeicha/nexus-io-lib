package com.github.baibeicha.nexus.io.format.nxdt;

import com.github.baibeicha.nexus.io.parquet.ParquetIo;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

public class NxdtWriter implements Closeable {

    private final ParquetWriter<GenericRecord> writer;
    private final Schema schema;

    public NxdtWriter(Path destination, Schema schema) throws IOException {
        this.schema = schema;

        OutputFile outputFile = new ParquetIo.NioOutputFile(destination);

        this.writer = AvroParquetWriter.<GenericRecord>builder(outputFile)
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(16L * 1024 * 1024) // 16 MB
                .withPageSize(1024 * 1024)           // 1 MB
                .withDataModel(GenericData.get())
                .withConf(new Configuration())       // Пустой конфиг
                .build();
    }

    public void writeRecord(GenericRecord record) throws IOException {
        writer.write(record);
    }

    public GenericRecord createRecord() {
        return new GenericData.Record(schema);
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
