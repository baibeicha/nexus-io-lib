package com.github.baibeicha.nexus.io.parquet;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Адаптер для работы Parquet через стандартный Java NIO.
 * Позволяет избегать использования Hadoop native IO (winutils.exe).
 */
public class ParquetIo {

    public static class NioOutputFile implements OutputFile {
        private final Path path;

        public NioOutputFile(Path path) {
            this.path = path;
        }

        @Override
        public PositionOutputStream create(long blockSizeHint) throws IOException {
            return new NioPositionOutputStream(Files.newOutputStream(
                    path,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            ));
        }

        @Override
        public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
            return create(blockSizeHint);
        }

        @Override
        public boolean supportsBlockSize() {
            return false;
        }

        @Override
        public long defaultBlockSize() {
            return 0;
        }
    }

    public static class NioInputFile implements InputFile {
        private final Path path;

        public NioInputFile(Path path) {
            this.path = path;
        }

        @Override
        public long getLength() throws IOException {
            return Files.size(path);
        }

        @Override
        public SeekableInputStream newStream() throws IOException {
            return new NioSeekableInputStream(Files.newByteChannel(path, StandardOpenOption.READ));
        }
    }

    private static class NioPositionOutputStream extends PositionOutputStream {
        private final OutputStream out;
        private long pos = 0;

        public NioPositionOutputStream(OutputStream out) {
            this.out = out;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            pos++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            pos += len;
        }

        @Override
        public void close() throws IOException {
            out.close();
        }
    }

    private static class NioSeekableInputStream extends SeekableInputStream {
        private final SeekableByteChannel channel;

        public NioSeekableInputStream(SeekableByteChannel channel) {
            this.channel = channel;
        }

        @Override
        public long getPos() throws IOException {
            return channel.position();
        }

        @Override
        public void seek(long newPos) throws IOException {
            channel.position(newPos);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException {
            readFully(bytes, 0, bytes.length);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException {
            ByteBuffer buf = ByteBuffer.wrap(bytes, start, len);
            while (buf.hasRemaining()) {
                if (channel.read(buf) < 0) throw new java.io.EOFException();
            }
        }

        @Override
        public int read(ByteBuffer buf) throws IOException {
            return channel.read(buf);
        }

        @Override
        public void readFully(ByteBuffer buf) throws IOException {
            while (buf.hasRemaining()) {
                if (channel.read(buf) < 0) throw new java.io.EOFException();
            }
        }

        @Override
        public int read() throws IOException {
            ByteBuffer buf = ByteBuffer.allocate(1);
            return (channel.read(buf) < 0) ? -1 : buf.get(0) & 0xFF;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }
    }
}
