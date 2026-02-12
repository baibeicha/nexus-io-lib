package com.github.baibeicha.nexus.io.format.nxd;

import com.github.baibeicha.nexus.io.crypto.AesGcmCipher;
import com.github.baibeicha.nexus.io.model.NxdManifest;
import tools.jackson.databind.ObjectMapper;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class NxdWriter implements AutoCloseable {

    private final ZipOutputStream zos;
    private final SecretKey masterKey;
    private final List<NxdManifest.FileEntry> entries = new ArrayList<>();
    private final ObjectMapper jsonMapper = new ObjectMapper();

    public NxdWriter(Path outputPath, SecretKey masterKey) throws IOException {
        this.zos = new ZipOutputStream(Files.newOutputStream(outputPath));
        this.masterKey = masterKey;
    }

    /**
     * Добавляет зашифрованный файл в контейнер.
     *
     * @param internalPath путь внутри архива (напр. "data/table.nxdt")
     * @param sourceFile   исходный файл на диске
     */
    public void addEncryptedFile(String internalPath, Path sourceFile, String type) throws Exception {
        byte[] iv = AesGcmCipher.generateIv();

        ZipEntry zipEntry = new ZipEntry(internalPath + ".enc");
        zos.putNextEntry(zipEntry);

        Cipher cipher = AesGcmCipher.getEncryptCipher(masterKey, iv);

        try (InputStream is = Files.newInputStream(sourceFile);
             CipherOutputStream cos = new CipherOutputStream(new NonClosingOutputStream(zos), cipher)) {
            is.transferTo(cos);
        }

        zos.closeEntry();

        entries.add(new NxdManifest.FileEntry(
                internalPath,
                type,
                Files.size(sourceFile),
                Base64.getEncoder().encodeToString(iv)
        ));
    }

    /**
     * Финализирует архив: пишет манифест и закрывает ZIP.
     */
    public void finish(NxdManifest baseManifest) throws Exception {
        NxdManifest finalManifest = new NxdManifest(
                baseManifest.version(),
                baseManifest.projectId(),
                baseManifest.projectName(),
                baseManifest.createdAt(),
                baseManifest.createdBy(),
                entries
        );

        ZipEntry manifestEntry = new ZipEntry("manifest.json");
        zos.putNextEntry(manifestEntry);
        jsonMapper.writeValue(new NonClosingOutputStream(zos), finalManifest);
        zos.closeEntry();
    }

    @Override
    public void close() throws IOException {
        zos.close();
    }

    private static class NonClosingOutputStream extends FilterOutputStream {
        public NonClosingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void close() throws IOException { /* ignore */ }
    }
}
