package com.github.baibeicha.nexus.io.format.nxd;


import com.github.baibeicha.nexus.io.crypto.AesGcmCipher;
import com.github.baibeicha.nexus.io.model.NxdManifest;
import tools.jackson.databind.ObjectMapper;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.SecretKey;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Optional;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class NxdReader implements AutoCloseable {

    private final ZipFile zipFile;
    private final NxdManifest manifest;
    private final SecretKey masterKey;

    public NxdReader(Path filePath, SecretKey masterKey) throws IOException {
        this.zipFile = new ZipFile(filePath.toFile());
        this.masterKey = masterKey;

        ZipEntry manifestEntry = zipFile.getEntry("manifest.json");
        if (manifestEntry == null) throw new IOException("Corrupted NXD: no manifest");

        try (InputStream is = zipFile.getInputStream(manifestEntry)) {
            this.manifest = new ObjectMapper().readValue(is, NxdManifest.class);
        }
    }

    public NxdManifest getManifest() {
        return manifest;
    }

    /**
     * Извлекает и расшифровывает файл во временную директорию.
     *
     * @param internalPath путь внутри архива (напр. "data/table.nxdt")
     * @param destination путь к директории
     */
    public void extractFile(String internalPath, Path destination) throws Exception {
        Optional<NxdManifest.FileEntry> entryInfo = manifest.files().stream()
                .filter(f -> f.path().equals(internalPath))
                .findFirst();

        if (entryInfo.isEmpty()) throw new FileNotFoundException(internalPath);

        ZipEntry zipEntry = zipFile.getEntry(internalPath + ".enc");
        if (zipEntry == null) throw new FileNotFoundException("In ZIP: " + internalPath + ".enc");

        byte[] iv = Base64.getDecoder().decode(entryInfo.get().iv());

        Cipher cipher = AesGcmCipher.getDecryptCipher(masterKey, iv);

        try (InputStream is = zipFile.getInputStream(zipEntry);
             CipherInputStream cis = new CipherInputStream(is, cipher)) {
            Files.copy(cis, destination);
        }
    }

    @Override
    public void close() throws IOException {
        zipFile.close();
    }
}
