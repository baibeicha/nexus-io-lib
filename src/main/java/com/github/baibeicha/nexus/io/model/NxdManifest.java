package com.github.baibeicha.nexus.io.model;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record NxdManifest(
        String version,
        UUID projectId,
        String projectName,
        Instant createdAt,
        String createdBy,
        List<FileEntry> files
) {
    public record FileEntry(
            String path,      // "data/file_name.nxdt"
            String type,      // "DATASET", "REPORT"
            long sizeOriginal,
            String iv         // Base64 IV для расшифровки этого файла
    ) {}
}
