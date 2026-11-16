package com.example.shubham.fileservice.bulkFileUpload.service;


import com.example.shubham.fileservice.bulkFileUpload.dto.FileUploadEvent;
import com.example.shubham.fileservice.bulkFileUpload.entity.DocumentMetadata;
import com.example.shubham.fileservice.bulkFileUpload.entity.DocumentContent;
import com.example.shubham.fileservice.bulkFileUpload.repository.DocumentContentRepository;
import com.example.shubham.fileservice.bulkFileUpload.repository.DocumentMetadataRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.engine.jdbc.proxy.BlobProxy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@Service
@RequiredArgsConstructor
@Slf4j
public class FileStorageService {

    private final DocumentMetadataRepository metadataRepo;
    private final DocumentContentRepository contentRepo;
    private final KafkaTemplate<String, FileUploadEvent> kafkaTemplate;

    @Value("${app.staging-dir}")
    private String stagingDir;

    @Value("${app.kafka.topic}")
    private String kafkaTopic;

    @PostConstruct
    public void init() throws IOException {
        Files.createDirectories(Paths.get(stagingDir));
    }

    // 1. Stage files locally and emit Kafka events
    public Map<String, Object> stageAndPublishFiles(MultipartFile[] files) throws IOException {
        log.info("Starting the stage and publish files");
        String batchId = UUID.randomUUID().toString();
        log.info("batchId = "+batchId);
        List<Long> documentIds = new ArrayList<>();

        for (MultipartFile file : files) {
            if (file.isEmpty()) continue;

            // Save Metadata as PROCESSING
            DocumentMetadata metadata = DocumentMetadata.builder()
                    .batchId(batchId)
                    .fileName(file.getOriginalFilename())
                    .contentType(file.getContentType())
                    .fileSize(file.getSize())
                    .status(DocumentMetadata.UploadStatus.PROCESSING)
                    .createdAt(LocalDateTime.now())
                    .build();
            log.info("metadata = "+metadata);
            metadata = metadataRepo.save(metadata);
            documentIds.add(metadata.getId());
            log.info("documentIds = "+documentIds);

            // Stream file to staging disk
            Path tempPath = Paths.get(stagingDir, metadata.getId() + "_" + file.getOriginalFilename());
            log.info("documentIds = "+documentIds);
            try (InputStream is = file.getInputStream()) {
                Files.copy(is, tempPath, StandardCopyOption.REPLACE_EXISTING);
            }

            // Publish Claim-Check event to Kafka
            log.info("metadataId = "+metadata.getId());
            log.info("batchId = "+batchId);
            FileUploadEvent event = new FileUploadEvent(metadata.getId(), batchId, tempPath.toString());
            log.info("kafkaTopic = "+kafkaTopic);
            log.info("event = "+event);
            kafkaTemplate.send(kafkaTopic, batchId, event);
        }

        return Map.of(
                "batchId", batchId,
                "documentIds", documentIds,
                "status", "PROCESSING"
        );
    }

    // 2. Async Kafka Consumer: Stream from staging into SQL Server
    @KafkaListener(topics = "${app.kafka.topic}", groupId = "file-processor-group")
    @Transactional
    public void consumeFileUpload(FileUploadEvent event) {
        log.info("Processing file event for documentId: {}", event.getDocumentId());
        Path tempPath = Paths.get(event.getTempFilePath());

        DocumentMetadata metadata = metadataRepo.findById(event.getDocumentId())
                .orElseThrow(() -> new RuntimeException("Metadata not found: " + event.getDocumentId()));

        try (InputStream fileStream = Files.newInputStream(tempPath)) {
            long fileSize = Files.size(tempPath);

            // Stream binary directly into SQL Server Blob without loading into byte[]
            DocumentContent content = DocumentContent.builder()
                    .documentId(metadata.getId())
                    .data(BlobProxy.generateProxy(fileStream, fileSize))
                    .build();

            contentRepo.saveAndFlush(content);

            metadata.setStatus(DocumentMetadata.UploadStatus.COMPLETED);
            metadataRepo.save(metadata);

            // Cleanup staging file
            Files.deleteIfExists(tempPath);
            log.info("Successfully persisted documentId: {} to SQL Server", event.getDocumentId());

        } catch (Exception e) {
            log.error("Failed processing documentId: {}", event.getDocumentId(), e);
            metadata.setStatus(DocumentMetadata.UploadStatus.FAILED);
            metadataRepo.save(metadata);
        }
    }

    public int reprocessPendingFiles() {
        // 1. Find all records stuck in PROCESSING or FAILED
        List<DocumentMetadata> pendingDocs = metadataRepo.findByStatusIn(
                List.of(DocumentMetadata.UploadStatus.PROCESSING, DocumentMetadata.UploadStatus.FAILED)
        );

        int requeuedCount = 0;
        for (DocumentMetadata meta : pendingDocs) {
            Path tempPath = Paths.get(stagingDir, meta.getId() + "_" + meta.getFileName());

            // 2. Check if the file still exists in your temp/staging directory
            if (Files.exists(tempPath)) {
                meta.setStatus(DocumentMetadata.UploadStatus.PROCESSING);
                metadataRepo.save(meta);

                // 3. Re-publish the Claim-Check event to Kafka
                FileUploadEvent event = new FileUploadEvent(meta.getId(), meta.getBatchId(), tempPath.toString());
                kafkaTemplate.send(kafkaTopic, meta.getBatchId(), event);
                requeuedCount++;
                log.info("Re-queued documentId: {} from staging path: {}", meta.getId(), tempPath);
            } else {
                log.warn("Staging file missing for documentId: {} at path: {}", meta.getId(), tempPath);
            }
        }
        return requeuedCount;
    }

    // 3. Stream multiple files directly from SQL Server into a ZIP OutputStream
    @Transactional(readOnly = true)
    public void streamFilesAsZip(List<Long> documentIds, OutputStream outputStream) throws IOException {
        try (ZipOutputStream zipOut = new ZipOutputStream(outputStream)) {
            Set<String> usedNames = new HashSet<>();

            for (Long docId : documentIds) {
                Optional<DocumentMetadata> metaOpt = metadataRepo.findById(docId);
                Optional<DocumentContent> contentOpt = contentRepo.findById(docId);

                if (metaOpt.isPresent() && contentOpt.isPresent()) {
                    DocumentMetadata meta = metaOpt.get();
                    String entryName = getUniqueFileName(meta.getFileName(), usedNames);

                    zipOut.putNextEntry(new ZipEntry(entryName));

                    try (InputStream blobStream = contentOpt.get().getData().getBinaryStream()) {
                        blobStream.transferTo(zipOut); // Streams in 8KB chunks
                    } catch (Exception e) {
                        throw new IOException("Error reading SQL Blob for ID: " + docId, e);
                    }

                    zipOut.closeEntry();
                }
            }
            zipOut.finish();
        }
    }

    // Prevents ZipException if two uploaded files share the same filename
    private String getUniqueFileName(String originalName, Set<String> usedNames) {
        String name = (originalName != null) ? originalName : "file";
        if (usedNames.add(name)) {
            return name;
        }
        int counter = 1;
        String baseName = name.contains(".") ? name.substring(0, name.lastIndexOf('.')) : name;
        String ext = name.contains(".") ? name.substring(name.lastIndexOf('.')) : "";
        while (!usedNames.add(baseName + "_" + counter + ext)) {
            counter++;
        }
        return baseName + "_" + counter + ext;
    }
}
