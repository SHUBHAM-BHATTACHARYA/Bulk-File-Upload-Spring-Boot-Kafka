package com.example.shubham.fileservice.bulkFileUpload.controller;


import com.example.shubham.fileservice.bulkFileUpload.entity.DocumentMetadata;
import com.example.shubham.fileservice.bulkFileUpload.repository.DocumentMetadataRepository;
import com.example.shubham.fileservice.bulkFileUpload.service.FileStorageService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/files")
@RequiredArgsConstructor
@Tag(name = "File Attachments", description = "Async multi-file upload via Kafka and ZIP streaming download")
public class FileController {

    private final FileStorageService fileStorageService;
    private final DocumentMetadataRepository metadataRepository;

    @Operation(summary = "Upload multiple files asynchronously")
    @PostMapping(value = "/upload", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseEntity<Map<String, Object>> uploadMultipleFiles(
            @Parameter(
                    description = "Select multiple files to upload",
                    content = @Content(mediaType = MediaType.MULTIPART_FORM_DATA_VALUE)
            )
            @RequestPart("files") MultipartFile[] files) throws IOException {

        Map<String, Object> response = fileStorageService.stageAndPublishFiles(files);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }

    @Operation(summary = "Check upload processing status by batchId")
    @GetMapping("/status/{batchId}")
    public ResponseEntity<List<DocumentMetadata>> getBatchStatus(@PathVariable String batchId) {
        return ResponseEntity.ok(metadataRepository.findByBatchId(batchId));
    }

    @Operation(summary = "Requeue failed or stuck files from staging directory")
    @PostMapping("/retry-failed") //here we are changing the state of the files so we are using POST instead of GET
    public ResponseEntity<Map<String, Object>> retryFailedOrUnprocessedFiles() {
        int requeuedCount = fileStorageService.reprocessPendingFiles();
        return ResponseEntity.ok(Map.of(
                "requeuedFiles", requeuedCount,
                "status", "PROCESSING"
        ));
    }

    @Operation(summary = "Download multiple files as a single ZIP archive")
    @GetMapping(value = "/download", produces = "application/zip")
    public void downloadMultipleFiles(
            @Parameter(description = "Comma-separated list of document IDs (e.g., 1,2,3)")
            @RequestParam List<Long> ids,
            HttpServletResponse response) throws IOException {

        response.setContentType("application/zip");
        response.setHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"attachments.zip\"");

        fileStorageService.streamFilesAsZip(ids, response.getOutputStream());
    }
}