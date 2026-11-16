package com.example.shubham.fileservice.bulkFileUpload.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "document_metadata")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocumentMetadata {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String batchId;
    private String fileName;
    private String contentType;
    private Long fileSize;

    @Enumerated(EnumType.STRING)
    private UploadStatus status; // PROCESSING, COMPLETED, FAILED

    private LocalDateTime createdAt;

    public enum UploadStatus {
        PROCESSING, COMPLETED, FAILED
    }
}