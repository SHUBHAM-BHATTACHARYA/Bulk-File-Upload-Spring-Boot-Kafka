package com.example.shubham.fileservice.bulkFileUpload.dto;

import lombok.*;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FileUploadEvent {
    private Long documentId;
    private String batchId;
    private String tempFilePath;
}