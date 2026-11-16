package com.example.shubham.fileservice.bulkFileUpload.repository;

import com.example.shubham.fileservice.bulkFileUpload.entity.DocumentMetadata;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface DocumentMetadataRepository extends JpaRepository<DocumentMetadata, Long> {
    List<DocumentMetadata> findByBatchId(String batchId);

    List<DocumentMetadata> findByStatusIn(List<DocumentMetadata.UploadStatus> processing);
}
