package com.example.shubham.fileservice.bulkFileUpload.repository;

import com.example.shubham.fileservice.bulkFileUpload.entity.DocumentContent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DocumentContentRepository extends JpaRepository<DocumentContent, Long> {
}