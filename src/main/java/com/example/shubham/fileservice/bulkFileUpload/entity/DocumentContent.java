package com.example.shubham.fileservice.bulkFileUpload.entity;

import jakarta.persistence.*;
import lombok.*;
import java.sql.Blob;

@Entity
@Table(name = "document_content")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DocumentContent {

    @Id
    private Long documentId; // Matches DocumentMetadata.id

    @Lob
    @Column(columnDefinition = "VARBINARY(MAX)")
    private Blob data;
}