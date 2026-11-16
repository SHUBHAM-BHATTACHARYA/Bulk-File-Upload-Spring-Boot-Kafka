-- 1. Create the Database
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'FileStorageDB')
BEGIN
    CREATE DATABASE FileStorageDB;
END
GO

USE FileStorageDB;
GO

-- 2. Create Document Metadata Table
IF OBJECT_ID(N'dbo.document_metadata', N'U') IS NULL
BEGIN
CREATE TABLE dbo.document_metadata (
    id BIGINT IDENTITY(1,1) NOT NULL,
    batch_id VARCHAR(36) NOT NULL,
    file_name NVARCHAR(255) NOT NULL,
    content_type VARCHAR(150) NULL,
    file_size BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at DATETIME2(6) NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT PK_document_metadata PRIMARY KEY CLUSTERED (id ASC),
    CONSTRAINT CHK_document_status CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED'))
);
END
GO

-- 3. Create Index on batch_id for fast status polling
IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_document_metadata_batch_id'
      AND object_id = OBJECT_ID(N'dbo.document_metadata')
)
BEGIN
    CREATE NONCLUSTERED INDEX IX_document_metadata_batch_id
    ON dbo.document_metadata (batch_id)
    INCLUDE (file_name, status, file_size, created_at);
END
GO

-- 4. Create Document Content Table (Separated to prevent table bloat)
IF OBJECT_ID(N'dbo.document_content', N'U') IS NULL
BEGIN
CREATE TABLE dbo.document_content (
    document_id BIGINT NOT NULL,
    data VARBINARY(MAX) NOT NULL,

    CONSTRAINT PK_document_content PRIMARY KEY CLUSTERED (document_id ASC),
    CONSTRAINT FK_document_content_metadata FOREIGN KEY (document_id)
      REFERENCES dbo.document_metadata (id)
      ON DELETE CASCADE
);
END
GO