-- =============================================
-- Data Warehouse Schema (Normalized)
-- Thiết kế theo mô hình Star Schema với các Dimension và Fact tables
-- =============================================

USE DW_MediaAnalytics
GO

-- =============================================
-- DIMENSION TABLES (Normalized)
-- =============================================

-- DimDate - Dimension table cho ngày tháng
IF OBJECT_ID('DimDate', 'U') IS NOT NULL
    DROP TABLE DimDate
GO

CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY,
    DateValue DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    MonthName NVARCHAR(20) NOT NULL,
    Day INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName NVARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL,
    CreatedDate DATETIME DEFAULT GETDATE()
)
GO

-- DimContract - Dimension table cho Contract
IF OBJECT_ID('DimContract', 'U') IS NOT NULL
    DROP TABLE DimContract
GO

CREATE TABLE DimContract (
    ContractKey INT IDENTITY(1,1) PRIMARY KEY,
    ContractID NVARCHAR(50) NOT NULL UNIQUE,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
)
GO

-- DimDevice - Dimension table cho Device
IF OBJECT_ID('DimDevice', 'U') IS NOT NULL
    DROP TABLE DimDevice
GO

CREATE TABLE DimDevice (
    DeviceKey INT IDENTITY(1,1) PRIMARY KEY,
    MacAddress NVARCHAR(50) NOT NULL UNIQUE,
    ContractKey INT NOT NULL,
    CreatedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (ContractKey) REFERENCES DimContract(ContractKey)
)
GO

CREATE INDEX IX_DimDevice_ContractKey ON DimDevice(ContractKey)
GO

-- DimApp - Dimension table cho Application
IF OBJECT_ID('DimApp', 'U') IS NOT NULL
    DROP TABLE DimApp
GO

CREATE TABLE DimApp (
    AppKey INT IDENTITY(1,1) PRIMARY KEY,
    AppName NVARCHAR(50) NOT NULL UNIQUE,
    AppCategory NVARCHAR(50) NOT NULL, -- Truyền Hình, Phim Truyện, Giải Trí, Thiếu Nhi, Thể Thao
    CreatedDate DATETIME DEFAULT GETDATE()
)
GO

-- DimContentType - Dimension table cho Content Type
IF OBJECT_ID('DimContentType', 'U') IS NOT NULL
    DROP TABLE DimContentType
GO

CREATE TABLE DimContentType (
    ContentTypeKey INT IDENTITY(1,1) PRIMARY KEY,
    TypeName NVARCHAR(50) NOT NULL UNIQUE, -- Truyền Hình, Phim Truyện, Giải Trí, Thiếu Nhi, Thể Thao
    Description NVARCHAR(255),
    CreatedDate DATETIME DEFAULT GETDATE()
)
GO

-- =============================================
-- FACT TABLES
-- =============================================

-- FactViewingSession - Fact table chính (Normalized)
IF OBJECT_ID('FactViewingSession', 'U') IS NOT NULL
    DROP TABLE FactViewingSession
GO

CREATE TABLE FactViewingSession (
    FactKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    DateKey INT NOT NULL,
    ContractKey INT NOT NULL,
    DeviceKey INT NOT NULL,
    AppKey INT NOT NULL,
    ContentTypeKey INT NOT NULL,
    TotalDuration BIGINT NOT NULL, -- Tổng thời gian xem (giây)
    SessionCount INT DEFAULT 1, -- Số lượng session
    CreatedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ContractKey) REFERENCES DimContract(ContractKey),
    FOREIGN KEY (DeviceKey) REFERENCES DimDevice(DeviceKey),
    FOREIGN KEY (AppKey) REFERENCES DimApp(AppKey),
    FOREIGN KEY (ContentTypeKey) REFERENCES DimContentType(ContentTypeKey)
)
GO

CREATE INDEX IX_FactViewingSession_DateKey ON FactViewingSession(DateKey)
CREATE INDEX IX_FactViewingSession_ContractKey ON FactViewingSession(ContractKey)
CREATE INDEX IX_FactViewingSession_ContentTypeKey ON FactViewingSession(ContentTypeKey)
GO

-- FactContractSummary - Fact table tổng hợp theo Contract (Denormalized một phần để tối ưu query)
IF OBJECT_ID('FactContractSummary', 'U') IS NOT NULL
    DROP TABLE FactContractSummary
GO

CREATE TABLE FactContractSummary (
    SummaryKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    DateKey INT NOT NULL,
    ContractKey INT NOT NULL,
    TotalDevices INT NOT NULL,
    TotalDuration_TruyenHinh BIGINT DEFAULT 0,
    TotalDuration_PhimTruyen BIGINT DEFAULT 0,
    TotalDuration_GiaiTri BIGINT DEFAULT 0,
    TotalDuration_ThieuNhi BIGINT DEFAULT 0,
    TotalDuration_TheThao BIGINT DEFAULT 0,
    TotalDuration_Error BIGINT DEFAULT 0,
    TotalDuration_All BIGINT DEFAULT 0,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey),
    FOREIGN KEY (ContractKey) REFERENCES DimContract(ContractKey)
)
GO

CREATE INDEX IX_FactContractSummary_DateKey ON FactContractSummary(DateKey)
CREATE INDEX IX_FactContractSummary_ContractKey ON FactContractSummary(ContractKey)
GO

-- =============================================
-- STAGING TABLE (Temporary table để load data)
-- =============================================

IF OBJECT_ID('Staging_RawData', 'U') IS NOT NULL
    DROP TABLE Staging_RawData
GO

CREATE TABLE Staging_RawData (
    StagingKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    Contract NVARCHAR(50),
    Mac NVARCHAR(50),
    AppName NVARCHAR(50),
    TotalDuration BIGINT,
    EventDate DATE,
    ProcessedDate DATETIME DEFAULT GETDATE()
)
GO

PRINT 'Data Warehouse schema created successfully!'
GO
