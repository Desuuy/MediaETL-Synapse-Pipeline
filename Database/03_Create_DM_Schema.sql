-- =============================================
-- Data Mart Schema (Denormalized)
-- Thiết kế denormalized để tối ưu cho reporting và dashboard
-- =============================================

USE DM_MediaAnalytics
GO

-- =============================================
-- DATA MART TABLES (Denormalized)
-- =============================================

-- DM_ContractAnalytics - Denormalized table cho phân tích Contract
IF OBJECT_ID('DM_ContractAnalytics', 'U') IS NOT NULL
    DROP TABLE DM_ContractAnalytics
GO

CREATE TABLE DM_ContractAnalytics (
    AnalyticsKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    -- Date Information
    DateValue DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    MonthName NVARCHAR(20) NOT NULL,
    Day INT NOT NULL,
    -- Contract Information
    ContractID NVARCHAR(50) NOT NULL,
    -- Device Information
    TotalDevices INT NOT NULL,
    -- Content Type Duration (Denormalized)
    Duration_TruyenHinh BIGINT DEFAULT 0,
    Duration_PhimTruyen BIGINT DEFAULT 0,
    Duration_GiaiTri BIGINT DEFAULT 0,
    Duration_ThieuNhi BIGINT DEFAULT 0,
    Duration_TheThao BIGINT DEFAULT 0,
    Duration_Error BIGINT DEFAULT 0,
    Duration_Total BIGINT DEFAULT 0,
    -- Metadata
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
)
GO

CREATE INDEX IX_DM_ContractAnalytics_DateValue ON DM_ContractAnalytics(DateValue)
CREATE INDEX IX_DM_ContractAnalytics_ContractID ON DM_ContractAnalytics(ContractID)
CREATE INDEX IX_DM_ContractAnalytics_Year_Month ON DM_ContractAnalytics(Year, Month)
GO

-- DM_DailySummary - Tổng hợp theo ngày
IF OBJECT_ID('DM_DailySummary', 'U') IS NOT NULL
    DROP TABLE DM_DailySummary
GO

CREATE TABLE DM_DailySummary (
    SummaryKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    DateValue DATE NOT NULL UNIQUE,
    Year INT NOT NULL,
    Month INT NOT NULL,
    Day INT NOT NULL,
    TotalContracts INT DEFAULT 0,
    TotalDevices INT DEFAULT 0,
    TotalDuration_TruyenHinh BIGINT DEFAULT 0,
    TotalDuration_PhimTruyen BIGINT DEFAULT 0,
    TotalDuration_GiaiTri BIGINT DEFAULT 0,
    TotalDuration_ThieuNhi BIGINT DEFAULT 0,
    TotalDuration_TheThao BIGINT DEFAULT 0,
    TotalDuration_All BIGINT DEFAULT 0,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
)
GO

CREATE INDEX IX_DM_DailySummary_DateValue ON DM_DailySummary(DateValue)
CREATE INDEX IX_DM_DailySummary_Year_Month ON DM_DailySummary(Year, Month)
GO

-- DM_ContentTypeTrend - Trend theo Content Type
IF OBJECT_ID('DM_ContentTypeTrend', 'U') IS NOT NULL
    DROP TABLE DM_ContentTypeTrend
GO

CREATE TABLE DM_ContentTypeTrend (
    TrendKey BIGINT IDENTITY(1,1) PRIMARY KEY,
    DateValue DATE NOT NULL,
    Year INT NOT NULL,
    Month INT NOT NULL,
    ContentType NVARCHAR(50) NOT NULL,
    TotalDuration BIGINT DEFAULT 0,
    TotalSessions INT DEFAULT 0,
    UniqueContracts INT DEFAULT 0,
    CreatedDate DATETIME DEFAULT GETDATE(),
    UpdatedDate DATETIME DEFAULT GETDATE()
)
GO

CREATE INDEX IX_DM_ContentTypeTrend_DateValue ON DM_ContentTypeTrend(DateValue)
CREATE INDEX IX_DM_ContentTypeTrend_ContentType ON DM_ContentTypeTrend(ContentType)
GO

PRINT 'Data Mart schema created successfully!'
GO
