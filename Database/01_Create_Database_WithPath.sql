-- =============================================
-- Script tạo Database với đường dẫn cụ thể
-- Sử dụng script này nếu bạn muốn chỉ định đường dẫn file database
-- =============================================
-- LƯU Ý: Phải tạo thư mục C:\Data\ trước khi chạy script này!

-- Tạo thư mục nếu chưa có (cần quyền admin và enable xp_cmdshell)
-- Hoặc tạo thủ công: mkdir C:\Data

-- Tạo Database Data Warehouse (Normalized)
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DW_MediaAnalytics')
BEGIN
    CREATE DATABASE DW_MediaAnalytics
    ON 
    ( NAME = 'DW_MediaAnalytics',
      FILENAME = 'C:\Data\DW_MediaAnalytics.mdf',
      SIZE = 100MB,
      MAXSIZE = 10GB,
      FILEGROWTH = 10MB )
    LOG ON 
    ( NAME = 'DW_MediaAnalytics_Log',
      FILENAME = 'C:\Data\DW_MediaAnalytics_Log.ldf',
      SIZE = 10MB,
      MAXSIZE = 1GB,
      FILEGROWTH = 10% );
END
GO

-- Tạo Database Data Mart (Denormalized)
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DM_MediaAnalytics')
BEGIN
    CREATE DATABASE DM_MediaAnalytics
    ON 
    ( NAME = 'DM_MediaAnalytics',
      FILENAME = 'C:\Data\DM_MediaAnalytics.mdf',
      SIZE = 100MB,
      MAXSIZE = 10GB,
      FILEGROWTH = 10MB )
    LOG ON 
    ( NAME = 'DM_MediaAnalytics_Log',
      FILENAME = 'C:\Data\DM_MediaAnalytics_Log.ldf',
      SIZE = 10MB,
      MAXSIZE = 1GB,
      FILEGROWTH = 10% );
END
GO

PRINT 'Databases created successfully in C:\Data\'
GO
