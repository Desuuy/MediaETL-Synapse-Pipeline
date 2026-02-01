-- =============================================
-- Script tạo Database cho Data Warehouse và Data Mart
-- =============================================
-- Lưu ý: Script này sẽ tạo database trong thư mục Data mặc định của SQL Server
-- Nếu muốn chỉ định đường dẫn cụ thể, tạo thư mục trước và uncomment phần FILENAME

-- Tạo Database Data Warehouse (Normalized)
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'DW_MediaAnalytics')
BEGIN
    CREATE DATABASE DW_MediaAnalytics
    ON 
    ( NAME = 'DW_MediaAnalytics',
      -- FILENAME = 'C:\Data\DW_MediaAnalytics.mdf',  -- Uncomment và tạo thư mục C:\Data\ trước nếu muốn dùng
      SIZE = 100MB,
      MAXSIZE = 10GB,
      FILEGROWTH = 10MB )
    LOG ON 
    ( NAME = 'DW_MediaAnalytics_Log',
      -- FILENAME = 'C:\Data\DW_MediaAnalytics_Log.ldf',  -- Uncomment nếu muốn chỉ định đường dẫn
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
      -- FILENAME = 'C:\Data\DM_MediaAnalytics.mdf',  -- Uncomment và tạo thư mục C:\Data\ trước nếu muốn dùng
      SIZE = 100MB,
      MAXSIZE = 10GB,
      FILEGROWTH = 10MB )
    LOG ON 
    ( NAME = 'DM_MediaAnalytics_Log',
      -- FILENAME = 'C:\Data\DM_MediaAnalytics_Log.ldf',  -- Uncomment nếu muốn chỉ định đường dẫn
      SIZE = 10MB,
      MAXSIZE = 1GB,
      FILEGROWTH = 10% );
END
GO

PRINT 'Databases created successfully!'
PRINT 'Databases được tạo trong thư mục Data mặc định của SQL Server'
PRINT 'Để xem đường dẫn:'
PRINT '  - DW: SELECT name, physical_name FROM sys.master_files WHERE database_id = DB_ID(''DW_MediaAnalytics'')'
PRINT '  - DM: SELECT name, physical_name FROM sys.master_files WHERE database_id = DB_ID(''DM_MediaAnalytics'')'
GO
