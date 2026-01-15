-- =============================================
-- ELT Script: Load data từ Data Warehouse xuống Data Mart
-- Script này sẽ được gọi định kỳ để sync data từ DW xuống DM
-- =============================================

USE DM_MediaAnalytics
GO

-- =============================================
-- Stored Procedure: Load từ DW xuống DM
-- =============================================

IF OBJECT_ID('sp_ELT_LoadToDataMart', 'P') IS NOT NULL
    DROP PROCEDURE sp_ELT_LoadToDataMart
GO

CREATE PROCEDURE sp_ELT_LoadToDataMart
    @ProcessDate DATE = NULL -- Nếu NULL thì process tất cả
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME = GETDATE()
    
    BEGIN TRY
        BEGIN TRANSACTION
        
        -- Set default date nếu không có
        IF @ProcessDate IS NULL
            SET @ProcessDate = CAST(GETDATE() AS DATE)
        
        PRINT 'Bắt đầu ELT Process từ DW xuống DM cho ngày: ' + CAST(@ProcessDate AS VARCHAR(10))
        
        -- 1. Load vào DM_ContractAnalytics (Denormalized)
        PRINT '1. Loading DM_ContractAnalytics...'
        
        -- Xóa dữ liệu cũ cho ngày này
        DELETE FROM DM_ContractAnalytics
        WHERE DateValue = @ProcessDate
        
        INSERT INTO DM_ContractAnalytics (
            DateValue, Year, Quarter, Month, MonthName, Day,
            ContractID, TotalDevices,
            Duration_TruyenHinh, Duration_PhimTruyen, Duration_GiaiTri,
            Duration_ThieuNhi, Duration_TheThao, Duration_Error, Duration_Total,
            CreatedDate, UpdatedDate
        )
        SELECT 
            d.DateValue,
            d.Year,
            d.Quarter,
            d.Month,
            d.MonthName,
            d.Day,
            c.ContractID,
            fcs.TotalDevices,
            fcs.TotalDuration_TruyenHinh,
            fcs.TotalDuration_PhimTruyen,
            fcs.TotalDuration_GiaiTri,
            fcs.TotalDuration_ThieuNhi,
            fcs.TotalDuration_TheThao,
            fcs.TotalDuration_Error,
            fcs.TotalDuration_All,
            GETDATE(),
            GETDATE()
        FROM DW_MediaAnalytics.dbo.FactContractSummary fcs
        INNER JOIN DW_MediaAnalytics.dbo.DimDate d ON fcs.DateKey = d.DateKey
        INNER JOIN DW_MediaAnalytics.dbo.DimContract c ON fcs.ContractKey = c.ContractKey
        WHERE d.DateValue = @ProcessDate
        
        PRINT 'Đã load ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' dòng vào DM_ContractAnalytics'
        
        -- 2. Load vào DM_DailySummary
        PRINT '2. Loading DM_DailySummary...'
        
        -- Xóa dữ liệu cũ cho ngày này
        DELETE FROM DM_DailySummary
        WHERE DateValue = @ProcessDate
        
        INSERT INTO DM_DailySummary (
            DateValue, Year, Month, Day,
            TotalContracts, TotalDevices,
            TotalDuration_TruyenHinh, TotalDuration_PhimTruyen, TotalDuration_GiaiTri,
            TotalDuration_ThieuNhi, TotalDuration_TheThao, TotalDuration_All,
            CreatedDate, UpdatedDate
        )
        SELECT 
            d.DateValue,
            d.Year,
            d.Month,
            d.Day,
            COUNT(DISTINCT fcs.ContractKey) AS TotalContracts,
            SUM(fcs.TotalDevices) AS TotalDevices,
            SUM(fcs.TotalDuration_TruyenHinh) AS TotalDuration_TruyenHinh,
            SUM(fcs.TotalDuration_PhimTruyen) AS TotalDuration_PhimTruyen,
            SUM(fcs.TotalDuration_GiaiTri) AS TotalDuration_GiaiTri,
            SUM(fcs.TotalDuration_ThieuNhi) AS TotalDuration_ThieuNhi,
            SUM(fcs.TotalDuration_TheThao) AS TotalDuration_TheThao,
            SUM(fcs.TotalDuration_All) AS TotalDuration_All,
            GETDATE(),
            GETDATE()
        FROM DW_MediaAnalytics.dbo.FactContractSummary fcs
        INNER JOIN DW_MediaAnalytics.dbo.DimDate d ON fcs.DateKey = d.DateKey
        WHERE d.DateValue = @ProcessDate
        GROUP BY d.DateValue, d.Year, d.Month, d.Day
        
        PRINT 'Đã load ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' dòng vào DM_DailySummary'
        
        -- 3. Load vào DM_ContentTypeTrend
        PRINT '3. Loading DM_ContentTypeTrend...'
        
        -- Xóa dữ liệu cũ cho ngày này
        DELETE FROM DM_ContentTypeTrend
        WHERE DateValue = @ProcessDate
        
        INSERT INTO DM_ContentTypeTrend (
            DateValue, Year, Month, ContentType,
            TotalDuration, TotalSessions, UniqueContracts,
            CreatedDate, UpdatedDate
        )
        SELECT 
            d.DateValue,
            d.Year,
            d.Month,
            ct.TypeName AS ContentType,
            SUM(fvs.TotalDuration) AS TotalDuration,
            SUM(fvs.SessionCount) AS TotalSessions,
            COUNT(DISTINCT fvs.ContractKey) AS UniqueContracts,
            GETDATE(),
            GETDATE()
        FROM DW_MediaAnalytics.dbo.FactViewingSession fvs
        INNER JOIN DW_MediaAnalytics.dbo.DimDate d ON fvs.DateKey = d.DateKey
        INNER JOIN DW_MediaAnalytics.dbo.DimContentType ct ON fvs.ContentTypeKey = ct.ContentTypeKey
        WHERE d.DateValue = @ProcessDate
        GROUP BY d.DateValue, d.Year, d.Month, ct.TypeName
        
        PRINT 'Đã load ' + CAST(@@ROWCOUNT AS VARCHAR(20)) + ' dòng vào DM_ContentTypeTrend'
        
        COMMIT TRANSACTION
        
        DECLARE @EndTime DATETIME = GETDATE()
        DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, @EndTime)
        
        PRINT 'ELT Process từ DW xuống DM hoàn thành!'
        PRINT 'Thời gian xử lý: ' + CAST(@Duration AS VARCHAR(20)) + ' giây'
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
        DECLARE @ErrorState INT = ERROR_STATE()
        
        PRINT 'Lỗi trong quá trình ELT: ' + @ErrorMessage
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState)
    END CATCH
END
GO

PRINT 'Stored Procedure sp_ELT_LoadToDataMart created successfully!'
GO
