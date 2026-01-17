USE [DM_MediaAnalytics]
GO
/****** Object:  StoredProcedure [dbo].[sp_ELT_LoadToDataMart]    Script Date: 1/17/2026 2:41:30 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER   PROCEDURE [dbo].[sp_ELT_LoadToDataMart]
    @ProcessDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON; -- [SỬA] Đảm bảo dọn dẹp Transaction khi có lỗi để Python không báo lỗi 25000

    
    BEGIN TRY
        BEGIN TRANSACTION;

        -- 1. Đảm bảo bảng đích tồn tại (Dùng Fact_MediaReport thay vì tên lạ)
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Fact_MediaReport')
        BEGIN
            CREATE TABLE [dbo].[Fact_MediaReport] (
                ReportDate DATE,
                AppName NVARCHAR(255),
                TotalDuration BIGINT,
                TotalAccess INT
            );
        END

        -- 2. Xóa dữ liệu cũ để nạp mới
        TRUNCATE TABLE [dbo].[Fact_MediaReport];

        -- 3. Tổng hợp dữ liệu từ DW sang DM
        -- Đảm bảo bạn gọi đúng Database DW_MediaAnalytics
        INSERT INTO [dbo].[Fact_MediaReport] (ReportDate, AppName, TotalDuration, TotalAccess)
        SELECT 
            EventDate, 
            AppName, 
            SUM(CAST(TotalDuration AS BIGINT)), 
            COUNT(*)
        FROM [DW_MediaAnalytics].[dbo].[Staging_RawData]
        WHERE (@ProcessDate IS NULL OR EventDate = @ProcessDate)
        GROUP BY EventDate, AppName;

        COMMIT TRANSACTION;
        PRINT '✓ Completed ELT from Warehouse to Data Mart.';
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        PRINT 'Error in ELT to DM process.: ' + @ErrorMessage;
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END
