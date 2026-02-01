USE [DW_MediaAnalytics]
GO
/****** Object:  StoredProcedure [dbo].[sp_ELT_LoadToDataWarehouse]    Script Date: 1/17/2026 1:33:52 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER PROCEDURE [dbo].[sp_ELT_LoadToDataWarehouse]
    @ProcessDate DATE = NULL -- Nếu NULL thì process tất cả
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON; -- [CẦN THÊM] Đảm bảo transaction được dọn dẹp sạch sẽ khi lỗi
    
    DECLARE @StartTime DATETIME = GETDATE()
    DECLARE @RowsProcessed INT = 0
    
    BEGIN TRY
        BEGIN TRANSACTION
        
        -- Nếu @ProcessDate IS NULL thì xử lý TẤT CẢ dữ liệu
        IF @ProcessDate IS NULL
            PRINT 'Bắt đầu ELT Process cho TẤT CẢ dữ liệu'
        ELSE
            PRINT 'Bắt đầu ELT Process cho ngày: ' + CAST(@ProcessDate AS VARCHAR(10))
        

        -- 1. Load vào DimContract
        PRINT '1. Loading DimContract...'
        INSERT INTO DimContract (ContractID)
        SELECT DISTINCT Contract
        FROM Staging_RawData
        WHERE Contract IS NOT NULL
          AND (@ProcessDate IS NULL OR EventDate = @ProcessDate)
          AND Contract NOT IN (SELECT ContractID FROM DimContract)
        
        -- 2. Load vào DimDevice
        PRINT '2. Loading DimDevice...'
        -- Sử dụng MERGE để xử lý duplicate MacAddress (nếu MacAddress đã tồn tại, không insert lại)
        -- Nếu MacAddress có nhiều Contract, lấy ContractKey đầu tiên (MIN)
        MERGE DimDevice AS target
        USING (
            SELECT DISTINCT 
                s.Mac AS MacAddress,
                MIN(c.ContractKey) AS ContractKey  -- Lấy ContractKey đầu tiên nếu có nhiều
            FROM Staging_RawData s
            INNER JOIN DimContract c ON s.Contract = c.ContractID
            WHERE s.Mac IS NOT NULL
              AND (@ProcessDate IS NULL OR s.EventDate = @ProcessDate)
            GROUP BY s.Mac
        ) AS source
        ON target.MacAddress = source.MacAddress
        WHEN NOT MATCHED THEN
            INSERT (MacAddress, ContractKey)
            VALUES (source.MacAddress, source.ContractKey);
        
        -- 3. Load vào FactViewingSession (Normalized)
        PRINT '3. Loading FactViewingSession...'
        INSERT INTO FactViewingSession (
            DateKey,
            ContractKey,
            DeviceKey,
            AppKey,
            ContentTypeKey,
            TotalDuration,
            SessionCount
        )
        SELECT 
            d.DateKey,
            c.ContractKey,
            dev.DeviceKey,
            a.AppKey,
            ct.ContentTypeKey,
            SUM(ISNULL(s.TotalDuration, 0)) AS TotalDuration,
            COUNT(*) AS SessionCount
        FROM Staging_RawData s
        INNER JOIN DimDate d ON CAST(FORMAT(s.EventDate, 'yyyyMMdd') AS INT) = d.DateKey
        INNER JOIN DimContract c ON s.Contract = c.ContractID
        INNER JOIN DimDevice dev ON s.Mac = dev.MacAddress
        INNER JOIN DimApp a ON s.AppName = a.AppName
        INNER JOIN DimContentType ct ON a.AppCategory = ct.TypeName
        WHERE (@ProcessDate IS NULL OR s.EventDate = @ProcessDate)
        GROUP BY 
            d.DateKey,
            c.ContractKey,
            dev.DeviceKey,
            a.AppKey,
            ct.ContentTypeKey
        
        SET @RowsProcessed = @@ROWCOUNT
        
        -- 4. Load vào FactContractSummary (Denormalized summary)
        PRINT '4. Loading FactContractSummary...'
        
        -- Xóa dữ liệu cũ cho ngày này (nếu có) hoặc tất cả (nếu @ProcessDate IS NULL)
        IF @ProcessDate IS NULL
        BEGIN
            -- Xóa tất cả dữ liệu cũ trong FactContractSummary
            DELETE FROM FactContractSummary
        END
        ELSE
        BEGIN
            -- Xóa dữ liệu cũ cho ngày cụ thể
            DELETE FROM FactContractSummary
            WHERE DateKey IN (
                SELECT DateKey FROM DimDate 
                WHERE DateValue = @ProcessDate
            )
        END
        
        INSERT INTO FactContractSummary (
            DateKey,
            ContractKey,
            TotalDevices,
            TotalDuration_TruyenHinh,
            TotalDuration_PhimTruyen,
            TotalDuration_GiaiTri,
            TotalDuration_ThieuNhi,
            TotalDuration_TheThao,
            TotalDuration_Error,
            TotalDuration_All
        )
        SELECT 
            d.DateKey,
            c.ContractKey,
            COUNT(DISTINCT dev.DeviceKey) AS TotalDevices,
            SUM(CASE WHEN ct.TypeName = 'Truyền Hình' THEN ISNULL(f.TotalDuration, 0) ELSE 0 END) AS TotalDuration_TruyenHinh,
            SUM(CASE WHEN ct.TypeName = 'Phim Truyện' THEN ISNULL(f.TotalDuration, 0) ELSE 0 END) AS TotalDuration_PhimTruyen,
            SUM(CASE WHEN ct.TypeName = 'Giải Trí' THEN ISNULL(f.TotalDuration, 0) ELSE 0 END) AS TotalDuration_GiaiTri,
            SUM(CASE WHEN ct.TypeName = 'Thiếu Nhi' THEN ISNULL(f.TotalDuration, 0) ELSE 0 END) AS TotalDuration_ThieuNhi,
            SUM(CASE WHEN ct.TypeName = 'Thể Thao' THEN ISNULL(f.TotalDuration, 0) ELSE 0 END) AS TotalDuration_TheThao,
            SUM(CASE WHEN ct.TypeName = 'Error' THEN ISNULL(f.TotalDuration, 0) ELSE 0 END) AS TotalDuration_Error,
            SUM(ISNULL(f.TotalDuration, 0)) AS TotalDuration_All
        FROM FactViewingSession f
        INNER JOIN DimDate d ON f.DateKey = d.DateKey
        INNER JOIN DimContract c ON f.ContractKey = c.ContractKey
        INNER JOIN DimDevice dev ON f.DeviceKey = dev.DeviceKey
        INNER JOIN DimContentType ct ON f.ContentTypeKey = ct.ContentTypeKey
        WHERE (@ProcessDate IS NULL OR d.DateValue = @ProcessDate)
        GROUP BY d.DateKey, c.ContractKey
        
        COMMIT TRANSACTION
            
        DECLARE @EndTime DATETIME = GETDATE()

        -- Đoạn tính toán thời gian nên để SAU khi COMMIT để chính xác
        DECLARE @Duration INT = DATEDIFF(SECOND, @StartTime, GETDATE())
        PRINT 'ELT Process hoàn thành!'
        PRINT 'Số dòng đã xử lý: ' + CAST(@RowsProcessed AS VARCHAR(20))
        PRINT 'Thời gian xử lý: ' + CAST(@Duration AS VARCHAR(20)) + ' giây'
        
    END TRY
    BEGIN CATCH
        -- Kiểm tra transaction trước khi Rollback
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
        -- In lỗi ra để dễ debug trong SSMS
        PRINT 'Lỗi trong quá trình ELT sang DM: ' + @ErrorMessage
        -- Ném lỗi về cho Python nhận diện
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH
END
