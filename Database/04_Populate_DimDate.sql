-- =============================================
-- Populate DimDate table với dữ liệu từ 2020 đến 2030
-- =============================================

USE DW_MediaAnalytics
GO

-- Xóa dữ liệu cũ nếu có
TRUNCATE TABLE DimDate
GO

-- Populate DimDate
DECLARE @StartDate DATE = '2020-01-01'
DECLARE @EndDate DATE = '2030-12-31'
DECLARE @CurrentDate DATE = @StartDate

WHILE @CurrentDate <= @EndDate
BEGIN
    INSERT INTO DimDate (
        DateKey,
        DateValue,
        Year,
        Quarter,
        Month,
        MonthName,
        Day,
        DayOfWeek,
        DayName,
        IsWeekend
    )
    VALUES (
        CAST(FORMAT(@CurrentDate, 'yyyyMMdd') AS INT), -- DateKey: 20220401
        @CurrentDate,
        YEAR(@CurrentDate),
        DATEPART(QUARTER, @CurrentDate),
        MONTH(@CurrentDate),
        DATENAME(MONTH, @CurrentDate),
        DAY(@CurrentDate),
        DATEPART(WEEKDAY, @CurrentDate),
        DATENAME(WEEKDAY, @CurrentDate),
        CASE WHEN DATEPART(WEEKDAY, @CurrentDate) IN (1, 7) THEN 1 ELSE 0 END -- 1=Sunday, 7=Saturday
    )
    
    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate)
END

PRINT 'DimDate populated successfully!'
GO

-- Populate DimApp và DimContentType với dữ liệu mặc định
INSERT INTO DimApp (AppName, AppCategory) VALUES
('CHANNEL', 'Truyền Hình'),
('DSHD', 'Truyền Hình'),
('KPLUS', 'Truyền Hình'),
('KPlus', 'Truyền Hình'),
('VOD', 'Phim Truyện'),
('FIMS_RES', 'Phim Truyện'),
('BHD_RES', 'Phim Truyện'),
('VOD_RES', 'Phim Truyện'),
('FIMS', 'Phim Truyện'),
('BHD', 'Phim Truyện'),
('DANET', 'Phim Truyện'),
('RELAX', 'Giải Trí'),
('CHILD', 'Thiếu Nhi'),
('SPORT', 'Thể Thao')
GO

INSERT INTO DimContentType (TypeName, Description) VALUES
('Truyền Hình', 'Nội dung truyền hình trực tiếp'),
('Phim Truyện', 'Phim và series'),
('Giải Trí', 'Nội dung giải trí'),
('Thiếu Nhi', 'Nội dung dành cho trẻ em'),
('Thể Thao', 'Nội dung thể thao'),
('Error', 'Lỗi hoặc không xác định')
GO

PRINT 'Dimension tables populated successfully!'
GO
