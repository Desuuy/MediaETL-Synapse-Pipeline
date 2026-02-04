-- =============================================
-- SQL Queries cho Grafana Dashboards
-- Copy các query này vào Grafana panels
-- =============================================

-- =============================================
-- OVERVIEW DASHBOARD QUERIES
-- =============================================

-- 1. Total Contracts (Last 7 Days)
SELECT SUM(TotalContracts) as value 
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -7, GETDATE())

-- 2. Total Devices (Last 7 Days)
SELECT SUM(TotalDevices) as value 
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -7, GETDATE())

-- 3. Total Duration - Hours (Last 7 Days)
SELECT SUM(TotalDuration_All) / 3600.0 as value 
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -7, GETDATE())

-- 4. Daily Duration Trend (Time Series)
SELECT 
    DateValue as time,
    TotalDuration_All / 3600.0 as duration_hours
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
ORDER BY DateValue

-- 5. Content Type Distribution (Pie Chart)
SELECT 
    'Truyền Hình' as label,
    SUM(TotalDuration_TruyenHinh) / 3600.0 as value
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
UNION ALL
SELECT 
    'Phim Truyện',
    SUM(TotalDuration_PhimTruyen) / 3600.0
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
UNION ALL
SELECT 
    'Giải Trí',
    SUM(TotalDuration_GiaiTri) / 3600.0
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
UNION ALL
SELECT 
    'Thiếu Nhi',
    SUM(TotalDuration_ThieuNhi) / 3600.0
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
UNION ALL
SELECT 
    'Thể Thao',
    SUM(TotalDuration_TheThao) / 3600.0
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -30, GETDATE())

-- =============================================
-- CONTRACT ANALYTICS DASHBOARD QUERIES
-- =============================================

-- 1. Top 10 Contracts by Duration
SELECT TOP 10 
    ContractID,
    SUM(Duration_Total) / 3600.0 as duration_hours
FROM DM_ContractAnalytics 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
GROUP BY ContractID
ORDER BY duration_hours DESC

-- 2. Contract Duration by Content Type (Time Series)
SELECT 
    DateValue as time,
    Duration_TruyenHinh / 3600.0 as TruyenHinh,
    Duration_PhimTruyen / 3600.0 as PhimTruyen,
    Duration_GiaiTri / 3600.0 as GiaiTri,
    Duration_ThieuNhi / 3600.0 as ThieuNhi,
    Duration_TheThao / 3600.0 as TheThao
FROM DM_ContractAnalytics 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
    AND ContractID = '$contract'  -- Variable từ Grafana
ORDER BY DateValue

-- 3. Contract Details Table
SELECT TOP 20
    ContractID,
    SUM(TotalDevices) as TotalDevices,
    SUM(Duration_Total) / 3600.0 as TotalDuration_Hours,
    SUM(Duration_TruyenHinh) / 3600.0 as TH_Hours,
    SUM(Duration_PhimTruyen) / 3600.0 as PT_Hours,
    SUM(Duration_GiaiTri) / 3600.0 as GT_Hours,
    SUM(Duration_ThieuNhi) / 3600.0 as TN_Hours,
    SUM(Duration_TheThao) / 3600.0 as TT_Hours
FROM DM_ContractAnalytics 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
GROUP BY ContractID
ORDER BY TotalDuration_Hours DESC

-- 4. Contract Variable Query (for dropdown)
SELECT DISTINCT ContractID 
FROM DM_ContractAnalytics 
ORDER BY ContractID

-- =============================================
-- CONTENT TYPE TRENDS DASHBOARD QUERIES
-- =============================================

-- 1. Content Type Trends Over Time (Time Series)
SELECT 
    DateValue as time,
    TotalDuration / 3600.0 as duration_hours,
    ContentType
FROM DM_ContentTypeTrend 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
ORDER BY DateValue, ContentType

-- 2. Content Type Comparison (Bar Chart)
SELECT 
    ContentType,
    SUM(TotalDuration) / 3600.0 as duration_hours
FROM DM_ContentTypeTrend 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
GROUP BY ContentType
ORDER BY duration_hours DESC

-- 3. Content Type Statistics (Table)
SELECT 
    ContentType,
    SUM(TotalDuration) / 3600.0 as TotalDuration_Hours,
    SUM(TotalSessions) as TotalSessions,
    SUM(UniqueContracts) as UniqueContracts,
    AVG(TotalDuration) / 3600.0 as AvgDuration_Hours
FROM DM_ContentTypeTrend 
WHERE DateValue >= DATEADD(day, -30, GETDATE())
GROUP BY ContentType
ORDER BY TotalDuration_Hours DESC

-- =============================================
-- REAL-TIME QUERIES (Last 24 Hours)
-- =============================================

-- Real-time Daily Summary
SELECT 
    DateValue as time,
    TotalContracts,
    TotalDevices,
    TotalDuration_All / 3600.0 as Duration_Hours
FROM DM_DailySummary 
WHERE DateValue >= DATEADD(day, -1, GETDATE())
ORDER BY DateValue DESC

-- Real-time Content Type
SELECT 
    ContentType,
    SUM(TotalDuration) / 3600.0 as Duration_Hours
FROM DM_ContentTypeTrend 
WHERE DateValue >= DATEADD(day, -1, GETDATE())
GROUP BY ContentType
ORDER BY Duration_Hours DESC
