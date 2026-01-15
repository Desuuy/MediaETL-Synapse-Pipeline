-- =============================================
-- Tạo SQL Server Agent Jobs để tự động chạy ELT
-- =============================================

USE msdb
GO

-- =============================================
-- Job 1: Chạy ELT từ Staging vào DW (Hàng ngày lúc 2:00 AM)
-- =============================================

IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE name = 'ETL_LoadToDataWarehouse')
    EXEC sp_delete_job @job_name = 'ETL_LoadToDataWarehouse'
GO

EXEC sp_add_job
    @job_name = 'ETL_LoadToDataWarehouse',
    @description = 'Tự động chạy ELT load data từ Staging vào Data Warehouse hàng ngày',
    @enabled = 1
GO

EXEC sp_add_jobstep
    @job_name = 'ETL_LoadToDataWarehouse',
    @step_name = 'Run ELT to DW',
    @subsystem = 'TSQL',
    @command = 'EXEC DW_MediaAnalytics.dbo.sp_ELT_LoadToDataWarehouse @ProcessDate = NULL',
    @database_name = 'DW_MediaAnalytics',
    @on_success_action = 1, -- Quit with success
    @on_fail_action = 2 -- Quit with failure
GO

EXEC sp_add_schedule
    @schedule_name = 'Daily_2AM',
    @freq_type = 4, -- Daily
    @freq_interval = 1, -- Every day
    @active_start_time = 20000 -- 2:00 AM (HHMMSS format)
GO

EXEC sp_attach_schedule
    @job_name = 'ETL_LoadToDataWarehouse',
    @schedule_name = 'Daily_2AM'
GO

EXEC sp_add_jobserver
    @job_name = 'ETL_LoadToDataWarehouse',
    @server_name = '(local)'
GO

-- =============================================
-- Job 2: Chạy ELT từ DW xuống DM (Hàng ngày lúc 3:00 AM)
-- =============================================

IF EXISTS (SELECT * FROM msdb.dbo.sysjobs WHERE name = 'ETL_LoadToDataMart')
    EXEC sp_delete_job @job_name = 'ETL_LoadToDataMart'
GO

EXEC sp_add_job
    @job_name = 'ETL_LoadToDataMart',
    @description = 'Tự động chạy ELT load data từ Data Warehouse xuống Data Mart hàng ngày',
    @enabled = 1
GO

EXEC sp_add_jobstep
    @job_name = 'ETL_LoadToDataMart',
    @step_name = 'Run ELT to DM',
    @subsystem = 'TSQL',
    @command = 'EXEC DM_MediaAnalytics.dbo.sp_ELT_LoadToDataMart @ProcessDate = NULL',
    @database_name = 'DM_MediaAnalytics',
    @on_success_action = 1,
    @on_fail_action = 2
GO

EXEC sp_add_schedule
    @schedule_name = 'Daily_3AM',
    @freq_type = 4,
    @freq_interval = 1,
    @active_start_time = 30000 -- 3:00 AM
GO

EXEC sp_attach_schedule
    @job_name = 'ETL_LoadToDataMart',
    @schedule_name = 'Daily_3AM'
GO

EXEC sp_add_jobserver
    @job_name = 'ETL_LoadToDataMart',
    @server_name = '(local)'
GO

PRINT 'SQL Server Agent Jobs created successfully!'
PRINT 'Job 1: ETL_LoadToDataWarehouse - Chạy hàng ngày lúc 2:00 AM'
PRINT 'Job 2: ETL_LoadToDataMart - Chạy hàng ngày lúc 3:00 AM'
GO
