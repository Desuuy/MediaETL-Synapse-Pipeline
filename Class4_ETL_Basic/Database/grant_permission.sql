-- =============================================
-- Script cấp quyền tạo database cho user
-- CHẠY VỚI QUYỀN SYSADMIN (sa hoặc admin)
-- =============================================

USE master;
GO

PRINT '========================================'
PRINT 'CẤP QUYỀN TẠO DATABASE'
PRINT '========================================'
PRINT ''

-- Cấp quyền cho user hiện tại (nếu chạy với quyền admin)
DECLARE @CurrentUser NVARCHAR(128) = SYSTEM_USER;
PRINT 'Đang cấp quyền cho: ' + @CurrentUser;
PRINT ''

-- Thêm vào role dbcreator (có quyền tạo database)
BEGIN TRY
    ALTER SERVER ROLE dbcreator ADD MEMBER @CurrentUser;
    PRINT '✓ Đã thêm ' + @CurrentUser + ' vào role dbcreator';
END TRY
BEGIN CATCH
    PRINT '✗ Lỗi: ' + ERROR_MESSAGE();
    PRINT '  Có thể user đã có quyền hoặc không tồn tại.';
END CATCH
GO

-- Hoặc cấp quyền trực tiếp
-- GRANT CREATE DATABASE TO [YourUsername];
-- GO

PRINT ''
PRINT 'Kiểm tra lại quyền:'
SELECT 
    SYSTEM_USER AS CurrentUser,
    CASE 
        WHEN IS_SRVROLEMEMBER('dbcreator') = 1 THEN '✓ Có quyền dbcreator'
        ELSE '✗ Không có quyền dbcreator'
    END AS Permission;
GO

PRINT ''
PRINT '========================================'
PRINT 'Hoàn thành!'
PRINT 'Bây giờ bạn có thể chạy script tạo database.'
PRINT '========================================'
GO
