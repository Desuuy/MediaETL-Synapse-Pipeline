-- =============================================
-- Script kiểm tra quyền của user hiện tại
-- =============================================

PRINT '========================================'
PRINT 'KIỂM TRA QUYỀN USER HIỆN TẠI'
PRINT '========================================'
PRINT ''

-- User hiện tại
PRINT '1. User hiện tại:'
SELECT SYSTEM_USER AS CurrentUser;
PRINT ''

-- Kiểm tra roles
PRINT '2. Roles của user:'
SELECT 
    CASE 
        WHEN IS_SRVROLEMEMBER('sysadmin') = 1 THEN '✓ Có quyền sysadmin'
        ELSE '✗ Không có quyền sysadmin'
    END AS SysAdmin,
    CASE 
        WHEN IS_SRVROLEMEMBER('dbcreator') = 1 THEN '✓ Có quyền dbcreator'
        ELSE '✗ Không có quyền dbcreator'
    END AS DbCreator,
    CASE 
        WHEN IS_SRVROLEMEMBER('serveradmin') = 1 THEN '✓ Có quyền serveradmin'
        ELSE '✗ Không có quyền serveradmin'
    END AS ServerAdmin;
PRINT ''

-- Chi tiết roles
PRINT '3. Chi tiết roles:'
SELECT 
    p.name AS PrincipalName,
    r.name AS RoleName
FROM sys.server_role_members rm
INNER JOIN sys.server_principals r ON rm.role_principal_id = r.principal_id
INNER JOIN sys.server_principals p ON rm.member_principal_id = p.principal_id
WHERE p.name = SYSTEM_USER;
PRINT ''

-- Kết luận
PRINT '4. Kết luận:'
IF IS_SRVROLEMEMBER('sysadmin') = 1 OR IS_SRVROLEMEMBER('dbcreator') = 1
BEGIN
    PRINT '✓ User có quyền tạo database!'
    PRINT '  Bạn có thể chạy script tạo database.'
END
ELSE
BEGIN
    PRINT '✗ User KHÔNG có quyền tạo database!'
    PRINT ''
    PRINT 'Giải pháp:'
    PRINT '1. Kết nối với sa account (SQL Server Authentication)'
    PRINT '2. Hoặc chạy SSMS với quyền Administrator'
    PRINT '3. Hoặc cấp quyền dbcreator cho user này'
    PRINT ''
    PRINT 'Để cấp quyền (chạy với quyền sysadmin):'
    PRINT '  ALTER SERVER ROLE dbcreator ADD MEMBER ''' + SYSTEM_USER + ''';'
END
PRINT ''
PRINT '========================================'
GO
