# Hệ thống ETL Media Analytics - Hướng dẫn Step-by-Step

## 📋 Tổng quan hệ thống

Hệ thống ETL hoàn chỉnh bao gồm:
1. **ETL Application** - Phần mềm ETL với giao diện interactive
2. **Data Warehouse (DW)** - Database normalized để lưu trữ dữ liệu chi tiết
3. **Data Mart (DM)** - Database denormalized để tối ưu cho reporting
4. **Real-time Dashboard** - Grafana dashboard hiển thị dữ liệu real-time

---

## 🚀 Hướng dẫn cài đặt và sử dụng

### Bước 1: Chuẩn bị môi trường

#### 1.1. Cài đặt Python và các thư viện cần thiết

```bash
# Cài đặt Python packages
pip install pyspark findspark pyodbc streamlit pandas plotly
```

#### 1.2. Cài đặt SQL Server và SQL Server JDBC Driver

- Cài đặt SQL Server (SQL Server 2019 hoặc mới hơn)
- Download và cài đặt [Microsoft JDBC Driver for SQL Server](https://docs.microsoft.com/en-us/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server)
- Copy file `sqljdbc_auth.dll` vào thư mục Spark jars:
  - Windows: `C:\spark\spark-4.0.0-bin-hadoop3\jars\`

#### 1.3. Cấu hình SQL Server

- Đảm bảo SQL Server đang chạy
- Cho phép SQL Server Authentication hoặc sử dụng Windows Authentication
- Tạo thư mục `C:\Data\` để lưu database files (hoặc thay đổi path trong script)

---

### Bước 2: Tạo Database và Schema

#### 2.1. Chạy các script SQL theo thứ tự

Mở **SQL Server Management Studio (SSMS)** và chạy các script theo thứ tự:

```sql
-- 1. Tạo databases
-- Chạy: Database/01_Create_Database.sql

-- 2. Tạo Data Warehouse schema (normalized)
-- Chạy: Database/02_Create_DW_Schema.sql

-- 3. Tạo Data Mart schema (denormalized)
-- Chạy: Database/03_Create_DM_Schema.sql

-- 4. Populate dimension tables
-- Chạy: Database/04_Populate_DimDate.sql

-- 5. Tạo ELT stored procedures
-- Chạy: Database/05_ELT_Load_DW.sql
-- Chạy: Database/06_ELT_Load_DM.sql

-- 6. Tạo SQL Server Agent Jobs (optional)
-- Chạy: Database/07_Create_Scheduler.sql
```

**Lưu ý:** Nếu bạn không có quyền tạo SQL Server Agent Jobs, có thể bỏ qua bước 6 và chạy stored procedures thủ công.

---

### Bước 3: Chạy ETL Application

#### 3.1. Chạy ETL Application (Xuất CSV)

```bash
python Class4_ETL_Basic/ETL_Application.py
```

**Các bước trong ứng dụng:**
1. Nhập đường dẫn thư mục chứa file JSON
2. Chọn chế độ:
   - **Option 1:** Chọn khoảng ngày (nhập YYYYMMDD)
   - **Option 2:** Chọn tất cả file trong thư mục
3. Chọn output:
   - **Option 1:** CSV File
   - **Option 2:** SQL Server Database

#### 3.2. Chạy ETL Load vào Database

```bash
python Class4_ETL_Basic/ETL_LoadToDatabase.py
```

**Các bước:**
1. Nhập thông tin SQL Server
2. Chọn authentication (Windows hoặc SQL Server)
3. Nhập đường dẫn file JSON
4. Chọn chế độ lấy file (khoảng ngày hoặc tất cả)

**Sau khi load xong, chạy stored procedures:**

```sql
-- Load từ Staging vào Data Warehouse
USE DW_MediaAnalytics
GO
EXEC sp_ELT_LoadToDataWarehouse @ProcessDate = NULL  -- NULL = process tất cả

-- Load từ Data Warehouse xuống Data Mart
USE DM_MediaAnalytics
GO
EXEC sp_ELT_LoadToDataMart @ProcessDate = NULL
```

---

### Bước 4: Cấu hình Grafana Dashboard

#### 4.1. Cài đặt Grafana

**Windows:**
1. Download: https://grafana.com/grafana/download?platform=windows
2. Cài đặt và chạy
3. Mở: http://localhost:3000
4. Login: admin/admin

**Docker:**
```bash
docker run -d -p 3000:3000 --name=grafana grafana/grafana
```

#### 4.2. Cài đặt SQL Server Plugin

1. Vào **Configuration** → **Plugins**
2. Tìm "Microsoft SQL Server"
3. Click **Install**

#### 4.3. Tạo Datasource

1. Vào **Configuration** → **Data Sources** → **Add data source**
2. Chọn **Microsoft SQL Server**
3. Điền thông tin:
   - Name: `SQL Server - Media Analytics`
   - Host: `localhost:1433`
   - Database: `DM_MediaAnalytics`
   - Authentication: Windows Authentication
4. Click **Save & Test**

#### 4.4. Import Dashboards

1. Vào **Dashboards** → **Import**
2. Upload các file JSON từ thư mục `Grafana/`:
   - `dashboard_overview.json` - Overview dashboard
   - `dashboard_contract_analytics.json` - Contract analytics
   - `dashboard_content_trends.json` - Content type trends
3. Chọn datasource: **SQL Server - Media Analytics**
4. Click **Import**

**Các tính năng Dashboard:**
- **Overview Dashboard:** KPI cards, daily trends, content type distribution
- **Contract Analytics:** Top contracts, contract details, filters
- **Content Type Trends:** Trends over time, comparisons, statistics

**Cấu hình Auto Refresh:**
- Mở dashboard → Click **⚙️** → Set Refresh: `10s` hoặc `30s`

**Xem hướng dẫn chi tiết:** `Grafana/HUONG_DAN_GRAFANA.md`

---

## 📊 Kiến trúc Database

### Data Warehouse (Normalized)

**Dimension Tables:**
- `DimDate` - Thông tin ngày tháng
- `DimContract` - Thông tin Contract
- `DimDevice` - Thông tin Device
- `DimApp` - Thông tin Application
- `DimContentType` - Loại nội dung

**Fact Tables:**
- `FactViewingSession` - Chi tiết session (normalized)
- `FactContractSummary` - Tổng hợp theo Contract (một phần denormalized)

**Staging Table:**
- `Staging_RawData` - Table tạm để load data từ Spark

### Data Mart (Denormalized)

**Tables:**
- `DM_ContractAnalytics` - Phân tích Contract (denormalized)
- `DM_DailySummary` - Tóm tắt theo ngày
- `DM_ContentTypeTrend` - Xu hướng Content Type

---

## 🔄 Quy trình ETL/ELT

### ETL Process (Extract → Transform → Load)

1. **Extract:** Đọc file JSON từ thư mục
2. **Transform:** 
   - Select fields từ `_source`
   - Transform AppName thành ContentType
   - Tính toán statistics
3. **Load:** 
   - **Option 1:** Lưu vào CSV
   - **Option 2:** Load vào SQL Server Staging table

### ELT Process (Extract → Load → Transform)

1. **Extract:** Đọc từ Staging_RawData
2. **Load:** Load vào Dimension và Fact tables
3. **Transform:** 
   - Transform và aggregate trong SQL
   - Load vào Data Mart (denormalized)

---

## ⏰ Tự động hóa với SQL Server Agent

Nếu đã tạo SQL Server Agent Jobs (bước 2.1), hệ thống sẽ tự động chạy:

- **Job 1:** `ETL_LoadToDataWarehouse` - Chạy hàng ngày lúc 2:00 AM
- **Job 2:** `ETL_LoadToDataMart` - Chạy hàng ngày lúc 3:00 AM

**Chạy thủ công nếu cần:**

```sql
-- Chạy Job thủ công
USE msdb
GO
EXEC sp_start_job @job_name = 'ETL_LoadToDataWarehouse'
EXEC sp_start_job @job_name = 'ETL_LoadToDataMart'
```

---

## 📝 Ví dụ sử dụng

### Ví dụ 1: ETL một khoảng ngày và xuất CSV

```bash
python ETL_Application.py
# Chọn: 1 (khoảng ngày)
# Nhập: 20220401 (start), 20220405 (end)
# Chọn: 1 (CSV)
# Nhập: C:\Output\ETL_Results
```

### Ví dụ 2: ETL tất cả file và load vào Database

```bash
python ETL_LoadToDatabase.py
# Nhập: localhost
# Database: DW_MediaAnalytics
# Auth: 1 (Windows)
# Path: C:\Data\log_content
# Mode: 2 (tất cả)
```

Sau đó chạy stored procedures trong SSMS.

### Ví dụ 3: Xem Dashboard

```bash
cd Dashboard
streamlit run app.py
```

Mở browser và cấu hình database connection trong sidebar.

---

## 🐛 Troubleshooting

### Lỗi: "No module named 'pyspark'"
```bash
pip install pyspark findspark
```

### Lỗi: "JDBC Driver not found"
- Download và cài đặt Microsoft JDBC Driver
- Copy `sqljdbc_auth.dll` vào thư mục Spark jars

### Lỗi: "Cannot connect to SQL Server"
- Kiểm tra SQL Server đang chạy
- Kiểm tra firewall settings
- Kiểm tra authentication method

### Lỗi: "Table does not exist"
- Chạy lại các script SQL tạo schema (bước 2.1)

---

## 📚 Tài liệu tham khảo

### Hướng dẫn chính
- **Quick Start:** `QUICK_START.md` - Chạy nhanh trong 5 phút
- **Hướng dẫn đầy đủ:** `CHAY_DAY_DU.md` - Từ đầu đến cuối, từng bước chi tiết
- **Hướng dẫn chi tiết:** `HUONG_DAN_CHI_TIET.md` - Giải thích kỹ thuật chi tiết

### Grafana
- **Hướng dẫn Grafana:** `Grafana/HUONG_DAN_GRAFANA.md` - Cài đặt, cấu hình, sử dụng (bao gồm Quick Start)
- **Troubleshooting:** `Grafana/TROUBLESHOOTING_SQL_CONNECTION.md` - Sửa lỗi kết nối SQL Server

### Database & Troubleshooting
- **Sửa lỗi permission:** `Database/FIX_PERMISSION.md` - Sửa lỗi CREATE DATABASE permission denied
- **Debug guide:** `DEBUG_GUIDE.md` - Hướng dẫn debug chung

**Tài liệu kỹ thuật:**
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [SQL Server Documentation](https://docs.microsoft.com/en-us/sql/)
- [Grafana Documentation](https://grafana.com/docs/grafana/latest/)

---

## Grafana Account
```
Username : grafana_user
Password : Grafana@123
Database : DM_MediaAnalytics
Auth : SQL Server Authentication
Role : db_datareader (read-only)
```

## SSMS Account 
```
Host : host.docker.internal:1433
Database : DM_MediaAnalytics
User : grafana_user
Password : Grafana@123
Encrypt : Disable (hoặc Trust Server Certificate)
```