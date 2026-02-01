"""
Script Master để chạy toàn bộ quy trình ETL tự động
Bao gồm:
1. Load data vào Staging
2. Chạy stored procedures (DW và DM)
3. Khởi động Streamlit dashboard với real-time
"""

from ETL_Application import ETLApplication
from ETL_LoadToDatabase import ETLDatabaseLoader
from datetime import datetime
import time
import os
import sys
import subprocess
import pyodbc
from pyspark.sql.functions import *
from pyspark.sql.session import SparkSession
import findspark
findspark.init()


# Thêm thư mục hiện tại vào path để import các module
script_dir = os.path.dirname(os.path.abspath(__file__))
if script_dir not in sys.path:
    sys.path.insert(0, script_dir)


class FullETLPipeline:
    def __init__(self, server, database_dw="DW_MediaAnalytics", database_dm="DM_MediaAnalytics",
                 username=None, password=None, port=1433, encrypt=True, trust_server_certificate=True):
        """
        Khởi tạo pipeline với thông tin database
        """
        self.server = server
        self.database_dw = database_dw
        self.database_dm = database_dm
        self.username = username
        self.password = password
        self.port = port
        self.encrypt = encrypt
        self.trust_server_certificate = trust_server_certificate

        # Tạo server string với port
        # Trong __init__
        if ':' in server:
            # Chuyển localhost:1433 thành localhost,1433
            server_with_port = server.replace(':', ',')
        else:
            server_with_port = f"{server},{port}"

        # Tạo connection string cho pyodbc
        if username and password:
            encrypt_str = "yes" if encrypt else "no"
            self.conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_with_port};DATABASE={database_dw};UID={username};PWD={password};Encrypt={encrypt_str};TrustServerCertificate={'yes' if trust_server_certificate else 'no'}"
        else:
            encrypt_str = "yes" if encrypt else "no"
            self.conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_with_port};DATABASE={database_dw};Trusted_Connection=yes;Encrypt={encrypt_str};TrustServerCertificate={'yes' if trust_server_certificate else 'no'}"

    def execute_stored_procedure(self, database, procedure_name, process_date=None):
        """
        Run stored procedure from Python
        """
        print(f"\n{'='*60}")
        print(f"Chạy stored procedure: {database}.dbo.{procedure_name}")
        print(f"{'='*60}")

        try:
            # Tạo server string với port
            if self.server.lower() == 'localhost':
                # Dùng IP và dấu phẩy cho ODBC
                server_with_port = f"127.0.0.1,{self.port}"
            else:
                # ODBC dùng dấu phẩy để phân tách port
                server_with_port = self.server.replace(':', ',')

            print(f"Connecting to: {server_with_port}")
            print(f"Database: {database}")

            # Tạo connection với database cụ thể
            encrypt_str = "yes" if self.encrypt else "no"
            trust_cert_str = "yes" if self.trust_server_certificate else "no"

            if self.username and self.password:
                conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_with_port};DATABASE={database};UID={self.username};PWD={self.password};Encrypt={encrypt_str};TrustServerCertificate={trust_cert_str}"
                print(f"Authentication: SQL Server (User: {self.username})")
            else:
                conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_with_port};DATABASE={database};Trusted_Connection=yes;Encrypt={encrypt_str};TrustServerCertificate={trust_cert_str}"
                print(f"Authentication: Windows Authentication")

            print(
                f"Encrypt: {encrypt_str}, TrustServerCertificate: {trust_cert_str}")
            print("Connecting...")

            conn = pyodbc.connect(conn_str, timeout=10)
            cursor = conn.cursor()

            print("✓ Connected successfully!")

            # Chạy stored procedure
            # Sử dụng database parameter thay vì hardcode
            full_procedure_path = f"{database}.dbo.{procedure_name}"

            print(f"Running stored procedure...")
            if process_date:
                # Sử dụng tham số (?) để tránh SQL Injection và lỗi định dạng ngày tháng
                cursor.execute(
                    f"EXEC {full_procedure_path} @ProcessDate = ?", process_date)
            else:
                print(f"Checking procedure: {full_procedure_path}")
                cursor.execute(
                    f"EXEC {full_procedure_path} @ProcessDate = NULL")

            # Commit transaction
            conn.commit()

            print(f"✓ Successfully ran: {procedure_name}")

            cursor.close()
            conn.close()
            return True

        except pyodbc.Error as e:
            """
            print(f"✗ Lỗi kết nối SQL Server khi chạy {procedure_name}:")
            print(f"   Error code: {e.args[0] if e.args else 'Unknown'}")
            print(
                f"   Error message: {e.args[1] if len(e.args) > 1 else str(e)}")
            print(f"\n💡 Kiểm tra:")
            print(f"   1. SQL Server đang chạy?")
            print(
                f"   2. Server name đúng chưa? (Hiện tại: {self.server}:{self.port})")
            print(f"   3. Port {self.port} có mở không?")
            print(f"   4. Firewall có chặn không?")
            print(f"   5. Database '{database}' có tồn tại không?")
            if self.username:
                print(f"   6. User '{self.username}' có quyền EXECUTE không?")
            """
            import traceback
            print("\nError details:")
            traceback.print_exc()
            return False
        except Exception as e:
            print(f"✗ Error when running {procedure_name}: {e}")
            import traceback
            print("\nError details:")
            traceback.print_exc()
            return False

    def run_etl_pipeline(self, file_paths, process_date=None):
        """
        Run the full ETL pipeline
        """
        print("\n" + "="*70)
        print("START FULL ETL PIPELINE")
        print("="*70)

        # Bước 1: Load vào Staging
        print("\n[1/3] Load data into Staging_RawData...")
        print(f"Server: {self.server}:{self.port}")
        print(f"Database: {self.database_dw}")
        loader = ETLDatabaseLoader(self.server, self.database_dw, self.username, self.password,
                                   self.port, encrypt=self.encrypt, trust_server_certificate=self.trust_server_certificate)
        success = loader.process_files_to_database(file_paths)

        if not success:
            print("✗ Error when loading into Staging. Stopping pipeline.")
            return False

        # Bước 2: Chạy stored procedure để load vào Data Warehouse
        print("\n[2/3] Load from Staging to Data Warehouse...")
        success = self.execute_stored_procedure(
            self.database_dw,
            "sp_ELT_LoadToDataWarehouse",
            process_date
        )

        if not success:
            print("✗ Error when loading into Data Warehouse. Stopping pipeline.")
            return False

        # Bước 3: Chạy stored procedure để load vào Data Mart
        print("\n[3/3] Load from Data Warehouse to Data Mart...")
        success = self.execute_stored_procedure(
            self.database_dm,
            "sp_ELT_LoadToDataMart",
            process_date
        )

        if not success:
            print("✗ Error when loading into Data Mart. Stopping pipeline.")
            return False

        print("\n" + "="*70)
        print("✓ FULL ETL PIPELINE COMPLETED!")
        print("="*70)
        return True

    def start_streamlit_dashboard(self, port=8501):
        """
        Khởi động Streamlit dashboard với real-time
        """
        print(f"\n{'='*70}")
        print(f"START STREAMLIT DASHBOARD (Port: {port})")
        print(f"{'='*70}")
        print("Dashboard will automatically refresh in real-time")
        print(f"Access: http://localhost:{port}")
        print("\nPress Ctrl+C to stop dashboard")
        print("="*70 + "\n")

        # Đường dẫn đến app.py
        dashboard_path = os.path.join(
            os.path.dirname(__file__), "Dashboard", "app.py")

        if not os.path.exists(dashboard_path):
            print(f"✗ Không tìm thấy file: {dashboard_path}")
            return

        try:
            # Chạy streamlit
            subprocess.run([
                sys.executable, "-m", "streamlit", "run", dashboard_path,
                "--server.port", str(port),
                "--server.headless", "true"
            ])
        except KeyboardInterrupt:
            print("\n✓ Stopped dashboard")
        except Exception as e:
            print(f"✗ Error when starting dashboard: {e}")


def main():
    """
    Main function with interactive mode
    """
    print("="*70)
    print("FULL ETL PIPELINE - Tự động hóa toàn bộ quy trình")
    print("="*70)

    # Nhập thông tin database
    print("\n[1] Configure Database:")
    print("Ex: host.docker.internal:1433 or localhost")
    server = input(
        "SQL Server (Ex: localhost or host.docker.internal:1433): ").strip()

    # Separate port if it exists in the server string
    port = 1433
    if ':' in server:
        parts = server.split(':')
        server = parts[0]
        try:
            port = int(parts[1])
        except:
            port = 1433

    database_dw = input(
        "Data Warehouse (DW_MediaAnalytics): ").strip() or "DW_MediaAnalytics"
    database_dm = input(
        "Data Mart (DM_MediaAnalytics): ").strip() or "DM_MediaAnalytics"

    auth_choice = input("Authentication (1-Windows, 2-SQL Server): ").strip()
    username = None
    password = None
    if auth_choice == "2":
        username = input("Username (Ex: grafana_user): ").strip()
        password = input("Password (Ex: Grafana@123): ").strip()

    # Cấu hình Encrypt
    encrypt_choice = input(
        "Encrypt (1-Enable, 2-Disable, Enter=Enable): ").strip() or "1"
    encrypt = (encrypt_choice == "1")

    trust_cert_choice = input(
        "Trust Server Certificate (1-Yes, 2-No, Enter=Yes): ").strip() or "1"
    trust_server_certificate = (trust_cert_choice == "1")

    # Nhập đường dẫn file
    print("\n[2] Chọn file data:")
    input_path = input(
        "Path to the folder containing the JSON files: ").strip()

    # Chọn mode
    print("\n[3] Choose mode:")
    print("  1. Choose date range")
    print("  2. All files in the folder")
    mode = input("Choose (1 or 2): ").strip()

    app = ETLApplication()
    file_paths = []

    if mode == "1":
        start_date = input("Start date (YYYYMMDD): ").strip()
        end_date = input("End date (YYYYMMDD): ").strip()
        file_paths = app.get_date_range_files(input_path, start_date, end_date)
    else:
        file_paths = app.get_files_from_folder(input_path)

    if not file_paths:
        print("✗ No files found!")
        return

    print(f"\n✓ Found {len(file_paths)} file(s)")

    # Chọn process date
    process_date_choice = input(
        "\n[4] Process date (Enter to process all, or enter YYYY-MM-DD): ").strip()
    process_date = None
    if process_date_choice:
        try:
            process_date = datetime.strptime(
                process_date_choice, "%Y-%m-%d").date()
        except:
            print("⚠ Date format is incorrect, will process all")

    # Khởi tạo pipeline
    pipeline = FullETLPipeline(server, database_dw, database_dm, username, password,
                               port, encrypt, trust_server_certificate)

    # Chạy ETL pipeline
    print("\n[5] Start running ETL Pipeline...")
    success = pipeline.run_etl_pipeline(file_paths, process_date)

    if not success:
        print("\n✗ Pipeline failed. Check the error above.")
        return

    # Hỏi có muốn chạy dashboard không
    print("\n[6] Start Dashboard:")
    start_dashboard = input(
        "Do you want to start Streamlit Dashboard now? (yes/no): ").strip().lower()

    if start_dashboard in ['yes', 'y']:
        port = input("Port (default 8501): ").strip() or "8501"
        try:
            port = int(port)
        except:
            port = 8501

        pipeline.start_streamlit_dashboard(port)
    else:
        print("\n✓ To start dashboard later, run the command:")
        print("  cd Class4_ETL_Basic/Dashboard")
        print("  streamlit run app.py")


if __name__ == "__main__":
    main()
