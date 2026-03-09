import pandas as pd
from sqlalchemy import create_engine
from azure.storage.blob import BlobServiceClient
import io

print("Starting Phase 4: Enterprise ETL to SQL Server...")

# ==========================================
# 1. EXTRACT: Pull Data from the Data Lake (Azurite)
# ==========================================
print("Connecting to Azurite Data Lake...")
azurite_connection = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
blob_service_client = BlobServiceClient.from_connection_string(azurite_connection)

try:
    # Get Dimension Data (Employees) from the 'raw-data' container
    print("Downloading Employee data...")
    raw_container = blob_service_client.get_container_client("raw-data")
    emp_blob = raw_container.get_blob_client("dim_employees.csv").download_blob().readall()
    df_employees = pd.read_csv(io.BytesIO(emp_blob))

    # Get Fact Data (Sales) from the 'processed-data' container
    print("Downloading Clean Sales data...")
    processed_container = blob_service_client.get_container_client("processed-data")
    sales_blob = processed_container.get_blob_client("fact_sales_2026_03.csv").download_blob().readall()
    df_sales = pd.read_csv(io.BytesIO(sales_blob))
    
    print("--> Data successfully extracted from the Data Lake!")

except Exception as e:
    print(f"--> AZURITE ERROR: Could not download files. Make sure Azurite is running! Details: {e}")
    exit()

# ==========================================
# 2. LOAD: Push Data to Data Warehouse (SQL Server)
# ==========================================
print("\nConnecting to SQL Server Data Warehouse...")
# Update 'localhost' if your SSMS server name is different (e.g., '.\\SQLEXPRESS')
server = '(localdb)\MSSQLLocalDB' 
database = 'RetailDW'
driver = 'ODBC Driver 17 for SQL Server'

try:
    # Create the connection engine using Windows Authentication
    sql_connection = f"mssql+pyodbc://@{server}/{database}?driver={driver}&Trusted_Connection=yes"
    engine = create_engine(sql_connection)

    print("Pushing tables into SQL Server...")
    # The 'to_sql' command automatically creates the Star Schema tables in SSMS!
    df_employees.to_sql('Dim_Employee', con=engine, if_exists='replace', index=False)
    df_sales.to_sql('Fact_Sales', con=engine, if_exists='replace', index=False)
    
    print("--> SUCCESS: Pipeline complete. Data is live in SQL Server!")

except Exception as e:
    print(f"--> SQL SERVER ERROR: Could not connect or upload. Details: {e}")
