import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
from datetime import timedelta

# Connect to Azurite
connection_string = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

print("Starting Phase 3: Transformation...")

# ==========================================
# 1. EXTRACT: Read Data from the 'raw-data' Data Lake
# ==========================================
print("Extracting raw data from Azurite...")
container_client = blob_service_client.get_container_client("raw-data")

# Download Employees
emp_blob = container_client.get_blob_client("dim_employees.csv").download_blob().readall()
df_employees = pd.read_csv(io.BytesIO(emp_blob))

# Download Sales
sales_blob = container_client.get_blob_client("raw_sales_2026_03.csv").download_blob().readall()
df_sales = pd.read_csv(io.BytesIO(sales_blob))

# ==========================================
# 2. TRANSFORM: Apply Business Logic
# ==========================================
print("Transforming data and applying business rules...")

# Convert timestamp to date and time objects
df_sales['timestamp'] = pd.to_datetime(df_sales['timestamp'])
df_sales['sale_date'] = df_sales['timestamp'].dt.date

# Rule 1: Aggregate daily sales per employee
daily_sales = df_sales.groupby(['employee_id', 'store_branch', 'sale_date'])['sale_amount_egp'].sum().reset_index()
daily_sales.rename(columns={'sale_amount_egp': 'total_daily_sales_egp'}, inplace=True)

# Merge with employee data to get the commission rate
df_transformed = pd.merge(daily_sales, df_employees[['employee_id', 'commission_rate']], on='employee_id', how='left')

# Rule 2: Calculate commission and apply the 14-day payout lag
df_transformed['daily_commission_earned'] = df_transformed['total_daily_sales_egp'] * df_transformed['commission_rate']
df_transformed['sale_date'] = pd.to_datetime(df_transformed['sale_date'])
df_transformed['payout_date'] = df_transformed['sale_date'] + timedelta(days=14)

# Clean up the final table for the database
df_transformed = df_transformed.round({'total_daily_sales_egp': 2, 'daily_commission_earned': 2})
df_fact_sales = df_transformed[['sale_date', 'employee_id', 'store_branch', 'total_daily_sales_egp', 'daily_commission_earned', 'payout_date']]

# ==========================================
# 3. LOAD: Save to 'processed-data' Data Lake
# ==========================================
processed_container = "processed-data"
try:
    blob_service_client.create_container(processed_container)
except Exception:
    pass # Container already exists

print(f"Loading transformed data into '{processed_container}'...")
processed_blob_client = blob_service_client.get_blob_client(container=processed_container, blob="fact_sales_2026_03.csv")

# Convert dataframe back to CSV in memory and upload
csv_data = df_fact_sales.to_csv(index=False)
processed_blob_client.upload_blob(csv_data, overwrite=True)

print("Phase 3 Complete! Data is clean and ready for the Data Warehouse.")