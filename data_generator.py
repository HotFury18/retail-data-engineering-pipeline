import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()

# ==========================================
# 1. Define Business Logic & Dimensions
# ==========================================
stores = ['Mokattam', 'Maadi', 'New Cairo']
roles = ['Junior Sales Associate', 'Senior Sales Associate', 'Store Manager']

# Generate Employee Data
employee_data = []
for i in range(1, 11): # 10 employees
    role = random.choices(roles, weights=[60, 30, 10])[0]
    
    # Setting realistic salary structures
    base_1st = 7000 if role == 'Junior Sales Associate' else 10000
    base_15th = 2000 if role == 'Junior Sales Associate' else 3000
    commission_rate = 0.01 # 1% commission on total sales
    
    employee_data.append({
        'employee_id': f'EMP{i:03d}',
        'name': fake.name(),
        'store_branch': random.choice(stores),
        'role': role,
        'salary_1st_egp': base_1st,
        'salary_15th_egp': base_15th,
        'commission_rate': commission_rate
    })

df_employees = pd.DataFrame(employee_data)
df_employees.to_csv('dim_employees.csv', index=False)
print("Created dim_employees.csv")

# ==========================================
# 2. Generate Transaction Data (The "Fact" Table)
# ==========================================
def generate_monthly_transactions(year, month):
    start_date = datetime(year, month, 1)
    # Get the last day of the month
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)

    transactions = []
    current_date = start_date
    transaction_id = 1

    while current_date <= end_date:
        # Business Rule: Shifts end at 11:00 PM (23:00) on Thu(3)/Fri(4), else 10:00 PM (22:00)
        is_weekend = current_date.weekday() in [3, 4]
        closing_hour = 23 if is_weekend else 22
        
        # Simulate 50 to 150 transactions per day
        daily_transactions = random.randint(50, 150)
        
        for _ in range(daily_transactions):
            # Random time between 10:00 AM opening and closing hour
            hour = random.randint(10, closing_hour - 1)
            minute = random.randint(0, 59)
            trans_time = current_date.replace(hour=hour, minute=minute)
            
            # Assign to a random employee
            emp = random.choice(employee_data)
            
            transactions.append({
                'transaction_id': f'TRX{transaction_id:06d}',
                'timestamp': trans_time,
                'employee_id': emp['employee_id'],
                'store_branch': emp['store_branch'],
                'sale_amount_egp': round(random.uniform(50.0, 1500.0), 2)
            })
            transaction_id += 1
            
        current_date += timedelta(days=1)

    df_transactions = pd.DataFrame(transactions)
    # Sort by time just like a real database log
    df_transactions = df_transactions.sort_values('timestamp') 
    
    filename = f'raw_sales_{year}_{month:02d}.csv'
    df_transactions.to_csv(filename, index=False)
    print(f"Created {filename} with {len(df_transactions)} records.")

# Run the generator for March 2026
generate_monthly_transactions(2026, 3)