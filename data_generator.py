import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

# Initialize Faker
fake = Faker()

# Number of records to generate
num_records = 5000

# Generate data
data = []

for i in range(num_records):
    # Generate random timestamp within the last year
    timestamp = fake.date_time_between(start_date='-1y', end_date='now')
    
    # Generate transaction details
    transaction_id = fake.uuid4()
    amount = round(random.uniform(1.0, 5000.0), 2)
    merchant = fake.company()
    user_id = f"USER_{random.randint(1000, 9999)}"
    
    # 5% chance of being fraudulent
    is_fraud = random.random() < 0.05
    
    # Add to data list
    data.append({
        'transaction_id': transaction_id,
        'timestamp': timestamp,
        'amount': amount,
        'merchant': merchant,
        'user_id': user_id,
        'is_fraud': is_fraud
    })

# Create DataFrame
df = pd.DataFrame(data)

# Sort by timestamp
df = df.sort_values('timestamp')

# Reset index
df = df.reset_index(drop=True)

# Display sample data
print(f"Generated {len(df)} records")
print(f"Number of fraudulent transactions: {df['is_fraud'].sum()} ({df['is_fraud'].mean()*100:.1f}%)")
print("\nFirst 10 records:")
print(df.head(10))

# Save to CSV
df.to_csv('transaction_data.csv', index=False)

# Display basic statistics
print("\nBasic Statistics:")
print(f"Total amount: ${df['amount'].sum():,.2f}")
print(f"Average amount: ${df['amount'].mean():,.2f}")
print(f"Number of unique users: {df['user_id'].nunique()}")
print(f"Number of unique merchants: {df['merchant'].nunique()}")

# Check the distribution of fraud
fraud_stats = df.groupby('is_fraud')['amount'].agg(['count', 'mean', 'sum'])
fraud_stats.index = ['Non-Fraud', 'Fraud']
print("\nFraud Statistics:")
print(fraud_stats)