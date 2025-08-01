import csv
import random
from uuid import uuid4
from datetime import datetime, timedelta

# Configuration
OUTPUT_FILE = 'transactions_10k.csv'
NUM_ROWS = 10000  # Increased to 10,000 rows
NUM_CUSTOMERS = 2000

# Possible values for each attribute
REGIONS = [
    'North America', 'South America', 'Western Europe', 'Eastern Europe', 
    'East Asia', 'South Asia', 'Southeast Asia', 'Middle East', 
    'North Africa', 'Sub-Saharan Africa', 'Oceania', 'Caribbean', 
    'Central America', 'Scandinavia', 'Baltic States', 'Balkans', 
    'Central Asia', 'Antarctica'
]

TRANSACTION_MODES = [
    'Credit Card', 'Debit Card', 'Online Banking', 'Mobile Wallet', 
    'Cryptocurrency', 'Bank Transfer', 'UPI'
]

# Date range - transactions from 2 years ago to current date
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=365*2)

# Generate customer IDs (2000 unique customers to allow for multiple transactions per customer)
customer_ids = list(range(1, NUM_CUSTOMERS + 1))

def random_date(start, end):
    """Generate a random datetime between two datetime objects"""
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds()))) 

def generate_transaction():
    """Generate a single transaction record"""
    customer_id = random.choice(customer_ids)
    transaction_id = str(uuid4())
    
    # Generate amount with 90% normal transactions and 10% potentially larger amounts
    if random.random() < 0.9:
        amount = round(random.uniform(10, 5000), 2)  # Most transactions between $10-$500
    else:
        amount = round(random.uniform(5000, 50000), 2)  # Some larger transactions
    
    region = random.choice(REGIONS)
    mode = random.choice(TRANSACTION_MODES)
    transaction_date = random_date(START_DATE, END_DATE).strftime('%Y-%m-%d %H:%M:%S')
    
    # Fraud probability depends on region, payment mode, and amount
    fraud_prob = 0.01  # Base 1% chance
    
    # Higher risk for certain modes
    if mode in ['Cryptocurrency', 'Mobile Wallet']:
        fraud_prob += 0.5
    
    # Higher risk for certain regions
    if region in ['Middle East', 'Southeast Asia', 'Sub-Saharan Africa']:
        fraud_prob += 0.35
    
    # Very large transactions have higher fraud probability
    if amount > 2000:
        fraud_prob += 0.1
    
    # Late night transactions (12am-4am) have higher fraud probability
    transaction_dt = datetime.strptime(transaction_date, '%Y-%m-%d %H:%M:%S')
    if 0 <= transaction_dt.hour < 4:
        fraud_prob += 0.02
    
    is_fraudulent = random.random() < fraud_prob
    
    return {
        'customer_id': customer_id,
        'transaction_id': transaction_id,
        'transaction_amount': amount,
        'region': region,
        'transaction_mode': mode,
        'transaction_date': transaction_date,
        'is_fraudulent': is_fraudulent
    }

# Generate all transactions
print(f"Generating {NUM_ROWS} transactions...")
transactions = [generate_transaction() for _ in range(NUM_ROWS)]

# Write to CSV file
with open(OUTPUT_FILE, 'w', newline='') as csvfile:
    fieldnames = [
        'customer_id', 'transaction_id', 'transaction_amount', 
        'region', 'transaction_mode', 'transaction_date', 'is_fraudulent'
    ]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    writer.writerows(transactions)

print(f"Successfully generated {NUM_ROWS} transactions in {OUTPUT_FILE}")
print(f"File size: {len(transactions)} records")
print(f"Time period covered: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")