import pandas as pd
import sqlite3
from datetime import datetime
import os
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)

class FraudDetectionPipeline:
    def __init__(self, input_file='transaction_data.csv', db_file='fraud_detection.db'):
        self.input_file = input_file
        self.db_file = db_file
        self.logger = logging.getLogger(__name__)
        
    def ingest_data(self):
        """Read CSV data into pandas DataFrame"""
        try:
            self.logger.info(f"Reading data from {self.input_file}")
            df = pd.read_csv(self.input_file)
            self.logger.info(f"Successfully read {len(df)} records")
            return df
        except Exception as e:
            self.logger.error(f"Error reading CSV: {str(e)}")
            raise
    
    def clean_data(self, df):
        """Clean and preprocess the data"""
        try:
            self.logger.info("Cleaning data...")
            
            # Remove duplicates based on transaction_id
            df_clean = df.drop_duplicates(subset=['transaction_id'])
            self.logger.info(f"Removed {len(df) - len(df_clean)} duplicate records")
            
            # Convert timestamp to datetime
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'])
            
            # Handle missing values
            df_clean = df_clean.dropna()
            self.logger.info(f"Removed {len(df) - len(df_clean)} records with missing values")
            
            # Standardize merchant names (remove special characters, trim spaces)
            df_clean['merchant'] = df_clean['merchant'].str.strip()
            df_clean['merchant'] = df_clean['merchant'].str.replace(r'[^\w\s]', '', regex=True)
            
            return df_clean
        except Exception as e:
            self.logger.error(f"Error cleaning data: {str(e)}")
            raise
    
    def apply_rule_based_detection(self, df):
        """Apply rule-based fraud detection logic"""
        try:
            self.logger.info("Applying rule-based fraud detection...")
            
            # Create a copy to avoid modifying original data
            df_processed = df.copy()
            
            # Rule 1: Flag transactions > $1,000
            high_amount_flag = df_processed['amount'] > 1000
            
            # Rule 2: Flag transactions from high-risk merchants
            high_risk_merchants = ['Casino', 'Gaming', 'Crypto', 'Betting']
            merchant_risk_flag = df_processed['merchant'].str.contains('|'.join(high_risk_merchants), 
                                                                      case=False, 
                                                                      na=False)
            
            # Rule 3: Flag multiple transactions from same user in short time
            df_processed['prev_transaction_time'] = df_processed.groupby('user_id')['timestamp'].shift(1)
            time_diff = (df_processed['timestamp'] - df_processed['prev_transaction_time']).dt.total_seconds()
            rapid_transactions_flag = time_diff < 60  # Less than 1 minute between transactions
            
            # Combine rules - mark as suspicious if any rule is triggered
            df_processed['rule_based_fraud_flag'] = (high_amount_flag | 
                                                   merchant_risk_flag | 
                                                   rapid_transactions_flag)
            
            # Calculate fraud score (0-3 based on how many rules triggered)
            df_processed['fraud_score'] = (high_amount_flag.astype(int) + 
                                         merchant_risk_flag.astype(int) + 
                                         rapid_transactions_flag.astype(int))
            
            # Log rule statistics
            self.logger.info(f"High amount transactions: {high_amount_flag.sum()}")
            self.logger.info(f"High-risk merchant transactions: {merchant_risk_flag.sum()}")
            self.logger.info(f"Rapid successive transactions: {rapid_transactions_flag.sum()}")
            self.logger.info(f"Total rule-based flagged: {df_processed['rule_based_fraud_flag'].sum()}")
            
            # Drop temporary column
            df_processed = df_processed.drop('prev_transaction_time', axis=1)
            
            return df_processed
        except Exception as e:
            self.logger.error(f"Error in rule-based detection: {str(e)}")
            raise
    
    def store_in_database(self, df):
        """Store processed data in SQLite database"""
        try:
            self.logger.info(f"Storing data in SQLite database: {self.db_file}")
            
            conn = sqlite3.connect(self.db_file)
            
            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id TEXT PRIMARY KEY,
                timestamp DATETIME,
                amount REAL,
                merchant TEXT,
                user_id TEXT,
                is_fraud BOOLEAN,
                rule_based_fraud_flag BOOLEAN,
                fraud_score INTEGER,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            conn.execute(create_table_query)
            
            # Insert data
            df.to_sql('transactions', conn, if_exists='append', index=False)
            
            # Create indexes for better query performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON transactions(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON transactions(user_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_fraud_flag ON transactions(rule_based_fraud_flag)")
            
            conn.commit()
            conn.close()
            
            self.logger.info(f"Successfully stored {len(df)} records")
        except Exception as e:
            self.logger.error(f"Error storing data: {str(e)}")
            raise
    
    def run_pipeline(self):
        """Execute the complete pipeline"""
        try:
            self.logger.info("Starting fraud detection pipeline...")
            
            # Ingest data
            df = self.ingest_data()
            
            # Clean data
            df_clean = self.clean_data(df)
            
            # Apply rule-based detection
            df_processed = self.apply_rule_based_detection(df_clean)
            
            # Store in database
            self.store_in_database(df_processed)
            
            self.logger.info("Pipeline completed successfully!")
            
            # Generate summary statistics
            summary = {
                'total_records': len(df_processed),
                'original_fraud_flags': df_processed['is_fraud'].sum(),
                'rule_based_flags': df_processed['rule_based_fraud_flag'].sum(),
                'high_risk_transactions': (df_processed['fraud_score'] >= 2).sum()
            }
            
            self.logger.info(f"Summary: {summary}")
            return summary
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise

# Pipeline automation script
def run_daily_pipeline():
    """Function to run as scheduled task"""
    pipeline = FraudDetectionPipeline()
    pipeline.run_pipeline()

if __name__ == "__main__":
    # Run the pipeline
    pipeline = FraudDetectionPipeline()
    pipeline.run_pipeline()