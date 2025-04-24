# Fraud Detection Pipeline
## Overview
A real-time fraud detection system that processes transaction data, applies rule-based detection algorithms, and provides interactive visualization through a Streamlit dashboard.

![Screenshot 2025-04-24 174051](https://github.com/user-attachments/assets/aaf55185-7b83-4b49-9d51-bf20ef2870e8)
![Screenshot 2025-04-24 175130](https://github.com/user-attachments/assets/118f7c55-641d-4c4d-947b-11aebbc130dc)



## Features

- **Automated Data Pipeline**: Ingests, cleans, and processes transaction data
- **Rule-Based Detection**: Multi-factor fraud detection using:
  - High-value transaction detection (>$1,000)
  - High-risk merchant identification
  - Rapid successive transaction flagging
- **SQLite Database**: Persistent storage with optimized indexing
- **Interactive Dashboard**: Real-time visualization and filtering
- **Comprehensive Logging**: Full audit trail of pipeline operations
- **Data Generation**: Synthetic transaction data generator for testing

##  Prerequisites

- Python 3.9+
- pip (Python package manager)

##  Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/fraud-detection-pipeline.git
cd fraud-detection-pipeline
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

##  Quick Start

1. Generate sample transaction data:
```bash
python data_generator.py
```

2. Run the fraud detection pipeline:
```bash
python Fraud_Detection_Data_Pipeline.py
```

3. Launch the dashboard:
```bash
streamlit run fraud_dashboard.py
```

Access the dashboard at `http://localhost:8501`

## Project Structure

```
fraud-detection-pipeline/
├── Fraud_Detection_Data_Pipeline.py    # Main pipeline script
├── data_generator.py                   # Transaction data generator
├── fraud_dashboard.py                  # Streamlit dashboard
├── requirements.txt                    # Python dependencies
├── pipeline.log                        # Pipeline execution logs
├── fraud_detection.db                  # SQLite database
└── screenshots/                        # Dashboard screenshots
```

##  Pipeline Components

### 1. Data Generation
- Generates synthetic transaction data with realistic patterns
- Configurable fraud rate (default: 5%)
- Creates diverse merchant and user profiles

### 2. Data Processing Pipeline
- **Ingestion**: Reads transaction data from CSV
- **Cleaning**: Removes duplicates, handles missing values
- **Detection**: Applies rule-based fraud detection
- **Storage**: Saves results to SQLite database

### 3. Detection Rules
- **High Amount Rule**: Flags transactions > $1,000
- **High-Risk Merchant Rule**: Flags transactions from predefined risky merchants
- **Rapid Transaction Rule**: Flags multiple transactions < 60 seconds apart

### 4. Interactive Dashboard
- Real-time transaction monitoring
- Date range and merchant filtering
- Key metrics and visualizations
- High-risk transaction identification
- Data export functionality

##  Dashboard Features

- **Key Metrics**: Total transactions, fraud rate, total amount, average transaction
- **Time Series Analysis**: Transaction volume and fraud detections over time
- **Distribution Charts**: Fraud detection and amount distributions
- **Top Merchants**: Ranked by transaction volume
- **High-Risk Transactions**: Detailed view of suspicious activities

##  Configuration

Customize detection rules in `Fraud_Detection_Data_Pipeline.py`:
```python
# Rule thresholds
HIGH_AMOUNT_THRESHOLD = 1000
RAPID_TRANSACTION_WINDOW = 60  # seconds
HIGH_RISK_MERCHANTS = ['Casino', 'Gaming', 'Crypto', 'Betting']
```

##  Deployment Options

1. **Local Execution**: Run directly on your machine
2. **Cloud Deployment**: Deploy to GCP Cloud Functions, AWS Lambda, or Heroku
3. **Docker Container**: Containerize for portable deployment

##  Sample Results

The pipeline processes transactions and generates:
- Rule-based fraud flags
- Fraud scores (0-3)
- Detailed transaction analysis
- Performance metrics and logs
