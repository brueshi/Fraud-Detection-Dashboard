# fraud_dashboard.py
import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# Page configuration
st.set_page_config(
    page_title="Fraud Detection Dashboard",
    page_icon="🔍",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .big-font {
        font-size:30px !important;
        font-weight: bold;
    }
    .medium-font {
        font-size:20px !important;
        font-weight: bold;
    }
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 2px 2px 10px rgba(0,0,0,0.1);
    }
    </style>
    """, unsafe_allow_html=True)

# Load data function
@st.cache_data
def load_data():
    """Load data from SQLite database"""
    try:
        conn = sqlite3.connect('fraud_detection.db')
        query = """
        SELECT * FROM transactions
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Ensure boolean columns are properly typed
        df['is_fraud'] = df['is_fraud'].astype(bool)
        df['rule_based_fraud_flag'] = df['rule_based_fraud_flag'].astype(bool)
        
        # Ensure numeric columns are properly typed
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['fraud_score'] = pd.to_numeric(df['fraud_score'], errors='coerce')
        
        # Drop any rows with NaN values in critical columns
        df = df.dropna(subset=['timestamp', 'amount', 'merchant'])
        
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

# Main dashboard
def main():
    # Title
    st.markdown('<p class="big-font">🔍 Fraud Detection Dashboard</p>', unsafe_allow_html=True)
    
    # Load data
    df = load_data()
    
    if df is None:
        st.error("No data available. Please run the pipeline first.")
        return
    
    # Sidebar filters
    st.sidebar.header("Filters")
    
    # Date range filter
    min_date = df['timestamp'].min().date()
    max_date = df['timestamp'].max().date()
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Merchant filter
    merchants = sorted(df['merchant'].unique())
    selected_merchants = st.sidebar.multiselect(
        "Select Merchants",
        options=merchants,
        default=None
    )
    
    # Fraud flag filter
    fraud_filter = st.sidebar.radio(
        "Transaction Type",
        options=["All", "Fraud Only", "Non-Fraud Only"]
    )
    
    # Apply filters
    filtered_df = df.copy()
    
    # Handle date range filter
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (filtered_df['timestamp'].dt.date >= start_date) &
            (filtered_df['timestamp'].dt.date <= end_date)
        ]
    elif isinstance(date_range, datetime) or isinstance(date_range, pd.Timestamp):
        # Single date selected
        selected_date = date_range.date() if hasattr(date_range, 'date') else date_range
        filtered_df = filtered_df[filtered_df['timestamp'].dt.date == selected_date]
    
    # Apply merchant filter
    if selected_merchants:
        filtered_df = filtered_df[filtered_df['merchant'].isin(selected_merchants)]
    
    # Apply fraud filter
    if fraud_filter == "Fraud Only":
        filtered_df = filtered_df[filtered_df['rule_based_fraud_flag'].astype(bool) == True]
    elif fraud_filter == "Non-Fraud Only":
        filtered_df = filtered_df[filtered_df['rule_based_fraud_flag'].astype(bool) == False]
    
    # Ensure we have data after filtering
    if filtered_df.empty:
        st.warning("No data matches the selected filters. Showing all data instead.")
        filtered_df = df.copy()
    
    # Key Metrics
    st.markdown("### Key Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric(
            label="Total Transactions",
            value=f"{len(filtered_df):,}"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        fraud_count = filtered_df['rule_based_fraud_flag'].sum()
        fraud_rate = (fraud_count / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric(
            label="Fraud Rate",
            value=f"{fraud_rate:.1f}%"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col3:
        total_amount = filtered_df['amount'].sum()
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric(
            label="Total Amount",
            value=f"${total_amount:,.2f}"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col4:
        avg_amount = filtered_df['amount'].mean()
        st.markdown('<div class="metric-card">', unsafe_allow_html=True)
        st.metric(
            label="Average Transaction",
            value=f"${avg_amount:.2f}"
        )
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Time Series Analysis
    st.markdown("### Transaction Volume Over Time")
    
    # Create daily aggregation with error handling
    if not filtered_df.empty:
        daily_data = filtered_df.set_index('timestamp').resample('D').agg({
            'amount': 'sum',
            'rule_based_fraud_flag': lambda x: x.astype(int).sum(),
            'transaction_id': 'count'
        }).reset_index()
    else:
        # Create empty DataFrame with expected columns
        daily_data = pd.DataFrame(columns=['timestamp', 'amount', 'rule_based_fraud_flag', 'transaction_id'])
    
    # Create time series plot
    fig = go.Figure()
    
    # Transaction amount line
    fig.add_trace(go.Scatter(
        x=daily_data['timestamp'],
        y=daily_data['amount'],
        name='Transaction Amount ($)',
        line=dict(color='#3498db', width=2)
    ))
    
    # Fraud flags bar
    fig.add_trace(go.Bar(
        x=daily_data['timestamp'],
        y=daily_data['rule_based_fraud_flag'] * 1000,  # Scale for visibility
        name='Fraud Flags (×1000)',
        marker_color='#e74c3c',
        opacity=0.6
    ))
    
    fig.update_layout(
        title='Daily Transaction Volume vs Fraud Detections',
        xaxis_title='Date',
        yaxis_title='Amount ($)',
        hovermode='x unified',
        height=400
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Two column layout for additional charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Fraud Detection Distribution")
        
        # Pie chart for fraud detection
        if not filtered_df.empty:
            fraud_counts = filtered_df['rule_based_fraud_flag'].value_counts()
            
            # Create labels based on available values
            labels = []
            values = []
            colors = []
            
            if False in fraud_counts.index:
                labels.append('Non-Fraud')
                values.append(fraud_counts[False])
                colors.append('#2ecc71')
            
            if True in fraud_counts.index:
                labels.append('Flagged as Fraud')
                values.append(fraud_counts[True])
                colors.append('#e74c3c')
            
            if labels:  # Only create pie chart if we have data
                fig_pie = px.pie(
                    values=values,
                    names=labels,
                    color_discrete_sequence=colors
                )
                fig_pie.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig_pie, use_container_width=True)
            else:
                st.info("No data available for pie chart.")
        else:
            st.info("No data available for pie chart.")
    
    with col2:
        st.markdown("### Transaction Amount Distribution")
        
        # Histogram of transaction amounts
        fig_hist = px.histogram(
            filtered_df,
            x='amount',
            nbins=50,
            color='rule_based_fraud_flag',
            color_discrete_map={True: '#e74c3c', False: '#3498db'},
            barmode='overlay',
            labels={'rule_based_fraud_flag': 'Fraud Flag'}
        )
        fig_hist.update_layout(
            xaxis_title='Transaction Amount ($)',
            yaxis_title='Count'
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    
    # Top Merchants Table
    st.markdown("### Top Merchants by Transaction Volume")
    
    merchant_stats = filtered_df.groupby('merchant').agg({
        'amount': ['count', 'sum', 'mean'],
        'rule_based_fraud_flag': 'sum'
    }).reset_index()
    
    merchant_stats.columns = ['Merchant', 'Transaction Count', 'Total Amount', 
                             'Average Amount', 'Fraud Flags']
    merchant_stats['Fraud Rate (%)'] = (merchant_stats['Fraud Flags'] / 
                                       merchant_stats['Transaction Count'] * 100).round(1)
    merchant_stats = merchant_stats.sort_values('Total Amount', ascending=False).head(10)
    
    # Format currency columns
    merchant_stats['Total Amount'] = merchant_stats['Total Amount'].apply(lambda x: f"${x:,.2f}")
    merchant_stats['Average Amount'] = merchant_stats['Average Amount'].apply(lambda x: f"${x:.2f}")
    
    st.dataframe(merchant_stats, use_container_width=True)
    
    # High Risk Transactions Table
    st.markdown("### High Risk Transactions")
    
    try:
        high_risk = filtered_df[filtered_df['fraud_score'] >= 2].sort_values(
            'fraud_score', ascending=False).head(10)
        
        if len(high_risk) > 0:
            high_risk_display = high_risk[['transaction_id', 'timestamp', 'amount', 
                                          'merchant', 'user_id', 'fraud_score']].copy()
            high_risk_display['amount'] = high_risk_display['amount'].apply(
                lambda x: f"${x:.2f}" if pd.notnull(x) else "N/A"
            )
            st.dataframe(high_risk_display, use_container_width=True)
        else:
            st.info("No high-risk transactions found with the current filters.")
    except Exception as e:
        st.error(f"Error displaying high-risk transactions: {str(e)}")
        st.info("Unable to display high-risk transactions.")
    
    # Data Export
    st.markdown("### Export Data")
    
    if st.button("Download Filtered Data as CSV"):
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name=f"fraud_detection_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )

if __name__ == "__main__":
    main()