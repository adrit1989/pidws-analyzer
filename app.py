import streamlit as st
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import os

# --- CONFIGURATION ---
st.set_page_config(page_title="PIDWS Trend Analyzer", layout="wide")
st.title("📈 PIDWS Historic Trend & Gap Analysis")

# --- AZURE CONNECTION ---
try:
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = "pidws"
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()
except Exception as e:
    st.error(f"⚠️ Azure Storage Error: {e}")
    st.stop()

# --- SMART FILE READER ---
def read_alarm_file(file_content):
    """
    Intelligently finds the header row in IOCL Alarm files 
    (skipping Revision No, Effective From, etc.)
    """
    try:
        # 1. Read first 10 rows to find the header
        # Using 'on_bad_lines=skip' to handle the metadata rows with varying commas
        peek_df = pd.read_csv(io.BytesIO(file_content), header=None, nrows=10, on_bad_lines='skip')
        
        header_row = -1
        for i, row in peek_df.iterrows():
            # Convert entire row to string and check for key columns
            row_str = row.astype(str).str.lower().to_string()
            if 'alert time' in row_str and 'severity' in row_str:
                header_row = i
                break
        
        if header_row == -1:
            return None # This is not a valid ALARMS file

        # 2. Read the full file using the detected header
        df = pd.read_csv(io.BytesIO(file_content), header=header_row, on_bad_lines='skip')
        
        # 3. Standardization
        # Remove empty rows or rows that are just repeated headers
        df = df[df['Alert Time'].notna()]
        return df

    except Exception as e:
        return None

# --- HELPER FUNCTIONS ---
def upload_to_blob(file, filename):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    blob_client.upload_blob(file, overwrite=True)
    st.success(f"✅ Saved {filename} to History!")

def load_history():
    all_data = []
    blob_list = container_client.list_blobs()
    
    with st.spinner("Scanning historic files..."):
        for blob in blob_list:
            # FILTER 1: Strict Name Check
            # Only process files that are explicitly ALARMS logs
            name = blob.name.upper()
            if "ALARMS.CSV" not in name:
                # This ignores "BIKERS KM.csv", "SUMMARY.csv", etc.
                continue
                
            # Download
            blob_client = container_client.get_blob_client(blob)
            download_stream = blob_client.download_blob()
            file_content = download_stream.readall()
            
            # Process using Smart Reader
            df = read_alarm_file(file_content)
            
            if df is not None and not df.empty:
                # Add Date Tag (from blob creation or filename)
                df['File_Date'] = blob.creation_time.date()
                all_data.append(df)
            
    if all_data:
        # clean concat to ensure dtypes align
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

# --- TABS ---
tab1, tab2 = st.tabs(["📤 Daily Upload", "📊 Historic Trends"])

# === TAB 1: DAILY UPLOAD ===
with tab1:
    st.header("Upload Today's Report")
    st.info("Please upload the file named '... - ALARMS.csv'")
    uploaded_file = st.file_uploader("Choose Excel/CSV File", type=['csv'])
    
    if uploaded_file:
        # Preview
        file_bytes = uploaded_file.getvalue()
        df = read_alarm_file(file_bytes)
        
        if df is not None:
            st.success("File recognized successfully!")
            st.dataframe(df.head())
            
            if st.button("💾 Save to Historic Database"):
                uploaded_file.seek(0)
                upload_to_blob(uploaded_file, uploaded_file.name)
        else:
            st.error("Could not find 'Alert Time' column. Are you sure this is the ALARMS.csv file?")

# === TAB 2: HISTORIC TRENDS ===
with tab2:
    st.header("Violation Trends Over Time")
    
    if st.button("🔄 Refresh Data"):
        history_df = load_history()
        
        if not history_df.empty:
            # --- DATA CLEANING ---
            # 1. Date Conversion
            history_df['Alert Time'] = pd.to_datetime(history_df['Alert Time'], errors='coerce')
            history_df['Verification Date/Time'] = pd.to_datetime(history_df['Verification Date/Time'], errors='coerce')
            
            # 2. Metric Calculation
            # Calculate duration in minutes
            history_df['Response_Time'] = (history_df['Verification Date/Time'] - history_df['Alert Time']).dt.total_seconds() / 60
            
            # Logic for "Late Response" (> 30 mins)
            history_df['Is_Late'] = history_df['Response_Time'] > 30
            
            # Logic for "Unverified High Severity"
            # Flexible column matching for Severity
            sev_col = [c for c in history_df.columns if 'severity' in c.lower()][0]
            history_df['Is_Unverified'] = (
                (history_df[sev_col].astype(str).str.lower().str.contains('high')) & 
                (history_df['Verification Date/Time'].isna())
            )
            
            # --- AGGREGATION ---
            daily_stats = history_df.groupby('File_Date').agg(
                Total_Alarms=('Alert Time', 'count'),
                Late_Responses=('Is_Late', 'sum'),
                Unverified_Critical=('Is_Unverified', 'sum')
            ).reset_index()
            
            # --- CHARTS ---
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("⚠️ Unverified Critical Alarms")
                st.line_chart(daily_stats, x='File_Date', y='Unverified_Critical')
                
            with col2:
                st.subheader("⏱️ Late Responses (>30 mins)")
                st.bar_chart(daily_stats, x='File_Date', y='Late_Responses')
            
            st.subheader("Raw Data")
            st.dataframe(daily_stats)
        else:
            st.warning("No ALARMS.csv files found in history.")
