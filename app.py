import streamlit as st
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import os

# --- CONFIGURATION ---
st.set_page_config(page_title="PIDWS Trend Analyzer", layout="wide")
st.title("📈 PIDWS Historic Trend & Gap Analysis")

# --- AZURE CONNECTION ---
# We get this from Azure Environment Variables (Secure)
try:
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_name = "permit-attachments"
    container_client = blob_service_client.get_container_client(container_name)
except Exception as e:
    st.error("⚠️ Azure Storage Connection Failed. Did you set the Environment Variable?")
    st.stop()

# --- HELPER FUNCTIONS ---
def upload_to_blob(file, filename):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    blob_client.upload_blob(file, overwrite=True)
    st.success(f"✅ Saved {filename} to History!")

def load_history():
    all_data = []
    # List all files in the blob container
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        if blob.name.endswith('.xlsx') or blob.name.endswith('.csv'):
            # Download file content to memory
            blob_client = container_client.get_blob_client(blob)
            download_stream = blob_client.download_blob()
            file_content = download_stream.readall()
            
            # Read into Pandas
            if blob.name.endswith('.csv'):
                df = pd.read_csv(io.BytesIO(file_content))
            else:
                df = pd.read_excel(io.BytesIO(file_content))
            
            # Add a 'Report Date' column based on filename or file date
            # Assuming filename format "05-02-2026-ALARMS.xlsx"
            # Or just use the blob creation date
            df['File_Date'] = blob.creation_time.date()
            all_data.append(df)
            
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

# --- TABS ---
tab1, tab2 = st.tabs(["📤 Daily Upload", "📊 Historic Trends"])

# === TAB 1: DAILY UPLOAD ===
with tab1:
    st.header("Upload Today's Report")
    uploaded_file = st.file_uploader("Choose Excel File", type=['xlsx', 'csv'])
    
    if uploaded_file:
        # 1. Process Logic (Same as before)
        df = pd.read_excel(uploaded_file)
        
        # ... [Insert your SOP Analysis Logic here] ...
        st.dataframe(df.head())
        
        # 2. Save to Azure Blob Button
        if st.button("💾 Save to Historic Database"):
            # Reset pointer to start of file so we can read it again for upload
            uploaded_file.seek(0)
            upload_to_blob(uploaded_file, uploaded_file.name)

# === TAB 2: HISTORIC TRENDS ===
with tab2:
    st.header("Violation Trends Over Time")
    
    if st.button("🔄 Refresh Data from Azure"):
        with st.spinner("Downloading history from cloud..."):
            history_df = load_history()
            
            if not history_df.empty:
                # --- PRE-PROCESSING ---
                history_df['Alert Time'] = pd.to_datetime(history_df['Alert Time'], errors='coerce')
                history_df['Verification Date/Time'] = pd.to_datetime(history_df['Verification Date/Time'], errors='coerce')
                
                # Calculate Violations
                history_df['Response_Time'] = (history_df['Verification Date/Time'] - history_df['Alert Time']).dt.total_seconds() / 60
                history_df['Is_Late'] = history_df['Response_Time'] > 30
                history_df['Is_Unverified'] = (history_df['Severity'] == 'high') & (history_df['Verification Date/Time'].isna())
                
                # Group by Date
                daily_stats = history_df.groupby('File_Date').agg(
                    Total_Alarms=('Alert Time', 'count'),
                    Late_Responses=('Is_Late', 'sum'),
                    Unverified_Critical=('Is_Unverified', 'sum')
                ).reset_index()
                
                # --- VISUALIZATION ---
                st.subheader("Critical Gaps Trend")
                st.line_chart(daily_stats, x='File_Date', y=['Unverified_Critical', 'Late_Responses'])
                
                st.subheader("Data Table")
                st.dataframe(daily_stats)
            else:
                st.warning("No historic data found in Azure Storage yet.")
