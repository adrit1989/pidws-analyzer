import streamlit as st
import pandas as pd
from azure.storage.blob import BlobServiceClient
import io
import os
from datetime import datetime

# --- CONFIGURATION ---
st.set_page_config(page_title="IOCL PIDWS Analyzer", layout="wide", page_icon="🛡️")

# Custom CSS for IOCL Branding
st.markdown("""
    <style>
    .main { background-color: #f5f5f5; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; border: 1px solid #e0e0e0; }
    </style>
    """, unsafe_allow_html=True)

st.title("🛡️ IOCL: PIDWS Historic Trend & Gap Analysis")
st.markdown("#### Eastern Region Pipelines (ERPL) | Muzaffarpur Station")

# --- AZURE CONNECTION ---
try:
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = "pidws" 
    
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Auto-create container if missing
    if not container_client.exists():
        container_client.create_container()
except Exception as e:
    st.error("🔴 Azure Storage Connection Failed. Please check Environment Variables.")
    st.stop()

# --- SMART FILE READER (Supports CSV & Excel) ---
def process_alarm_df(file_content, filename):
    try:
        df = None
        input_data = io.BytesIO(file_content)
        
        # 1. Determine File Type
        if filename.lower().endswith(('.xls', '.xlsx')):
            # Read Excel
            xls = pd.ExcelFile(input_data)
            
            # Smart Sheet Selection: Look for a sheet named 'ALARMS' (case insensitive)
            target_sheet = xls.sheet_names[0] # Default to first sheet
            for sheet in xls.sheet_names:
                if "ALARM" in sheet.upper():
                    target_sheet = sheet
                    break
            
            # Read the specific sheet, Header is at Index 3 (Row 4)
            df = pd.read_excel(xls, sheet_name=target_sheet, header=3)
            
        else:
            # Read CSV (Header at Index 3)
            df = pd.read_csv(input_data, header=3, on_bad_lines='skip')

        # 2. Clean Column Names
        if df is not None:
            df.columns = [str(c).strip().replace('\n', ' ') for c in df.columns]
            
            # 3. Critical Column Check
            required_cols = ['Alert Time', 'Verification Date/Time', 'Alert Type/Severity']
            # We relax the check slightly to allow for minor spelling variations if needed
            if not all(col in df.columns for col in required_cols):
                return None
                
            # 4. Data Cleaning
            df = df[df['Alert Time'].notna()]
            
            # Convert to Datetime (Day First for India Format)
            df['Alert Time'] = pd.to_datetime(df['Alert Time'], dayfirst=True, errors='coerce')
            df['Verification Date/Time'] = pd.to_datetime(df['Verification Date/Time'], dayfirst=True, errors='coerce')
            
            # Calculate Response Metrics
            df['Response_Time_Mins'] = (df['Verification Date/Time'] - df['Alert Time']).dt.total_seconds() / 60
            df['SOP_Violation'] = df['Response_Time_Mins'] > 30
            
            # Identify Critical Vulnerabilities (High Severity + No Verification)
            df['Severity_Clean'] = df['Alert Type/Severity'].astype(str).str.lower()
            df['Is_Critical_Gap'] = (df['Severity_Clean'].str.contains('high')) & (df['Verification Date/Time'].isna())
            
            return df
            
    except Exception as e:
        # st.error(f"Error processing {filename}: {e}") # Uncomment for debugging
        return None
    return None

# --- DATABASE FUNCTIONS ---
def upload_to_azure(file, filename):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    blob_client.upload_blob(file, overwrite=True)
    st.success(f"✅ {filename} saved to historic database.")

def get_historic_data():
    all_dfs = []
    blobs = container_client.list_blobs()
    
    for blob in blobs:
        # We now accept Excel files in history too
        if "ALARM" in blob.name.upper() or blob.name.endswith(('.xlsx', '.xls')):
            try:
                b_client = container_client.get_blob_client(blob)
                content = b_client.download_blob().readall()
                
                # Pass filename so we know how to parse it (CSV vs Excel)
                df = process_alarm_df(content, blob.name)
                
                if df is not None:
                    df['Source_File'] = blob.name
                    df['Log_Date'] = blob.creation_time.date()
                    all_dfs.append(df)
            except Exception:
                continue
    
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

# --- UI TABS ---
tab1, tab2, tab3 = st.tabs(["📤 Daily Upload", "📈 Trend Analytics", "⚠️ Vulnerability Map"])

# === TAB 1: DAILY UPLOAD ===
with tab1:
    st.header("Upload New PIDWS Alarm Log")
    st.info("Supported formats: .xlsx, .xls, .csv")
    
    # Updated to accept Excel files
    uploaded_file = st.file_uploader("Select File", type=['csv', 'xlsx', 'xls'])
    
    if uploaded_file:
        file_bytes = uploaded_file.getvalue()
        df_preview = process_alarm_df(file_bytes, uploaded_file.name)
        
        if df_preview is not None:
            st.success("File validated successfully (Row 4 Headers Detected)")
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Total Alarms", len(df_preview))
            c2.metric("Critical Gaps", df_preview['Is_Critical_Gap'].sum())
            c3.metric("SOP Violations", df_preview['SOP_Violation'].sum())
            
            st.dataframe(df_preview[['Section', 'Alert Time', 'Alert Type/Severity', 'Event Type', 'Verification Date/Time']].head(10))
            
            if st.button("💾 Commit to Azure History"):
                uploaded_file.seek(0)
                upload_to_azure(uploaded_file, uploaded_file.name)
        else:
            st.error("Error: Could not find required columns. Ensure this is the correct Alarm Log.")

# === TAB 2: TREND ANALYTICS ===
with tab2:
    st.header("Historic Compliance Trends")
    
    if st.button("🔄 Regenerate Historical Reports"):
        hist_df = get_historic_data()
        
        if not hist_df.empty:
            daily_stats = hist_df.groupby('Log_Date').agg(
                Total_Alarms=('Alert Time', 'count'),
                Avg_Response=('Response_Time_Mins', 'mean'),
                Critical_Gaps=('Is_Critical_Gap', 'sum'),
                SOP_Breaches=('SOP_Violation', 'sum')
            ).reset_index()
            
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("Avg Alarms/Day", round(daily_stats['Total_Alarms'].mean(), 1))
            kpi2.metric("Historic Critical Gaps", daily_stats['Critical_Gaps'].sum())
            kpi3.metric("Compliance Rate", f"{round(100 - (daily_stats['SOP_Breaches'].sum()/daily_stats['Total_Alarms'].sum()*100), 1)}%")

            st.subheader("Critical Vulnerability Trend")
            st.line_chart(daily_stats, x='Log_Date', y='Critical_Gaps')

            st.subheader("Response Latency Trend (Minutes)")
            st.area_chart(daily_stats, x='Log_Date', y='Avg_Response')
            
            st.subheader("Daily Data Summary")
            st.dataframe(daily_stats)
        else:
            st.warning("No valid alarm history found. (Check if uploaded files have correct columns/sheets)")

# === TAB 3: VULNERABILITY MAP ===
with tab3:
    st.header("High-Risk Section Analysis")
    
    if st.button("🔍 Analyze Vulnerabilities"):
        hist_df = get_historic_data()
        if not hist_df.empty:
            hotspots = hist_df[hist_df['Is_Critical_Gap']].groupby('Section').size().reset_index(name='Gap_Count')
            hotspots = hotspots.sort_values(by='Gap_Count', ascending=False)
            
            col_a, col_b = st.columns(2)
            with col_a:
                st.subheader("Top 5 Risky Sections")
                st.bar_chart(hotspots.head(5), x='Section', y='Gap_Count')
            
            with col_b:
                st.subheader("Raw Gap Log")
                st.dataframe(hist_df[hist_df['Is_Critical_Gap']][['Log_Date', 'Section', 'LPG . No.', 'Alert Time', 'Event Type']])
        else:
            st.write("Refresh data to view analysis.")

# --- SIDEBAR INFO ---
with st.sidebar:
    st.image("https://www.iocl.com/assets/images/logo.png", width=100)
    st.header("App Info")
    st.write("**Station:** Muzaffarpur (ERPL)")
    st.write("**SOP Ref:** SP/SEC/02 (PIDWS)")
    st.write("**Safety Ref:** SP/ML/05 (Excavation)")
    st.divider()
    st.info("This app identifies gaps in guard verification patterns.")
