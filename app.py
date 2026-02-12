import streamlit as st
import pandas as pd
import numpy as np
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

# --- SMART FILE READER ---
def process_alarm_df(file_content, filename):
    try:
        df = None
        input_data = io.BytesIO(file_content)
        
        # 1. Determine File Type
        if filename.lower().endswith(('.xls', '.xlsx')):
            xls = pd.ExcelFile(input_data)
            # Smart Sheet Selection
            target_sheet = xls.sheet_names[0]
            for sheet in xls.sheet_names:
                if "ALARM" in sheet.upper():
                    target_sheet = sheet
                    break
            # Read Excel (Header at Index 3 = Row 4)
            df = pd.read_excel(xls, sheet_name=target_sheet, header=3)
        else:
            # Read CSV (Header at Index 3 = Row 4)
            df = pd.read_csv(input_data, header=3, on_bad_lines='skip')

        # 2. Clean Column Names
        if df is not None:
            df.columns = [str(c).strip().replace('\n', ' ') for c in df.columns]
            
            # 3. Critical Column Check
            required_cols = ['Alert Time', 'Verification Date/Time', 'Alert Type/Severity']
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
            df['Is_High'] = df['Severity_Clean'].str.contains('high')
            df['Is_Critical_Gap'] = (df['Is_High']) & (df['Verification Date/Time'].isna())
            
            # PARSE DURATION FOR ADVANCED FORENSICS
            def parse_duration(time_str):
                try:
                    h, m, s = map(int, str(time_str).split(':'))
                    return h * 60 + m + s / 60
                except:
                    return 0
            
            if 'Alert Duration(HH:MM:SS)' in df.columns:
                df['Duration_Mins'] = df['Alert Duration(HH:MM:SS)'].apply(parse_duration)
            else:
                df['Duration_Mins'] = 0

            return df
            
    except Exception:
        return None
    return None

# --- DATABASE FUNCTIONS ---
def upload_to_azure(file, filename):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    blob_client.upload_blob(file, overwrite=True)
    st.success(f"✅ {filename} saved to historic database.")

# Cache this function so it doesn't reload on every click
@st.cache_data(ttl=600) 
def get_historic_data():
    all_dfs = []
    blobs = container_client.list_blobs()
    
    for blob in blobs:
        if "ALARM" in blob.name.upper() or blob.name.endswith(('.xlsx', '.xls')):
            try:
                b_client = container_client.get_blob_client(blob)
                content = b_client.download_blob().readall()
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
            
            st.dataframe(df_preview[['Section', 'Alert Time', 'Alert Type/Severity', 'Event Type']].head(10), hide_index=True)
            
            if st.button("💾 Commit to Azure History"):
                uploaded_file.seek(0)
                upload_to_azure(uploaded_file, uploaded_file.name)
                # Clear cache so new file appears immediately in analysis
                st.cache_data.clear()
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
            st.dataframe(daily_stats, hide_index=True)
        else:
            st.warning("No valid alarm history found. (Check if uploaded files have correct columns/sheets)")

# === TAB 3: VULNERABILITY MAP (CORRECTED) ===
with tab3:
    st.header("Pipeline Risk Analysis")
    
    # 1. Initialize Session State for Data Persistence
    if 'vuln_data' not in st.session_state:
        st.session_state.vuln_data = pd.DataFrame()
    if 'analysis_active' not in st.session_state:
        st.session_state.analysis_active = False

    # 2. Button to Load Data (Only runs once)
    if st.button("🔍 Analyze Vulnerabilities"):
        with st.spinner("Scanning historic records..."):
            data = get_historic_data()
            if not data.empty:
                st.session_state.vuln_data = data
                st.session_state.analysis_active = True
            else:
                st.warning("No data found in history.")

    # 3. Persistent Analysis View (Runs whenever 'analysis_active' is True)
    if st.session_state.analysis_active:
        hist_df = st.session_state.vuln_data
        
        # --- LEVEL 1: MACRO ANALYSIS ---
        st.subheader("1. Macro-Analysis: Most Vulnerable Sections")
        hotspots = hist_df[hist_df['Is_Critical_Gap']].groupby('Section').size().reset_index(name='Gap_Count')
        hotspots = hotspots.sort_values(by='Gap_Count', ascending=False)
        st.bar_chart(hotspots.set_index('Section'))
        
        st.divider()
        
        # --- LEVEL 2: MICRO ANALYSIS (1 KM STRETCH) ---
        st.subheader("2. Micro-Analysis: Top 5 Vulnerable 1 KM Stretches")
        st.markdown("*Scoring Formula: No. of High Alarms + No. of Unverified High Alarms*")
        
        # Dropdown uses the persistent dataframe
        unique_sections = hist_df['Section'].dropna().unique()
        selected_section = st.selectbox("Select Pipeline Section to Audit:", unique_sections)
        
        if selected_section:
            section_df = hist_df[hist_df['Section'] == selected_section].copy()
            
            if 'LPG . No.' in section_df.columns:
                section_df['KM_Raw'] = pd.to_numeric(section_df['LPG . No.'], errors='coerce')
                section_df = section_df.dropna(subset=['KM_Raw'])
                
                # Bucket into 1 KM
                section_df['KM_Start'] = section_df['KM_Raw'].apply(np.floor).astype(int)
                section_df['Stretch_Label'] = "KM " + section_df['KM_Start'].astype(str) + " - " + (section_df['KM_Start'] + 1).astype(str)
                
                stretch_stats = section_df.groupby('Stretch_Label').agg(
                    High_Alarms=('Is_High', 'sum'),
                    Unverified_High=('Is_Critical_Gap', 'sum')
                ).reset_index()
                
                stretch_stats['Vulnerability_Score'] = stretch_stats['High_Alarms'] + stretch_stats['Unverified_High']
                top_5_stretches = stretch_stats.sort_values(by='Vulnerability_Score', ascending=False).head(5)
                
                c1, c2 = st.columns([1, 2])
                with c1:
                    st.markdown(f"**Top Risky Stretches in {selected_section}**")
                    st.dataframe(top_5_stretches, hide_index=True)
                with c2:
                    st.markdown("**Vulnerability Graph**")
                    st.bar_chart(top_5_stretches.set_index('Stretch_Label')['Vulnerability_Score'])
            else:
                st.warning("Could not find 'LPG . No.' column.")
        
        # --- LEVEL 3: ADVANCED FORENSICS ---
        st.divider()
        st.header("3. Advanced Forensics")
        
        if 'Alert Time' in hist_df.columns:
            hist_df['Hour'] = hist_df['Alert Time'].dt.hour
            
            st.subheader("⏰ The 'Thief's Schedule' (Alarms by Hour)")
            hourly_risk = hist_df[hist_df['Is_High']].groupby('Hour').size().reset_index(name='Alarm_Count')
            full_day = pd.DataFrame({'Hour': range(24)})
            hourly_risk = full_day.merge(hourly_risk, on='Hour', how='left').fillna(0)
            
            st.bar_chart(hourly_risk.set_index('Hour'))
            st.caption("Peaks indicate the most dangerous times of day for this pipeline.")

        st.divider()
        st.subheader("🔥 Threat Intensity (Duration vs. Location)")
        
        if selected_section:
            bubble_data = hist_df[
                (hist_df['Section'] == selected_section) & 
                (hist_df['Is_High']) &
                (hist_df['Duration_Mins'] > 0)
            ].copy()
            
            if not bubble_data.empty:
                bubble_data['KM_Point'] = pd.to_numeric(bubble_data['LPG . No.'], errors='coerce')
                
                st.markdown("**Longer Duration = Higher Risk (Bubble Size)**")
                st.scatter_chart(
                    bubble_data,
                    x='KM_Point',
                    y='Duration_Mins',
                    color='Event Type',
                    size='Duration_Mins', 
                )
                st.caption(f"Visualizing specific incidents in {selected_section}.")
            else:
                st.info("No high-severity data with duration available for this section.")

# --- SIDEBAR INFO ---
with st.sidebar:
    st.image("https://www.iocl.com/assets/images/logo.png", width=100)
    st.header("App Info")
    st.write("**Station:** Muzaffarpur (ERPL)")
    st.write("**SOP Ref:** SP/SEC/02 (PIDWS)")
    st.write("**Safety Ref:** SP/ML/05 (Excavation)")
    st.divider()
    st.info("This app identifies gaps in guard verification patterns.")
