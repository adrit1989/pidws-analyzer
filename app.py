import streamlit as st
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient
import io
import os
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# --- CONFIGURATION & THEME ---
st.set_page_config(
    page_title="IOCL PIDWS Command Center", 
    layout="wide", 
    page_icon="🛡️",
    initial_sidebar_state="expanded"
)

# --- BRANDING PALETTE ---
IOCL_BLUE = "#003366"
IOCL_ORANGE = "#F47920"
BG_COLOR = "#F4F6F9"
CARD_BG = "#FFFFFF"

# --- CUSTOM CSS (MODERN UI) ---
st.markdown(f"""
    <style>
    /* Main Background */
    .stApp {{
        background-color: {BG_COLOR};
    }}
    
    /* Sidebar Styling */
    [data-testid="stSidebar"] {{
        background-color: {IOCL_BLUE};
    }}
    [data-testid="stSidebar"] * {{
        color: white !important;
    }}
    
    /* Titles & Headers */
    h1, h2, h3 {{
        color: {IOCL_BLUE} !important;
        font-family: 'Segoe UI', sans-serif;
    }}
    
    /* Metric Cards Styling */
    div[data-testid="stMetric"] {{
        background-color: {CARD_BG};
        border-left: 5px solid {IOCL_ORANGE};
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0px 4px 10px rgba(0,0,0,0.05);
    }}
    
    /* Tabs Styling */
    .stTabs [data-baseweb="tab-list"] {{
        gap: 10px;
    }}
    .stTabs [data-baseweb="tab"] {{
        background-color: white;
        border-radius: 5px;
        padding: 10px 20px;
        box-shadow: 0px 2px 5px rgba(0,0,0,0.05);
    }}
    .stTabs [aria-selected="true"] {{
        background-color: {IOCL_ORANGE} !important;
        color: white !important;
    }}
    
    /* Custom divider */
    hr {{
        border-top: 2px solid #e0e0e0;
    }}
    </style>
    """, unsafe_allow_html=True)

# --- HEADER SECTION ---
col_logo, col_title = st.columns([1, 5])
with col_logo:
    # Placeholder for Logo (Using a web link for now)
    st.image("https://upload.wikimedia.org/wikipedia/en/thumb/4/4e/Indian_Oil_Logo.svg/1200px-Indian_Oil_Logo.svg.png", width=80)
with col_title:
    st.title("PIDWS Command Center")
    st.markdown(f"**Eastern Region Pipelines (ERPL) | Muzaffarpur Station**")

# --- AZURE CONNECTION ---
try:
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = "pidws" 
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()
except Exception as e:
    st.error("🔴 Azure Connection Failed. Check Environment Variables.")
    st.stop()

# --- HELPER FUNCTIONS ---
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
            except:
                continue
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

def process_alarm_df(file_content, filename):
    try:
        input_data = io.BytesIO(file_content)
        if filename.lower().endswith(('.xls', '.xlsx')):
            xls = pd.ExcelFile(input_data)
            target_sheet = xls.sheet_names[0]
            for sheet in xls.sheet_names:
                if "ALARM" in sheet.upper():
                    target_sheet = sheet
                    break
            df = pd.read_excel(xls, sheet_name=target_sheet, header=3)
        else:
            df = pd.read_csv(input_data, header=3, on_bad_lines='skip')

        if df is not None:
            df.columns = [str(c).strip().replace('\n', ' ') for c in df.columns]
            required_cols = ['Alert Time', 'Verification Date/Time', 'Alert Type/Severity']
            if not all(col in df.columns for col in required_cols): return None
            
            df = df[df['Alert Time'].notna()]
            df['Alert Time'] = pd.to_datetime(df['Alert Time'], dayfirst=True, errors='coerce')
            df['Verification Date/Time'] = pd.to_datetime(df['Verification Date/Time'], dayfirst=True, errors='coerce')
            df['Response_Time_Mins'] = (df['Verification Date/Time'] - df['Alert Time']).dt.total_seconds() / 60
            df['SOP_Violation'] = df['Response_Time_Mins'] > 30
            df['Severity_Clean'] = df['Alert Type/Severity'].astype(str).str.lower()
            df['Is_High'] = df['Severity_Clean'].str.contains('high')
            df['Is_Critical_Gap'] = (df['Is_High']) & (df['Verification Date/Time'].isna())
            
            # Duration Parser
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
    except:
        return None
    return None

def upload_to_azure(file, filename):
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    blob_client.upload_blob(file, overwrite=True)
    st.toast(f"✅ {filename} Uploaded Successfully!", icon="☁️")

# --- UI TABS ---
tab1, tab2, tab3 = st.tabs(["📤  Data Upload", "📈  Executive Dashboard", "🎯  Vulnerability Map"])

# === TAB 1: UPLOAD ===
with tab1:
    st.markdown("### 📂 Import Daily Logs")
    st.caption("Upload one or multiple PIDWS extraction files. System supports .xlsx, .xls, and .csv formats.")
    
    with st.container():
        # CHANGED: Added accept_multiple_files=True
        uploaded_files = st.file_uploader("Drop your files here", type=['csv', 'xlsx', 'xls'], 
                                          label_visibility="collapsed", accept_multiple_files=True)
        
    if uploaded_files:
        with st.spinner("Analyzing Files..."):
            all_valid_dfs = []
            valid_files = [] # To keep track of which file objects are actually good
            
            for file in uploaded_files:
                # Process each file individually
                df = process_alarm_df(file.getvalue(), file.name)
                if df is not None:
                    all_valid_dfs.append(df)
                    valid_files.append(file)
            
        if all_valid_dfs:
            # Combine all valid dataframes into one for the preview
            df_preview = pd.concat(all_valid_dfs, ignore_index=True)
            
            st.success(f"✅ {len(valid_files)} File(s) Validated: Standard PIDWS Format Detected")
            
            # KPI Cards (Calculated on the combined data)
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Total Events", len(df_preview))
            k2.metric("Critical Gaps", df_preview['Is_Critical_Gap'].sum())
            k3.metric("SOP Violations", df_preview['SOP_Violation'].sum())
            
            # Safe Mean Calculation (The fix we discussed)
            avg_response = df_preview['Response_Time_Mins'].mean()
            if pd.isna(avg_response):
                avg_response = 0
            k4.metric("Avg Response", f"{int(avg_response)} min")
            
            st.dataframe(df_preview.head(5), use_container_width=True, hide_index=True)
            
            # Upload Button
            if st.button("🚀 Commit All to Historic Database", type="primary"):
                progress_text = st.empty()
                progress_bar = st.progress(0)
                
                for i, file in enumerate(valid_files):
                    file.seek(0) # Reset file pointer before upload
                    upload_to_azure(file, file.name)
                    # Update progress
                    progress = (i + 1) / len(valid_files)
                    progress_bar.progress(progress)
                    progress_text.text(f"Uploading {i+1}/{len(valid_files)}: {file.name}")
                
                st.cache_data.clear()
                progress_text.text("✅ All files uploaded successfully!")
                st.success("Historic Database Updated.")
        else:
            st.error("❌ Format Error: None of the uploaded files matched the required columns.")

# === TAB 2: DASHBOARD ===
with tab2:
    col_header, col_refresh = st.columns([6,1])
    col_header.markdown("### 📊 Historic Compliance Trends")
    if col_refresh.button("🔄 Refresh"):
        st.cache_data.clear()
        hist_df = get_historic_data()
    else:
        hist_df = get_historic_data()

    if not hist_df.empty:
        # Aggregations
        daily_stats = hist_df.groupby('Log_Date').agg(
            Total_Alarms=('Alert Time', 'count'),
            Avg_Response=('Response_Time_Mins', 'mean'),
            Critical_Gaps=('Is_Critical_Gap', 'sum'),
            SOP_Breaches=('SOP_Violation', 'sum')
        ).reset_index()

        # BIG KPI ROW
        st.markdown("##### 📅 Performance Overview")
        k1, k2, k3, k4 = st.columns(4)
        avg_alarms = int(daily_stats['Total_Alarms'].mean())
        total_gaps = daily_stats['Critical_Gaps'].sum()
        compliance = 100 - (daily_stats['SOP_Breaches'].sum() / daily_stats['Total_Alarms'].sum() * 100)
        
        k1.metric("Avg Alarms/Day", avg_alarms, delta_color="off")
        k2.metric("Total Critical Gaps", total_gaps, delta="-High Risk", delta_color="inverse")
        k3.metric("Compliance Rate", f"{compliance:.1f}%", delta="Target 98%")
        k4.metric("Days Analyzed", len(daily_stats))
        
        st.divider()

        # CHARTS ROW 1
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("**🚨 Critical Vulnerability Trend**")
            fig_line = px.line(daily_stats, x='Log_Date', y='Critical_Gaps', markers=True, 
                               labels={'Critical_Gaps': 'Unverified High Alarms'}, template="plotly_white")
            fig_line.update_traces(line_color=IOCL_ORANGE, line_width=3)
            st.plotly_chart(fig_line, use_container_width=True)
            
        with c2:
            st.markdown("**⏱️ Response Latency (SOP Adherence)**")
            fig_area = px.area(daily_stats, x='Log_Date', y='Avg_Response', 
                               labels={'Avg_Response': 'Minutes'}, template="plotly_white")
            fig_area.update_traces(line_color=IOCL_BLUE)
            fig_area.add_hline(y=30, line_dash="dash", annotation_text="SOP Limit (30m)", line_color="red")
            st.plotly_chart(fig_area, use_container_width=True)

# === TAB 3: VULNERABILITY MAP ===
with tab3:
    st.markdown("### 🎯 Pipeline Risk & Forensics")
    
    # Session State
    if 'vuln_data' not in st.session_state: st.session_state.vuln_data = pd.DataFrame()
    if 'analysis_active' not in st.session_state: st.session_state.analysis_active = False

    if st.button("🔍 Run Deep Analysis", type="primary"):
        with st.spinner("Processing geospatial data..."):
            data = get_historic_data()
            if not data.empty:
                st.session_state.vuln_data = data
                st.session_state.analysis_active = True

    if st.session_state.analysis_active:
        df = st.session_state.vuln_data
        
        # --- SECTION 1: HOTSPOTS ---
        st.info("💡 **Insight:** This chart identifies which pipeline sections are most frequently targeted.")
        hotspots = df[df['Is_Critical_Gap']].groupby('Section').size().reset_index(name='Gap_Count')
        hotspots = hotspots.sort_values(by='Gap_Count', ascending=True) # Ascending for horizontal bar
        
        fig_bar = px.bar(hotspots, x='Gap_Count', y='Section', orientation='h', 
                         text='Gap_Count', color='Gap_Count', 
                         color_continuous_scale=[IOCL_BLUE, IOCL_ORANGE])
        fig_bar.update_layout(xaxis_title="Unverified Alarms", yaxis_title=None, template="plotly_white")
        st.plotly_chart(fig_bar, use_container_width=True)
        
        st.divider()

        # --- SECTION 2: 1KM MICRO ANALYSIS ---
        c_control, c_graph = st.columns([1, 2])
        
        with c_control:
            st.markdown("#### 📍 Micro-Audit")
            st.caption("Select a section to analyze risk at 1-KM intervals.")
            sections = df['Section'].dropna().unique()
            sel_sec = st.selectbox("Pipeline Section", sections)
            
            # Logic
            sec_df = df[df['Section'] == sel_sec].copy()
            if 'LPG . No.' in sec_df.columns:
                sec_df['KM_Raw'] = pd.to_numeric(sec_df['LPG . No.'], errors='coerce')
                sec_df = sec_df.dropna(subset=['KM_Raw'])
                sec_df['KM_Start'] = sec_df['KM_Raw'].apply(np.floor).astype(int)
                sec_df['Stretch_Label'] = "KM " + sec_df['KM_Start'].astype(str)
                
                stats = sec_df.groupby('Stretch_Label').agg(
                    High_Alarms=('Is_High', 'sum'),
                    Unverified=('Is_Critical_Gap', 'sum')
                ).reset_index()
                stats['Risk_Score'] = stats['High_Alarms'] + stats['Unverified']
                top_risky = stats.sort_values(by='Risk_Score', ascending=False).head(5)
                
                st.markdown("**Top 5 Risky Stretches**")
                st.dataframe(top_risky, hide_index=True, use_container_width=True)
            else:
                st.warning("No KM Data found.")

        with c_graph:
            if 'LPG . No.' in sec_df.columns and not top_risky.empty:
                st.markdown(f"**Risk Profile: {sel_sec}**")
                fig_km = px.bar(top_risky, x='Stretch_Label', y='Risk_Score', 
                                color='Unverified', title="Vulnerability Score per KM",
                                color_continuous_scale='Reds')
                st.plotly_chart(fig_km, use_container_width=True)

        st.divider()
        
        # --- SECTION 3: FORENSICS ---
        st.markdown("### 🕵️ Advanced Forensics")
        f1, f2 = st.columns(2)
        
        with f1:
            st.markdown("**⏰ The 'Thief's Schedule'**")
            if 'Alert Time' in df.columns:
                df['Hour'] = df['Alert Time'].dt.hour
                hourly = df[df['Is_High']].groupby('Hour').size().reset_index(name='Count')
                full_day = pd.DataFrame({'Hour': range(24)})
                hourly = full_day.merge(hourly, on='Hour', how='left').fillna(0)
                
                fig_time = px.bar(hourly, x='Hour', y='Count', 
                                  color='Count', color_continuous_scale='Oranges')
                fig_time.update_layout(xaxis=dict(tickmode='linear'), template="plotly_white")
                st.plotly_chart(fig_time, use_container_width=True)
        
        with f2:
            st.markdown("**🔥 Threat Intensity (Duration)**")
            if sel_sec:
                bubble_data = df[(df['Section'] == sel_sec) & (df['Is_High']) & (df['Duration_Mins'] > 0)]
                if not bubble_data.empty:
                    bubble_data['KM'] = pd.to_numeric(bubble_data['LPG . No.'], errors='coerce')
                    fig_bub = px.scatter(bubble_data, x='KM', y='Duration_Mins', 
                                         size='Duration_Mins', color='Event Type',
                                         hover_data=['Alert Time'], template="plotly_white")
                    st.plotly_chart(fig_bub, use_container_width=True)
                else:
                    st.info("No duration data available for this section.")

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("### ⚙️ System Status")
    st.info("🟢 Azure Storage: Connected")
    st.markdown("---")
    st.markdown("**Station:** Muzaffarpur")
    st.markdown("**SOP:** SP/SEC/02")
    st.caption("© 2026 Indian Oil Corporation Ltd.")
