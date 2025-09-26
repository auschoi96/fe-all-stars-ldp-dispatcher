# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 FE All Stars Agent Dispatcher - Windmill IoT Dashboard
# MAGIC
# MAGIC Real-time windmill IoT monitoring dashboard with pipeline analytics

# COMMAND ----------

# MAGIC %pip install streamlit plotly numpy

# COMMAND ----------

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta

# Configure Streamlit page
st.set_page_config(
    page_title="FE All Stars Agent Dispatcher",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# COMMAND ----------

# Custom CSS for windmill styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #1e1b4b 0%, #7c3aed 50%, #ec4899 100%);
        padding: 2rem;
        border-radius: 20px;
        margin-bottom: 2rem;
        text-align: center;
        color: white;
    }
    .metric-card {
        background: rgba(0, 0, 0, 0.2);
        padding: 1.5rem;
        border-radius: 16px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)

# COMMAND ----------

# Header
st.markdown("""
<div class="main-header">
    <h1>🎯 FE All Stars Agent Dispatcher</h1>
    <p>Real-time windmill IoT monitoring dashboard</p>
</div>
""", unsafe_allow_html=True)

# COMMAND ----------

@st.cache_data(ttl=30)
def get_pipeline_metrics():
    """Get pipeline and job metrics from Databricks"""
    try:
        # Get raw sensor data count
        raw_data = spark.sql("""
            SELECT COUNT(*) as total_records,
                   MAX(timestamp) as latest_timestamp
            FROM users.justin_edrington.raw_sensor_data
        """).collect()[0]

        # Get silver data metrics
        silver_data = spark.sql("""
            SELECT COUNT(*) as processed_records,
                   AVG(power_output) as avg_power,
                   COUNT(CASE WHEN alert_level = 'HIGH' THEN 1 END) as high_alerts
            FROM users.justin_edrington.silver_windmill_data
        """).collect()[0]

        return {
            'total_records': raw_data['total_records'],
            'latest_timestamp': raw_data['latest_timestamp'],
            'processed_records': silver_data['processed_records'],
            'avg_power': silver_data['avg_power'] or 0,
            'high_alerts': silver_data['high_alerts']
        }

    except Exception as e:
        st.info(f"Using demo data - Database tables not ready yet: {str(e)[:100]}...")
        # Return realistic demo data
        current_time = datetime.now()
        return {
            'total_records': 125847,
            'latest_timestamp': current_time,
            'processed_records': 125847,
            'avg_power': 1847.3,
            'high_alerts': 23
        }

# COMMAND ----------

@st.cache_data(ttl=60)
def get_windmill_data():
    """Get recent windmill data for visualization"""
    try:
        df = spark.sql("""
            SELECT timestamp,
                   turbine_id,
                   power_output,
                   wind_speed,
                   temperature,
                   alert_level
            FROM users.justin_edrington.silver_windmill_data
            WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
            ORDER BY timestamp DESC
            LIMIT 1000
        """).toPandas()

        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

        return df

    except Exception as e:
        st.info(f"Using demo data - Database tables not ready yet: {str(e)[:100]}...")
        # Generate realistic demo data
        current_time = datetime.now()
        timestamps = [current_time - timedelta(minutes=x*5) for x in range(60)]

        data = []
        for timestamp in timestamps:
            for turbine_id in ['T001', 'T002', 'T003', 'T004', 'T005']:
                # Simulate realistic windmill data
                wind_speed = np.random.normal(8, 2)  # m/s
                wind_speed = max(0, wind_speed)

                # Power output based on wind speed (simplified curve)
                if wind_speed < 3:
                    power_output = 0
                elif wind_speed < 25:
                    power_output = min(2000, (wind_speed - 3) * 100 + np.random.normal(0, 50))
                else:
                    power_output = 0  # Too windy, shut down

                temperature = np.random.normal(20, 5)

                # Alert level based on conditions
                if power_output > 1800 or temperature > 30:
                    alert_level = 'HIGH'
                elif power_output > 1500 or temperature > 25:
                    alert_level = 'MEDIUM'
                else:
                    alert_level = 'LOW'

                data.append({
                    'timestamp': timestamp,
                    'turbine_id': turbine_id,
                    'power_output': max(0, power_output),
                    'wind_speed': wind_speed,
                    'temperature': temperature,
                    'alert_level': alert_level
                })

        return pd.DataFrame(data)

# COMMAND ----------

# Main dashboard
col1, col2, col3, col4 = st.columns(4)

# Get metrics
metrics = get_pipeline_metrics()

with col1:
    st.metric(
        label="🎯 Total Records",
        value=f"{metrics['total_records']:,}",
        delta=f"Latest: {metrics['latest_timestamp']}" if metrics['latest_timestamp'] else "No data"
    )

with col2:
    st.metric(
        label="⚡ Processed Records",
        value=f"{metrics['processed_records']:,}",
        delta="Silver Layer"
    )

with col3:
    st.metric(
        label="🔋 Avg Power Output",
        value=f"{metrics['avg_power']:.1f} kW" if metrics['avg_power'] else "0 kW",
        delta="Current Average"
    )

with col4:
    st.metric(
        label="🚨 High Alerts",
        value=f"{metrics['high_alerts']:,}",
        delta="Critical Issues"
    )

# COMMAND ----------

# Windmill data visualization
st.header("🌪️ Real-time Windmill Monitoring")

windmill_data = get_windmill_data()

if not windmill_data.empty:
    # Power output over time
    fig_power = px.line(
        windmill_data,
        x='timestamp',
        y='power_output',
        color='turbine_id',
        title='Power Output Over Time',
        labels={'power_output': 'Power (kW)', 'timestamp': 'Time'}
    )
    fig_power.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white'
    )
    st.plotly_chart(fig_power, use_container_width=True)

    col1, col2 = st.columns(2)

    with col1:
        # Wind speed vs Power output scatter
        fig_scatter = px.scatter(
            windmill_data,
            x='wind_speed',
            y='power_output',
            color='alert_level',
            title='Wind Speed vs Power Output',
            labels={'wind_speed': 'Wind Speed (m/s)', 'power_output': 'Power (kW)'}
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col2:
        # Alert distribution
        alert_counts = windmill_data['alert_level'].value_counts()
        fig_alerts = px.pie(
            values=alert_counts.values,
            names=alert_counts.index,
            title='Alert Level Distribution',
            color_discrete_map={'LOW': 'green', 'MEDIUM': 'orange', 'HIGH': 'red'}
        )
        st.plotly_chart(fig_alerts, use_container_width=True)

    # Recent data table
    st.subheader("📊 Recent Windmill Data")
    st.dataframe(
        windmill_data.head(20),
        use_container_width=True,
        hide_index=True
    )

else:
    st.warning("No windmill data available. Check that your pipeline is running and generating data.")

# COMMAND ----------

# Sidebar with pipeline information
with st.sidebar:
    st.header("📋 Pipeline Status")

    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

    st.info("""
    **FE All Stars Agent Dispatcher**

    This dashboard monitors your windmill IoT data pipeline:

    🎯 **Data Generator**: Creates synthetic windmill sensor data
    🌪️ **DLT Pipeline**: Processes data through Bronze → Silver → Gold layers
    📊 **Real-time Monitoring**: Live metrics and visualizations
    🚨 **Alert System**: Monitors turbine health and performance

    **Data Sources:**
    - Raw: `users.justin_edrington.raw_sensor_data`
    - Processed: `users.justin_edrington.silver_windmill_data`
    """)

    # Auto-refresh toggle
    auto_refresh = st.checkbox("Auto-refresh (30s)", value=True)

    if auto_refresh:
        st.rerun()

# COMMAND ----------

# Footer
st.markdown("---")
st.markdown("🤖 Generated with [Claude Code](https://claude.ai/code) | Co-Authored-By: Claude")