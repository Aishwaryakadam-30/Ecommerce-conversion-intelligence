import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
import google.generativeai as gemini
import os
import json
from dotenv import load_dotenv

# ===== SETUP =====
load_dotenv()
warehouse_connection = create_engine(os.getenv('DATABASE_URL'))
gemini.configure(api_key=os.getenv('GEMINI_API_KEY'))
brain = gemini.GenerativeModel('gemini-flash-latest')

st.set_page_config(page_title="Funnel Intelligence", page_icon="📊", layout="wide")

# ===== CUSTOM CSS =====
st.markdown("""
<style>
    .main {background-color: #f8f9fc;}
    .metric-card {
        background: white; border-radius: 12px; padding: 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08); text-align: center;
    }
    .metric-value {font-size: 32px; font-weight: 700; color: #1F4E79;}
    .metric-label {font-size: 14px; color: #718096; margin-top: 4px;}
    h1 {color: #1F4E79 !important;}
</style>
""", unsafe_allow_html=True)

# ===== SIDEBAR NAVIGATION =====
st.sidebar.title("📊 Funnel Intelligence")
st.sidebar.markdown("*E-commerce Analytics Platform*")
st.sidebar.markdown("---")
selected_page = st.sidebar.radio(
    "Navigate",
    ["🏠 Overview", "💬 Chat with Data", "🤖 AI Insights"],
    label_visibility="collapsed"
)
st.sidebar.markdown("---")
st.sidebar.caption("Built by Aishwarya Kadam")
st.sidebar.caption("GitHub: Aishwaryakadam-30")


# =======================================
# PAGE 1: OVERVIEW
# =======================================
if selected_page == "🏠 Overview":
    st.title("🏠 Conversion Funnel Overview")
    st.markdown("Real-time analytics from the PostgreSQL warehouse")

    # --- KPI CARDS ---
    kpi_data = pd.read_sql("""
        SELECT
            COUNT(DISTINCT user_id) AS shoppers,
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS orders,
            ROUND(SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END)::numeric, 0) AS revenue,
            ROUND(AVG(CASE WHEN event_type = 'purchase' THEN amount END)::numeric, 2) AS avg_basket
        FROM fact_events
    """, warehouse_connection).iloc[0]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("👥 Total Shoppers", f"{int(kpi_data['shoppers']):,}")
    col2.metric("🛒 Total Orders", f"{int(kpi_data['orders']):,}")
    col3.metric("💰 Total Revenue", f"${int(kpi_data['revenue']):,}")
    col4.metric("🧾 Avg Basket", f"${kpi_data['avg_basket']:.2f}")

    st.markdown("---")

    # --- FUNNEL CHART ---
    left_col, right_col = st.columns([3, 2])

    with left_col:
        st.subheader("📉 Conversion Funnel")
        funnel_data = pd.read_sql("""
            WITH user_stages AS (
                SELECT user_id,
                    MAX(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS s1,
                    MAX(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS s2,
                    MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS s3,
                    MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS s4
                FROM fact_events GROUP BY user_id
            )
            SELECT 'Page View' AS stage, SUM(s1) AS shoppers FROM user_stages
            UNION ALL SELECT 'Product View', SUM(s2) FROM user_stages
            UNION ALL SELECT 'Add to Cart', SUM(s3) FROM user_stages
            UNION ALL SELECT 'Purchase', SUM(s4) FROM user_stages
        """, warehouse_connection)

        funnel_fig = go.Figure(go.Funnel(
            y=funnel_data['stage'],
            x=funnel_data['shoppers'],
            textinfo="value+percent initial",
            marker=dict(color=["#1F4E79", "#2E75B6", "#5B9BD5", "#9DC3E6"])
        ))
        funnel_fig.update_layout(height=400, margin=dict(l=20, r=20, t=20, b=20))
        st.plotly_chart(funnel_fig, use_container_width=True)

    # --- SEGMENT DONUT ---
    with right_col:
        st.subheader("🍩 Customer Segments")
        segment_data = pd.read_csv('data/processed/rfm_segmentation.csv')
        segment_counts = segment_data['customer_segment'].value_counts().reset_index()
        segment_counts.columns = ['segment', 'count']

        donut_fig = px.pie(
            segment_counts, values='count', names='segment', hole=0.5,
            color_discrete_sequence=["#48BB78", "#4299E1", "#9F7AEA", "#ECC94B", "#FC8181", "#718096"]
        )
        donut_fig.update_layout(height=400, margin=dict(l=20, r=20, t=20, b=20))
        st.plotly_chart(donut_fig, use_container_width=True)

    st.markdown("---")

    # --- REVENUE TREND ---
    st.subheader("📈 Monthly Revenue Trend")
    monthly_revenue = pd.read_sql("""
        SELECT DATE_TRUNC('month', timestamp) AS month,
            SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) AS revenue
        FROM fact_events
        GROUP BY DATE_TRUNC('month', timestamp)
        ORDER BY month
    """, warehouse_connection)

    revenue_fig = px.area(monthly_revenue, x='month', y='revenue',
                          color_discrete_sequence=["#2E75B6"])
    revenue_fig.update_layout(height=300, margin=dict(l=20, r=20, t=20, b=20))
    st.plotly_chart(revenue_fig, use_container_width=True)


# =======================================
# PAGE 2: CHAT WITH DATA
# =======================================
elif selected_page == "💬 Chat with Data":
    st.title("💬 Chat with Your Data")
    st.markdown("Ask questions in plain English. AI converts to SQL and runs it.")

    user_question = st.text_input(
        "Ask a question about your e-commerce data:",
        placeholder="e.g. What is the total revenue from purchases?"
    )

    if user_question:
        with st.spinner("🤖 AI is thinking..."):
            sql_prompt = f"""
Convert this question into a PostgreSQL query.

DATABASE SCHEMA:
- fact_events: event_id, user_id, session_id, timestamp, event_type, product_id, amount
  event_type values: 'page_view', 'product_view', 'click', 'add_to_cart', 'purchase', 'login', 'logout'
- dim_users: user_id, first_seen, last_seen, total_events

QUESTION: {user_question}

Return ONLY the SQL query, no explanation, no markdown, no semicolon.
"""
            try:
                ai_response = brain.generate_content(sql_prompt)
                generated_sql = ai_response.text.strip().replace('```sql', '').replace('```', '').strip()

                st.code(generated_sql, language="sql")

                query_results = pd.read_sql(generated_sql, warehouse_connection)
                st.dataframe(query_results, use_container_width=True)

                if len(query_results) > 0 and len(query_results.columns) >= 2:
                    st.subheader("📊 Auto-Visualization")
                    chart_fig = px.bar(query_results, x=query_results.columns[0],
                                       y=query_results.columns[1],
                                       color_discrete_sequence=["#2E75B6"])
                    st.plotly_chart(chart_fig, use_container_width=True)

            except Exception as err:
                st.error(f"Query failed: {err}")

    st.markdown("---")
    st.subheader("💡 Try These Questions:")
    sample_questions = [
        "What is the total revenue from all purchases?",
        "How many unique users made a purchase?",
        "What are the top 5 products by revenue?",
        "What hour of the day has the most purchases?",
        "How many users added items to cart but never purchased?"
    ]
    for q in sample_questions:
        st.markdown(f"• *{q}*")


# =======================================
# PAGE 3: AI INSIGHTS
# =======================================
elif selected_page == "🤖 AI Insights":
    st.title("🤖 AI-Generated Business Insights")
    st.markdown("Powered by Google Gemini")

    # Load saved insights if they exist
    insights_path = 'docs/ai_insights/executive_summary.txt'
    recommendations_path = 'docs/ai_insights/segment_recommendations.txt'

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("📋 Executive Summary")
        if os.path.exists(insights_path):
            with open(insights_path, 'r') as f:
                saved_summary = f.read()
            st.markdown(saved_summary)
        else:
            st.info("Run the GenAI notebook (Day 10) first to generate insights.")

    with col_right:
        st.subheader("🎯 Segment Recommendations")
        if os.path.exists(recommendations_path):
            with open(recommendations_path, 'r') as f:
                saved_recs = f.read()
            st.markdown(saved_recs)
        else:
            st.info("Run the GenAI notebook (Day 10) first to generate recommendations.")

    st.markdown("---")

    # Live AI generation
    st.subheader("🔄 Generate Fresh Insights")
    if st.button("🧠 Ask AI to Analyze Current Data", type="primary"):
        with st.spinner("AI is analyzing your warehouse data..."):
            live_metrics = pd.read_sql("""
                SELECT COUNT(DISTINCT user_id) AS users,
                    COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases,
                    ROUND(SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END)::numeric, 2) AS revenue
                FROM fact_events
            """, warehouse_connection).iloc[0].to_dict()

            live_prompt = f"""
You are a senior e-commerce analyst. Analyze these metrics and give 5 actionable insights:
{json.dumps({k: float(v) for k, v in live_metrics.items()}, indent=2)}
Be specific with numbers and recommend actions with estimated revenue impact.
"""
            live_response = brain.generate_content(live_prompt)
            st.markdown(live_response.text)
