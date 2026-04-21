# E-commerce Conversion Intelligence Platform

An end-to-end e-commerce funnel analytics platform analyzing 270K+ clickstream events with GenAI-powered insights.

**Author:** [Aishwarya Kadam](https://linkedin.com/in/aish0830)

## Business Problem
E-commerce companies lose revenue when users drop off during the purchase journey. This platform identifies exactly WHERE users abandon, WHY they leave, and WHAT actions to take.

## Key Findings
- Total Events Analyzed: 270,914
- Unique Users: 5,000
- Overall Conversion Rate: 18.6%
- Total Revenue: $255,376
- Biggest Drop-off: Product View to Cart (61% drop)
- AI-Identified Revenue Opportunity: $77,000

## Tech Stack
Python, SQL, dbt, PostgreSQL (Supabase), Google Gemini API, Tableau, Streamlit, Plotly, Pandas, NumPy

## What This Platform Does
1. Conversion Funnel Analysis (4-stage funnel with drop-off rates)
2. Cohort Retention Analysis (monthly cohort heatmaps)
3. Multi-Touch Attribution (4 models: first-touch, last-touch, linear, time-decay)
4. RFM Customer Segmentation (6 segments, 905 shoppers)
5. GenAI Intelligence Layer (auto-insights, NL-to-SQL, recommendations)
6. Interactive Dashboards (Tableau + Streamlit web app)

## Notebooks
1. 01_data_exploration - Data profiling and baseline metrics
2. 02_data_cleaning - Cleaning pipeline + synthetic 2025 data
3. 03_load_to_warehouse - Star schema + PostgreSQL load
4. 04_funnel_analysis - Core funnel SQL queries
5. 05_cohort_analysis - Cohort retention heatmaps
6. 06_attribution_modeling - 4 attribution models
7. 07_rfm_segmentation - RFM customer segmentation
8. 08_genai_insights - AI executive summary + NL-to-SQL
9. 09_dashboard_data_prep - Export data for Tableau

## How to Run
1. Clone this repo
2. pip install -r requirements.txt
3. Add .env file with DATABASE_URL and GEMINI_API_KEY
4. streamlit run streamlit_app/app.py

## Author
Aishwarya Kadam - MS Computer Science, University of Texas at Arlington
