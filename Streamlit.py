# Streamlit App: Upload Master Excel, Clean, and Insert into PostgreSQL
import streamlit as st
import pandas as pd
import psycopg2
from datetime import datetime

st.set_page_config(page_title="Master Sheet ETL", layout="wide")
st.title("Production ETL – Upload and Cleaning")

# PostgreSQL Connection (update with your actual credentials)
def get_connection():
    return psycopg2.connect(
        host="your_host",
        port="5432",
        dbname="your_db",
        user="your_user",
        password="your_password"
    )

# Function to enrich the date with Dim_Date fields
def enrich_date_fields(df, date_col):
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True)
    df['day'] = df[date_col].dt.day
    df['month'] = df[date_col].dt.month
    df['year'] = df[date_col].dt.year
    df['day_name'] = df[date_col].dt.day_name()
    df['month_name'] = df[date_col].dt.month_name()
    df['quarter'] = df[date_col].dt.quarter
    return df

uploaded_file = st.file_uploader("Upload the Master Sheet Excel file", type=["xlsx"])

if uploaded_file:
    try:
        sheets = pd.read_excel(uploaded_file, sheet_name=None)
        st.success("File uploaded successfully")

        # Basic cleaning for each sheet if it exists
        cleaned_data = {}
        required_sheets = ["Intake", "Sorting", "Low-Risk", "High Care", "Assembly", "Dispatch"]

        for sheet in required_sheets:
            if sheet in sheets:
                df = sheets[sheet].copy()
                if 'Date' in df.columns:
                    df = enrich_date_fields(df, 'Date')
                elif 'Entry_Date' in df.columns:
                    df = enrich_date_fields(df, 'Entry_Date')
                elif 'Departure_Date_ID' in df.columns:
                    df = enrich_date_fields(df, 'Departure_Date_ID')
                cleaned_data[sheet] = df
                st.subheader(f"📄 {sheet}")
                st.dataframe(df.head())
            else:
                st.warning(f"The sheet '{sheet}' was not found in the Excel file.")

        # Confirm upload to PostgreSQL
        if st.button("Upload all to the Data Warehouse (PostgreSQL)"):
            conn = get_connection()
            cursor = conn.cursor()

            for sheet_name, df in cleaned_data.items():
                table_name = "cleaned_" + sheet_name.lower().replace(" ", "_")
                st.write(f"Uploading sheet: {sheet_name} → Table: {table_name}")
                # Create table if not exists and upload data
                df.head(0).to_sql(table_name, con=conn, if_exists='append', index=False, method='multi')
                for i, row in df.iterrows():
                    values = tuple(row.values)
                    placeholders = ','.join(['%s'] * len(values))
                    columns = ','.join(df.columns)
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(sql, values)

            conn.commit()
            cursor.close()
            conn.close()
            st.success("Data successfully uploaded to the warehouse!")

    except Exception as e:
        st.error(f"Error processing the file: {str(e)}")