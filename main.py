# Streamlit App: Upload Master Excel, Clean, and Insert into PostgreSQL
import streamlit as st
import pandas as pd
from datetime import datetime

# Uncomment when PostgreSQL is ready
# import psycopg2

st.set_page_config(page_title="Master Sheet ETL", layout="wide")
st.title("📊 Production ETL – Upload and Cleaning")

# ✅ PostgreSQL connection (use when database is ready)
def get_connection():
    import psycopg2
    return psycopg2.connect(
        host="your_host",
        port="5432",
        dbname="your_db",
        user="your_user",
        password="your_password"
    )

# Function to enrich the date column
def enrich_date_fields(df, date_col):
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce', dayfirst=True)
    df['day'] = df[date_col].dt.day
    df['month'] = df[date_col].dt.month
    df['year'] = df[date_col].dt.year
    df['day_name'] = df[date_col].dt.day_name()
    df['month_name'] = df[date_col].dt.month_name()
    df['quarter'] = df[date_col].dt.quarter
    return df

uploaded_file = st.file_uploader("📤 Upload the Master Sheet Excel file", type=["xlsx"])

if uploaded_file:
    try:
        sheets = pd.read_excel(uploaded_file, sheet_name=None)
        st.success("✅ File uploaded successfully")

        cleaned_data = {}
        required_sheets = ["Intake", "Sorting", "Low-Risk", "High-Care", "Assembly", "Dispatch"]

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
                st.warning(f"⚠️ Sheet '{sheet}' not found in the file.")

        # Upload toggle
        upload_enabled = st.checkbox("✅ Enable PostgreSQL Upload", value=False)

        if st.button("🚀 Upload to PostgreSQL Data Warehouse") and upload_enabled:
            try:
                conn = get_connection()
                cursor = conn.cursor()

                for sheet_name, df in cleaned_data.items():
                    table_name = "cleaned_" + sheet_name.lower().replace(" ", "_")
                    st.write(f"📦 Uploading: {sheet_name} → Table: {table_name}")

                    # Create INSERT statement for each row
                    for i, row in df.iterrows():
                        values = tuple(row.values)
                        placeholders = ','.join(['%s'] * len(values))
                        columns = ','.join(df.columns)
                        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                        cursor.execute(sql, values)

                conn.commit()
                cursor.close()
                conn.close()
                st.success("✅ Data successfully uploaded to PostgreSQL!")

            except Exception as db_err:
                st.error(f"❌ PostgreSQL Error: {str(db_err)}")

        elif not upload_enabled:
            st.info("ℹ️ PostgreSQL upload is disabled. You can review the data above.")

    except Exception as e:
        st.error(f"❌ Error processing the file: {str(e)}")
