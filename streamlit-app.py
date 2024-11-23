import streamlit as st
import psycopg2
import pandas as pd
from streamlit_autorefresh import st_autorefresh
import time


def fetch_clickstream_stats(conn):
    query = """
        SELECT 
            COUNT(*) AS click_count
        FROM events;
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    records_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(DISTINCT user_id) FROM events;
    """)
    users = cursor.fetchone()[0]
    return records_count, users

def query_most_added_to_cart(conn):
    query = """
        select
        split_part(url,'/',5) product_id,
        count(*) product_count
        from events
        where event_type='add_to_cart'
        group by url
        order by product_count DESC 
        limit 5;
    """
    df = pd.read_sql_query(query,conn)
    return df
def query_most_popular_pages(conn):

    query = """
        select
            split_part(url,'/',4) page_name,
            count(*) number_of_views
        from events
        where event_type='page_view'
        group by url
        order by number_of_views DESC
        limit 5;
    """
    df = pd.read_sql_query(query,conn)
    return df
def update_data():
    db_config = {
    "host":"localhost", # Docker service name
    "port":5433, # docker internal port
    "dbname":"clickstream",
    "user":"maodo",
    "password":"pass123"
    }
    conn = psycopg2.connect(**db_config)
    # Placeholder to display last refresh time
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch records and users count
    records_count, users = fetch_clickstream_stats(conn)
    # Display total records
    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Records", records_count)
    col2.metric("Total Active users on the website", users)

    # Display data as a bar chart
    # Top 5 popular pages
    df_popular_pages = query_most_popular_pages(conn)
    st.header("Most Popular Pages")
    st.bar_chart(df_popular_pages.set_index("page_name")["number_of_views"])

    # Most added to cart product
    st.header("Most Added Products to Cart ")
    df_added_to_cart = query_most_added_to_cart(conn)
    st.table(df_added_to_cart)

    # Close connection to DB
    conn.close()
    # Update the last refresh time
    st.session_state['last_update'] = time.time()
# Sidebar layout
def sidebar():
    # Initialize last update time if not present in session state
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    # Slider to control refresh interval
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    # Button to manually refresh data
    if st.sidebar.button('Refresh Data'):
        update_data()

st.title("Clickstream realtime Dashboard")
# Display sidebar
sidebar()
# Update and display data on the dashboard
update_data()