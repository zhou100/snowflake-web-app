import streamlit
import snowflake.connector
import pandas as pd
import matplotlib.pyplot as plt 

# Initialize Streamlit app
streamlit.title("Snowflake Data Analysis App")

# Snowflake connection parameters
streamlit.subheader("Snowflake Connection Parameters")
SNOWFLAKE_ACCOUNT = 'your_account_name'
SNOWFLAKE_USER = 'your_username'
SNOWFLAKE_PASSWORD = 'your_password'
SNOWFLAKE_WAREHOUSE = 'your_warehouse_name'
SNOWFLAKE_DATABASE = 'your_database_name'
SNOWFLAKE_SCHEMA = 'your_schema_name'

# Create Snowflake connection
cnx = None
cur = None  # Initialize cur to None

try:
    cnx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
except Exception as e:
    streamlit.error(f"Error connecting to Snowflake: {e}")

# Create a cursor object if the connection was successful
if cnx:
    cur = cnx.cursor()

    # Section for Average Stock Price and Free Cash Flow
    streamlit.subheader("Average Stock Price and Free Cash Flow")

    # Create an entry box for ticker with default value 'SNOW'
    ticker = streamlit.text_input("Enter Ticker", value="SNOW")

    # Query to get average stock price and free cash flow for current and previous quarters
    query = f"""
        WITH quarterly_data AS (
            SELECT 
                DATE_TRUNC('quarter', date) AS quarter,
                AVG(stock_price) AS avg_stock_price,
                SUM(free_cash_flow) AS total_free_cash_flow
            FROM 
                your_table_name
            WHERE 
                ticker = '{ticker}'
            GROUP BY 
                quarter
        )
        SELECT 
            quarter,
            avg_stock_price,
            total_free_cash_flow,
            LAG(avg_stock_price) OVER (ORDER BY quarter) AS prev_avg_stock_price,
            LAG(total_free_cash_flow) OVER (ORDER BY quarter) AS prev_total_free_cash_flow
        FROM 
            quarterly_data
        ORDER BY 
            quarter DESC
        LIMIT 2
    """

    # Execute the query
    try:
        cur.execute(query)
        results = cur.fetchall()
        df = pd.DataFrame(results, columns=['Quarter', 'Average Stock Price', 'Free Cash Flow', 'Previous Average Stock Price', 'Previous Free Cash Flow'])
        streamlit.write(df)
    except Exception as e:
        streamlit.error(f"Error executing query for average stock price: {e}")

    # Section for Free Cash Flow Over Time
    streamlit.subheader("Free Cash Flow Over Time")

    # Query to get free cash flow over time
    query_cash_flow = f"""
        SELECT 
            DATE_TRUNC('quarter', date) AS quarter,
            SUM(free_cash_flow) AS total_free_cash_flow
        FROM 
            your_table_name
        WHERE 
            ticker = '{ticker}'
        GROUP BY 
            quarter
        ORDER BY 
            quarter ASC
    """

    # Execute the query
    try:
        cur.execute(query_cash_flow)
        results_cash_flow = cur.fetchall()
        df_cash_flow = pd.DataFrame(results_cash_flow, columns=['Quarter', 'Free Cash Flow'])

        # Create a line chart to display the free cash flow over time
        fig, ax = plt.subplots()
        ax.plot(df_cash_flow['Quarter'], df_cash_flow['Free Cash Flow'])
        ax.set_title('Free Cash Flow Over Time')
        ax.set_xlabel('Quarter')
        ax.set_ylabel('Free Cash Flow')
        streamlit.pyplot(fig)
    except Exception as e:
        streamlit.error(f"Error executing query for cash flow: {e}")

# Close the cursor and connection if they were created
if cur:
    cur.close()
if cnx:
    cnx.close()




