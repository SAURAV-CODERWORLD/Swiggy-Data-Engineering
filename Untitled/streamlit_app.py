# ============================================================
# SWIGGY REVENUE INTELLIGENCE DASHBOARD
# ============================================================

# ============================================================
# 1. IMPORT LIBRARIES
# ============================================================

import streamlit as st
import pandas as pd

from snowflake.snowpark.context import get_active_session


# ============================================================
# 2. PAGE CONFIGURATION
# ============================================================
# ============================================================
# 2. PAGE CONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Swiggy Revenue Dashboard",
    page_icon="🍔",
    layout="wide"
)


# ============================================================
# 3. CUSTOM DASHBOARD DESIGN
# ============================================================

st.markdown("""
<style>

    /* Main dashboard background */
    .stApp {
        background-color: #111827;
    }

    /* Sidebar */
    [data-testid="stSidebar"] {
        background-color: #0B1220;
        border-right: 1px solid #263244;
    }

    /* Main headings */
    h1, h2, h3 {
        color: #FFFFFF !important;
    }

    /* Normal text */
    p, label, span {
        color: #D1D5DB;
    }

    /* KPI cards */
    [data-testid="stMetric"] {
        background-color: #1F2937;
        border: 1px solid #374151;
        border-radius: 12px;
        padding: 18px;
        box-shadow: 0px 4px 12px rgba(0,0,0,0.25);
    }

    /* KPI labels */
    [data-testid="stMetricLabel"] {
        color: #9CA3AF !important;
    }

    /* KPI values */
    [data-testid="stMetricValue"] {
        color: #FFFFFF !important;
        font-weight: 700;
    }

    /* Buttons */
    .stButton > button {
        background-color: #FC8019;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 8px 18px;
        font-weight: 600;
    }

    .stButton > button:hover {
        background-color: #E66F0F;
        color: white;
    }

    /* Select boxes */
    div[data-baseweb="select"] > div {
        background-color: #1F2937;
        border-radius: 8px;
        border: 1px solid #374151;
    }

    /* Text input */
    div[data-baseweb="input"] {
        background-color: #1F2937;
        border-radius: 8px;
    }

    /* Dataframes */
    [data-testid="stDataFrame"] {
        border-radius: 10px;
        overflow: hidden;
    }

    /* Divider */
    hr {
        border-color: #374151;
    }

    /* Captions */
    .stCaption {
        color: #9CA3AF !important;
    }

</style>
""", unsafe_allow_html=True)


# ============================================================
# 3. SNOWFLAKE SESSION
# ============================================================

session = get_active_session()


# ============================================================
# 4. HEADER
# ============================================================

st.title("🍔 Swiggy Revenue Intelligence Platform")
st.caption("Snowflake Data Engineering & Analytics Dashboard")


# ============================================================
# 5. REFRESH BUTTON
# ============================================================

if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()


# ============================================================
# 6. LOAD DATA FROM SNOWFLAKE
# ============================================================

@st.cache_data
def load_data():

    query = """
        SELECT
            F.ORDER_ITEM_ID,
            F.ORDER_ID,
            F.CUSTOMER_DIM_KEY,
            F.CUSTOMER_ADDRESS_DIM_KEY,
            F.RESTAURANT_DIM_KEY,
            F.RESTAURANT_LOCATION_DIM_KEY,
            F.MENU_DIM_KEY,
            F.DELIVERY_AGENT_DIM_KEY,
            F.ORDER_DATE_DIM_KEY,
            F.QUANTITY,
            F.PRICE,
            F.SUBTOTAL,
            F.DELIVERY_STATUS,
            F.ESTIMATED_TIME,

            D.CALENDAR_DATE,
            D.YEAR,
            D.QUARTER,
            D.MONTH,
            D.WEEK,
            D.DAY_OF_YEAR,
            D.DAY_OF_WEEK,
            D.DAY_OF_THE_MONTH,
            D.DAY_NAME,

            R.RESTAURANT_ID,
            R.NAME AS RESTAURANT_NAME,
            R.LOCATION_ID_FK

        FROM SANDBOX.CONSUMPTION_SCH.ORDER_ITEM_FACT F

        LEFT JOIN SANDBOX.CONSUMPTION_SCH.DATE_DIM D
            ON F.ORDER_DATE_DIM_KEY = D.DATE_DIM_HK

        LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM R
            ON F.RESTAURANT_DIM_KEY = R.RESTAURANT_HK

        ORDER BY D.CALENDAR_DATE
    """

    # Execute Snowflake query
    df = session.sql(query).to_pandas()

    return df


# Load dataset
df = load_data()


# ============================================================
# 7. BASIC DATA VALIDATION
# ============================================================

if df.empty:

    st.warning("No data found in ORDER_ITEM_FACT.")

    st.stop()


# Convert important columns
df["CALENDAR_DATE"] = pd.to_datetime(
    df["CALENDAR_DATE"],
    errors="coerce"
)

df["SUBTOTAL"] = pd.to_numeric(
    df["SUBTOTAL"],
    errors="coerce"
).fillna(0)

df["QUANTITY"] = pd.to_numeric(
    df["QUANTITY"],
    errors="coerce"
).fillna(0)


# ============================================================
# 8. SIDEBAR NAVIGATION
# ============================================================

st.sidebar.title("🍔 SWIGGY")

st.sidebar.caption(
    "Revenue Intelligence Platform"
)

page = st.sidebar.radio(
    "Navigation",
    [
        "🏠 Overview",
        "💰 Revenue Analytics",
        "🏪 Restaurant Analytics",
        "🚚 Delivery Analytics",
        "📊 Data Explorer"
    ]
)


# ============================================================
# 9. GLOBAL FILTERS
# ============================================================

st.sidebar.divider()

st.sidebar.subheader("Global Filters")


# Year filter
years = sorted(
    df["YEAR"].dropna().unique().tolist()
)

selected_year = st.sidebar.selectbox(
    "Year",
    ["All"] + years
)


# Month filter
months = sorted(
    df["MONTH"].dropna().unique().tolist()
)

selected_month = st.sidebar.selectbox(
    "Month",
    ["All"] + months
)


# Restaurant filter
restaurants = sorted(
    df["RESTAURANT_NAME"]
    .dropna()
    .unique()
    .tolist()
)

selected_restaurant = st.sidebar.selectbox(
    "Restaurant",
    ["All"] + restaurants
)


# ============================================================
# 10. APPLY FILTERS
# ============================================================

filtered_df = df.copy()


# Apply year filter
if selected_year != "All":

    filtered_df = filtered_df[
        filtered_df["YEAR"] == selected_year
    ]


# Apply month filter
if selected_month != "All":

    filtered_df = filtered_df[
        filtered_df["MONTH"] == selected_month
    ]


# Apply restaurant filter
if selected_restaurant != "All":

    filtered_df = filtered_df[
        filtered_df["RESTAURANT_NAME"] == selected_restaurant
    ]


# ============================================================
# 11. KPI CALCULATIONS
# ============================================================

total_revenue = filtered_df["SUBTOTAL"].sum()

total_orders = filtered_df["ORDER_ID"].nunique()

items_sold = filtered_df["ORDER_ITEM_ID"].nunique()

active_restaurants = filtered_df["RESTAURANT_ID"].nunique()

avg_order_value = (
    total_revenue / total_orders
    if total_orders > 0
    else 0
)

avg_item_value = (
    total_revenue / items_sold
    if items_sold > 0
    else 0
)


# ============================================================
# 12. OVERVIEW PAGE
# ============================================================

if page == "🏠 Overview":

    st.header("📊 Business Overview")

    st.caption(
        "Overall Swiggy revenue and order performance"
    )

    # KPI cards
    col1, col2, col3, col4, col5, col6 = st.columns(6)

    col1.metric(
        "💰 Revenue",
        f"₹{total_revenue:,.0f}"
    )

    col2.metric(
        "🛒 Orders",
        f"{total_orders:,}"
    )

    col3.metric(
        "📦 Items Sold",
        f"{items_sold:,}"
    )

    col4.metric(
        "🏪 Restaurants",
        f"{active_restaurants:,}"
    )

    col5.metric(
        "💵 Avg Order Value",
        f"₹{avg_order_value:,.2f}"
    )

    col6.metric(
        "📦 Avg Item Value",
        f"₹{avg_item_value:,.2f}"
    )

    st.divider()

    # --------------------------------------------------------
    # Revenue Trend
    # --------------------------------------------------------

    st.subheader("📈 Revenue Trend")

    revenue_trend = (
        filtered_df
        .groupby("CALENDAR_DATE")["SUBTOTAL"]
        .sum()
        .reset_index()
    )

    revenue_trend = revenue_trend.set_index(
        "CALENDAR_DATE"
    )

    st.line_chart(
        revenue_trend["SUBTOTAL"]
    )

    # --------------------------------------------------------
    # Revenue by Restaurant
    # --------------------------------------------------------

    st.subheader("🏪 Top Restaurants by Revenue")

    restaurant_revenue = (
        filtered_df
        .groupby("RESTAURANT_NAME")["SUBTOTAL"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    st.bar_chart(
        restaurant_revenue
    )

    # --------------------------------------------------------
    # Orders Trend
    # --------------------------------------------------------

    st.subheader("🛒 Orders Trend")

    orders_trend = (
        filtered_df
        .groupby("CALENDAR_DATE")["ORDER_ID"]
        .nunique()
    )

    st.line_chart(
        orders_trend
    )


# ============================================================
# 13. REVENUE ANALYTICS PAGE
# ============================================================

elif page == "💰 Revenue Analytics":

    st.header("💰 Revenue Analytics")

    st.caption(
        "Detailed analysis of revenue, orders and average order value"
    )

    # KPI row
    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Total Revenue",
        f"₹{total_revenue:,.0f}"
    )

    col2.metric(
        "Total Orders",
        f"{total_orders:,}"
    )

    col3.metric(
        "Average Order Value",
        f"₹{avg_order_value:,.2f}"
    )

    st.divider()

    # --------------------------------------------------------
    # Revenue by Month
    # --------------------------------------------------------

    st.subheader("📅 Revenue by Month")

    monthly_revenue = (
        filtered_df
        .groupby(["YEAR", "MONTH"])["SUBTOTAL"]
        .sum()
        .reset_index()
    )

    monthly_revenue["PERIOD"] = (
        monthly_revenue["YEAR"].astype(str)
        + "-"
        + monthly_revenue["MONTH"].astype(str)
    )

    monthly_chart = monthly_revenue.set_index(
        "PERIOD"
    )["SUBTOTAL"]

    st.bar_chart(
        monthly_chart
    )

    # --------------------------------------------------------
    # Daily Revenue
    # --------------------------------------------------------

    st.subheader("📅 Daily Revenue")

    daily_revenue = (
        filtered_df
        .groupby("CALENDAR_DATE")["SUBTOTAL"]
        .sum()
    )

    st.line_chart(
        daily_revenue
    )

    # --------------------------------------------------------
    # Revenue Table
    # --------------------------------------------------------

    st.subheader("📋 Revenue Summary")

    revenue_summary = (
        filtered_df
        .groupby("CALENDAR_DATE")
        .agg(
            Revenue=("SUBTOTAL", "sum"),
            Orders=("ORDER_ID", "nunique"),
            Items=("ORDER_ITEM_ID", "nunique")
        )
        .reset_index()
    )

    revenue_summary["AOV"] = (
        revenue_summary["Revenue"]
        / revenue_summary["Orders"]
    )

    st.dataframe(
        revenue_summary,
        use_container_width=True
    )


# ============================================================
# 14. RESTAURANT ANALYTICS PAGE
# ============================================================

elif page == "🏪 Restaurant Analytics":

    st.header("🏪 Restaurant Analytics")

    st.caption(
        "Restaurant-level revenue and order performance"
    )

    restaurant_summary = (
        filtered_df
        .groupby(
            [
                "RESTAURANT_ID",
                "RESTAURANT_NAME"
            ]
        )
        .agg(
            Revenue=("SUBTOTAL", "sum"),
            Orders=("ORDER_ID", "nunique"),
            Items=("ORDER_ITEM_ID", "nunique")
        )
        .reset_index()
    )

    restaurant_summary["AOV"] = (
        restaurant_summary["Revenue"]
        / restaurant_summary["Orders"]
    )

    restaurant_summary = restaurant_summary.sort_values(
        "Revenue",
        ascending=False
    )

    # --------------------------------------------------------
    # Restaurant KPIs
    # --------------------------------------------------------

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Restaurants",
        f"{len(restaurant_summary):,}"
    )

    col2.metric(
        "Top Restaurant Revenue",
        (
            f"₹{restaurant_summary.iloc[0]['Revenue']:,.0f}"
            if not restaurant_summary.empty
            else "₹0"
        )
    )

    col3.metric(
        "Average Restaurant Revenue",
        (
            f"₹{restaurant_summary['Revenue'].mean():,.0f}"
            if not restaurant_summary.empty
            else "₹0"
        )
    )

    st.divider()

    # --------------------------------------------------------
    # Top Restaurants
    # --------------------------------------------------------

    st.subheader("🏆 Top 10 Restaurants")

    top_restaurants = (
        restaurant_summary
        .head(10)
        .set_index("RESTAURANT_NAME")["Revenue"]
    )

    st.bar_chart(
        top_restaurants
    )

    # --------------------------------------------------------
    # Restaurant Performance
    # --------------------------------------------------------

    st.subheader("📊 Restaurant Performance")

    st.dataframe(
        restaurant_summary,
        use_container_width=True
    )


# ============================================================
# 15. DELIVERY ANALYTICS PAGE
# ============================================================

elif page == "🚚 Delivery Analytics":

    st.header("🚚 Delivery Analytics")

    st.caption(
        "Delivery status and operational performance"
    )

    # --------------------------------------------------------
    # Delivery KPIs
    # --------------------------------------------------------

    total_deliveries = filtered_df["ORDER_ID"].nunique()

    delivered_orders = filtered_df[
        filtered_df["DELIVERY_STATUS"]
        .astype(str)
        .str.lower()
        .eq("delivered")
    ]["ORDER_ID"].nunique()

    pending_orders = filtered_df[
        filtered_df["DELIVERY_STATUS"]
        .astype(str)
        .str.lower()
        .eq("pending")
    ]["ORDER_ID"].nunique()

    cancelled_orders = filtered_df[
        filtered_df["DELIVERY_STATUS"]
        .astype(str)
        .str.lower()
        .eq("cancelled")
    ]["ORDER_ID"].nunique()

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Total Deliveries",
        f"{total_deliveries:,}"
    )

    col2.metric(
        "Delivered",
        f"{delivered_orders:,}"
    )

    col3.metric(
        "Pending",
        f"{pending_orders:,}"
    )

    col4.metric(
        "Cancelled",
        f"{cancelled_orders:,}"
    )

    st.divider()

    # --------------------------------------------------------
    # Delivery Status
    # --------------------------------------------------------

    st.subheader("🚚 Delivery Status")

    status_data = (
        filtered_df
        .groupby("DELIVERY_STATUS")["ORDER_ID"]
        .nunique()
        .sort_values(ascending=False)
    )

    st.bar_chart(
        status_data
    )

    # --------------------------------------------------------
    # Delivery Status Table
    # --------------------------------------------------------

    st.subheader("📋 Delivery Status Summary")

    status_summary = (
        filtered_df
        .groupby("DELIVERY_STATUS")
        .agg(
            Orders=("ORDER_ID", "nunique"),
            Revenue=("SUBTOTAL", "sum")
        )
        .reset_index()
    )

    st.dataframe(
        status_summary,
        use_container_width=True
    )


# ============================================================
# 16. DATA EXPLORER PAGE
# ============================================================

elif page == "📊 Data Explorer":

    st.header("📊 Data Explorer")

    st.caption(
        "Explore the underlying fact data"
    )

    # Search restaurant
    search_text = st.text_input(
        "🔎 Search Restaurant"
    )

    explorer_df = filtered_df.copy()

    if search_text:

        explorer_df = explorer_df[
            explorer_df["RESTAURANT_NAME"]
            .astype(str)
            .str.contains(
                search_text,
                case=False,
                na=False
            )
        ]

    # Columns to display
    display_columns = [
        "ORDER_ITEM_ID",
        "ORDER_ID",
        "RESTAURANT_ID",
        "RESTAURANT_NAME",
        "QUANTITY",
        "PRICE",
        "SUBTOTAL",
        "DELIVERY_STATUS",
        "CALENDAR_DATE"
    ]

    display_columns = [
        col
        for col in display_columns
        if col in explorer_df.columns
    ]

    st.dataframe(
        explorer_df[display_columns],
        use_container_width=True
    )

    # --------------------------------------------------------
    # Download
    # --------------------------------------------------------

    csv_data = explorer_df[
        display_columns
    ].to_csv(
        index=False
    )

    st.download_button(
        label="⬇️ Download CSV",
        data=csv_data,
        file_name="swiggy_data_export.csv",
        mime="text/csv"
    )


# ============================================================
# 17. FOOTER
# ============================================================

st.divider()

st.caption(
    "🍔 Swiggy Revenue Intelligence Platform | "
    "Powered by Snowflake + Streamlit"
)