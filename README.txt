# Swiggy End-to-End Data Engineering & Revenue Intelligence Platform

![Snowflake](https://img.shields.io/badge/Snowflake-Data%20Warehouse-29B5E8?logo=snowflake)
![Python](https://img.shields.io/badge/Python-Development-3776AB?logo=python)
![SQL](https://img.shields.io/badge/SQL-Data%20Transformation-4479A1)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?logo=streamlit)

## Project Overview

The **Swiggy End-to-End Data Engineering & Revenue Intelligence Platform** is a data engineering and analytics project designed to organise, transform and analyse food delivery data using Snowflake, SQL, Python and Streamlit.

The project focuses on building a structured data warehouse, processing multiple business entities, managing transactional data and generating business insights through an interactive analytics dashboard.

## Technologies Used

| Technology | Purpose |
|---|---|
| Snowflake | Cloud Data Warehouse |
| SQL | Data transformation and querying |
| Python | Application development |
| Streamlit | Interactive dashboard |
| Snowpark | Snowflake-Python integration |

## Project Architecture

The project follows a layered data warehouse architecture.

### 1. Staging Layer
- Database and schema setup.
- File format and staging configuration.
- Loading source CSV datasets.
- Preparing raw data for transformation.

### 2. Clean Layer
- Restaurant entity processing.
- Customer entity processing.
- Customer address processing.
- Menu and delivery agent processing.
- Order and delivery transaction processing.
- Data transformation and cleaning.

### 3. Consumption Layer
- Date dimension.
- Order item fact table.
- KPI views.
- Analytics-ready data for business reporting.

## Key Components

- **Data Warehouse Development:** Structured database and schema organisation using Snowflake.
- **Data Transformation:** SQL-based processing of restaurant, customer, menu, order and delivery data.
- **Data Modelling:** Fact and dimension table development.
- **Business Intelligence:** KPI views and revenue analytics.
- **Dashboard Development:** Interactive Streamlit application for exploring business performance.

## Project Structure

```text
Swiggy-Data-Engineering/
│
├── Food Aggregator Scripts/
│   ├── 01CreateDB+Schema.sql
│   ├── 02LocationEntity.sql
│   ├── 03RestaurantEntity.sql
│   ├── 04CustomerEntity.sql
│   ├── 05CustomerAddressEntity.sql
│   ├── 06MenuEntity.sql
│   ├── 07DeliveryAgent.sql
│   ├── 08DeliveryEntityTX.sql
│   ├── 09OrderEntityTX.sql
│   ├── 10OrderItemEntityTX.sql
│   ├── 11DateDim.sql
│   ├── 12OrderItemFact.sql
│   └── 13KpiView.sql
│
├── streamlit/
│   ├── streamlit_app.py
│   └── .streamlit/
│       └── config.toml
│
├── pyproject.toml
├── snowflake.yml
└── README.md
```

## Business Analytics

The platform is designed to support analysis of:

- Revenue performance.
- Order trends.
- Restaurant performance.
- Customer activity.
- Food delivery operations.
- Business KPIs.

## How to Run

1. Clone the repository.
2. Configure your Snowflake connection securely.
3. Execute the SQL scripts in the appropriate order.
4. Configure the required Python dependencies.
5. Run the Streamlit application.

```bash
git clone https://github.com/yourusername/Swiggy-Data-Engineering.git

cd Swiggy-Data-Engineering

pip install -r requirements.txt

streamlit run streamlit_app.py
```

*Note: Update the setup commands and configuration according to your final repository structure and dependency files.*

## Project Objective

The objective of this project is to demonstrate practical understanding of cloud data warehousing, SQL-based data processing, data modelling and business intelligence using modern data engineering technologies.

## Author

**Saurav Baluni**

Aspiring Data Engineer | Snowflake | Python | SQL | Microsoft Fabric

GitHub: https://github.com/yourusername

---

*This project is developed for educational and portfolio purposes.*
