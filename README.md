```markdown
# Dispensary On-Hands & Sales Data App

## Overview
The Dispensary On-Hands & Sales Data App is a web application built with Streamlit to manage and visualize inventory and sales data from various dispensaries. The app fetches data from both dynamic inventory sources and the Typesense API, processes it, and provides users with interactive dashboards and filtering options.

## Tech Stack
- **Streamlit**: Framework for building interactive web applications directly from Python scripts.
- **Python**: The primary programming language used in this project.
- **Pandas**: Library for data manipulation and analysis.
- **SQLAlchemy**: SQL toolkit and ORM library for interacting with PostgreSQL.
- **PostgreSQL**: Relational database management system for storing and retrieving data.
- **Plotly**: Graphing library for creating interactive visualizations.
- **psycopg2**: PostgreSQL adapter for Python.

## Installation and Setup

### 1. Clone the Repository
```bash
git clone https://github.com/your-repo/dispensary-sales-app.git
cd dispensary-sales-app
```

### 2. Set Up Python Environment
- Create a virtual environment and install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate  # For Linux/Mac
# venv\Scripts\activate  # For Windows
pip install -r requirements.txt
```

### 3. Configure the Database
- Ensure your PostgreSQL database is running.
- Create a `secrets.toml` file in the root directory with your database credentials:
```toml
dbname = "your_db_name"
user = "your_db_user"
password = "your_db_password"
host = "your_db_host"
port = "your_db_port"
```

### 4. Run the Scripts
- Fetch and store Typesense data:
```bash
python3 newTypesense.py
```
- Clean and standardize product names:
```bash
python3 cleanNames.py
```
- Update the dynamic inventory data:
```bash
python3 dynamicInventoryNames.py
```
- Launch the Streamlit web application:
```bash
streamlit run newstreamlit.py
```

## Usage
- **Fetch ALL**: Retrieves all inventory and sales data from both dynamic inventory and Typesense.
- **Fetch TGC**: Retrieves data related to "The Grower Circle" brand.
- **Filtering**: Use the filters (Brand, Dispensary, Category, Subcategory) to narrow down the data displayed.
- **Dashboard**: View top-selling products and performance metrics over different time periods.

## Troubleshooting
- **Missing Columns**: Ensure the database schema matches the expected structure.
- **Data Not Updating**: Verify that the data-fetching scripts have run successfully.
- **Streamlit Errors**: Check for dependency issues or port conflicts, and verify your Python environment.

## License
[MIT License](LICENSE)

## Contributing
Contributions are welcome! Please fork this repository and submit a pull request with your changes.

## Contact
For questions or support, please contact [your-email@example.com](mailto:your-email@example.com).
```
