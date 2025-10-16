# ETL Pipeline with dbt, Snowflake, and Airflow

This project implements a complete ETL pipeline using dbt for transformations, Snowflake as the data warehouse, and Airflow for orchestration. The pipeline processes TPC-H sample data to create a dimensional data model.

## Architecture

```
Raw Data (Snowflake Sample Data) → dbt Transformations → Data Marts → Airflow Orchestration
```

## Project Structure

```
├── models/
│   ├── staging/           # Raw data transformations
│   │   ├── tpch_sources.yml
│   │   ├── stg_tpch_orders.sql
│   │   └── stg_tpch_line_items.sql
│   └── marts/             # Business logic models
│       ├── int_order_items.sql
│       ├── int_order_items_summary.sql
│       ├── fct_orders.sql
│       └── generic_tests.yml
├── macros/                # Reusable SQL functions
│   └── pricing.sql
├── tests/                 # Data quality tests
│   ├── fct_orders_discount.sql
│   └── fct_orders_date_valid.sql
├── dags/                  # Airflow DAGs
│   └── dbt_dag.py
├── dbt_project.yml        # dbt configuration
├── profiles.yml           # Database connection config
├── packages.yml           # dbt packages
├── requirements.txt       # Python dependencies
└── Dockerfile            # Container configuration
```

## Setup Instructions

### 1. Snowflake Setup

First, set up your Snowflake environment:

```sql
-- Create accounts and resources
use role accountadmin;

create warehouse dbt_wh with warehouse_size='x-small';
create database if not exists dbt_db;
create role if not exists dbt_role;

-- Grant permissions
grant role dbt_role to user <your_username>;
grant usage on warehouse dbt_wh to role dbt_role;
grant all on database dbt_db to role dbt_role;

use role dbt_role;
create schema if not exists dbt_db.dbt_schema;
```

### 2. Configure dbt

Update the `profiles.yml` file with your Snowflake credentials:

```yaml
snowflake_workshop:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account_locator>-<your_account_name>
      user: <your_username>
      password: <your_password>
      role: dbt_role
      database: dbt_db
      warehouse: dbt_wh
      schema: dbt_schema
      threads: 4
```

### 3. Install Dependencies

```bash
# Install dbt and dependencies
pip install dbt-snowflake dbt-utils

# Install dbt packages
dbt deps
```

### 4. Run dbt Models

```bash
# Test connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

### 5. Airflow Setup

#### Option A: Local Development

```bash
# Install Airflow
pip install apache-airflow

# Install additional packages
pip install -r requirements.txt

# Initialize Airflow database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com

# Start Airflow webserver
airflow webserver --port 8080

# Start Airflow scheduler (in another terminal)
airflow scheduler
```

#### Option B: Docker

```bash
# Build the Docker image
docker build -t etl-pipeline .

# Run with docker-compose (create docker-compose.yml)
docker-compose up
```

### 6. Configure Airflow Connection

In the Airflow UI (http://localhost:8080), create a new connection:

- **Connection Id**: `snowflake_conn`
- **Connection Type**: `Snowflake`
- **Host**: `<account_locator>-<account_name>.snowflakecomputing.com`
- **Schema**: `dbt_schema`
- **Login**: `<your_username>`
- **Password**: `<your_password>`
- **Extra**: 
```json
{
  "account": "<account_locator>-<account_name>",
  "warehouse": "dbt_wh",
  "database": "dbt_db",
  "role": "dbt_role",
  "insecure_mode": false
}
```

## Data Model

### Staging Layer
- **stg_tpch_orders**: Cleaned order data with standardized column names
- **stg_tpch_line_items**: Cleaned line item data with surrogate keys

### Intermediate Layer
- **int_order_items**: Joined orders and line items with calculated discount amounts
- **int_order_items_summary**: Aggregated order-level metrics

### Marts Layer
- **fct_orders**: Final fact table with order and item-level metrics

## Key Features

### 1. Data Quality Tests
- **Generic Tests**: Unique, not null, relationships, accepted values
- **Singular Tests**: Custom business logic validation

### 2. Macros
- **discounted_amount**: Reusable function for calculating discount amounts

### 3. Orchestration
- **Airflow DAG**: Daily execution with dependency management
- **Cosmos Integration**: Native dbt integration with Airflow

## Usage

### Running the Pipeline

1. **Manual Execution**:
   ```bash
   dbt run --select staging
   dbt run --select marts
   dbt test
   ```

2. **Airflow Execution**:
   - Enable the `dbt_dag` in Airflow UI
   - Monitor execution in the Airflow dashboard

### Monitoring

- **dbt**: Use `dbt docs serve` to view model lineage and documentation
- **Airflow**: Monitor DAG runs, task status, and logs in the Airflow UI

## Cleanup

To clean up Snowflake resources:

```sql
use role accountadmin;
drop warehouse if exists dbt_wh;
drop database if exists dbt_db;
drop role if exists dbt_role;
```

## Troubleshooting

### Common Issues

1. **Connection Issues**: Verify Snowflake credentials and network access
2. **Permission Errors**: Ensure the dbt_role has proper permissions
3. **Package Dependencies**: Run `dbt deps` to install required packages
4. **Airflow Connection**: Verify the Snowflake connection configuration

### Useful Commands

```bash
# Debug dbt configuration
dbt debug

# Check model compilation
dbt compile

# Run specific models
dbt run --select stg_tpch_orders

# Run tests for specific models
dbt test --select fct_orders

# Generate and serve documentation
dbt docs generate && dbt docs serve
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License.
dbt, Snowflake, Airflow
