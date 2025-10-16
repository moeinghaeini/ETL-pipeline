FROM apache/airflow:2.8.1

# Install dbt and dependencies
RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake dbt-utils && deactivate

# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy dbt project
COPY . /usr/local/airflow/dags/dbt/data_pipeline

# Set environment variables
ENV AIRFLOW_HOME=/usr/local/airflow
