#!/bin/bash

# ETL Pipeline Setup Script
echo "🚀 Setting up ETL Pipeline with dbt, Snowflake, and Airflow..."

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed. Please install Python 3 first."
    exit 1
fi

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 is required but not installed. Please install pip3 first."
    exit 1
fi

echo "✅ Python and pip are available"

# Install dbt and dependencies
echo "📦 Installing dbt and dependencies..."
pip3 install dbt-snowflake dbt-utils

# Install dbt packages
echo "📦 Installing dbt packages..."
dbt deps

# Check if Docker is installed (for Airflow)
if command -v docker &> /dev/null; then
    echo "✅ Docker is available"
    
    # Check if docker-compose is installed
    if command -v docker-compose &> /dev/null; then
        echo "✅ Docker Compose is available"
        echo "🐳 You can run 'docker-compose up' to start Airflow"
    else
        echo "⚠️  Docker Compose not found. Install it to use the Docker setup."
    fi
else
    echo "⚠️  Docker not found. You can still use dbt locally."
    echo "📦 To install Airflow locally, run:"
    echo "   pip3 install apache-airflow"
    echo "   pip3 install -r requirements.txt"
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p logs plugins

# Set permissions for Airflow
echo "🔐 Setting up permissions..."
export AIRFLOW_UID=50000

echo ""
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. Update profiles.yml with your Snowflake credentials"
echo "2. Run 'dbt debug' to test your connection"
echo "3. Run 'dbt run' to execute the pipeline"
echo "4. For Airflow:"
echo "   - Copy .env.example to .env and update with your credentials"
echo "   - Run 'docker-compose up' to start Airflow"
echo "   - Access Airflow UI at http://localhost:8080"
echo ""
echo "📚 See README.md for detailed instructions"
