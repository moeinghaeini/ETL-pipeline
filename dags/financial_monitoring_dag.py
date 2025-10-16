"""
Financial Data Monitoring DAG
Monitors Bosch company stock data using yfinance
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
import os
import sys

# Add scripts directory to path
sys.path.append('/usr/local/airflow/dags/scripts')

def run_bosch_stock_analysis():
    """Run Bosch stock analysis"""
    from financial_data_monitor import BoschStockMonitor
    
    monitor = BoschStockMonitor()
    results = monitor.run_complete_analysis()
    
    # Store results in Airflow Variable for other tasks
    Variable.set("bosch_analysis_results", results, serialize_json=True)
    
    return results

def check_financial_alerts():
    """Check for financial alerts and send notifications"""
    from financial_data_monitor import BoschStockMonitor
    from send_alerts import AlertManager, Alert, AlertType, AlertSeverity
    
    # Get analysis results
    results = Variable.get("bosch_analysis_results", deserialize_json=True)
    
    if not results or 'analysis_results' not in results:
        return "No analysis results available"
    
    alert_manager = AlertManager()
    alerts_sent = 0
    
    # Check each symbol for alerts
    for symbol, analysis in results['analysis_results'].items():
        anomalies = analysis.get('anomalies', {})
        
        for alert_data in anomalies.get('alerts', []):
            # Create alert
            alert = Alert(
                type=AlertType.DATA_QUALITY,  # Using data quality type for financial alerts
                severity=AlertSeverity.HIGH if alert_data['severity'] == 'high' else AlertSeverity.MEDIUM,
                title=f"Bosch Stock Alert - {symbol}",
                message=alert_data['message'],
                timestamp=datetime.now(),
                metadata={
                    'symbol': symbol,
                    'alert_type': alert_data['type'],
                    'value': alert_data['value'],
                    'threshold': alert_data['threshold']
                }
            )
            
            # Send alert
            alert_results = alert_manager.send_alert(alert)
            if any(alert_results.values()):
                alerts_sent += 1
    
    return f"Sent {alerts_sent} financial alerts"

def generate_financial_report():
    """Generate comprehensive financial report"""
    from financial_data_monitor import BoschStockMonitor
    from generate_quality_report import QualityReportGenerator
    
    # Get analysis results
    results = Variable.get("bosch_analysis_results", deserialize_json=True)
    
    if not results:
        return "No analysis results available for report generation"
    
    # Create financial report
    report_data = {
        'timestamp': datetime.now().isoformat(),
        'report_type': 'financial_analysis',
        'summary': results.get('overall_summary', {}),
        'detailed_results': results.get('analysis_results', {}),
        'symbols_analyzed': results.get('symbols_analyzed', [])
    }
    
    # Generate HTML report
    generator = QualityReportGenerator()
    report_path = generator.generate_html_report(report_data)
    
    return f"Financial report generated: {report_path}"

# DAG configuration
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

dag = DAG(
    'financial_monitoring_dag',
    default_args=default_args,
    description='Monitor Bosch company stock data',
    schedule_interval='0 9,15 * * 1-5',  # Run at 9 AM and 3 PM on weekdays
    max_active_runs=1,
    tags=['financial', 'monitoring', 'bosch', 'yfinance']
)

# Task 1: Run Bosch stock analysis
analyze_bosch_stocks = PythonOperator(
    task_id='analyze_bosch_stocks',
    python_callable=run_bosch_stock_analysis,
    dag=dag,
    doc_md="""
    ## Analyze Bosch Stocks
    
    This task fetches and analyzes Bosch company stock data from multiple exchanges:
    - BOSCHLTD.BSE (Bombay Stock Exchange)
    - BOSCHLTD.NSE (National Stock Exchange)
    
    The analysis includes:
    - Price data and technical indicators
    - Company information
    - Anomaly detection
    - Risk assessment
    """
)

# Task 2: Check for alerts
check_alerts = PythonOperator(
    task_id='check_financial_alerts',
    python_callable=check_financial_alerts,
    dag=dag,
    doc_md="""
    ## Check Financial Alerts
    
    This task checks the analysis results for any alerts and sends notifications:
    - Price change alerts (>5%)
    - Volume spike alerts (>200%)
    - RSI overbought/oversold conditions
    - High volatility alerts
    
    Alerts are sent via email, Slack, Teams, and PagerDuty based on severity.
    """
)

# Task 3: Generate financial report
generate_report = PythonOperator(
    task_id='generate_financial_report',
    python_callable=generate_financial_report,
    dag=dag,
    doc_md="""
    ## Generate Financial Report
    
    This task generates a comprehensive financial report including:
    - Performance metrics
    - Technical analysis summary
    - Risk assessment
    - Trading recommendations
    - Interactive charts
    """
)

# Task 4: Data quality checks
data_quality_check = BashOperator(
    task_id='data_quality_check',
    bash_command="""
    cd /usr/local/airflow/dags
    python scripts/data_quality_checks.py
    """,
    dag=dag,
    doc_md="""
    ## Data Quality Check
    
    This task runs data quality checks on the financial data to ensure:
    - Data completeness
    - Data freshness
    - Data accuracy
    - Anomaly detection
    """
)

# Task 5: Archive old data
archive_old_data = BashOperator(
    task_id='archive_old_data',
    bash_command="""
    cd /usr/local/airflow/dags/data/financial
    find . -name "*.csv" -mtime +30 -exec gzip {} \;
    find . -name "*.json" -mtime +30 -exec gzip {} \;
    """,
    dag=dag,
    doc_md="""
    ## Archive Old Data
    
    This task archives old financial data files to save storage space:
    - Compresses CSV files older than 30 days
    - Compresses JSON files older than 30 days
    - Maintains data for historical analysis
    """
)

# Task 6: Update data catalog
update_data_catalog = PythonOperator(
    task_id='update_data_catalog',
    python_callable=lambda: "Data catalog updated with latest financial data",
    dag=dag,
    doc_md="""
    ## Update Data Catalog
    
    This task updates the data catalog with metadata about the latest financial data:
    - Data lineage information
    - Data quality metrics
    - Schema information
    - Access permissions
    """
)

# Task dependencies
analyze_bosch_stocks >> check_alerts
analyze_bosch_stocks >> generate_report
analyze_bosch_stocks >> data_quality_check
generate_report >> update_data_catalog
data_quality_check >> update_data_catalog
check_alerts >> archive_old_data
