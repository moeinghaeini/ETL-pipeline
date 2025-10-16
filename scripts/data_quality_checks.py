#!/usr/bin/env python3
"""
Data Quality Monitoring Script using Great Expectations
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import DataContextConfig, FilesystemStoreBackendDefaults

import snowflake.connector
from snowflake.connector import DictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_quality.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DataQualityMonitor:
    """Comprehensive data quality monitoring using Great Expectations"""
    
    def __init__(self):
        self.context = self._setup_great_expectations()
        self.snowflake_conn = self._setup_snowflake_connection()
        
    def _setup_great_expectations(self) -> BaseDataContext:
        """Initialize Great Expectations context"""
        try:
            data_context_config = DataContextConfig(
                config_version=3.0,
                datasources={
                    "snowflake_datasource": {
                        "class_name": "Datasource",
                        "execution_engine": {
                            "class_name": "SqlAlchemyExecutionEngine",
                            "connection_string": self._get_connection_string()
                        },
                        "data_connectors": {
                            "default_runtime_data_connector": {
                                "class_name": "RuntimeDataConnector",
                                "batch_identifiers": ["default_identifier_name"]
                            }
                        }
                    }
                },
                stores={
                    "expectations_store": {
                        "class_name": "ExpectationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": "expectations/"
                        }
                    },
                    "validations_store": {
                        "class_name": "ValidationsStore",
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": "validations/"
                        }
                    },
                    "evaluation_parameter_store": {
                        "class_name": "EvaluationParameterStore"
                    }
                },
                expectations_store_name="expectations_store",
                validations_store_name="validations_store",
                evaluation_parameter_store_name="evaluation_parameter_store",
                data_docs_sites={
                    "local_site": {
                        "class_name": "SiteBuilder",
                        "show_how_to_buttons": True,
                        "store_backend": {
                            "class_name": "TupleFilesystemStoreBackend",
                            "base_directory": "uncommitted/data_docs/local_site/"
                        },
                        "site_index_builder": {
                            "class_name": "DefaultSiteIndexBuilder"
                        }
                    }
                },
                anonymous_usage_statistics={
                    "enabled": False
                }
            )
            
            context = BaseDataContext(project_config=data_context_config)
            logger.info("Great Expectations context initialized successfully")
            return context
            
        except Exception as e:
            logger.error(f"Failed to initialize Great Expectations: {e}")
            raise
    
    def _setup_snowflake_connection(self):
        """Setup Snowflake connection"""
        try:
            conn = snowflake.connector.connect(
                user=os.getenv('SNOWFLAKE_USER'),
                password=os.getenv('SNOWFLAKE_PASSWORD'),
                account=os.getenv('SNOWFLAKE_ACCOUNT'),
                warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
                database=os.getenv('SNOWFLAKE_DATABASE'),
                schema=os.getenv('SNOWFLAKE_SCHEMA'),
                role='dbt_role'
            )
            logger.info("Snowflake connection established successfully")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def _get_connection_string(self) -> str:
        """Get Snowflake connection string for Great Expectations"""
        return (
            f"snowflake://{os.getenv('SNOWFLAKE_USER')}:{os.getenv('SNOWFLAKE_PASSWORD')}"
            f"@{os.getenv('SNOWFLAKE_ACCOUNT')}/{os.getenv('SNOWFLAKE_DATABASE')}"
            f"/{os.getenv('SNOWFLAKE_SCHEMA')}?warehouse={os.getenv('SNOWFLAKE_WAREHOUSE')}"
            f"&role=dbt_role"
        )
    
    def check_data_freshness(self) -> Dict[str, Any]:
        """Check data freshness for all tables"""
        logger.info("Checking data freshness...")
        
        freshness_checks = {
            'stg_tpch_orders': {
                'table': 'stg_tpch_orders',
                'date_column': 'order_date',
                'max_delay_hours': 24
            },
            'fct_orders': {
                'table': 'fct_orders', 
                'date_column': 'order_date',
                'max_delay_hours': 25
            }
        }
        
        results = {}
        
        for check_name, config in freshness_checks.items():
            try:
                query = f"""
                SELECT MAX({config['date_column']}) as latest_date,
                       DATEDIFF('hour', MAX({config['date_column']}), CURRENT_TIMESTAMP()) as hours_old
                FROM {config['table']}
                """
                
                cursor = self.snowflake_conn.cursor(DictCursor)
                cursor.execute(query)
                result = cursor.fetchone()
                
                is_fresh = result['hours_old'] <= config['max_delay_hours']
                
                results[check_name] = {
                    'latest_date': str(result['latest_date']),
                    'hours_old': result['hours_old'],
                    'max_delay_hours': config['max_delay_hours'],
                    'is_fresh': is_fresh,
                    'status': 'PASS' if is_fresh else 'FAIL'
                }
                
                logger.info(f"{check_name}: {'PASS' if is_fresh else 'FAIL'} - "
                          f"Latest data: {result['latest_date']}, "
                          f"Age: {result['hours_old']} hours")
                
            except Exception as e:
                logger.error(f"Failed to check freshness for {check_name}: {e}")
                results[check_name] = {
                    'status': 'ERROR',
                    'error': str(e)
                }
        
        return results
    
    def check_data_volume(self) -> Dict[str, Any]:
        """Check data volume anomalies"""
        logger.info("Checking data volume...")
        
        volume_checks = {
            'stg_tpch_orders': {
                'table': 'stg_tpch_orders',
                'expected_min_rows': 1000,
                'expected_max_rows': 1000000
            },
            'fct_orders': {
                'table': 'fct_orders',
                'expected_min_rows': 1000,
                'expected_max_rows': 1000000
            }
        }
        
        results = {}
        
        for check_name, config in volume_checks.items():
            try:
                query = f"SELECT COUNT(*) as row_count FROM {config['table']}"
                
                cursor = self.snowflake_conn.cursor(DictCursor)
                cursor.execute(query)
                result = cursor.fetchone()
                
                row_count = result['row_count']
                is_valid = config['expected_min_rows'] <= row_count <= config['expected_max_rows']
                
                results[check_name] = {
                    'row_count': row_count,
                    'expected_min': config['expected_min_rows'],
                    'expected_max': config['expected_max_rows'],
                    'is_valid': is_valid,
                    'status': 'PASS' if is_valid else 'FAIL'
                }
                
                logger.info(f"{check_name}: {'PASS' if is_valid else 'FAIL'} - "
                          f"Row count: {row_count}")
                
            except Exception as e:
                logger.error(f"Failed to check volume for {check_name}: {e}")
                results[check_name] = {
                    'status': 'ERROR',
                    'error': str(e)
                }
        
        return results
    
    def check_data_quality_with_great_expectations(self) -> Dict[str, Any]:
        """Run comprehensive data quality checks using Great Expectations"""
        logger.info("Running Great Expectations data quality checks...")
        
        # Define data quality checks for each table
        quality_checks = {
            'fct_orders': [
                {
                    'expectation': 'expect_column_to_exist',
                    'column': 'order_key',
                    'kwargs': {}
                },
                {
                    'expectation': 'expect_column_values_to_not_be_null',
                    'column': 'order_key',
                    'kwargs': {}
                },
                {
                    'expectation': 'expect_column_values_to_be_unique',
                    'column': 'order_key',
                    'kwargs': {}
                },
                {
                    'expectation': 'expect_column_values_to_be_in_set',
                    'column': 'status_code',
                    'kwargs': {'value_set': ['P', 'O', 'F']}
                },
                {
                    'expectation': 'expect_column_values_to_be_between',
                    'column': 'total_price',
                    'kwargs': {'min_value': 0, 'max_value': 1000000}
                }
            ]
        }
        
        results = {}
        
        for table_name, expectations in quality_checks.items():
            try:
                # Create batch request
                batch_request = RuntimeBatchRequest(
                    datasource_name="snowflake_datasource",
                    data_connector_name="default_runtime_data_connector",
                    data_asset_name=table_name,
                    runtime_parameters={
                        "query": f"SELECT * FROM {table_name} LIMIT 10000"
                    },
                    batch_identifiers={"default_identifier_name": "default_identifier"}
                )
                
                # Create validator
                validator = self.context.get_validator(
                    batch_request=batch_request,
                    expectation_suite_name=f"{table_name}_suite"
                )
                
                # Add expectations
                for expectation_config in expectations:
                    expectation_method = getattr(validator, expectation_config['expectation'])
                    expectation_method(
                        column=expectation_config['column'],
                        **expectation_config['kwargs']
                    )
                
                # Run validation
                validation_result = validator.validate()
                
                results[table_name] = {
                    'success': validation_result.success,
                    'statistics': validation_result.statistics,
                    'results': [
                        {
                            'expectation_type': result.expectation_config.expectation_type,
                            'success': result.success,
                            'result': result.result
                        }
                        for result in validation_result.results
                    ]
                }
                
                logger.info(f"{table_name}: {'PASS' if validation_result.success else 'FAIL'}")
                
            except Exception as e:
                logger.error(f"Failed to run quality checks for {table_name}: {e}")
                results[table_name] = {
                    'success': False,
                    'error': str(e)
                }
        
        return results
    
    def run_all_checks(self) -> Dict[str, Any]:
        """Run all data quality checks"""
        logger.info("Starting comprehensive data quality monitoring...")
        
        start_time = datetime.now()
        
        results = {
            'timestamp': start_time.isoformat(),
            'freshness': self.check_data_freshness(),
            'volume': self.check_data_volume(),
            'quality': self.check_data_quality_with_great_expectations()
        }
        
        # Calculate overall status
        all_checks_passed = True
        for category in ['freshness', 'volume', 'quality']:
            for check_name, check_result in results[category].items():
                if check_result.get('status') == 'FAIL' or not check_result.get('success', True):
                    all_checks_passed = False
                    break
        
        results['overall_status'] = 'PASS' if all_checks_passed else 'FAIL'
        results['execution_time_seconds'] = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Data quality monitoring completed. Overall status: {results['overall_status']}")
        
        return results
    
    def close_connections(self):
        """Close database connections"""
        if self.snowflake_conn:
            self.snowflake_conn.close()
            logger.info("Snowflake connection closed")

def main():
    """Main execution function"""
    try:
        monitor = DataQualityMonitor()
        results = monitor.run_all_checks()
        
        # Save results to file
        import json
        with open('logs/data_quality_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        # Exit with error code if any checks failed
        if results['overall_status'] == 'FAIL':
            logger.error("Data quality checks failed!")
            sys.exit(1)
        else:
            logger.info("All data quality checks passed!")
            sys.exit(0)
            
    except Exception as e:
        logger.error(f"Data quality monitoring failed: {e}")
        sys.exit(1)
    finally:
        if 'monitor' in locals():
            monitor.close_connections()

if __name__ == "__main__":
    main()
