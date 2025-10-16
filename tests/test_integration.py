#!/usr/bin/env python3
"""
Integration Tests for ETL Pipeline
Tests the complete pipeline end-to-end functionality
"""

import os
import sys
import pytest
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
import subprocess
import json
import time

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.environment_manager import EnvironmentManager
from scripts.data_quality_checks import DataQualityMonitor
from scripts.secrets_manager import SecretsManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestETLPipelineIntegration:
    """Integration tests for the complete ETL pipeline"""
    
    @pytest.fixture(autouse=True)
    def setup_test_environment(self):
        """Setup test environment before each test"""
        self.test_env = "testing"
        self.env_manager = EnvironmentManager()
        self.secrets_manager = SecretsManager()
        
        # Ensure test environment is properly configured
        assert self.env_manager.validate_environment(self.test_env), \
            f"Test environment '{self.test_env}' is not properly configured"
    
    def test_environment_validation(self):
        """Test that all environments are properly configured"""
        environments = ["development", "staging", "production", "testing"]
        
        for env in environments:
            config = self.env_manager.get_environment_config(env)
            assert config, f"Environment '{env}' configuration not found"
            
            # Check required configuration sections
            required_sections = ["dbt", "snowflake", "monitoring"]
            for section in required_sections:
                assert section in config, f"Missing '{section}' section in '{env}' environment"
    
    def test_dbt_project_structure(self):
        """Test that dbt project has correct structure"""
        required_files = [
            "dbt_project.yml",
            "packages.yml",
            "models/staging/stg_tpch_orders.sql",
            "models/staging/stg_tpch_line_items.sql",
            "models/marts/fct_orders.sql",
            "models/marts/int_order_items.sql",
            "models/marts/int_order_items_summary.sql",
            "macros/pricing.sql",
            "tests/fct_orders_discount.sql",
            "tests/fct_orders_date_valid.sql"
        ]
        
        for file_path in required_files:
            assert os.path.exists(file_path), f"Required file not found: {file_path}"
    
    def test_dbt_compilation(self):
        """Test that dbt models compile successfully"""
        result = self.env_manager.run_dbt_command("compile", self.test_env)
        assert result, "dbt compilation failed"
    
    def test_dbt_dependencies(self):
        """Test that dbt dependencies can be installed"""
        result = self.env_manager.run_dbt_command("deps", self.test_env)
        assert result, "dbt dependencies installation failed"
    
    def test_dbt_models_run(self):
        """Test that dbt models can be executed"""
        # First compile and install deps
        assert self.env_manager.run_dbt_command("deps", self.test_env)
        assert self.env_manager.run_dbt_command("compile", self.test_env)
        
        # Then run models
        result = self.env_manager.run_dbt_command("run", self.test_env)
        assert result, "dbt models execution failed"
    
    def test_dbt_tests_pass(self):
        """Test that all dbt tests pass"""
        # Ensure models are run first
        assert self.env_manager.run_dbt_command("run", self.test_env)
        
        # Run tests
        result = self.env_manager.run_dbt_command("test", self.test_env)
        assert result, "dbt tests failed"
    
    def test_data_quality_monitoring(self):
        """Test data quality monitoring functionality"""
        monitor = DataQualityMonitor()
        
        # Test data freshness checks
        freshness_results = monitor.check_data_freshness()
        assert isinstance(freshness_results, dict), "Freshness check should return dictionary"
        
        # Test data volume checks
        volume_results = monitor.check_data_volume()
        assert isinstance(volume_results, dict), "Volume check should return dictionary"
        
        # Test Great Expectations integration
        quality_results = monitor.check_data_quality_with_great_expectations()
        assert isinstance(quality_results, dict), "Quality check should return dictionary"
        
        monitor.close_connections()
    
    def test_secrets_management(self):
        """Test secrets management functionality"""
        test_key = "test_secret_key"
        test_value = "test_secret_value"
        test_env = "testing"
        
        # Test storing secret
        assert self.secrets_manager.store_secret(test_key, test_value, test_env)
        
        # Test retrieving secret
        retrieved_value = self.secrets_manager.retrieve_secret(test_key, test_env)
        assert retrieved_value == test_value, "Retrieved secret value doesn't match stored value"
        
        # Test deleting secret
        assert self.secrets_manager.delete_secret(test_key, test_env)
        
        # Verify secret is deleted
        deleted_value = self.secrets_manager.retrieve_secret(test_key, test_env)
        assert deleted_value is None, "Secret should be deleted"
    
    def test_airflow_dag_structure(self):
        """Test that Airflow DAG is properly structured"""
        dag_file = "dags/dbt_dag.py"
        assert os.path.exists(dag_file), "Airflow DAG file not found"
        
        # Check that DAG file can be imported
        try:
            import importlib.util
            spec = importlib.util.spec_from_file_location("dbt_dag", dag_file)
            dbt_dag_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(dbt_dag_module)
            
            # Check that DAG object exists
            assert hasattr(dbt_dag_module, 'dbt_snowflake_dag'), "DAG object not found in module"
            
        except Exception as e:
            pytest.fail(f"Failed to import DAG file: {e}")
    
    def test_docker_compose_configuration(self):
        """Test Docker Compose configuration"""
        docker_compose_file = "docker-compose.yml"
        assert os.path.exists(docker_compose_file), "Docker Compose file not found"
        
        # Check that docker-compose file is valid YAML
        try:
            import yaml
            with open(docker_compose_file, 'r') as f:
                docker_config = yaml.safe_load(f)
            
            # Check required services
            required_services = ["postgres", "airflow-webserver", "airflow-scheduler"]
            for service in required_services:
                assert service in docker_config.get('services', {}), \
                    f"Required service '{service}' not found in Docker Compose"
                    
        except Exception as e:
            pytest.fail(f"Docker Compose file is invalid: {e}")
    
    def test_environment_deployment_script(self):
        """Test environment deployment script generation"""
        script_path = self.env_manager.create_environment_script(self.test_env)
        assert script_path, "Failed to create deployment script"
        assert os.path.exists(script_path), "Deployment script file not created"
        
        # Check script permissions
        assert os.access(script_path, os.X_OK), "Deployment script is not executable"
        
        # Clean up
        os.remove(script_path)
    
    def test_pipeline_performance(self):
        """Test pipeline performance benchmarks"""
        start_time = time.time()
        
        # Run complete pipeline
        assert self.env_manager.run_dbt_command("deps", self.test_env)
        assert self.env_manager.run_dbt_command("run", self.test_env)
        assert self.env_manager.run_dbt_command("test", self.test_env)
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Performance threshold: pipeline should complete within 5 minutes
        max_execution_time = 300  # 5 minutes
        assert execution_time < max_execution_time, \
            f"Pipeline execution time ({execution_time:.2f}s) exceeds threshold ({max_execution_time}s)"
        
        logger.info(f"Pipeline execution time: {execution_time:.2f} seconds")
    
    def test_data_consistency_across_environments(self):
        """Test data consistency across different environments"""
        environments = ["development", "staging"]
        
        for env in environments:
            if not self.env_manager.validate_environment(env):
                pytest.skip(f"Environment '{env}' not configured, skipping consistency test")
            
            # Run models in each environment
            assert self.env_manager.run_dbt_command("run", env), \
                f"Failed to run models in {env} environment"
            
            # Run tests to ensure data quality
            assert self.env_manager.run_dbt_command("test", env), \
                f"Tests failed in {env} environment"
    
    def test_error_handling_and_recovery(self):
        """Test error handling and recovery mechanisms"""
        # Test with invalid dbt command
        result = self.env_manager.run_dbt_command("invalid_command", self.test_env)
        assert not result, "Invalid command should fail"
        
        # Test environment validation with missing configuration
        original_env = os.environ.get('ENVIRONMENT')
        os.environ['ENVIRONMENT'] = 'nonexistent'
        
        try:
            env_manager = EnvironmentManager()
            config = env_manager.get_environment_config('nonexistent')
            assert not config, "Nonexistent environment should return empty config"
        finally:
            if original_env:
                os.environ['ENVIRONMENT'] = original_env
            else:
                os.environ.pop('ENVIRONMENT', None)
    
    def test_monitoring_and_alerting_integration(self):
        """Test monitoring and alerting system integration"""
        from scripts.send_alerts import AlertManager, Alert, AlertType, AlertSeverity
        
        alert_manager = AlertManager()
        
        # Create test alert
        test_alert = Alert(
            type=AlertType.DATA_QUALITY,
            severity=AlertSeverity.LOW,
            title="Integration Test Alert",
            message="This is a test alert for integration testing",
            timestamp=datetime.now()
        )
        
        # Test alert creation (don't actually send)
        assert test_alert.type == AlertType.DATA_QUALITY
        assert test_alert.severity == AlertSeverity.LOW
        assert test_alert.title == "Integration Test Alert"
    
    def test_data_lineage_tracking(self):
        """Test data lineage tracking functionality"""
        # This would typically test lineage tracking tools
        # For now, we'll test that lineage documentation can be generated
        result = self.env_manager.run_dbt_command("docs generate", self.test_env)
        assert result, "Failed to generate dbt documentation (lineage)"
        
        # Check that docs were generated
        docs_dir = "target"
        assert os.path.exists(docs_dir), "Documentation directory not created"
    
    def test_backup_and_recovery(self):
        """Test backup and recovery procedures"""
        # Test that we can export environment configuration
        env_config = self.env_manager.get_environment_config(self.test_env)
        assert env_config, "Failed to get environment configuration for backup"
        
        # Test that we can create environment file
        env_file = f".env.{self.test_env}.test"
        result = self.secrets_manager.create_env_file(self.test_env, env_file)
        assert result, "Failed to create environment file for backup"
        
        # Clean up
        if os.path.exists(env_file):
            os.remove(env_file)

class TestDataQualityIntegration:
    """Integration tests specifically for data quality"""
    
    def test_data_freshness_validation(self):
        """Test data freshness validation across the pipeline"""
        monitor = DataQualityMonitor()
        
        try:
            freshness_results = monitor.check_data_freshness()
            
            # Check that all tables have freshness data
            expected_tables = ['stg_tpch_orders', 'fct_orders']
            for table in expected_tables:
                assert table in freshness_results, f"Freshness check missing for {table}"
                
                table_result = freshness_results[table]
                assert 'status' in table_result, f"Status missing in freshness result for {table}"
                assert 'hours_old' in table_result, f"Age missing in freshness result for {table}"
        
        finally:
            monitor.close_connections()
    
    def test_data_volume_validation(self):
        """Test data volume validation"""
        monitor = DataQualityMonitor()
        
        try:
            volume_results = monitor.check_data_volume()
            
            # Check that all tables have volume data
            expected_tables = ['stg_tpch_orders', 'fct_orders']
            for table in expected_tables:
                assert table in volume_results, f"Volume check missing for {table}"
                
                table_result = volume_results[table]
                assert 'row_count' in table_result, f"Row count missing in volume result for {table}"
                assert 'status' in table_result, f"Status missing in volume result for {table}"
        
        finally:
            monitor.close_connections()
    
    def test_data_quality_thresholds(self):
        """Test that data quality meets defined thresholds"""
        monitor = DataQualityMonitor()
        
        try:
            # Run all quality checks
            results = monitor.run_all_checks()
            
            # Check overall status
            assert 'overall_status' in results, "Overall status missing from quality results"
            
            # Check execution time is reasonable
            assert 'execution_time_seconds' in results, "Execution time missing from quality results"
            assert results['execution_time_seconds'] < 300, "Quality checks taking too long"
        
        finally:
            monitor.close_connections()

if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
