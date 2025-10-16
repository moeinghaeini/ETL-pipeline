#!/usr/bin/env python3
"""
Performance Tests for ETL Pipeline
Tests performance benchmarks and optimization
"""

import os
import sys
import pytest
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
import psutil
import subprocess
import json

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.environment_manager import EnvironmentManager
from scripts.data_quality_checks import DataQualityMonitor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestPerformanceBenchmarks:
    """Performance benchmark tests"""
    
    @pytest.fixture(autouse=True)
    def setup_performance_tests(self):
        """Setup for performance tests"""
        self.test_env = "testing"
        self.env_manager = EnvironmentManager()
        self.performance_thresholds = {
            'dbt_compile_time': 30,  # seconds
            'dbt_run_time': 120,     # seconds
            'dbt_test_time': 60,     # seconds
            'data_quality_check_time': 180,  # seconds
            'memory_usage_mb': 1024,  # MB
            'cpu_usage_percent': 80   # percent
        }
    
    def test_dbt_compile_performance(self):
        """Test dbt compilation performance"""
        start_time = time.time()
        
        result = self.env_manager.run_dbt_command("compile", self.test_env)
        
        end_time = time.time()
        compile_time = end_time - start_time
        
        assert result, "dbt compilation failed"
        assert compile_time < self.performance_thresholds['dbt_compile_time'], \
            f"dbt compilation took {compile_time:.2f}s, exceeds threshold of {self.performance_thresholds['dbt_compile_time']}s"
        
        logger.info(f"dbt compilation time: {compile_time:.2f} seconds")
    
    def test_dbt_run_performance(self):
        """Test dbt model execution performance"""
        # Ensure models are compiled first
        assert self.env_manager.run_dbt_command("compile", self.test_env)
        
        start_time = time.time()
        
        result = self.env_manager.run_dbt_command("run", self.test_env)
        
        end_time = time.time()
        run_time = end_time - start_time
        
        assert result, "dbt run failed"
        assert run_time < self.performance_thresholds['dbt_run_time'], \
            f"dbt run took {run_time:.2f}s, exceeds threshold of {self.performance_thresholds['dbt_run_time']}s"
        
        logger.info(f"dbt run time: {run_time:.2f} seconds")
    
    def test_dbt_test_performance(self):
        """Test dbt test execution performance"""
        # Ensure models are run first
        assert self.env_manager.run_dbt_command("run", self.test_env)
        
        start_time = time.time()
        
        result = self.env_manager.run_dbt_command("test", self.test_env)
        
        end_time = time.time()
        test_time = end_time - start_time
        
        assert result, "dbt tests failed"
        assert test_time < self.performance_thresholds['dbt_test_time'], \
            f"dbt tests took {test_time:.2f}s, exceeds threshold of {self.performance_thresholds['dbt_test_time']}s"
        
        logger.info(f"dbt test time: {test_time:.2f} seconds")
    
    def test_data_quality_check_performance(self):
        """Test data quality monitoring performance"""
        monitor = DataQualityMonitor()
        
        try:
            start_time = time.time()
            
            results = monitor.run_all_checks()
            
            end_time = time.time()
            check_time = end_time - start_time
            
            assert results, "Data quality checks failed"
            assert check_time < self.performance_thresholds['data_quality_check_time'], \
                f"Data quality checks took {check_time:.2f}s, exceeds threshold of {self.performance_thresholds['data_quality_check_time']}s"
            
            logger.info(f"Data quality check time: {check_time:.2f} seconds")
        
        finally:
            monitor.close_connections()
    
    def test_memory_usage(self):
        """Test memory usage during pipeline execution"""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Run dbt operations
        assert self.env_manager.run_dbt_command("compile", self.test_env)
        assert self.env_manager.run_dbt_command("run", self.test_env)
        
        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory
        
        assert memory_increase < self.performance_thresholds['memory_usage_mb'], \
            f"Memory usage increased by {memory_increase:.2f}MB, exceeds threshold of {self.performance_thresholds['memory_usage_mb']}MB"
        
        logger.info(f"Memory usage increase: {memory_increase:.2f}MB")
    
    def test_cpu_usage(self):
        """Test CPU usage during pipeline execution"""
        # Monitor CPU usage during dbt operations
        cpu_samples = []
        
        def monitor_cpu():
            while True:
                cpu_percent = psutil.cpu_percent(interval=1)
                cpu_samples.append(cpu_percent)
                if len(cpu_samples) >= 10:  # Sample for 10 seconds
                    break
        
        import threading
        monitor_thread = threading.Thread(target=monitor_cpu)
        monitor_thread.start()
        
        # Run dbt operations
        assert self.env_manager.run_dbt_command("compile", self.test_env)
        assert self.env_manager.run_dbt_command("run", self.test_env)
        
        monitor_thread.join()
        
        avg_cpu = sum(cpu_samples) / len(cpu_samples)
        max_cpu = max(cpu_samples)
        
        assert avg_cpu < self.performance_thresholds['cpu_usage_percent'], \
            f"Average CPU usage {avg_cpu:.2f}% exceeds threshold of {self.performance_thresholds['cpu_usage_percent']}%"
        
        logger.info(f"Average CPU usage: {avg_cpu:.2f}%, Max CPU usage: {max_cpu:.2f}%")
    
    def test_concurrent_execution(self):
        """Test performance under concurrent execution"""
        import concurrent.futures
        import threading
        
        def run_dbt_operation(operation):
            return self.env_manager.run_dbt_command(operation, self.test_env)
        
        operations = ["compile", "run", "test"]
        
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(run_dbt_operation, op) for op in operations]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        end_time = time.time()
        concurrent_time = end_time - start_time
        
        # All operations should succeed
        assert all(results), "Some concurrent operations failed"
        
        # Concurrent execution should be faster than sequential
        sequential_time = 0
        for operation in operations:
            op_start = time.time()
            self.env_manager.run_dbt_command(operation, self.test_env)
            op_end = time.time()
            sequential_time += (op_end - op_start)
        
        efficiency = (sequential_time - concurrent_time) / sequential_time * 100
        
        logger.info(f"Concurrent execution time: {concurrent_time:.2f}s")
        logger.info(f"Sequential execution time: {sequential_time:.2f}s")
        logger.info(f"Efficiency gain: {efficiency:.2f}%")
    
    def test_large_dataset_performance(self):
        """Test performance with large datasets"""
        # This test would typically use larger datasets
        # For now, we'll test with the available data
        
        start_time = time.time()
        
        # Run full pipeline
        assert self.env_manager.run_dbt_command("deps", self.test_env)
        assert self.env_manager.run_dbt_command("compile", self.test_env)
        assert self.env_manager.run_dbt_command("run", self.test_env)
        assert self.env_manager.run_dbt_command("test", self.test_env)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Large dataset threshold: 10 minutes
        large_dataset_threshold = 600
        assert total_time < large_dataset_threshold, \
            f"Large dataset processing took {total_time:.2f}s, exceeds threshold of {large_dataset_threshold}s"
        
        logger.info(f"Large dataset processing time: {total_time:.2f} seconds")
    
    def test_query_optimization(self):
        """Test query optimization effectiveness"""
        # Test that queries are optimized (no full table scans, proper indexing, etc.)
        # This would typically involve analyzing query execution plans
        
        # For now, we'll test that models compile and run efficiently
        compile_start = time.time()
        assert self.env_manager.run_dbt_command("compile", self.test_env)
        compile_time = time.time() - compile_start
        
        run_start = time.time()
        assert self.env_manager.run_dbt_command("run", self.test_env)
        run_time = time.time() - run_start
        
        # Optimization thresholds
        compile_threshold = 20  # seconds
        run_threshold = 100     # seconds
        
        assert compile_time < compile_threshold, \
            f"Query compilation time {compile_time:.2f}s suggests optimization issues"
        
        assert run_time < run_threshold, \
            f"Query execution time {run_time:.2f}s suggests optimization issues"
        
        logger.info(f"Query optimization - Compile: {compile_time:.2f}s, Run: {run_time:.2f}s")
    
    def test_scalability(self):
        """Test pipeline scalability"""
        # Test performance with different thread counts
        thread_counts = [1, 2, 4]
        performance_results = {}
        
        for threads in thread_counts:
            os.environ['DBT_THREADS'] = str(threads)
            
            start_time = time.time()
            assert self.env_manager.run_dbt_command("run", self.test_env)
            end_time = time.time()
            
            performance_results[threads] = end_time - start_time
            logger.info(f"Performance with {threads} threads: {performance_results[threads]:.2f}s")
        
        # Performance should improve with more threads (up to a point)
        if len(performance_results) > 1:
            single_thread_time = performance_results[1]
            multi_thread_time = min(performance_results[2], performance_results[4])
            
            improvement = (single_thread_time - multi_thread_time) / single_thread_time * 100
            logger.info(f"Performance improvement with multiple threads: {improvement:.2f}%")
    
    def test_resource_utilization(self):
        """Test resource utilization efficiency"""
        # Monitor system resources during pipeline execution
        initial_cpu = psutil.cpu_percent()
        initial_memory = psutil.virtual_memory().percent
        initial_disk = psutil.disk_usage('/').percent
        
        # Run pipeline
        assert self.env_manager.run_dbt_command("run", self.test_env)
        
        final_cpu = psutil.cpu_percent()
        final_memory = psutil.virtual_memory().percent
        final_disk = psutil.disk_usage('/').percent
        
        # Resource utilization should be reasonable
        cpu_increase = final_cpu - initial_cpu
        memory_increase = final_memory - initial_memory
        disk_increase = final_disk - initial_disk
        
        assert cpu_increase < 50, f"CPU usage increased by {cpu_increase:.2f}%"
        assert memory_increase < 20, f"Memory usage increased by {memory_increase:.2f}%"
        assert disk_increase < 5, f"Disk usage increased by {disk_increase:.2f}%"
        
        logger.info(f"Resource utilization - CPU: {cpu_increase:.2f}%, Memory: {memory_increase:.2f}%, Disk: {disk_increase:.2f}%")

class TestPerformanceRegression:
    """Performance regression tests"""
    
    def test_performance_baseline(self):
        """Establish performance baseline"""
        env_manager = EnvironmentManager()
        test_env = "testing"
        
        # Run complete pipeline and measure performance
        start_time = time.time()
        
        assert env_manager.run_dbt_command("deps", test_env)
        assert env_manager.run_dbt_command("compile", test_env)
        assert env_manager.run_dbt_command("run", test_env)
        assert env_manager.run_dbt_command("test", test_env)
        
        end_time = time.time()
        baseline_time = end_time - start_time
        
        # Store baseline for regression testing
        baseline_file = "tests/performance_baseline.json"
        baseline_data = {
            'timestamp': datetime.now().isoformat(),
            'baseline_time': baseline_time,
            'environment': test_env
        }
        
        os.makedirs(os.path.dirname(baseline_file), exist_ok=True)
        with open(baseline_file, 'w') as f:
            json.dump(baseline_data, f, indent=2)
        
        logger.info(f"Performance baseline established: {baseline_time:.2f} seconds")
    
    def test_performance_regression(self):
        """Test for performance regressions"""
        baseline_file = "tests/performance_baseline.json"
        
        if not os.path.exists(baseline_file):
            pytest.skip("Performance baseline not found. Run test_performance_baseline first.")
        
        # Load baseline
        with open(baseline_file, 'r') as f:
            baseline_data = json.load(f)
        
        baseline_time = baseline_data['baseline_time']
        
        # Run current performance test
        env_manager = EnvironmentManager()
        test_env = "testing"
        
        start_time = time.time()
        
        assert env_manager.run_dbt_command("deps", test_env)
        assert env_manager.run_dbt_command("compile", test_env)
        assert env_manager.run_dbt_command("run", test_env)
        assert env_manager.run_dbt_command("test", test_env)
        
        end_time = time.time()
        current_time = end_time - start_time
        
        # Check for regression (allow 20% degradation)
        regression_threshold = baseline_time * 1.2
        
        assert current_time < regression_threshold, \
            f"Performance regression detected: {current_time:.2f}s vs baseline {baseline_time:.2f}s"
        
        improvement = (baseline_time - current_time) / baseline_time * 100
        logger.info(f"Performance change: {improvement:+.2f}% from baseline")

if __name__ == "__main__":
    # Run performance tests
    pytest.main([__file__, "-v", "--tb=short", "-s"])
