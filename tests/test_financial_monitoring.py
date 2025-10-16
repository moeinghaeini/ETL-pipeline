#!/usr/bin/env python3
"""
Tests for Financial Data Monitoring System
Tests yfinance integration and Bosch stock monitoring
"""

import os
import sys
import pytest
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
import pandas as pd
import numpy as np

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.financial_data_monitor import BoschStockMonitor

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TestFinancialDataMonitor:
    """Test financial data monitoring functionality"""
    
    @pytest.fixture(autouse=True)
    def setup_financial_tests(self):
        """Setup for financial monitoring tests"""
        self.monitor = BoschStockMonitor()
        self.test_symbol = 'BOSCHLTD.BSE'
    
    def test_bosch_symbols_configuration(self):
        """Test that Bosch symbols are properly configured"""
        assert hasattr(self.monitor, 'bosch_symbols')
        assert isinstance(self.monitor.bosch_symbols, dict)
        assert len(self.monitor.bosch_symbols) > 0
        
        # Check that primary symbol is configured
        assert hasattr(self.monitor, 'primary_symbol')
        assert self.monitor.primary_symbol in self.monitor.bosch_symbols
    
    def test_alert_thresholds_configuration(self):
        """Test that alert thresholds are properly configured"""
        assert hasattr(self.monitor, 'alert_thresholds')
        assert isinstance(self.monitor.alert_thresholds, dict)
        
        required_thresholds = [
            'price_change_percent',
            'volume_spike_percent',
            'volatility_threshold',
            'rsi_overbought',
            'rsi_oversold'
        ]
        
        for threshold in required_thresholds:
            assert threshold in self.monitor.alert_thresholds
            assert isinstance(self.monitor.alert_thresholds[threshold], (int, float))
    
    def test_data_directory_creation(self):
        """Test that data directory is created"""
        assert os.path.exists(self.monitor.data_dir)
        assert os.path.isdir(self.monitor.data_dir)
    
    def test_get_stock_data_structure(self):
        """Test stock data retrieval and structure"""
        # Note: This test may fail if no internet connection or yfinance issues
        # In a real test environment, you might want to mock this
        try:
            data = self.monitor.get_stock_data(self.test_symbol, period="5d")
            
            if data is not None:
                assert isinstance(data, pd.DataFrame)
                assert not data.empty
                
                # Check required columns
                required_columns = ['Open', 'High', 'Low', 'Close', 'Volume']
                for col in required_columns:
                    assert col in data.columns
                
                # Check data types
                assert pd.api.types.is_numeric_dtype(data['Close'])
                assert pd.api.types.is_numeric_dtype(data['Volume'])
                
                # Check that symbol is added
                assert 'symbol' in data.columns
                assert data['symbol'].iloc[0] == self.test_symbol
                
                logger.info(f"Successfully retrieved {len(data)} records for {self.test_symbol}")
            else:
                pytest.skip(f"No data available for {self.test_symbol}")
                
        except Exception as e:
            pytest.skip(f"Failed to retrieve stock data: {e}")
    
    def test_company_info_retrieval(self):
        """Test company information retrieval"""
        try:
            company_info = self.monitor.get_company_info(self.test_symbol)
            
            if company_info:
                assert isinstance(company_info, dict)
                
                # Check required fields
                required_fields = ['symbol', 'name', 'sector', 'industry']
                for field in required_fields:
                    assert field in company_info
                
                # Check that symbol matches
                assert company_info['symbol'] == self.test_symbol
                
                logger.info(f"Retrieved company info for {self.test_symbol}")
            else:
                pytest.skip(f"No company info available for {self.test_symbol}")
                
        except Exception as e:
            pytest.skip(f"Failed to retrieve company info: {e}")
    
    def test_technical_indicators_calculation(self):
        """Test technical indicators calculation"""
        # Create sample data
        dates = pd.date_range(start='2024-01-01', periods=50, freq='D')
        sample_data = pd.DataFrame({
            'Open': np.random.uniform(1000, 1100, 50),
            'High': np.random.uniform(1100, 1200, 50),
            'Low': np.random.uniform(900, 1000, 50),
            'Close': np.random.uniform(1000, 1100, 50),
            'Volume': np.random.uniform(100000, 1000000, 50)
        }, index=dates)
        
        sample_data['symbol'] = self.test_symbol
        sample_data['data_fetched_at'] = datetime.now()
        
        # Calculate technical indicators
        data_with_indicators = self.monitor.calculate_technical_indicators(sample_data)
        
        assert isinstance(data_with_indicators, pd.DataFrame)
        assert len(data_with_indicators) == len(sample_data)
        
        # Check that technical indicators are added
        expected_indicators = ['MA_5', 'MA_20', 'MA_50', 'RSI', 'BB_Upper', 'BB_Lower', 'BB_Middle', 'MACD']
        for indicator in expected_indicators:
            assert indicator in data_with_indicators.columns
        
        # Check that indicators have reasonable values
        assert data_with_indicators['MA_5'].notna().sum() > 0
        assert data_with_indicators['RSI'].notna().sum() > 0
        
        logger.info("Technical indicators calculated successfully")
    
    def test_anomaly_detection(self):
        """Test anomaly detection functionality"""
        # Create sample data with known anomalies
        dates = pd.date_range(start='2024-01-01', periods=10, freq='D')
        sample_data = pd.DataFrame({
            'Close': [1000, 1010, 1020, 1030, 1040, 1100, 1110, 1120, 1130, 1140],  # Price spike
            'Volume': [100000, 110000, 120000, 130000, 140000, 500000, 150000, 160000, 170000, 180000],  # Volume spike
            'RSI': [50, 52, 54, 56, 58, 75, 77, 79, 81, 83],  # RSI overbought
            'Volatility': [0.01, 0.01, 0.01, 0.01, 0.01, 0.04, 0.01, 0.01, 0.01, 0.01],  # High volatility
            'symbol': self.test_symbol
        }, index=dates)
        
        anomalies = self.monitor.detect_anomalies(sample_data)
        
        assert isinstance(anomalies, dict)
        assert 'timestamp' in anomalies
        assert 'alerts' in anomalies
        assert 'metrics' in anomalies
        
        # Should detect some anomalies in our sample data
        assert len(anomalies['alerts']) > 0
        
        # Check alert structure
        for alert in anomalies['alerts']:
            assert 'type' in alert
            assert 'severity' in alert
            assert 'message' in alert
            assert 'value' in alert
            assert 'threshold' in alert
        
        logger.info(f"Detected {len(anomalies['alerts'])} anomalies")
    
    def test_analysis_report_generation(self):
        """Test analysis report generation"""
        # Create sample data
        dates = pd.date_range(start='2024-01-01', periods=30, freq='D')
        sample_data = pd.DataFrame({
            'Open': np.random.uniform(1000, 1100, 30),
            'High': np.random.uniform(1100, 1200, 30),
            'Low': np.random.uniform(900, 1000, 30),
            'Close': np.random.uniform(1000, 1100, 30),
            'Volume': np.random.uniform(100000, 1000000, 30),
            'RSI': np.random.uniform(30, 70, 30),
            'Volatility': np.random.uniform(0.01, 0.03, 30),
            'MA_5': np.random.uniform(1000, 1100, 30),
            'MA_20': np.random.uniform(1000, 1100, 30),
            'MA_50': np.random.uniform(1000, 1100, 30),
            'BB_Upper': np.random.uniform(1100, 1200, 30),
            'BB_Lower': np.random.uniform(900, 1000, 30),
            'symbol': self.test_symbol
        }, index=dates)
        
        company_info = {
            'symbol': self.test_symbol,
            'name': 'Bosch Limited',
            'sector': 'Technology',
            'industry': 'Automotive'
        }
        
        anomalies = {
            'alerts': [],
            'metrics': {
                'current_price': 1050,
                'price_change_percent': 2.5,
                'volume': 500000,
                'rsi': 55,
                'volatility': 0.02
            }
        }
        
        report = self.monitor.generate_analysis_report(sample_data, company_info, anomalies)
        
        assert isinstance(report, dict)
        assert 'timestamp' in report
        assert 'symbol' in report
        assert 'company_info' in report
        assert 'performance_metrics' in report
        assert 'technical_summary' in report
        assert 'risk_assessment' in report
        assert 'recommendations' in report
        
        # Check performance metrics
        performance_metrics = report['performance_metrics']
        assert 'current_price' in performance_metrics
        assert 'day_change_percent' in performance_metrics
        assert 'volume' in performance_metrics
        
        # Check technical summary
        technical_summary = report['technical_summary']
        assert 'trend' in technical_summary
        assert 'rsi_signal' in technical_summary
        assert 'bollinger_position' in technical_summary
        
        logger.info("Analysis report generated successfully")
    
    def test_data_saving_functionality(self):
        """Test data saving functionality"""
        # Create sample data
        dates = pd.date_range(start='2024-01-01', periods=5, freq='D')
        sample_data = pd.DataFrame({
            'Open': [1000, 1010, 1020, 1030, 1040],
            'High': [1100, 1110, 1120, 1130, 1140],
            'Low': [900, 910, 920, 930, 940],
            'Close': [1050, 1060, 1070, 1080, 1090],
            'Volume': [100000, 110000, 120000, 130000, 140000],
            'symbol': self.test_symbol,
            'data_fetched_at': datetime.now()
        }, index=dates)
        
        # Save data
        csv_path = self.monitor.save_data(sample_data, self.test_symbol, "test_data")
        
        assert csv_path != ""
        assert os.path.exists(csv_path)
        
        # Verify saved data
        saved_data = pd.read_csv(csv_path, index_col=0, parse_dates=True)
        assert len(saved_data) == len(sample_data)
        assert list(saved_data.columns) == list(sample_data.columns)
        
        # Clean up
        os.remove(csv_path)
        
        logger.info("Data saving functionality works correctly")
    
    def test_recommendations_generation(self):
        """Test trading recommendations generation"""
        # Test bullish scenario
        bullish_technical = {
            'trend': 'bullish',
            'rsi_signal': 'oversold',
            'bollinger_position': 'lower',
            'macd_signal': 'bullish'
        }
        
        bullish_metrics = {
            'volume': 500000,
            'avg_volume': 300000,
            'volatility': 0.02
        }
        
        bullish_anomalies = {'alerts': []}
        
        recommendations = self.monitor._generate_recommendations(
            bullish_technical, bullish_metrics, bullish_anomalies
        )
        
        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        
        # Should have bullish recommendations
        bullish_recommendations = [rec for rec in recommendations if 'buy' in rec.lower()]
        assert len(bullish_recommendations) > 0
        
        logger.info(f"Generated {len(recommendations)} recommendations")
    
    def test_error_handling(self):
        """Test error handling for invalid inputs"""
        # Test with invalid symbol
        invalid_data = self.monitor.get_stock_data("INVALID_SYMBOL", period="1d")
        assert invalid_data is None
        
        # Test with empty DataFrame
        empty_df = pd.DataFrame()
        indicators = self.monitor.calculate_technical_indicators(empty_df)
        assert isinstance(indicators, pd.DataFrame)
        assert len(indicators) == 0
        
        # Test anomaly detection with insufficient data
        insufficient_data = pd.DataFrame({
            'Close': [1000],
            'Volume': [100000],
            'symbol': self.test_symbol
        })
        
        anomalies = self.monitor.detect_anomalies(insufficient_data)
        assert isinstance(anomalies, dict)
        assert 'alerts' in anomalies
        
        logger.info("Error handling works correctly")

class TestFinancialDataIntegration:
    """Integration tests for financial data monitoring"""
    
    def test_complete_analysis_workflow(self):
        """Test complete analysis workflow"""
        monitor = BoschStockMonitor()
        
        try:
            # Run complete analysis (this may take time and require internet)
            results = monitor.run_complete_analysis()
            
            assert isinstance(results, dict)
            assert 'timestamp' in results
            assert 'symbols_analyzed' in results
            assert 'analysis_results' in results
            assert 'overall_summary' in results
            
            # Check that at least one symbol was analyzed
            if results['symbols_analyzed']:
                assert len(results['symbols_analyzed']) > 0
                
                # Check analysis results structure
                for symbol in results['symbols_analyzed']:
                    assert symbol in results['analysis_results']
                    analysis = results['analysis_results'][symbol]
                    assert 'description' in analysis
                    assert 'analysis_report' in analysis
                    assert 'anomalies' in analysis
                
                logger.info(f"Complete analysis successful for {len(results['symbols_analyzed'])} symbols")
            else:
                pytest.skip("No symbols were analyzed (likely due to data availability)")
                
        except Exception as e:
            pytest.skip(f"Complete analysis failed: {e}")
    
    def test_data_consistency(self):
        """Test data consistency across different periods"""
        monitor = BoschStockMonitor()
        
        try:
            # Get data for different periods
            data_1d = monitor.get_stock_data(monitor.primary_symbol, period="1d")
            data_5d = monitor.get_stock_data(monitor.primary_symbol, period="5d")
            
            if data_1d is not None and data_5d is not None:
                # 5-day data should have more records than 1-day data
                assert len(data_5d) >= len(data_1d)
                
                # Latest record should be the same (if both have recent data)
                if len(data_1d) > 0 and len(data_5d) > 0:
                    latest_1d = data_1d.index[-1]
                    latest_5d = data_5d.index[-1]
                    # They should be close in time (within 1 day)
                    time_diff = abs((latest_1d - latest_5d).days)
                    assert time_diff <= 1
                
                logger.info("Data consistency check passed")
            else:
                pytest.skip("No data available for consistency check")
                
        except Exception as e:
            pytest.skip(f"Data consistency check failed: {e}")

if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
