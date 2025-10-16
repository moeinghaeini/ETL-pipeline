#!/usr/bin/env python3
"""
Financial Data Monitoring System using yfinance
Monitors Bosch company stock data and integrates with ETL pipeline
"""

import os
import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
import json
import pandas as pd
import numpy as np
import yfinance as yf
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/financial_monitor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BoschStockMonitor:
    """Comprehensive Bosch stock monitoring system"""
    
    def __init__(self):
        # Bosch stock symbols (multiple exchanges)
        self.bosch_symbols = {
            'BOSCHLTD.BSE': 'Bosch Limited (BSE)',
            'BOSCHLTD.NSE': 'Bosch Limited (NSE)', 
            'BOSCHLTD.NS': 'Bosch Limited (NSE Alt)',
            'BOSCHLTD.BO': 'Bosch Limited (BSE Alt)'
        }
        
        # Primary symbol to use
        self.primary_symbol = 'BOSCHLTD.BSE'
        
        # Data storage
        self.data_dir = "data/financial"
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Alert thresholds
        self.alert_thresholds = {
            'price_change_percent': 5.0,  # 5% price change
            'volume_spike_percent': 200.0,  # 200% volume increase
            'volatility_threshold': 0.02,  # 2% daily volatility
            'rsi_overbought': 70,
            'rsi_oversold': 30
        }
    
    def get_stock_data(self, symbol: str, period: str = "1mo", interval: str = "1d") -> Optional[pd.DataFrame]:
        """Fetch stock data from yfinance"""
        try:
            logger.info(f"Fetching data for {symbol} - Period: {period}, Interval: {interval}")
            
            ticker = yf.Ticker(symbol)
            data = ticker.history(period=period, interval=interval)
            
            if data.empty:
                logger.warning(f"No data returned for {symbol}")
                return None
            
            # Add metadata
            data['symbol'] = symbol
            data['data_fetched_at'] = datetime.now()
            
            logger.info(f"Successfully fetched {len(data)} records for {symbol}")
            return data
            
        except Exception as e:
            logger.error(f"Failed to fetch data for {symbol}: {e}")
            return None
    
    def get_company_info(self, symbol: str) -> Dict[str, Any]:
        """Get company information"""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            
            # Extract relevant information
            company_info = {
                'symbol': symbol,
                'name': info.get('longName', 'N/A'),
                'sector': info.get('sector', 'N/A'),
                'industry': info.get('industry', 'N/A'),
                'market_cap': info.get('marketCap', 0),
                'employees': info.get('fullTimeEmployees', 0),
                'website': info.get('website', 'N/A'),
                'description': info.get('longBusinessSummary', 'N/A'),
                'country': info.get('country', 'N/A'),
                'currency': info.get('currency', 'N/A'),
                'exchange': info.get('exchange', 'N/A'),
                'fetched_at': datetime.now().isoformat()
            }
            
            logger.info(f"Retrieved company info for {symbol}")
            return company_info
            
        except Exception as e:
            logger.error(f"Failed to get company info for {symbol}: {e}")
            return {}
    
    def calculate_technical_indicators(self, data: pd.DataFrame) -> pd.DataFrame:
        """Calculate technical indicators"""
        try:
            df = data.copy()
            
            # Moving averages
            df['MA_5'] = df['Close'].rolling(window=5).mean()
            df['MA_20'] = df['Close'].rolling(window=20).mean()
            df['MA_50'] = df['Close'].rolling(window=50).mean()
            
            # RSI (Relative Strength Index)
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            
            # Bollinger Bands
            df['BB_Middle'] = df['Close'].rolling(window=20).mean()
            bb_std = df['Close'].rolling(window=20).std()
            df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
            df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
            
            # MACD
            exp1 = df['Close'].ewm(span=12).mean()
            exp2 = df['Close'].ewm(span=26).mean()
            df['MACD'] = exp1 - exp2
            df['MACD_Signal'] = df['MACD'].ewm(span=9).mean()
            df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']
            
            # Volume indicators
            df['Volume_MA'] = df['Volume'].rolling(window=20).mean()
            df['Volume_Ratio'] = df['Volume'] / df['Volume_MA']
            
            # Price change indicators
            df['Price_Change'] = df['Close'].pct_change()
            df['Price_Change_5d'] = df['Close'].pct_change(periods=5)
            df['Volatility'] = df['Price_Change'].rolling(window=20).std()
            
            logger.info("Technical indicators calculated successfully")
            return df
            
        except Exception as e:
            logger.error(f"Failed to calculate technical indicators: {e}")
            return data
    
    def detect_anomalies(self, data: pd.DataFrame) -> Dict[str, Any]:
        """Detect anomalies and alert conditions"""
        try:
            latest_data = data.iloc[-1]
            previous_data = data.iloc[-2] if len(data) > 1 else latest_data
            
            anomalies = {
                'timestamp': datetime.now().isoformat(),
                'symbol': latest_data.get('symbol', 'N/A'),
                'alerts': [],
                'metrics': {}
            }
            
            # Price change alert
            price_change = latest_data['Close'] - previous_data['Close']
            price_change_percent = (price_change / previous_data['Close']) * 100
            
            if abs(price_change_percent) > self.alert_thresholds['price_change_percent']:
                anomalies['alerts'].append({
                    'type': 'price_change',
                    'severity': 'high' if abs(price_change_percent) > 10 else 'medium',
                    'message': f"Significant price change: {price_change_percent:.2f}%",
                    'value': price_change_percent,
                    'threshold': self.alert_thresholds['price_change_percent']
                })
            
            # Volume spike alert
            if 'Volume_Ratio' in latest_data and latest_data['Volume_Ratio'] > (self.alert_thresholds['volume_spike_percent'] / 100):
                anomalies['alerts'].append({
                    'type': 'volume_spike',
                    'severity': 'medium',
                    'message': f"Volume spike detected: {latest_data['Volume_Ratio']:.2f}x normal",
                    'value': latest_data['Volume_Ratio'],
                    'threshold': self.alert_thresholds['volume_spike_percent'] / 100
                })
            
            # RSI alerts
            if 'RSI' in latest_data:
                rsi = latest_data['RSI']
                if rsi > self.alert_thresholds['rsi_overbought']:
                    anomalies['alerts'].append({
                        'type': 'rsi_overbought',
                        'severity': 'medium',
                        'message': f"RSI indicates overbought condition: {rsi:.2f}",
                        'value': rsi,
                        'threshold': self.alert_thresholds['rsi_overbought']
                    })
                elif rsi < self.alert_thresholds['rsi_oversold']:
                    anomalies['alerts'].append({
                        'type': 'rsi_oversold',
                        'severity': 'medium',
                        'message': f"RSI indicates oversold condition: {rsi:.2f}",
                        'value': rsi,
                        'threshold': self.alert_thresholds['rsi_oversold']
                    })
            
            # Volatility alert
            if 'Volatility' in latest_data and latest_data['Volatility'] > self.alert_thresholds['volatility_threshold']:
                anomalies['alerts'].append({
                    'type': 'high_volatility',
                    'severity': 'medium',
                    'message': f"High volatility detected: {latest_data['Volatility']:.4f}",
                    'value': latest_data['Volatility'],
                    'threshold': self.alert_thresholds['volatility_threshold']
                })
            
            # Store key metrics
            anomalies['metrics'] = {
                'current_price': latest_data['Close'],
                'price_change_percent': price_change_percent,
                'volume': latest_data['Volume'],
                'rsi': latest_data.get('RSI', 0),
                'volatility': latest_data.get('Volatility', 0),
                'ma_20': latest_data.get('MA_20', 0),
                'ma_50': latest_data.get('MA_50', 0)
            }
            
            logger.info(f"Anomaly detection completed. Found {len(anomalies['alerts'])} alerts")
            return anomalies
            
        except Exception as e:
            logger.error(f"Failed to detect anomalies: {e}")
            return {'timestamp': datetime.now().isoformat(), 'alerts': [], 'metrics': {}, 'error': str(e)}
    
    def create_price_chart(self, data: pd.DataFrame, symbol: str) -> str:
        """Create interactive price chart"""
        try:
            fig = make_subplots(
                rows=3, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                subplot_titles=('Price & Moving Averages', 'Volume', 'RSI'),
                row_width=[0.2, 0.1, 0.1]
            )
            
            # Price and moving averages
            fig.add_trace(
                go.Scatter(x=data.index, y=data['Close'], name='Close Price', line=dict(color='blue')),
                row=1, col=1
            )
            
            if 'MA_20' in data.columns:
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['MA_20'], name='MA 20', line=dict(color='orange')),
                    row=1, col=1
                )
            
            if 'MA_50' in data.columns:
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['MA_50'], name='MA 50', line=dict(color='red')),
                    row=1, col=1
                )
            
            # Bollinger Bands
            if all(col in data.columns for col in ['BB_Upper', 'BB_Lower', 'BB_Middle']):
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['BB_Upper'], name='BB Upper', line=dict(color='gray', dash='dash')),
                    row=1, col=1
                )
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['BB_Lower'], name='BB Lower', line=dict(color='gray', dash='dash')),
                    row=1, col=1
                )
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['BB_Middle'], name='BB Middle', line=dict(color='gray')),
                    row=1, col=1
                )
            
            # Volume
            fig.add_trace(
                go.Bar(x=data.index, y=data['Volume'], name='Volume', marker_color='lightblue'),
                row=2, col=1
            )
            
            # RSI
            if 'RSI' in data.columns:
                fig.add_trace(
                    go.Scatter(x=data.index, y=data['RSI'], name='RSI', line=dict(color='purple')),
                    row=3, col=1
                )
                
                # RSI overbought/oversold lines
                fig.add_hline(y=70, line_dash="dash", line_color="red", row=3, col=1)
                fig.add_hline(y=30, line_dash="dash", line_color="green", row=3, col=1)
            
            fig.update_layout(
                title=f'{symbol} - Stock Analysis',
                xaxis_title='Date',
                height=800,
                showlegend=True
            )
            
            # Save chart
            chart_path = os.path.join(self.data_dir, f'{symbol}_chart.html')
            fig.write_html(chart_path)
            
            logger.info(f"Price chart created: {chart_path}")
            return chart_path
            
        except Exception as e:
            logger.error(f"Failed to create price chart: {e}")
            return ""
    
    def generate_analysis_report(self, data: pd.DataFrame, company_info: Dict[str, Any], anomalies: Dict[str, Any]) -> Dict[str, Any]:
        """Generate comprehensive analysis report"""
        try:
            latest_data = data.iloc[-1]
            
            # Calculate performance metrics
            performance_metrics = {
                'current_price': latest_data['Close'],
                'day_change': latest_data['Close'] - data.iloc[-2]['Close'] if len(data) > 1 else 0,
                'day_change_percent': ((latest_data['Close'] - data.iloc[-2]['Close']) / data.iloc[-2]['Close'] * 100) if len(data) > 1 else 0,
                'week_change_percent': ((latest_data['Close'] - data.iloc[-5]['Close']) / data.iloc[-5]['Close'] * 100) if len(data) > 5 else 0,
                'month_change_percent': ((latest_data['Close'] - data.iloc[0]['Close']) / data.iloc[0]['Close'] * 100) if len(data) > 0 else 0,
                'volume': latest_data['Volume'],
                'avg_volume': data['Volume'].mean(),
                'volatility': latest_data.get('Volatility', 0),
                'rsi': latest_data.get('RSI', 0)
            }
            
            # Technical analysis summary
            technical_summary = {
                'trend': 'bullish' if latest_data.get('MA_5', 0) > latest_data.get('MA_20', 0) > latest_data.get('MA_50', 0) else 'bearish',
                'rsi_signal': 'overbought' if latest_data.get('RSI', 0) > 70 else 'oversold' if latest_data.get('RSI', 0) < 30 else 'neutral',
                'bollinger_position': 'upper' if latest_data['Close'] > latest_data.get('BB_Upper', 0) else 'lower' if latest_data['Close'] < latest_data.get('BB_Lower', 0) else 'middle',
                'macd_signal': 'bullish' if latest_data.get('MACD', 0) > latest_data.get('MACD_Signal', 0) else 'bearish'
            }
            
            # Risk assessment
            risk_level = 'low'
            if latest_data.get('Volatility', 0) > 0.03:
                risk_level = 'high'
            elif latest_data.get('Volatility', 0) > 0.02:
                risk_level = 'medium'
            
            # Generate report
            report = {
                'timestamp': datetime.now().isoformat(),
                'symbol': latest_data.get('symbol', 'N/A'),
                'company_info': company_info,
                'performance_metrics': performance_metrics,
                'technical_summary': technical_summary,
                'risk_assessment': {
                    'level': risk_level,
                    'volatility': latest_data.get('Volatility', 0),
                    'rsi': latest_data.get('RSI', 0)
                },
                'anomalies': anomalies,
                'recommendations': self._generate_recommendations(technical_summary, performance_metrics, anomalies)
            }
            
            logger.info("Analysis report generated successfully")
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate analysis report: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def _generate_recommendations(self, technical_summary: Dict[str, Any], performance_metrics: Dict[str, Any], anomalies: Dict[str, Any]) -> List[str]:
        """Generate trading recommendations based on analysis"""
        recommendations = []
        
        # Trend-based recommendations
        if technical_summary['trend'] == 'bullish':
            recommendations.append("Consider buying on dips - bullish trend detected")
        elif technical_summary['trend'] == 'bearish':
            recommendations.append("Consider selling or shorting - bearish trend detected")
        
        # RSI-based recommendations
        if technical_summary['rsi_signal'] == 'overbought':
            recommendations.append("RSI indicates overbought condition - consider taking profits")
        elif technical_summary['rsi_signal'] == 'oversold':
            recommendations.append("RSI indicates oversold condition - potential buying opportunity")
        
        # Volume-based recommendations
        if performance_metrics['volume'] > performance_metrics['avg_volume'] * 1.5:
            recommendations.append("High volume detected - significant price movement expected")
        
        # Volatility-based recommendations
        if performance_metrics['volatility'] > 0.03:
            recommendations.append("High volatility - use stop-loss orders for risk management")
        
        # Alert-based recommendations
        for alert in anomalies.get('alerts', []):
            if alert['type'] == 'price_change' and alert['severity'] == 'high':
                recommendations.append("Significant price movement - monitor closely for trend continuation")
        
        return recommendations
    
    def save_data(self, data: pd.DataFrame, symbol: str, data_type: str = "stock_data") -> str:
        """Save data to files"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # Save as CSV
            csv_path = os.path.join(self.data_dir, f"{symbol}_{data_type}_{timestamp}.csv")
            data.to_csv(csv_path)
            
            # Save as JSON (for metadata)
            json_path = os.path.join(self.data_dir, f"{symbol}_{data_type}_{timestamp}.json")
            metadata = {
                'symbol': symbol,
                'data_type': data_type,
                'timestamp': timestamp,
                'records_count': len(data),
                'date_range': {
                    'start': data.index[0].isoformat() if len(data) > 0 else None,
                    'end': data.index[-1].isoformat() if len(data) > 0 else None
                }
            }
            
            with open(json_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            logger.info(f"Data saved: {csv_path}, {json_path}")
            return csv_path
            
        except Exception as e:
            logger.error(f"Failed to save data: {e}")
            return ""
    
    def run_complete_analysis(self) -> Dict[str, Any]:
        """Run complete Bosch stock analysis"""
        try:
            logger.info("Starting complete Bosch stock analysis...")
            
            results = {
                'timestamp': datetime.now().isoformat(),
                'symbols_analyzed': [],
                'analysis_results': {},
                'overall_summary': {}
            }
            
            # Analyze each Bosch symbol
            for symbol, description in self.bosch_symbols.items():
                logger.info(f"Analyzing {symbol} - {description}")
                
                # Get stock data
                data = self.get_stock_data(symbol, period="3mo")
                if data is None:
                    logger.warning(f"Skipping {symbol} - no data available")
                    continue
                
                # Calculate technical indicators
                data_with_indicators = self.calculate_technical_indicators(data)
                
                # Get company info
                company_info = self.get_company_info(symbol)
                
                # Detect anomalies
                anomalies = self.detect_anomalies(data_with_indicators)
                
                # Generate analysis report
                analysis_report = self.generate_analysis_report(data_with_indicators, company_info, anomalies)
                
                # Create charts
                chart_path = self.create_price_chart(data_with_indicators, symbol)
                
                # Save data
                csv_path = self.save_data(data_with_indicators, symbol)
                
                # Store results
                results['symbols_analyzed'].append(symbol)
                results['analysis_results'][symbol] = {
                    'description': description,
                    'analysis_report': analysis_report,
                    'chart_path': chart_path,
                    'data_path': csv_path,
                    'anomalies': anomalies
                }
            
            # Generate overall summary
            results['overall_summary'] = self._generate_overall_summary(results['analysis_results'])
            
            # Save complete results
            results_path = os.path.join(self.data_dir, f"bosch_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(results_path, 'w') as f:
                json.dump(results, f, indent=2, default=str)
            
            logger.info(f"Complete analysis finished. Results saved to: {results_path}")
            return results
            
        except Exception as e:
            logger.error(f"Failed to run complete analysis: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def _generate_overall_summary(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate overall summary across all symbols"""
        try:
            if not analysis_results:
                return {'error': 'No analysis results available'}
            
            # Aggregate metrics
            total_alerts = 0
            symbols_with_alerts = 0
            avg_volatility = 0
            avg_rsi = 0
            price_changes = []
            
            for symbol, result in analysis_results.items():
                anomalies = result.get('anomalies', {})
                total_alerts += len(anomalies.get('alerts', []))
                if anomalies.get('alerts'):
                    symbols_with_alerts += 1
                
                metrics = anomalies.get('metrics', {})
                avg_volatility += metrics.get('volatility', 0)
                avg_rsi += metrics.get('rsi', 0)
                price_changes.append(metrics.get('price_change_percent', 0))
            
            num_symbols = len(analysis_results)
            avg_volatility /= num_symbols
            avg_rsi /= num_symbols
            
            return {
                'total_symbols': num_symbols,
                'total_alerts': total_alerts,
                'symbols_with_alerts': symbols_with_alerts,
                'average_volatility': avg_volatility,
                'average_rsi': avg_rsi,
                'price_change_range': {
                    'min': min(price_changes) if price_changes else 0,
                    'max': max(price_changes) if price_changes else 0,
                    'avg': sum(price_changes) / len(price_changes) if price_changes else 0
                },
                'risk_assessment': 'high' if avg_volatility > 0.03 or total_alerts > 3 else 'medium' if avg_volatility > 0.02 or total_alerts > 1 else 'low'
            }
            
        except Exception as e:
            logger.error(f"Failed to generate overall summary: {e}")
            return {'error': str(e)}

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Bosch Stock Monitor')
    parser.add_argument('--symbol', default='BOSCHLTD.BSE', help='Stock symbol to analyze')
    parser.add_argument('--period', default='3mo', help='Data period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)')
    parser.add_argument('--interval', default='1d', help='Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)')
    parser.add_argument('--complete', action='store_true', help='Run complete analysis for all Bosch symbols')
    parser.add_argument('--save', action='store_true', help='Save data to files')
    
    args = parser.parse_args()
    
    try:
        monitor = BoschStockMonitor()
        
        if args.complete:
            # Run complete analysis
            results = monitor.run_complete_analysis()
            print(json.dumps(results, indent=2, default=str))
        else:
            # Analyze single symbol
            data = monitor.get_stock_data(args.symbol, args.period, args.interval)
            if data is None:
                print(f"Failed to get data for {args.symbol}")
                sys.exit(1)
            
            # Calculate indicators
            data_with_indicators = monitor.calculate_technical_indicators(data)
            
            # Get company info
            company_info = monitor.get_company_info(args.symbol)
            
            # Detect anomalies
            anomalies = monitor.detect_anomalies(data_with_indicators)
            
            # Generate report
            report = monitor.generate_analysis_report(data_with_indicators, company_info, anomalies)
            
            print(json.dumps(report, indent=2, default=str))
            
            if args.save:
                monitor.save_data(data_with_indicators, args.symbol)
                monitor.create_price_chart(data_with_indicators, args.symbol)
                print(f"Data and charts saved to {monitor.data_dir}")
        
    except Exception as e:
        logger.error(f"Bosch stock monitor failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
