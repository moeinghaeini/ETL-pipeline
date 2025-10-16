#!/usr/bin/env python3
"""
Alerting System for ETL Pipeline
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum

import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/alerts.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class AlertSeverity(Enum):
    """Alert severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

class AlertType(Enum):
    """Alert types"""
    DATA_QUALITY = "data_quality"
    PIPELINE_FAILURE = "pipeline_failure"
    PERFORMANCE = "performance"
    SECURITY = "security"
    SYSTEM = "system"

@dataclass
class Alert:
    """Alert data structure"""
    type: AlertType
    severity: AlertSeverity
    title: str
    message: str
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None
    recipients: Optional[List[str]] = None

class AlertManager:
    """Comprehensive alerting system"""
    
    def __init__(self):
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_username = os.getenv('SMTP_USERNAME')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL')
        self.teams_webhook_url = os.getenv('TEAMS_WEBHOOK_URL')
        self.pagerduty_integration_key = os.getenv('PAGERDUTY_INTEGRATION_KEY')
        
        # Default recipients
        self.default_recipients = {
            AlertSeverity.LOW: os.getenv('ALERT_EMAIL_LOW', '').split(','),
            AlertSeverity.MEDIUM: os.getenv('ALERT_EMAIL_MEDIUM', '').split(','),
            AlertSeverity.HIGH: os.getenv('ALERT_EMAIL_HIGH', '').split(','),
            AlertSeverity.CRITICAL: os.getenv('ALERT_EMAIL_CRITICAL', '').split(',')
        }
        
        # Clean up empty strings
        for severity in self.default_recipients:
            self.default_recipients[severity] = [
                email.strip() for email in self.default_recipients[severity] 
                if email.strip()
            ]
    
    def send_email_alert(self, alert: Alert) -> bool:
        """Send email alert"""
        try:
            if not self.smtp_username or not self.smtp_password:
                logger.warning("SMTP credentials not configured, skipping email alert")
                return False
            
            recipients = alert.recipients or self.default_recipients.get(alert.severity, [])
            if not recipients:
                logger.warning(f"No recipients configured for severity {alert.severity.value}")
                return False
            
            # Create message
            msg = MIMEMultipart()
            msg['From'] = self.smtp_username
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = f"[{alert.severity.value.upper()}] {alert.title}"
            
            # Create email body
            body = f"""
            Alert Type: {alert.type.value}
            Severity: {alert.severity.value.upper()}
            Timestamp: {alert.timestamp.isoformat()}
            
            {alert.message}
            
            """
            
            if alert.metadata:
                body += "\nMetadata:\n"
                for key, value in alert.metadata.items():
                    body += f"  {key}: {value}\n"
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Send email
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.starttls()
            server.login(self.smtp_username, self.smtp_password)
            text = msg.as_string()
            server.sendmail(self.smtp_username, recipients, text)
            server.quit()
            
            logger.info(f"Email alert sent successfully to {len(recipients)} recipients")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {e}")
            return False
    
    def send_slack_alert(self, alert: Alert) -> bool:
        """Send Slack alert"""
        try:
            if not self.slack_webhook_url:
                logger.warning("Slack webhook URL not configured, skipping Slack alert")
                return False
            
            # Determine color based on severity
            color_map = {
                AlertSeverity.LOW: "#36a64f",      # Green
                AlertSeverity.MEDIUM: "#ffaa00",    # Yellow
                AlertSeverity.HIGH: "#ff6600",      # Orange
                AlertSeverity.CRITICAL: "#ff0000"   # Red
            }
            
            payload = {
                "attachments": [
                    {
                        "color": color_map.get(alert.severity, "#36a64f"),
                        "title": alert.title,
                        "text": alert.message,
                        "fields": [
                            {
                                "title": "Type",
                                "value": alert.type.value,
                                "short": True
                            },
                            {
                                "title": "Severity",
                                "value": alert.severity.value.upper(),
                                "short": True
                            },
                            {
                                "title": "Timestamp",
                                "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC"),
                                "short": True
                            }
                        ],
                        "footer": "ETL Pipeline Monitoring",
                        "ts": int(alert.timestamp.timestamp())
                    }
                ]
            }
            
            if alert.metadata:
                for key, value in alert.metadata.items():
                    payload["attachments"][0]["fields"].append({
                        "title": key.replace('_', ' ').title(),
                        "value": str(value),
                        "short": True
                    })
            
            response = requests.post(self.slack_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info("Slack alert sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Slack alert: {e}")
            return False
    
    def send_teams_alert(self, alert: Alert) -> bool:
        """Send Microsoft Teams alert"""
        try:
            if not self.teams_webhook_url:
                logger.warning("Teams webhook URL not configured, skipping Teams alert")
                return False
            
            # Determine color based on severity
            color_map = {
                AlertSeverity.LOW: "00ff00",        # Green
                AlertSeverity.MEDIUM: "ffaa00",     # Yellow
                AlertSeverity.HIGH: "ff6600",       # Orange
                AlertSeverity.CRITICAL: "ff0000"    # Red
            }
            
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "themeColor": color_map.get(alert.severity, "00ff00"),
                "summary": alert.title,
                "sections": [
                    {
                        "activityTitle": alert.title,
                        "activitySubtitle": f"Type: {alert.type.value} | Severity: {alert.severity.value.upper()}",
                        "activityImage": "https://img.icons8.com/color/48/000000/warning-shield.png",
                        "text": alert.message,
                        "facts": [
                            {
                                "name": "Timestamp",
                                "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S UTC")
                            }
                        ]
                    }
                ]
            }
            
            if alert.metadata:
                for key, value in alert.metadata.items():
                    payload["sections"][0]["facts"].append({
                        "name": key.replace('_', ' ').title(),
                        "value": str(value)
                    })
            
            response = requests.post(self.teams_webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info("Teams alert sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send Teams alert: {e}")
            return False
    
    def send_pagerduty_alert(self, alert: Alert) -> bool:
        """Send PagerDuty alert for critical issues"""
        try:
            if not self.pagerduty_integration_key:
                logger.warning("PagerDuty integration key not configured, skipping PagerDuty alert")
                return False
            
            # Only send to PagerDuty for high/critical severity
            if alert.severity not in [AlertSeverity.HIGH, AlertSeverity.CRITICAL]:
                return True
            
            payload = {
                "routing_key": self.pagerduty_integration_key,
                "event_action": "trigger",
                "dedup_key": f"{alert.type.value}_{alert.severity.value}",
                "payload": {
                    "summary": alert.title,
                    "source": "ETL Pipeline",
                    "severity": alert.severity.value,
                    "custom_details": {
                        "message": alert.message,
                        "type": alert.type.value,
                        "timestamp": alert.timestamp.isoformat(),
                        **alert.metadata or {}
                    }
                }
            }
            
            response = requests.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            response.raise_for_status()
            
            logger.info("PagerDuty alert sent successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send PagerDuty alert: {e}")
            return False
    
    def send_alert(self, alert: Alert) -> Dict[str, bool]:
        """Send alert through all configured channels"""
        results = {
            'email': self.send_email_alert(alert),
            'slack': self.send_slack_alert(alert),
            'teams': self.send_teams_alert(alert),
            'pagerduty': self.send_pagerduty_alert(alert)
        }
        
        # Log overall result
        successful_channels = [channel for channel, success in results.items() if success]
        if successful_channels:
            logger.info(f"Alert sent successfully via: {', '.join(successful_channels)}")
        else:
            logger.error("Failed to send alert through any channel")
        
        return results
    
    def create_data_quality_alert(self, check_results: Dict[str, Any]) -> Alert:
        """Create data quality alert from check results"""
        failed_checks = []
        
        for category, checks in check_results.items():
            if category in ['freshness', 'volume', 'quality']:
                for check_name, result in checks.items():
                    if result.get('status') == 'FAIL' or not result.get('success', True):
                        failed_checks.append(f"{category}: {check_name}")
        
        if failed_checks:
            severity = AlertSeverity.HIGH if len(failed_checks) > 3 else AlertSeverity.MEDIUM
            title = f"Data Quality Issues Detected ({len(failed_checks)} failures)"
            message = f"The following data quality checks failed:\n" + "\n".join(f"- {check}" for check in failed_checks)
        else:
            severity = AlertSeverity.LOW
            title = "Data Quality Checks Passed"
            message = "All data quality checks completed successfully"
        
        return Alert(
            type=AlertType.DATA_QUALITY,
            severity=severity,
            title=title,
            message=message,
            timestamp=datetime.now(),
            metadata=check_results
        )
    
    def create_pipeline_failure_alert(self, error_message: str, dag_id: str, task_id: str) -> Alert:
        """Create pipeline failure alert"""
        return Alert(
            type=AlertType.PIPELINE_FAILURE,
            severity=AlertSeverity.HIGH,
            title=f"Pipeline Failure: {dag_id}",
            message=f"Task '{task_id}' failed with error: {error_message}",
            timestamp=datetime.now(),
            metadata={
                'dag_id': dag_id,
                'task_id': task_id,
                'error_message': error_message
            }
        )

def main():
    """Main execution function for command line usage"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Send ETL Pipeline Alerts')
    parser.add_argument('--type', required=True, choices=[t.value for t in AlertType],
                       help='Alert type')
    parser.add_argument('--severity', required=True, choices=[s.value for s in AlertSeverity],
                       help='Alert severity')
    parser.add_argument('--title', required=True, help='Alert title')
    parser.add_argument('--message', required=True, help='Alert message')
    parser.add_argument('--metadata', help='JSON metadata')
    
    args = parser.parse_args()
    
    try:
        alert_manager = AlertManager()
        
        alert = Alert(
            type=AlertType(args.type),
            severity=AlertSeverity(args.severity),
            title=args.title,
            message=args.message,
            timestamp=datetime.now(),
            metadata=json.loads(args.metadata) if args.metadata else None
        )
        
        results = alert_manager.send_alert(alert)
        
        if any(results.values()):
            logger.info("Alert sent successfully")
            sys.exit(0)
        else:
            logger.error("Failed to send alert")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
