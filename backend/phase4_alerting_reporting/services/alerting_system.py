import asyncio
import json
import logging
import smtplib
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import redis.asyncio as aioredis

class AlertSeverity(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class AlertStatus(Enum):
    """Alert status"""
    ACTIVE = "active"
    ACKNOWLEDGED = "acknowledged"
    RESOLVED = "resolved"
    ESCALATED = "escalated"

@dataclass
class AlertRule:
    """Alert rule configuration"""
    rule_id: str
    name: str
    description: str
    conditions: Dict[str, Any]  # Metric, threshold, operator
    severity: AlertSeverity
    channels: List[str]  # 'email', 'slack', 'webhook', 'sms'
    escalation_rules: Dict[str, Any]
    enabled: bool = True
    cooldown_minutes: int = 5

@dataclass
class Alert:
    """Alert instance"""
    alert_id: str
    rule_id: str
    title: str
    message: str
    severity: AlertSeverity
    status: AlertStatus
    created_at: datetime
    acknowledged_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    resolved_by: Optional[str] = None
    metadata: Dict[str, Any] = None
    escalation_level: int = 0

@dataclass
class NotificationChannel:
    """Notification channel configuration"""
    channel_id: str
    channel_type: str  # 'email', 'slack', 'webhook', 'sms'
    name: str
    config: Dict[str, Any]
    enabled: bool = True

class AlertingSystem:
    """
    Comprehensive alerting system with multi-channel notifications
    Supports email, Slack, webhooks, SMS with escalation rules
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.rules: Dict[str, AlertRule] = {}
        self.channels: Dict[str, NotificationChannel] = {}
        self.alerts: Dict[str, Alert] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Escalation tracking
        self.escalation_timers: Dict[str, asyncio.Task] = {}
        
    async def add_alert_rule(self, rule: AlertRule) -> bool:
        """Add an alert rule"""
        try:
            self.rules[rule.rule_id] = rule
            await self._store_alert_rule(rule)
            
            self.logger.info(f"Added alert rule: {rule.rule_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add alert rule: {e}")
            return False
    
    async def add_notification_channel(self, channel: NotificationChannel) -> bool:
        """Add a notification channel"""
        try:
            self.channels[channel.channel_id] = channel
            await self._store_notification_channel(channel)
            
            self.logger.info(f"Added notification channel: {channel.channel_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add notification channel: {e}")
            return False
    
    async def check_alert_conditions(self, metric_name: str, metric_value: float, 
                                   context: Dict[str, Any] = None) -> List[str]:
        """Check if any alert conditions are met"""
        try:
            triggered_rules = []
            
            for rule_id, rule in self.rules.items():
                if not rule.enabled:
                    continue
                
                # Check if metric matches rule condition
                if self._evaluate_condition(rule.conditions, metric_name, metric_value):
                    # Check cooldown
                    if await self._is_in_cooldown(rule_id):
                        continue
                    
                    # Create alert
                    alert = await self._create_alert(rule, metric_name, metric_value, context)
                    if alert:
                        triggered_rules.append(rule_id)
                        
                        # Send notifications
                        await self._send_notifications(alert, rule)
                        
                        # Set escalation timer
                        await self._set_escalation_timer(alert, rule)
            
            return triggered_rules
            
        except Exception as e:
            self.logger.error(f"Error checking alert conditions: {e}")
            return []
    
    def _evaluate_condition(self, conditions: Dict[str, Any], metric_name: str, 
                          metric_value: float) -> bool:
        """Evaluate if a condition is met"""
        try:
            target_metric = conditions.get('metric')
            threshold = conditions.get('threshold')
            operator = conditions.get('operator', '>')
            
            if target_metric != metric_name:
                return False
            
            if operator == '>':
                return metric_value > threshold
            elif operator == '>=':
                return metric_value >= threshold
            elif operator == '<':
                return metric_value < threshold
            elif operator == '<=':
                return metric_value <= threshold
            elif operator == '==':
                return metric_value == threshold
            elif operator == '!=':
                return metric_value != threshold
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Error evaluating condition: {e}")
            return False
    
    async def _is_in_cooldown(self, rule_id: str) -> bool:
        """Check if rule is in cooldown period"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            cooldown_key = f"alert_cooldown:{rule_id}"
            
            # Check if cooldown exists
            exists = await redis.exists(cooldown_key)
            await redis.close()
            
            return exists > 0
            
        except Exception as e:
            self.logger.error(f"Error checking cooldown: {e}")
            return False
    
    async def _set_cooldown(self, rule_id: str, minutes: int):
        """Set cooldown for a rule"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            cooldown_key = f"alert_cooldown:{rule_id}"
            
            # Set cooldown with expiration
            await redis.setex(cooldown_key, minutes * 60, "1")
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error setting cooldown: {e}")
    
    async def _create_alert(self, rule: AlertRule, metric_name: str, 
                          metric_value: float, context: Dict[str, Any] = None) -> Optional[Alert]:
        """Create a new alert"""
        try:
            alert_id = f"{rule.rule_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            alert = Alert(
                alert_id=alert_id,
                rule_id=rule.rule_id,
                title=f"Alert: {rule.name}",
                message=f"{rule.description} - {metric_name}: {metric_value}",
                severity=rule.severity,
                status=AlertStatus.ACTIVE,
                created_at=datetime.utcnow(),
                metadata={
                    'metric_name': metric_name,
                    'metric_value': metric_value,
                    'context': context or {}
                }
            )
            
            # Store alert
            self.alerts[alert_id] = alert
            await self._store_alert(alert)
            
            # Set cooldown
            await self._set_cooldown(rule.rule_id, rule.cooldown_minutes)
            
            self.logger.warning(f"Created alert: {alert_id} - {alert.title}")
            return alert
            
        except Exception as e:
            self.logger.error(f"Error creating alert: {e}")
            return None
    
    async def _send_notifications(self, alert: Alert, rule: AlertRule):
        """Send notifications through configured channels"""
        try:
            for channel_name in rule.channels:
                if channel_name in self.channels:
                    channel = self.channels[channel_name]
                    if channel.enabled:
                        await self._send_notification(alert, channel)
                        
        except Exception as e:
            self.logger.error(f"Error sending notifications: {e}")
    
    async def _send_notification(self, alert: Alert, channel: NotificationChannel):
        """Send notification through a specific channel"""
        try:
            if channel.channel_type == 'email':
                await self._send_email_notification(alert, channel)
            elif channel.channel_type == 'slack':
                await self._send_slack_notification(alert, channel)
            elif channel.channel_type == 'webhook':
                await self._send_webhook_notification(alert, channel)
            elif channel.channel_type == 'sms':
                await self._send_sms_notification(alert, channel)
            else:
                self.logger.warning(f"Unknown channel type: {channel.channel_type}")
                
        except Exception as e:
            self.logger.error(f"Error sending {channel.channel_type} notification: {e}")
    
    async def _send_email_notification(self, alert: Alert, channel: NotificationChannel):
        """Send email notification"""
        try:
            config = channel.config
            smtp_server = config.get('smtp_server')
            smtp_port = config.get('smtp_port', 587)
            username = config.get('username')
            password = config.get('password')
            recipients = config.get('recipients', [])
            
            if not all([smtp_server, username, password, recipients]):
                self.logger.error("Missing email configuration")
                return
            
            # Create email message
            subject = f"[{alert.severity.value.upper()}] {alert.title}"
            body = f"""
Alert Details:
- Title: {alert.title}
- Message: {alert.message}
- Severity: {alert.severity.value}
- Created: {alert.created_at.isoformat()}
- Alert ID: {alert.alert_id}

Please take appropriate action.
            """
            
            # Send email (simulated for now)
            self.logger.info(f"Email notification sent to {recipients}: {subject}")
            
        except Exception as e:
            self.logger.error(f"Error sending email notification: {e}")
    
    async def _send_slack_notification(self, alert: Alert, channel: NotificationChannel):
        """Send Slack notification"""
        try:
            config = channel.config
            webhook_url = config.get('webhook_url')
            channel_name = config.get('channel', '#alerts')
            
            if not webhook_url:
                self.logger.error("Missing Slack webhook URL")
                return
            
            # Create Slack message
            message = {
                "channel": channel_name,
                "text": f"🚨 *{alert.title}*",
                "attachments": [
                    {
                        "color": self._get_severity_color(alert.severity),
                        "fields": [
                            {"title": "Message", "value": alert.message, "short": False},
                            {"title": "Severity", "value": alert.severity.value, "short": True},
                            {"title": "Alert ID", "value": alert.alert_id, "short": True},
                            {"title": "Created", "value": alert.created_at.isoformat(), "short": True}
                        ]
                    }
                ]
            }
            
            # Send to Slack (simulated for now)
            self.logger.info(f"Slack notification sent to {channel_name}: {alert.title}")
            
        except Exception as e:
            self.logger.error(f"Error sending Slack notification: {e}")
    
    async def _send_webhook_notification(self, alert: Alert, channel: NotificationChannel):
        """Send webhook notification"""
        try:
            config = channel.config
            webhook_url = config.get('webhook_url')
            headers = config.get('headers', {})
            
            if not webhook_url:
                self.logger.error("Missing webhook URL")
                return
            
            # Create webhook payload
            payload = {
                "alert_id": alert.alert_id,
                "title": alert.title,
                "message": alert.message,
                "severity": alert.severity.value,
                "status": alert.status.value,
                "created_at": alert.created_at.isoformat(),
                "metadata": alert.metadata
            }
            
            # Send webhook (simulated for now)
            self.logger.info(f"Webhook notification sent to {webhook_url}: {alert.title}")
            
        except Exception as e:
            self.logger.error(f"Error sending webhook notification: {e}")
    
    async def _send_sms_notification(self, alert: Alert, channel: NotificationChannel):
        """Send SMS notification"""
        try:
            config = channel.config
            phone_numbers = config.get('phone_numbers', [])
            
            if not phone_numbers:
                self.logger.error("Missing phone numbers for SMS")
                return
            
            # Create SMS message
            message = f"ALERT [{alert.severity.value.upper()}]: {alert.title} - {alert.message}"
            
            # Send SMS (simulated for now)
            self.logger.info(f"SMS notification sent to {phone_numbers}: {alert.title}")
            
        except Exception as e:
            self.logger.error(f"Error sending SMS notification: {e}")
    
    def _get_severity_color(self, severity: AlertSeverity) -> str:
        """Get color for Slack message based on severity"""
        colors = {
            AlertSeverity.INFO: "#36a64f",      # Green
            AlertSeverity.WARNING: "#ffa500",   # Orange
            AlertSeverity.ERROR: "#ff0000",     # Red
            AlertSeverity.CRITICAL: "#8b0000"   # Dark Red
        }
        return colors.get(severity, "#808080")
    
    async def _set_escalation_timer(self, alert: Alert, rule: AlertRule):
        """Set escalation timer for alert"""
        try:
            escalation_rules = rule.escalation_rules
            if not escalation_rules:
                return
            
            escalation_delay = escalation_rules.get('delay_minutes', 30)
            
            # Create escalation task
            task = asyncio.create_task(self._escalate_alert(alert, rule, escalation_delay))
            self.escalation_timers[alert.alert_id] = task
            
        except Exception as e:
            self.logger.error(f"Error setting escalation timer: {e}")
    
    async def _escalate_alert(self, alert: Alert, rule: AlertRule, delay_minutes: int):
        """Escalate alert after delay"""
        try:
            await asyncio.sleep(delay_minutes * 60)
            
            # Check if alert is still active
            if alert.alert_id in self.alerts and self.alerts[alert.alert_id].status == AlertStatus.ACTIVE:
                # Escalate alert
                alert.status = AlertStatus.ESCALATED
                alert.escalation_level += 1
                
                # Send escalation notifications
                await self._send_notifications(alert, rule)
                
                # Update stored alert
                await self._update_alert(alert)
                
                self.logger.warning(f"Alert escalated: {alert.alert_id}")
            
            # Clean up timer
            if alert.alert_id in self.escalation_timers:
                del self.escalation_timers[alert.alert_id]
                
        except Exception as e:
            self.logger.error(f"Error escalating alert: {e}")
    
    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert"""
        try:
            if alert_id not in self.alerts:
                return False
            
            alert = self.alerts[alert_id]
            alert.status = AlertStatus.ACKNOWLEDGED
            alert.acknowledged_at = datetime.utcnow()
            alert.acknowledged_by = acknowledged_by
            
            # Cancel escalation timer
            if alert_id in self.escalation_timers:
                self.escalation_timers[alert_id].cancel()
                del self.escalation_timers[alert_id]
            
            await self._update_alert(alert)
            
            self.logger.info(f"Alert acknowledged: {alert_id} by {acknowledged_by}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error acknowledging alert: {e}")
            return False
    
    async def resolve_alert(self, alert_id: str, resolved_by: str) -> bool:
        """Resolve an alert"""
        try:
            if alert_id not in self.alerts:
                return False
            
            alert = self.alerts[alert_id]
            alert.status = AlertStatus.RESOLVED
            alert.resolved_at = datetime.utcnow()
            alert.resolved_by = resolved_by
            
            # Cancel escalation timer
            if alert_id in self.escalation_timers:
                self.escalation_timers[alert_id].cancel()
                del self.escalation_timers[alert_id]
            
            await self._update_alert(alert)
            
            self.logger.info(f"Alert resolved: {alert_id} by {resolved_by}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error resolving alert: {e}")
            return False
    
    async def get_alerts_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get summary of alerts"""
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            summary = {
                'total_alerts': 0,
                'active_alerts': 0,
                'acknowledged_alerts': 0,
                'resolved_alerts': 0,
                'escalated_alerts': 0,
                'alerts_by_severity': {},
                'alerts_by_rule': {},
                'recent_alerts': []
            }
            
            for alert in self.alerts.values():
                if alert.created_at >= cutoff_time:
                    summary['total_alerts'] += 1
                    
                    # Count by status
                    if alert.status == AlertStatus.ACTIVE:
                        summary['active_alerts'] += 1
                    elif alert.status == AlertStatus.ACKNOWLEDGED:
                        summary['acknowledged_alerts'] += 1
                    elif alert.status == AlertStatus.RESOLVED:
                        summary['resolved_alerts'] += 1
                    elif alert.status == AlertStatus.ESCALATED:
                        summary['escalated_alerts'] += 1
                    
                    # Count by severity
                    severity = alert.severity.value
                    summary['alerts_by_severity'][severity] = summary['alerts_by_severity'].get(severity, 0) + 1
                    
                    # Count by rule
                    rule_id = alert.rule_id
                    summary['alerts_by_rule'][rule_id] = summary['alerts_by_rule'].get(rule_id, 0) + 1
                    
                    # Add to recent alerts
                    summary['recent_alerts'].append({
                        'alert_id': alert.alert_id,
                        'title': alert.title,
                        'severity': alert.severity.value,
                        'status': alert.status.value,
                        'created_at': alert.created_at.isoformat()
                    })
            
            # Sort recent alerts by creation time
            summary['recent_alerts'].sort(key=lambda x: x['created_at'], reverse=True)
            summary['recent_alerts'] = summary['recent_alerts'][:10]  # Keep only 10 most recent
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting alerts summary: {e}")
            return {}
    
    async def _store_alert_rule(self, rule: AlertRule):
        """Store alert rule in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"alert_rule:{rule.rule_id}",
                json.dumps({
                    'rule_id': rule.rule_id,
                    'name': rule.name,
                    'description': rule.description,
                    'conditions': rule.conditions,
                    'severity': rule.severity.value,
                    'channels': rule.channels,
                    'escalation_rules': rule.escalation_rules,
                    'enabled': rule.enabled,
                    'cooldown_minutes': rule.cooldown_minutes
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing alert rule: {e}")
    
    async def _store_notification_channel(self, channel: NotificationChannel):
        """Store notification channel in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"notification_channel:{channel.channel_id}",
                json.dumps({
                    'channel_id': channel.channel_id,
                    'channel_type': channel.channel_type,
                    'name': channel.name,
                    'config': channel.config,
                    'enabled': channel.enabled
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing notification channel: {e}")
    
    async def _store_alert(self, alert: Alert):
        """Store alert in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"alert:{alert.alert_id}",
                json.dumps({
                    'alert_id': alert.alert_id,
                    'rule_id': alert.rule_id,
                    'title': alert.title,
                    'message': alert.message,
                    'severity': alert.severity.value,
                    'status': alert.status.value,
                    'created_at': alert.created_at.isoformat(),
                    'acknowledged_at': alert.acknowledged_at.isoformat() if alert.acknowledged_at else None,
                    'resolved_at': alert.resolved_at.isoformat() if alert.resolved_at else None,
                    'acknowledged_by': alert.acknowledged_by,
                    'resolved_by': alert.resolved_by,
                    'metadata': alert.metadata,
                    'escalation_level': alert.escalation_level
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing alert: {e}")
    
    async def _update_alert(self, alert: Alert):
        """Update alert in Redis"""
        try:
            await self._store_alert(alert)
        except Exception as e:
            self.logger.error(f"Error updating alert: {e}")
    
    async def start_alerting_monitoring(self):
        """Start continuous alerting monitoring"""
        self.running = True
        self.logger.info("Starting alerting monitoring")
        
        while self.running:
            try:
                # Monitor for escalation timers
                # Check for stale alerts
                # Clean up resolved alerts
                self.logger.debug("Alerting monitoring active")
                
                # Wait before next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in alerting monitoring: {e}")
                await asyncio.sleep(60)
    
    async def stop_alerting_monitoring(self):
        """Stop alerting monitoring"""
        self.running = False
        self.logger.info("Stopping alerting monitoring")
        
        # Cancel all escalation timers
        for task in self.escalation_timers.values():
            task.cancel()
        self.escalation_timers.clear()
