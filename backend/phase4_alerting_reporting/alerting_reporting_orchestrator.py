import asyncio
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import redis.asyncio as aioredis

from services.alerting_system import AlertingSystem, AlertRule, AlertSeverity, NotificationChannel
from services.reporting_analytics import ReportingAnalyticsSystem, ReportConfig, ReportType, ReportFormat, AnalyticsQuery
from services.root_cause_analysis import RootCauseAnalysisSystem, RCATrigger, RCASeverity

@dataclass
class Phase4Config:
    """Configuration for Phase 4 systems"""
    alerting_enabled: bool = True
    reporting_enabled: bool = True
    rca_enabled: bool = True
    monitoring_interval: int = 300  # seconds
    integration_enabled: bool = True

@dataclass
class Phase4Result:
    """Result of Phase 4 operations"""
    timestamp: datetime
    alerting_status: Dict[str, Any]
    reporting_status: Dict[str, Any]
    rca_status: Dict[str, Any]
    integration_status: Dict[str, Any]
    processing_time: float
    success: bool
    errors: List[str]

class AlertingReportingOrchestrator:
    """
    Main orchestrator for Phase 4 Alerting, Reporting & Analytics
    Coordinates alerting system, reporting analytics, and root cause analysis
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.config = Phase4Config()
        self.alerting_system = AlertingSystem(redis_url)
        self.reporting_system = ReportingAnalyticsSystem(redis_url)
        self.rca_system = RootCauseAnalysisSystem(redis_url)
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Phase 4 history
        self.phase4_history: List[Phase4Result] = []
        
    async def setup_default_configurations(self):
        """Setup default configurations for all systems"""
        try:
            # Setup default alert rules
            await self._setup_default_alert_rules()
            
            # Setup default notification channels
            await self._setup_default_notification_channels()
            
            # Setup default report configurations
            await self._setup_default_report_configs()
            
            # Setup default analytics queries
            await self._setup_default_analytics_queries()
            
            # Setup default RCA triggers
            await self._setup_default_rca_triggers()
            
            self.logger.info("Setup default configurations for Phase 4 systems")
            
        except Exception as e:
            self.logger.error(f"Error setting up default configurations: {e}")
    
    async def _setup_default_alert_rules(self):
        """Setup default alert rules"""
        try:
            # Performance degradation alert
            performance_alert = AlertRule(
                rule_id="performance_degradation",
                name="Performance Degradation",
                description="Alert when model performance drops below threshold",
                conditions={
                    'metric': 'accuracy',
                    'threshold': 0.85,
                    'operator': '<'
                },
                severity=AlertSeverity.WARNING,
                channels=['email', 'slack'],
                escalation_rules={'delay_minutes': 30},
                cooldown_minutes=10
            )
            await self.alerting_system.add_alert_rule(performance_alert)
            
            # High latency alert
            latency_alert = AlertRule(
                rule_id="high_latency",
                name="High Latency",
                description="Alert when model latency exceeds threshold",
                conditions={
                    'metric': 'latency_ms',
                    'threshold': 200,
                    'operator': '>'
                },
                severity=AlertSeverity.ERROR,
                channels=['email', 'slack', 'webhook'],
                escalation_rules={'delay_minutes': 15},
                cooldown_minutes=5
            )
            await self.alerting_system.add_alert_rule(latency_alert)
            
            # Data drift alert
            drift_alert = AlertRule(
                rule_id="data_drift",
                name="Data Drift Detected",
                description="Alert when significant data drift is detected",
                conditions={
                    'metric': 'drift_score',
                    'threshold': 0.1,
                    'operator': '>'
                },
                severity=AlertSeverity.WARNING,
                channels=['email', 'slack'],
                escalation_rules={'delay_minutes': 60},
                cooldown_minutes=30
            )
            await self.alerting_system.add_alert_rule(drift_alert)
            
        except Exception as e:
            self.logger.error(f"Error setting up default alert rules: {e}")
    
    async def _setup_default_notification_channels(self):
        """Setup default notification channels"""
        try:
            # Email channel
            email_channel = NotificationChannel(
                channel_id="email_alerts",
                channel_type="email",
                name="Email Alerts",
                config={
                    'smtp_server': 'smtp.gmail.com',
                    'smtp_port': 587,
                    'username': 'alerts@company.com',
                    'password': 'password',
                    'recipients': ['admin@company.com', 'ml-team@company.com']
                }
            )
            await self.alerting_system.add_notification_channel(email_channel)
            
            # Slack channel
            slack_channel = NotificationChannel(
                channel_id="slack_alerts",
                channel_type="slack",
                name="Slack Alerts",
                config={
                    'webhook_url': 'https://hooks.slack.com/services/xxx/yyy/zzz',
                    'channel': '#ml-alerts'
                }
            )
            await self.alerting_system.add_notification_channel(slack_channel)
            
            # Webhook channel
            webhook_channel = NotificationChannel(
                channel_id="webhook_alerts",
                channel_type="webhook",
                name="Webhook Alerts",
                config={
                    'webhook_url': 'https://api.company.com/alerts',
                    'headers': {'Authorization': 'Bearer token'}
                }
            )
            await self.alerting_system.add_notification_channel(webhook_channel)
            
        except Exception as e:
            self.logger.error(f"Error setting up default notification channels: {e}")
    
    async def _setup_default_report_configs(self):
        """Setup default report configurations"""
        try:
            # Daily performance report
            daily_performance_report = ReportConfig(
                report_id="daily_performance",
                name="Daily Performance Report",
                description="Daily summary of model performance metrics",
                report_type=ReportType.PERFORMANCE,
                data_sources=['performance_metrics', 'quality_metrics'],
                metrics=['accuracy', 'latency_ms', 'throughput_rps', 'drift_score'],
                time_range={'hours': 24},
                schedule={'frequency': 'daily', 'time': '09:00'},
                recipients=['ml-team@company.com'],
                format=ReportFormat.HTML
            )
            await self.reporting_system.add_report_config(daily_performance_report)
            
            # Weekly comprehensive report
            weekly_report = ReportConfig(
                report_id="weekly_comprehensive",
                name="Weekly Comprehensive Report",
                description="Weekly comprehensive analysis of all metrics",
                report_type=ReportType.CUSTOM,
                data_sources=['performance_metrics', 'quality_metrics', 'alerts', 'drift_metrics'],
                metrics=['accuracy', 'latency_ms', 'throughput_rps', 'drift_score', 'alerts_count'],
                time_range={'hours': 168},  # 7 days
                schedule={'frequency': 'weekly', 'time': '10:00'},
                recipients=['management@company.com', 'ml-team@company.com'],
                format=ReportFormat.PDF
            )
            await self.reporting_system.add_report_config(weekly_report)
            
            # Monthly executive report
            monthly_report = ReportConfig(
                report_id="monthly_executive",
                name="Monthly Executive Report",
                description="Monthly executive summary for stakeholders",
                report_type=ReportType.CUSTOM,
                data_sources=['performance_metrics', 'alerts', 'model_metrics'],
                metrics=['accuracy', 'latency_ms', 'alerts_count', 'deployments'],
                time_range={'hours': 720},  # 30 days
                schedule={'frequency': 'monthly', 'time': '11:00'},
                recipients=['executives@company.com'],
                format=ReportFormat.PDF
            )
            await self.reporting_system.add_report_config(monthly_report)
            
        except Exception as e:
            self.logger.error(f"Error setting up default report configs: {e}")
    
    async def _setup_default_analytics_queries(self):
        """Setup default analytics queries"""
        try:
            # Accuracy trend analysis
            accuracy_trend = AnalyticsQuery(
                query_id="accuracy_trend",
                name="Accuracy Trend Analysis",
                query_type="trend",
                parameters={
                    'metric': 'accuracy',
                    'window_size': 20
                },
                time_range={'hours': 24}
            )
            await self.reporting_system.add_analytics_query(accuracy_trend)
            
            # Latency correlation analysis
            latency_correlation = AnalyticsQuery(
                query_id="latency_correlation",
                name="Latency Correlation Analysis",
                query_type="correlation",
                parameters={
                    'metric1': 'latency_ms',
                    'metric2': 'throughput_rps'
                },
                time_range={'hours': 24}
            )
            await self.reporting_system.add_analytics_query(latency_correlation)
            
            # Performance aggregation
            performance_agg = AnalyticsQuery(
                query_id="performance_aggregation",
                name="Performance Aggregation",
                query_type="aggregation",
                parameters={
                    'metric': 'accuracy',
                    'aggregation': 'mean'
                },
                time_range={'hours': 24}
            )
            await self.reporting_system.add_analytics_query(performance_agg)
            
        except Exception as e:
            self.logger.error(f"Error setting up default analytics queries: {e}")
    
    async def _setup_default_rca_triggers(self):
        """Setup default RCA triggers"""
        try:
            # Critical performance degradation
            critical_performance_trigger = RCATrigger(
                trigger_id="critical_performance",
                name="Critical Performance Degradation",
                description="Trigger RCA for critical performance issues",
                trigger_conditions={
                    'type': 'threshold',
                    'metric': 'accuracy',
                    'threshold': 0.75,
                    'operator': '<'
                },
                severity=RCASeverity.CRITICAL
            )
            await self.rca_system.add_rca_trigger(critical_performance_trigger)
            
            # Sustained high latency
            sustained_latency_trigger = RCATrigger(
                trigger_id="sustained_latency",
                name="Sustained High Latency",
                description="Trigger RCA for sustained high latency",
                trigger_conditions={
                    'type': 'trend',
                    'metric': 'latency_ms',
                    'trend_direction': 'increasing',
                    'window_size': 10,
                    'threshold': 0.1
                },
                severity=RCASeverity.HIGH
            )
            await self.rca_system.add_rca_trigger(sustained_latency_trigger)
            
            # Multiple alert correlation
            multiple_alerts_trigger = RCATrigger(
                trigger_id="multiple_alerts",
                name="Multiple Alerts Correlation",
                description="Trigger RCA when multiple alerts occur simultaneously",
                trigger_conditions={
                    'type': 'compound',
                    'logic': 'AND',
                    'conditions': [
                        {'type': 'threshold', 'metric': 'alerts_count', 'threshold': 5, 'operator': '>'},
                        {'type': 'threshold', 'metric': 'accuracy', 'threshold': 0.8, 'operator': '<'}
                    ]
                },
                severity=RCASeverity.MEDIUM
            )
            await self.rca_system.add_rca_trigger(multiple_alerts_trigger)
            
        except Exception as e:
            self.logger.error(f"Error setting up default RCA triggers: {e}")
    
    async def process_metrics(self, metrics: Dict[str, Any], context: Dict[str, Any] = None) -> Phase4Result:
        """Process metrics through all Phase 4 systems"""
        start_time = datetime.utcnow()
        errors = []
        
        try:
            alerting_status = {}
            reporting_status = {}
            rca_status = {}
            integration_status = {}
            
            # Process through alerting system
            if self.config.alerting_enabled:
                try:
                    triggered_rules = await self.alerting_system.check_alert_conditions(
                        'accuracy', metrics.get('accuracy', 0.0), context
                    )
                    alerting_status = {
                        'triggered_rules': triggered_rules,
                        'total_rules': len(self.alerting_system.rules),
                        'active_alerts': len(self.alerting_system.alerts)
                    }
                except Exception as e:
                    errors.append(f"Alerting error: {e}")
                    alerting_status = {'error': str(e)}
            
            # Process through RCA system
            if self.config.rca_enabled:
                try:
                    triggered_investigations = await self.rca_system.check_rca_triggers(metrics, context)
                    rca_status = {
                        'triggered_investigations': triggered_investigations,
                        'total_triggers': len(self.rca_system.triggers),
                        'active_investigations': len(self.rca_system.investigations)
                    }
                except Exception as e:
                    errors.append(f"RCA error: {e}")
                    rca_status = {'error': str(e)}
            
            # Generate reports if scheduled
            if self.config.reporting_enabled:
                try:
                    # Check if any reports should be generated
                    reports_generated = await self._check_scheduled_reports()
                    reporting_status = {
                        'reports_generated': reports_generated,
                        'total_configs': len(self.reporting_system.report_configs),
                        'scheduled_reports': len(self.reporting_system.scheduled_tasks)
                    }
                except Exception as e:
                    errors.append(f"Reporting error: {e}")
                    reporting_status = {'error': str(e)}
            
            # Integration status
            if self.config.integration_enabled:
                try:
                    integration_status = {
                        'systems_healthy': len(errors) == 0,
                        'total_errors': len(errors),
                        'last_processed': start_time.isoformat()
                    }
                except Exception as e:
                    errors.append(f"Integration error: {e}")
                    integration_status = {'error': str(e)}
            
            # Create result
            result = Phase4Result(
                timestamp=start_time,
                alerting_status=alerting_status,
                reporting_status=reporting_status,
                rca_status=rca_status,
                integration_status=integration_status,
                processing_time=(datetime.utcnow() - start_time).total_seconds(),
                success=len(errors) == 0,
                errors=errors
            )
            
            # Store result
            await self._store_phase4_result(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing metrics: {e}")
            return Phase4Result(
                timestamp=start_time,
                alerting_status={},
                reporting_status={},
                rca_status={},
                integration_status={},
                processing_time=(datetime.utcnow() - start_time).total_seconds(),
                success=False,
                errors=[str(e)]
            )
    
    async def _check_scheduled_reports(self) -> List[str]:
        """Check and generate scheduled reports"""
        try:
            generated_reports = []
            
            for config in self.reporting_system.report_configs.values():
                if config.schedule and config.enabled:
                    # Check if it's time to generate the report
                    if await self._should_generate_report(config):
                        report = await self.reporting_system.generate_report(config.report_id)
                        if report:
                            generated_reports.append(config.report_id)
            
            return generated_reports
            
        except Exception as e:
            self.logger.error(f"Error checking scheduled reports: {e}")
            return []
    
    async def _should_generate_report(self, config: ReportConfig) -> bool:
        """Check if a report should be generated based on schedule"""
        try:
            schedule = config.schedule
            if not schedule:
                return False
            
            frequency = schedule.get('frequency', 'daily')
            time_str = schedule.get('time', '09:00')
            
            now = datetime.utcnow()
            hour, minute = map(int, time_str.split(':'))
            scheduled_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # Check if we're within 5 minutes of scheduled time
            time_diff = abs((now - scheduled_time).total_seconds())
            return time_diff < 300  # 5 minutes
            
        except Exception as e:
            self.logger.error(f"Error checking report schedule: {e}")
            return False
    
    async def get_comprehensive_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of all Phase 4 activities"""
        try:
            summary = {
                'alerting_summary': await self.alerting_system.get_alerts_summary(),
                'reporting_summary': await self.reporting_system.get_reports_summary(),
                'rca_summary': await self.rca_system.get_investigation_summary(),
                'phase4_overview': {
                    'total_operations': len(self.phase4_history),
                    'successful_operations': sum(1 for result in self.phase4_history if result.success),
                    'failed_operations': sum(1 for result in self.phase4_history if not result.success),
                    'avg_processing_time': np.mean([result.processing_time for result in self.phase4_history]) if self.phase4_history else 0.0,
                    'last_operation': self.phase4_history[-1].timestamp.isoformat() if self.phase4_history else None
                },
                'system_health': {
                    'alerting_enabled': self.config.alerting_enabled,
                    'reporting_enabled': self.config.reporting_enabled,
                    'rca_enabled': self.config.rca_enabled,
                    'integration_enabled': self.config.integration_enabled
                },
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive summary: {e}")
            return {}
    
    async def generate_custom_report(self, report_config: ReportConfig) -> Optional[str]:
        """Generate a custom report"""
        try:
            report = await self.reporting_system.generate_report(report_config.report_id)
            if report:
                return report.report_id
            return None
            
        except Exception as e:
            self.logger.error(f"Error generating custom report: {e}")
            return None
    
    async def trigger_manual_rca(self, trigger_id: str, metrics: Dict[str, Any], 
                               context: Dict[str, Any] = None) -> Optional[str]:
        """Manually trigger an RCA investigation"""
        try:
            if trigger_id in self.rca_system.triggers:
                trigger = self.rca_system.triggers[trigger_id]
                investigation = await self.rca_system._create_investigation(trigger, metrics, context)
                if investigation:
                    await self.rca_system._start_investigation(investigation)
                    return investigation.investigation_id
            return None
            
        except Exception as e:
            self.logger.error(f"Error triggering manual RCA: {e}")
            return None
    
    async def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert"""
        try:
            return await self.alerting_system.acknowledge_alert(alert_id, acknowledged_by)
        except Exception as e:
            self.logger.error(f"Error acknowledging alert: {e}")
            return False
    
    async def resolve_alert(self, alert_id: str, resolved_by: str) -> bool:
        """Resolve an alert"""
        try:
            return await self.alerting_system.resolve_alert(alert_id, resolved_by)
        except Exception as e:
            self.logger.error(f"Error resolving alert: {e}")
            return False
    
    async def _store_phase4_result(self, result: Phase4Result):
        """Store Phase 4 result"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store result
            await redis.lpush(
                "phase4_results",
                json.dumps({
                    'timestamp': result.timestamp.isoformat(),
                    'alerting_status': result.alerting_status,
                    'reporting_status': result.reporting_status,
                    'rca_status': result.rca_status,
                    'integration_status': result.integration_status,
                    'processing_time': result.processing_time,
                    'success': result.success,
                    'errors': result.errors
                })
            )
            
            # Keep only recent results
            await redis.ltrim("phase4_results", 0, 999)
            
            # Update phase4 history
            self.phase4_history.append(result)
            
            # Keep only recent history
            if len(self.phase4_history) > 1000:
                self.phase4_history = self.phase4_history[-1000:]
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing Phase 4 result: {e}")
    
    async def start_phase4_monitoring(self):
        """Start the Phase 4 monitoring pipeline"""
        self.running = True
        self.logger.info("Starting Phase 4 monitoring pipeline")
        
        # Start individual components
        await self.alerting_system.start_alerting_monitoring()
        await self.reporting_system.start_reporting_monitoring()
        await self.rca_system.start_rca_monitoring()
        
        while self.running:
            try:
                # Simulate metrics processing
                mock_metrics = {
                    'accuracy': np.random.normal(0.95, 0.02),
                    'latency_ms': np.random.normal(50, 10),
                    'throughput_rps': np.random.normal(1000, 100),
                    'drift_score': np.random.normal(0.03, 0.01),
                    'alerts_count': np.random.randint(0, 5)
                }
                
                # Process metrics
                result = await self.process_metrics(mock_metrics)
                
                if not result.success:
                    self.logger.error(f"Phase 4 processing failed: {result.errors}")
                else:
                    self.logger.info(f"Phase 4 processing completed in {result.processing_time:.2f}s")
                
                # Wait before next processing cycle
                await asyncio.sleep(self.config.monitoring_interval)
                
            except Exception as e:
                self.logger.error(f"Error in Phase 4 monitoring pipeline: {e}")
                await asyncio.sleep(60)
    
    async def stop_phase4_monitoring(self):
        """Stop the Phase 4 monitoring pipeline"""
        self.running = False
        self.logger.info("Stopping Phase 4 monitoring pipeline")
        
        # Stop individual components
        await self.alerting_system.stop_alerting_monitoring()
        await self.reporting_system.stop_reporting_monitoring()
        await self.rca_system.stop_rca_monitoring()
