import asyncio
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import redis.asyncio as aioredis
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import base64

class ReportType(Enum):
    """Report types"""
    PERFORMANCE = "performance"
    QUALITY = "quality"
    ALERTS = "alerts"
    DRIFT = "drift"
    CUSTOM = "custom"

class ReportFormat(Enum):
    """Report formats"""
    JSON = "json"
    CSV = "csv"
    PDF = "pdf"
    HTML = "html"

@dataclass
class ReportConfig:
    """Report configuration"""
    report_id: str
    name: str
    description: str
    report_type: ReportType
    data_sources: List[str]  # List of data sources to include
    metrics: List[str]  # List of metrics to include
    time_range: Dict[str, Any]  # Time range configuration
    schedule: Optional[Dict[str, Any]] = None  # Scheduling configuration
    recipients: List[str] = None
    format: ReportFormat = ReportFormat.JSON
    enabled: bool = True

@dataclass
class Report:
    """Generated report"""
    report_id: str
    config: ReportConfig
    generated_at: datetime
    data: Dict[str, Any]
    summary: Dict[str, Any]
    visualizations: List[Dict[str, Any]]
    export_url: Optional[str] = None

@dataclass
class AnalyticsQuery:
    """Analytics query configuration"""
    query_id: str
    name: str
    query_type: str  # 'aggregation', 'trend', 'correlation', 'custom'
    parameters: Dict[str, Any]
    time_range: Dict[str, Any]
    enabled: bool = True

class ReportingAnalyticsSystem:
    """
    Comprehensive reporting and analytics system
    Provides custom report generation, scheduling, and export capabilities
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.report_configs: Dict[str, ReportConfig] = {}
        self.analytics_queries: Dict[str, AnalyticsQuery] = {}
        self.generated_reports: Dict[str, Report] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Scheduled report tasks
        self.scheduled_tasks: Dict[str, asyncio.Task] = {}
        
    async def add_report_config(self, config: ReportConfig) -> bool:
        """Add a report configuration"""
        try:
            self.report_configs[config.report_id] = config
            await self._store_report_config(config)
            
            # Set up scheduling if configured
            if config.schedule and config.enabled:
                await self._schedule_report(config)
            
            self.logger.info(f"Added report config: {config.report_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add report config: {e}")
            return False
    
    async def add_analytics_query(self, query: AnalyticsQuery) -> bool:
        """Add an analytics query"""
        try:
            self.analytics_queries[query.query_id] = query
            await self._store_analytics_query(query)
            
            self.logger.info(f"Added analytics query: {query.query_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add analytics query: {e}")
            return False
    
    async def generate_report(self, report_id: str, custom_params: Dict[str, Any] = None) -> Optional[Report]:
        """Generate a report"""
        try:
            if report_id not in self.report_configs:
                raise ValueError(f"Report config not found: {report_id}")
            
            config = self.report_configs[report_id]
            
            # Collect data from sources
            data = await self._collect_report_data(config, custom_params)
            
            # Generate analytics
            analytics = await self._generate_analytics(config, data)
            
            # Create visualizations
            visualizations = await self._create_visualizations(config, data, analytics)
            
            # Generate summary
            summary = await self._generate_summary(config, data, analytics)
            
            # Create report
            report = Report(
                report_id=report_id,
                config=config,
                generated_at=datetime.utcnow(),
                data=data,
                summary=summary,
                visualizations=visualizations
            )
            
            # Store report
            self.generated_reports[report_id] = report
            await self._store_report(report)
            
            # Export if requested
            if config.format != ReportFormat.JSON:
                export_url = await self._export_report(report, config.format)
                report.export_url = export_url
            
            self.logger.info(f"Generated report: {report_id}")
            return report
            
        except Exception as e:
            self.logger.error(f"Error generating report: {e}")
            return None
    
    async def _collect_report_data(self, config: ReportConfig, custom_params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Collect data for report generation"""
        try:
            data = {}
            
            for source in config.data_sources:
                if source == 'performance_metrics':
                    data[source] = await self._get_performance_data(config.time_range)
                elif source == 'quality_metrics':
                    data[source] = await self._get_quality_data(config.time_range)
                elif source == 'alerts':
                    data[source] = await self._get_alerts_data(config.time_range)
                elif source == 'drift_metrics':
                    data[source] = await self._get_drift_data(config.time_range)
                elif source == 'model_metrics':
                    data[source] = await self._get_model_metrics_data(config.time_range)
                else:
                    self.logger.warning(f"Unknown data source: {source}")
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error collecting report data: {e}")
            return {}
    
    async def _get_performance_data(self, time_range: Dict[str, Any]) -> Dict[str, Any]:
        """Get performance metrics data"""
        try:
            # Simulate performance data collection
            hours = time_range.get('hours', 24)
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            # Mock performance data
            performance_data = {
                'accuracy': {
                    'values': np.random.normal(0.95, 0.02, 100).tolist(),
                    'timestamps': [(cutoff_time + timedelta(minutes=i*15)).isoformat() for i in range(100)]
                },
                'latency_ms': {
                    'values': np.random.normal(50, 10, 100).tolist(),
                    'timestamps': [(cutoff_time + timedelta(minutes=i*15)).isoformat() for i in range(100)]
                },
                'throughput_rps': {
                    'values': np.random.normal(1000, 100, 100).tolist(),
                    'timestamps': [(cutoff_time + timedelta(minutes=i*15)).isoformat() for i in range(100)]
                }
            }
            
            return performance_data
            
        except Exception as e:
            self.logger.error(f"Error getting performance data: {e}")
            return {}
    
    async def _get_quality_data(self, time_range: Dict[str, Any]) -> Dict[str, Any]:
        """Get data quality metrics"""
        try:
            hours = time_range.get('hours', 24)
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            quality_data = {
                'missing_values_pct': {
                    'values': np.random.normal(0.05, 0.02, 100).tolist(),
                    'timestamps': [(cutoff_time + timedelta(minutes=i*15)).isoformat() for i in range(100)]
                },
                'outliers_pct': {
                    'values': np.random.normal(0.02, 0.01, 100).tolist(),
                    'timestamps': [(cutoff_time + timedelta(minutes=i*15)).isoformat() for i in range(100)]
                },
                'drift_score': {
                    'values': np.random.normal(0.03, 0.01, 100).tolist(),
                    'timestamps': [(cutoff_time + timedelta(minutes=i*15)).isoformat() for i in range(100)]
                }
            }
            
            return quality_data
            
        except Exception as e:
            self.logger.error(f"Error getting quality data: {e}")
            return {}
    
    async def _get_alerts_data(self, time_range: Dict[str, Any]) -> Dict[str, Any]:
        """Get alerts data"""
        try:
            hours = time_range.get('hours', 24)
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            alerts_data = {
                'total_alerts': 15,
                'active_alerts': 3,
                'resolved_alerts': 12,
                'alerts_by_severity': {
                    'critical': 2,
                    'error': 5,
                    'warning': 6,
                    'info': 2
                },
                'alerts_by_type': {
                    'performance': 8,
                    'quality': 4,
                    'drift': 3
                }
            }
            
            return alerts_data
            
        except Exception as e:
            self.logger.error(f"Error getting alerts data: {e}")
            return {}
    
    async def _get_drift_data(self, time_range: Dict[str, Any]) -> Dict[str, Any]:
        """Get data drift metrics"""
        try:
            hours = time_range.get('hours', 24)
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            drift_data = {
                'feature_drift_scores': {
                    'feature_1': np.random.normal(0.05, 0.02, 100).tolist(),
                    'feature_2': np.random.normal(0.03, 0.01, 100).tolist(),
                    'feature_3': np.random.normal(0.07, 0.03, 100).tolist()
                },
                'concept_drift_detected': False,
                'drift_alerts': 2
            }
            
            return drift_data
            
        except Exception as e:
            self.logger.error(f"Error getting drift data: {e}")
            return {}
    
    async def _get_model_metrics_data(self, time_range: Dict[str, Any]) -> Dict[str, Any]:
        """Get model metrics data"""
        try:
            hours = time_range.get('hours', 24)
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            model_data = {
                'models': {
                    'fraud_detection': {
                        'accuracy': 0.96,
                        'latency_ms': 45,
                        'throughput_rps': 1200,
                        'status': 'healthy'
                    },
                    'recommendation': {
                        'accuracy': 0.89,
                        'latency_ms': 120,
                        'throughput_rps': 800,
                        'status': 'warning'
                    }
                },
                'deployments': {
                    'total': 5,
                    'successful': 4,
                    'failed': 1
                }
            }
            
            return model_data
            
        except Exception as e:
            self.logger.error(f"Error getting model metrics data: {e}")
            return {}
    
    async def _generate_analytics(self, config: ReportConfig, data: Dict[str, Any]) -> Dict[str, Any]:
        """Generate analytics from data"""
        try:
            analytics = {}
            
            for query_id, query in self.analytics_queries.items():
                if query.enabled:
                    if query.query_type == 'aggregation':
                        analytics[query_id] = await self._run_aggregation_query(query, data)
                    elif query.query_type == 'trend':
                        analytics[query_id] = await self._run_trend_query(query, data)
                    elif query.query_type == 'correlation':
                        analytics[query_id] = await self._run_correlation_query(query, data)
                    else:
                        self.logger.warning(f"Unknown query type: {query.query_type}")
            
            return analytics
            
        except Exception as e:
            self.logger.error(f"Error generating analytics: {e}")
            return {}
    
    async def _run_aggregation_query(self, query: AnalyticsQuery, data: Dict[str, Any]) -> Dict[str, Any]:
        """Run aggregation analytics query"""
        try:
            metric = query.parameters.get('metric')
            aggregation = query.parameters.get('aggregation', 'mean')
            
            if metric in data.get('performance_metrics', {}):
                values = data['performance_metrics'][metric]['values']
                
                if aggregation == 'mean':
                    result = np.mean(values)
                elif aggregation == 'median':
                    result = np.median(values)
                elif aggregation == 'std':
                    result = np.std(values)
                elif aggregation == 'min':
                    result = np.min(values)
                elif aggregation == 'max':
                    result = np.max(values)
                else:
                    result = np.mean(values)
                
                return {
                    'metric': metric,
                    'aggregation': aggregation,
                    'value': float(result),
                    'timestamp': datetime.utcnow().isoformat()
                }
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Error running aggregation query: {e}")
            return {}
    
    async def _run_trend_query(self, query: AnalyticsQuery, data: Dict[str, Any]) -> Dict[str, Any]:
        """Run trend analytics query"""
        try:
            metric = query.parameters.get('metric')
            window_size = query.parameters.get('window_size', 10)
            
            if metric in data.get('performance_metrics', {}):
                values = data['performance_metrics'][metric]['values']
                
                # Calculate trend using linear regression
                if len(values) >= window_size:
                    recent_values = values[-window_size:]
                    x = np.arange(len(recent_values))
                    slope, intercept = np.polyfit(x, recent_values, 1)
                    
                    trend_direction = 'increasing' if slope > 0 else 'decreasing'
                    trend_strength = abs(slope)
                    
                    return {
                        'metric': metric,
                        'trend_direction': trend_direction,
                        'trend_strength': float(trend_strength),
                        'slope': float(slope),
                        'window_size': window_size,
                        'timestamp': datetime.utcnow().isoformat()
                    }
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Error running trend query: {e}")
            return {}
    
    async def _run_correlation_query(self, query: AnalyticsQuery, data: Dict[str, Any]) -> Dict[str, Any]:
        """Run correlation analytics query"""
        try:
            metric1 = query.parameters.get('metric1')
            metric2 = query.parameters.get('metric2')
            
            if (metric1 in data.get('performance_metrics', {}) and 
                metric2 in data.get('performance_metrics', {})):
                
                values1 = data['performance_metrics'][metric1]['values']
                values2 = data['performance_metrics'][metric2]['values']
                
                if len(values1) == len(values2):
                    correlation = np.corrcoef(values1, values2)[0, 1]
                    
                    return {
                        'metric1': metric1,
                        'metric2': metric2,
                        'correlation': float(correlation),
                        'strength': 'strong' if abs(correlation) > 0.7 else 'moderate' if abs(correlation) > 0.3 else 'weak',
                        'timestamp': datetime.utcnow().isoformat()
                    }
            
            return {}
            
        except Exception as e:
            self.logger.error(f"Error running correlation query: {e}")
            return {}
    
    async def _create_visualizations(self, config: ReportConfig, data: Dict[str, Any], 
                                   analytics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Create visualizations for the report"""
        try:
            visualizations = []
            
            # Performance trends
            if 'performance_metrics' in data:
                for metric in ['accuracy', 'latency_ms', 'throughput_rps']:
                    if metric in data['performance_metrics']:
                        viz = await self._create_time_series_plot(
                            data['performance_metrics'][metric],
                            f"{metric.replace('_', ' ').title()} Over Time",
                            metric
                        )
                        visualizations.append(viz)
            
            # Quality metrics
            if 'quality_metrics' in data:
                viz = await self._create_quality_dashboard(data['quality_metrics'])
                visualizations.append(viz)
            
            # Alerts summary
            if 'alerts' in data:
                viz = await self._create_alerts_summary(data['alerts'])
                visualizations.append(viz)
            
            return visualizations
            
        except Exception as e:
            self.logger.error(f"Error creating visualizations: {e}")
            return []
    
    async def _create_time_series_plot(self, data: Dict[str, Any], title: str, metric: str) -> Dict[str, Any]:
        """Create time series plot"""
        try:
            plt.figure(figsize=(10, 6))
            plt.plot(data['timestamps'], data['values'])
            plt.title(title)
            plt.xlabel('Time')
            plt.ylabel(metric.replace('_', ' ').title())
            plt.xticks(rotation=45)
            plt.tight_layout()
            
            # Convert to base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode()
            plt.close()
            
            return {
                'type': 'time_series',
                'title': title,
                'metric': metric,
                'image_base64': image_base64,
                'data_points': len(data['values'])
            }
            
        except Exception as e:
            self.logger.error(f"Error creating time series plot: {e}")
            return {}
    
    async def _create_quality_dashboard(self, quality_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create quality metrics dashboard"""
        try:
            fig, axes = plt.subplots(1, 3, figsize=(15, 5))
            
            metrics = ['missing_values_pct', 'outliers_pct', 'drift_score']
            titles = ['Missing Values %', 'Outliers %', 'Drift Score']
            
            for i, (metric, title) in enumerate(zip(metrics, titles)):
                if metric in quality_data:
                    axes[i].plot(quality_data[metric]['timestamps'], quality_data[metric]['values'])
                    axes[i].set_title(title)
                    axes[i].set_xlabel('Time')
                    axes[i].tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            # Convert to base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode()
            plt.close()
            
            return {
                'type': 'quality_dashboard',
                'title': 'Data Quality Metrics',
                'image_base64': image_base64,
                'metrics': list(quality_data.keys())
            }
            
        except Exception as e:
            self.logger.error(f"Error creating quality dashboard: {e}")
            return {}
    
    async def _create_alerts_summary(self, alerts_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create alerts summary visualization"""
        try:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
            
            # Severity breakdown
            if 'alerts_by_severity' in alerts_data:
                severities = list(alerts_data['alerts_by_severity'].keys())
                counts = list(alerts_data['alerts_by_severity'].values())
                colors = ['#ff0000', '#ffa500', '#ffff00', '#00ff00']
                
                ax1.pie(counts, labels=severities, colors=colors[:len(severities)], autopct='%1.1f%%')
                ax1.set_title('Alerts by Severity')
            
            # Type breakdown
            if 'alerts_by_type' in alerts_data:
                types = list(alerts_data['alerts_by_type'].keys())
                counts = list(alerts_data['alerts_by_type'].values())
                
                ax2.bar(types, counts)
                ax2.set_title('Alerts by Type')
                ax2.set_ylabel('Count')
                ax2.tick_params(axis='x', rotation=45)
            
            plt.tight_layout()
            
            # Convert to base64
            buffer = BytesIO()
            plt.savefig(buffer, format='png')
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode()
            plt.close()
            
            return {
                'type': 'alerts_summary',
                'title': 'Alerts Summary',
                'image_base64': image_base64,
                'total_alerts': alerts_data.get('total_alerts', 0)
            }
            
        except Exception as e:
            self.logger.error(f"Error creating alerts summary: {e}")
            return {}
    
    async def _generate_summary(self, config: ReportConfig, data: Dict[str, Any], 
                              analytics: Dict[str, Any]) -> Dict[str, Any]:
        """Generate report summary"""
        try:
            summary = {
                'report_name': config.name,
                'generated_at': datetime.utcnow().isoformat(),
                'time_range': config.time_range,
                'data_sources': config.data_sources,
                'key_insights': [],
                'recommendations': []
            }
            
            # Generate insights based on data
            if 'performance_metrics' in data:
                accuracy_values = data['performance_metrics'].get('accuracy', {}).get('values', [])
                if accuracy_values:
                    avg_accuracy = np.mean(accuracy_values)
                    summary['key_insights'].append(f"Average accuracy: {avg_accuracy:.3f}")
                    
                    if avg_accuracy < 0.9:
                        summary['recommendations'].append("Consider model retraining due to low accuracy")
            
            if 'quality_metrics' in data:
                drift_values = data['quality_metrics'].get('drift_score', {}).get('values', [])
                if drift_values:
                    max_drift = np.max(drift_values)
                    summary['key_insights'].append(f"Maximum drift score: {max_drift:.3f}")
                    
                    if max_drift > 0.1:
                        summary['recommendations'].append("High data drift detected - investigate data quality")
            
            if 'alerts' in data:
                active_alerts = data['alerts'].get('active_alerts', 0)
                summary['key_insights'].append(f"Active alerts: {active_alerts}")
                
                if active_alerts > 5:
                    summary['recommendations'].append("High number of active alerts - review system health")
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error generating summary: {e}")
            return {}
    
    async def _export_report(self, report: Report, format: ReportFormat) -> Optional[str]:
        """Export report in specified format"""
        try:
            if format == ReportFormat.CSV:
                return await self._export_csv(report)
            elif format == ReportFormat.HTML:
                return await self._export_html(report)
            elif format == ReportFormat.PDF:
                return await self._export_pdf(report)
            else:
                return None
                
        except Exception as e:
            self.logger.error(f"Error exporting report: {e}")
            return None
    
    async def _export_csv(self, report: Report) -> str:
        """Export report as CSV"""
        try:
            # Create CSV from report data
            csv_data = []
            
            for source, data in report.data.items():
                if isinstance(data, dict) and 'values' in data:
                    for i, value in enumerate(data['values']):
                        csv_data.append({
                            'source': source,
                            'timestamp': data['timestamps'][i] if 'timestamps' in data else '',
                            'value': value
                        })
            
            # Convert to DataFrame and save
            df = pd.DataFrame(csv_data)
            filename = f"report_{report.report_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # In a real implementation, save to file system or cloud storage
            # For now, return the filename
            return filename
            
        except Exception as e:
            self.logger.error(f"Error exporting CSV: {e}")
            return ""
    
    async def _export_html(self, report: Report) -> str:
        """Export report as HTML"""
        try:
            # Create HTML report
            html_content = f"""
            <html>
            <head>
                <title>{report.config.name}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .header {{ background-color: #f0f0f0; padding: 10px; }}
                    .section {{ margin: 20px 0; }}
                    .visualization {{ text-align: center; margin: 20px 0; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>{report.config.name}</h1>
                    <p>Generated: {report.generated_at}</p>
                </div>
                
                <div class="section">
                    <h2>Summary</h2>
                    <ul>
                        {''.join([f'<li>{insight}</li>' for insight in report.summary.get('key_insights', [])])}
                    </ul>
                </div>
                
                <div class="section">
                    <h2>Visualizations</h2>
                    {''.join([f'<div class="visualization"><h3>{viz["title"]}</h3><img src="data:image/png;base64,{viz["image_base64"]}" /></div>' for viz in report.visualizations])}
                </div>
            </body>
            </html>
            """
            
            filename = f"report_{report.report_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.html"
            
            # In a real implementation, save to file system or cloud storage
            return filename
            
        except Exception as e:
            self.logger.error(f"Error exporting HTML: {e}")
            return ""
    
    async def _export_pdf(self, report: Report) -> str:
        """Export report as PDF"""
        try:
            # In a real implementation, use a library like reportlab or weasyprint
            # For now, return a placeholder
            filename = f"report_{report.report_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.pdf"
            return filename
            
        except Exception as e:
            self.logger.error(f"Error exporting PDF: {e}")
            return ""
    
    async def _schedule_report(self, config: ReportConfig):
        """Schedule a report for automatic generation"""
        try:
            schedule = config.schedule
            if not schedule:
                return
            
            frequency = schedule.get('frequency', 'daily')  # 'daily', 'weekly', 'monthly'
            time = schedule.get('time', '09:00')  # HH:MM format
            
            # Create scheduled task
            task = asyncio.create_task(self._run_scheduled_report(config, frequency, time))
            self.scheduled_tasks[config.report_id] = task
            
        except Exception as e:
            self.logger.error(f"Error scheduling report: {e}")
    
    async def _run_scheduled_report(self, config: ReportConfig, frequency: str, time: str):
        """Run scheduled report generation"""
        try:
            while self.running:
                # Calculate next run time
                now = datetime.utcnow()
                hour, minute = map(int, time.split(':'))
                
                if frequency == 'daily':
                    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                    if next_run <= now:
                        next_run += timedelta(days=1)
                elif frequency == 'weekly':
                    # Run on Monday
                    days_ahead = 7 - now.weekday()
                    next_run = now.replace(hour=hour, minute=minute, second=0, microsecond=0) + timedelta(days=days_ahead)
                else:
                    # Monthly - run on first day of month
                    next_run = now.replace(day=1, hour=hour, minute=minute, second=0, microsecond=0)
                    if next_run <= now:
                        next_run = next_run.replace(month=next_run.month + 1)
                
                # Wait until next run time
                wait_seconds = (next_run - now).total_seconds()
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)
                
                # Generate report
                if self.running:
                    await self.generate_report(config.report_id)
                    
                    # Send to recipients if configured
                    if config.recipients:
                        await self._send_report_to_recipients(config.report_id, config.recipients)
                
        except Exception as e:
            self.logger.error(f"Error in scheduled report: {e}")
    
    async def _send_report_to_recipients(self, report_id: str, recipients: List[str]):
        """Send report to recipients"""
        try:
            # In a real implementation, send via email or other channels
            self.logger.info(f"Report {report_id} sent to recipients: {recipients}")
            
        except Exception as e:
            self.logger.error(f"Error sending report to recipients: {e}")
    
    async def get_reports_summary(self) -> Dict[str, Any]:
        """Get summary of all reports"""
        try:
            summary = {
                'total_configs': len(self.report_configs),
                'active_configs': sum(1 for config in self.report_configs.values() if config.enabled),
                'scheduled_reports': len(self.scheduled_tasks),
                'total_generated': len(self.generated_reports),
                'recent_reports': [],
                'reports_by_type': {}
            }
            
            # Count by type
            for config in self.report_configs.values():
                report_type = config.report_type.value
                summary['reports_by_type'][report_type] = summary['reports_by_type'].get(report_type, 0) + 1
            
            # Get recent reports
            recent_reports = sorted(
                self.generated_reports.values(),
                key=lambda r: r.generated_at,
                reverse=True
            )[:10]
            
            for report in recent_reports:
                summary['recent_reports'].append({
                    'report_id': report.report_id,
                    'name': report.config.name,
                    'type': report.config.report_type.value,
                    'generated_at': report.generated_at.isoformat(),
                    'export_url': report.export_url
                })
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting reports summary: {e}")
            return {}
    
    async def _store_report_config(self, config: ReportConfig):
        """Store report config in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"report_config:{config.report_id}",
                json.dumps({
                    'report_id': config.report_id,
                    'name': config.name,
                    'description': config.description,
                    'report_type': config.report_type.value,
                    'data_sources': config.data_sources,
                    'metrics': config.metrics,
                    'time_range': config.time_range,
                    'schedule': config.schedule,
                    'recipients': config.recipients,
                    'format': config.format.value,
                    'enabled': config.enabled
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing report config: {e}")
    
    async def _store_analytics_query(self, query: AnalyticsQuery):
        """Store analytics query in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"analytics_query:{query.query_id}",
                json.dumps({
                    'query_id': query.query_id,
                    'name': query.name,
                    'query_type': query.query_type,
                    'parameters': query.parameters,
                    'time_range': query.time_range,
                    'enabled': query.enabled
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing analytics query: {e}")
    
    async def _store_report(self, report: Report):
        """Store generated report in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store report metadata (not the full data to avoid size issues)
            await redis.set(
                f"report:{report.report_id}",
                json.dumps({
                    'report_id': report.report_id,
                    'config_id': report.config.report_id,
                    'generated_at': report.generated_at.isoformat(),
                    'summary': report.summary,
                    'export_url': report.export_url
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing report: {e}")
    
    async def start_reporting_monitoring(self):
        """Start continuous reporting monitoring"""
        self.running = True
        self.logger.info("Starting reporting monitoring")
        
        while self.running:
            try:
                # Monitor scheduled reports
                # Clean up old reports
                # Check for failed generations
                self.logger.debug("Reporting monitoring active")
                
                # Wait before next check
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in reporting monitoring: {e}")
                await asyncio.sleep(60)
    
    async def stop_reporting_monitoring(self):
        """Stop reporting monitoring"""
        self.running = False
        self.logger.info("Stopping reporting monitoring")
        
        # Cancel all scheduled tasks
        for task in self.scheduled_tasks.values():
            task.cancel()
        self.scheduled_tasks.clear()
