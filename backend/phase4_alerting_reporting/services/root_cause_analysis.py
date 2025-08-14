import asyncio
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import redis.asyncio as aioredis
from scipy import stats
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

class RCAStatus(Enum):
    """Root cause analysis status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"

class RCASeverity(Enum):
    """RCA severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

@dataclass
class RCATrigger:
    """RCA trigger configuration"""
    trigger_id: str
    name: str
    description: str
    trigger_conditions: Dict[str, Any]  # Conditions that trigger RCA
    severity: RCASeverity
    enabled: bool = True
    auto_trigger: bool = True

@dataclass
class RCAInvestigation:
    """RCA investigation instance"""
    investigation_id: str
    trigger_id: str
    title: str
    description: str
    status: RCAStatus
    severity: RCASeverity
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    assigned_to: Optional[str] = None
    findings: List[Dict[str, Any]] = None
    root_causes: List[Dict[str, Any]] = None
    recommendations: List[str] = None
    impact_assessment: Dict[str, Any] = None

@dataclass
class CorrelationAnalysis:
    """Correlation analysis result"""
    analysis_id: str
    investigation_id: str
    metric_pairs: List[Tuple[str, str]]
    correlation_matrix: Dict[str, Dict[str, float]]
    significant_correlations: List[Dict[str, Any]]
    timestamp: datetime

@dataclass
class AnomalyDetection:
    """Anomaly detection result"""
    detection_id: str
    investigation_id: str
    metric_name: str
    anomaly_type: str  # 'spike', 'drop', 'trend_change', 'pattern_change'
    severity: float
    confidence: float
    timestamp: datetime
    details: Dict[str, Any]

class RootCauseAnalysisSystem:
    """
    Comprehensive root cause analysis system
    Provides automated investigation, correlation analysis, and impact assessment
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.triggers: Dict[str, RCATrigger] = {}
        self.investigations: Dict[str, RCAInvestigation] = {}
        self.correlation_analyses: Dict[str, CorrelationAnalysis] = {}
        self.anomaly_detections: Dict[str, AnomalyDetection] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Investigation tasks
        self.investigation_tasks: Dict[str, asyncio.Task] = {}
        
    async def add_rca_trigger(self, trigger: RCATrigger) -> bool:
        """Add an RCA trigger"""
        try:
            self.triggers[trigger.trigger_id] = trigger
            await self._store_rca_trigger(trigger)
            
            self.logger.info(f"Added RCA trigger: {trigger.trigger_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add RCA trigger: {e}")
            return False
    
    async def check_rca_triggers(self, metrics: Dict[str, Any], context: Dict[str, Any] = None) -> List[str]:
        """Check if any RCA triggers should be activated"""
        try:
            triggered_investigations = []
            
            for trigger_id, trigger in self.triggers.items():
                if not trigger.enabled or not trigger.auto_trigger:
                    continue
                
                # Check trigger conditions
                if self._evaluate_trigger_conditions(trigger.trigger_conditions, metrics, context):
                    # Create investigation
                    investigation = await self._create_investigation(trigger, metrics, context)
                    if investigation:
                        triggered_investigations.append(investigation.investigation_id)
                        
                        # Start investigation
                        await self._start_investigation(investigation)
            
            return triggered_investigations
            
        except Exception as e:
            self.logger.error(f"Error checking RCA triggers: {e}")
            return []
    
    def _evaluate_trigger_conditions(self, conditions: Dict[str, Any], metrics: Dict[str, Any], 
                                   context: Dict[str, Any] = None) -> bool:
        """Evaluate trigger conditions"""
        try:
            condition_type = conditions.get('type', 'threshold')
            
            if condition_type == 'threshold':
                return self._evaluate_threshold_condition(conditions, metrics)
            elif condition_type == 'trend':
                return self._evaluate_trend_condition(conditions, metrics)
            elif condition_type == 'anomaly':
                return self._evaluate_anomaly_condition(conditions, metrics)
            elif condition_type == 'compound':
                return self._evaluate_compound_condition(conditions, metrics, context)
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Error evaluating trigger conditions: {e}")
            return False
    
    def _evaluate_threshold_condition(self, conditions: Dict[str, Any], metrics: Dict[str, Any]) -> bool:
        """Evaluate threshold-based condition"""
        try:
            metric_name = conditions.get('metric')
            threshold = conditions.get('threshold')
            operator = conditions.get('operator', '>')
            
            if metric_name not in metrics:
                return False
            
            metric_value = metrics[metric_name]
            
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
            self.logger.error(f"Error evaluating threshold condition: {e}")
            return False
    
    def _evaluate_trend_condition(self, conditions: Dict[str, Any], metrics: Dict[str, Any]) -> bool:
        """Evaluate trend-based condition"""
        try:
            metric_name = conditions.get('metric')
            trend_direction = conditions.get('trend_direction', 'decreasing')
            window_size = conditions.get('window_size', 10)
            threshold = conditions.get('threshold', 0.1)
            
            if metric_name not in metrics or 'history' not in metrics[metric_name]:
                return False
            
            history = metrics[metric_name]['history']
            if len(history) < window_size:
                return False
            
            # Calculate trend
            recent_values = history[-window_size:]
            x = np.arange(len(recent_values))
            slope, _ = np.polyfit(x, recent_values, 1)
            
            if trend_direction == 'decreasing':
                return slope < -threshold
            elif trend_direction == 'increasing':
                return slope > threshold
            else:
                return abs(slope) > threshold
                
        except Exception as e:
            self.logger.error(f"Error evaluating trend condition: {e}")
            return False
    
    def _evaluate_anomaly_condition(self, conditions: Dict[str, Any], metrics: Dict[str, Any]) -> bool:
        """Evaluate anomaly-based condition"""
        try:
            metric_name = conditions.get('metric')
            anomaly_threshold = conditions.get('anomaly_threshold', 2.0)
            
            if metric_name not in metrics or 'history' not in metrics[metric_name]:
                return False
            
            history = metrics[metric_name]['history']
            if len(history) < 10:
                return False
            
            # Calculate z-score for recent value
            recent_value = history[-1]
            mean_value = np.mean(history[:-1])
            std_value = np.std(history[:-1])
            
            if std_value == 0:
                return False
            
            z_score = abs(recent_value - mean_value) / std_value
            return z_score > anomaly_threshold
            
        except Exception as e:
            self.logger.error(f"Error evaluating anomaly condition: {e}")
            return False
    
    def _evaluate_compound_condition(self, conditions: Dict[str, Any], metrics: Dict[str, Any], 
                                   context: Dict[str, Any] = None) -> bool:
        """Evaluate compound condition (multiple conditions)"""
        try:
            sub_conditions = conditions.get('conditions', [])
            logic = conditions.get('logic', 'AND')  # 'AND', 'OR'
            
            if not sub_conditions:
                return False
            
            results = []
            for sub_condition in sub_conditions:
                result = self._evaluate_trigger_conditions(sub_condition, metrics, context)
                results.append(result)
            
            if logic == 'AND':
                return all(results)
            elif logic == 'OR':
                return any(results)
            else:
                return False
                
        except Exception as e:
            self.logger.error(f"Error evaluating compound condition: {e}")
            return False
    
    async def _create_investigation(self, trigger: RCATrigger, metrics: Dict[str, Any], 
                                  context: Dict[str, Any] = None) -> Optional[RCAInvestigation]:
        """Create a new RCA investigation"""
        try:
            investigation_id = f"rca_{trigger.trigger_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            
            investigation = RCAInvestigation(
                investigation_id=investigation_id,
                trigger_id=trigger.trigger_id,
                title=f"RCA: {trigger.name}",
                description=f"Root cause analysis triggered by {trigger.name}",
                status=RCAStatus.PENDING,
                severity=trigger.severity,
                created_at=datetime.utcnow(),
                findings=[],
                root_causes=[],
                recommendations=[],
                impact_assessment={}
            )
            
            # Store investigation
            self.investigations[investigation_id] = investigation
            await self._store_investigation(investigation)
            
            self.logger.warning(f"Created RCA investigation: {investigation_id}")
            return investigation
            
        except Exception as e:
            self.logger.error(f"Error creating investigation: {e}")
            return None
    
    async def _start_investigation(self, investigation: RCAInvestigation):
        """Start an RCA investigation"""
        try:
            investigation.status = RCAStatus.IN_PROGRESS
            investigation.started_at = datetime.utcnow()
            
            # Create investigation task
            task = asyncio.create_task(self._run_investigation(investigation))
            self.investigation_tasks[investigation.investigation_id] = task
            
            await self._update_investigation(investigation)
            
            self.logger.info(f"Started RCA investigation: {investigation.investigation_id}")
            
        except Exception as e:
            self.logger.error(f"Error starting investigation: {e}")
    
    async def _run_investigation(self, investigation: RCAInvestigation):
        """Run the RCA investigation"""
        try:
            # Step 1: Collect data
            data = await self._collect_investigation_data(investigation)
            
            # Step 2: Perform correlation analysis
            correlation_analysis = await self._perform_correlation_analysis(investigation, data)
            
            # Step 3: Detect anomalies
            anomalies = await self._detect_anomalies(investigation, data)
            
            # Step 4: Identify root causes
            root_causes = await self._identify_root_causes(investigation, data, correlation_analysis, anomalies)
            
            # Step 5: Assess impact
            impact_assessment = await self._assess_impact(investigation, data, root_causes)
            
            # Step 6: Generate recommendations
            recommendations = await self._generate_recommendations(investigation, root_causes, impact_assessment)
            
            # Update investigation
            investigation.findings = [
                {'type': 'correlation_analysis', 'data': correlation_analysis},
                {'type': 'anomaly_detection', 'data': anomalies},
                {'type': 'root_causes', 'data': root_causes},
                {'type': 'impact_assessment', 'data': impact_assessment}
            ]
            investigation.root_causes = root_causes
            investigation.recommendations = recommendations
            investigation.impact_assessment = impact_assessment
            investigation.status = RCAStatus.COMPLETED
            investigation.completed_at = datetime.utcnow()
            
            await self._update_investigation(investigation)
            
            self.logger.info(f"Completed RCA investigation: {investigation.investigation_id}")
            
        except Exception as e:
            self.logger.error(f"Error running investigation: {e}")
            investigation.status = RCAStatus.FAILED
            await self._update_investigation(investigation)
    
    async def _collect_investigation_data(self, investigation: RCAInvestigation) -> Dict[str, Any]:
        """Collect data for investigation"""
        try:
            # Collect metrics from the last 24 hours
            hours = 24
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            data = {
                'performance_metrics': await self._get_performance_data(cutoff_time),
                'quality_metrics': await self._get_quality_data(cutoff_time),
                'system_metrics': await self._get_system_data(cutoff_time),
                'alerts': await self._get_alerts_data(cutoff_time),
                'deployments': await self._get_deployments_data(cutoff_time)
            }
            
            return data
            
        except Exception as e:
            self.logger.error(f"Error collecting investigation data: {e}")
            return {}
    
    async def _get_performance_data(self, cutoff_time: datetime) -> Dict[str, Any]:
        """Get performance data for investigation"""
        try:
            # Simulate performance data collection
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
    
    async def _get_quality_data(self, cutoff_time: datetime) -> Dict[str, Any]:
        """Get quality data for investigation"""
        try:
            quality_data = {
                'missing_values_pct': {
                    'values': np.random.normal(0.05, 0.02, 100).tolist(),
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
    
    async def _get_system_data(self, cutoff_time: datetime) -> Dict[str, Any]:
        """Get system data for investigation"""
        try:
            system_data = {
                'cpu_usage': {
                    'values': np.random.normal(60, 15, 100).tolist(),
                    'timestamps': [(cutoff_time + timedelta(minutes=i*15)).isoformat() for i in range(100)]
                },
                'memory_usage': {
                    'values': np.random.normal(70, 10, 100).tolist(),
                    'timestamps': [(cutoff_time + timedelta(minutes=i*15)).isoformat() for i in range(100)]
                },
                'disk_usage': {
                    'values': np.random.normal(80, 5, 100).tolist(),
                    'timestamps': [(cutoff_time + timedelta(minutes=i*15)).isoformat() for i in range(100)]
                }
            }
            
            return system_data
            
        except Exception as e:
            self.logger.error(f"Error getting system data: {e}")
            return {}
    
    async def _get_alerts_data(self, cutoff_time: datetime) -> Dict[str, Any]:
        """Get alerts data for investigation"""
        try:
            alerts_data = {
                'total_alerts': 25,
                'critical_alerts': 3,
                'error_alerts': 8,
                'warning_alerts': 14,
                'alerts_timeline': [
                    {'timestamp': (cutoff_time + timedelta(hours=i)).isoformat(), 'count': np.random.randint(0, 5)}
                    for i in range(24)
                ]
            }
            
            return alerts_data
            
        except Exception as e:
            self.logger.error(f"Error getting alerts data: {e}")
            return {}
    
    async def _get_deployments_data(self, cutoff_time: datetime) -> Dict[str, Any]:
        """Get deployments data for investigation"""
        try:
            deployments_data = {
                'recent_deployments': [
                    {
                        'deployment_id': f'deploy_{i}',
                        'timestamp': (cutoff_time + timedelta(hours=i*2)).isoformat(),
                        'status': 'success' if i % 3 != 0 else 'failed',
                        'model_id': f'model_{i % 3}'
                    }
                    for i in range(12)
                ]
            }
            
            return deployments_data
            
        except Exception as e:
            self.logger.error(f"Error getting deployments data: {e}")
            return {}
    
    async def _perform_correlation_analysis(self, investigation: RCAInvestigation, 
                                          data: Dict[str, Any]) -> CorrelationAnalysis:
        """Perform correlation analysis on metrics"""
        try:
            analysis_id = f"corr_{investigation.investigation_id}"
            
            # Extract all metrics
            all_metrics = {}
            for source, source_data in data.items():
                if isinstance(source_data, dict):
                    for metric_name, metric_data in source_data.items():
                        if isinstance(metric_data, dict) and 'values' in metric_data:
                            all_metrics[f"{source}_{metric_name}"] = metric_data['values']
            
            # Calculate correlation matrix
            correlation_matrix = {}
            significant_correlations = []
            
            metric_names = list(all_metrics.keys())
            for i, metric1 in enumerate(metric_names):
                correlation_matrix[metric1] = {}
                for j, metric2 in enumerate(metric_names):
                    if i != j:
                        values1 = all_metrics[metric1]
                        values2 = all_metrics[metric2]
                        
                        if len(values1) == len(values2) and len(values1) > 10:
                            correlation = np.corrcoef(values1, values2)[0, 1]
                            correlation_matrix[metric1][metric2] = float(correlation)
                            
                            # Check for significant correlations
                            if abs(correlation) > 0.7:
                                significant_correlations.append({
                                    'metric1': metric1,
                                    'metric2': metric2,
                                    'correlation': float(correlation),
                                    'strength': 'strong' if abs(correlation) > 0.8 else 'moderate'
                                })
            
            correlation_analysis = CorrelationAnalysis(
                analysis_id=analysis_id,
                investigation_id=investigation.investigation_id,
                metric_pairs=[(metric_names[i], metric_names[j]) for i in range(len(metric_names)) for j in range(i+1, len(metric_names))],
                correlation_matrix=correlation_matrix,
                significant_correlations=significant_correlations,
                timestamp=datetime.utcnow()
            )
            
            # Store correlation analysis
            self.correlation_analyses[analysis_id] = correlation_analysis
            await self._store_correlation_analysis(correlation_analysis)
            
            return correlation_analysis
            
        except Exception as e:
            self.logger.error(f"Error performing correlation analysis: {e}")
            return None
    
    async def _detect_anomalies(self, investigation: RCAInvestigation, 
                               data: Dict[str, Any]) -> List[AnomalyDetection]:
        """Detect anomalies in metrics"""
        try:
            anomalies = []
            
            for source, source_data in data.items():
                if isinstance(source_data, dict):
                    for metric_name, metric_data in source_data.items():
                        if isinstance(metric_data, dict) and 'values' in metric_data:
                            values = metric_data['values']
                            
                            if len(values) > 10:
                                # Detect statistical anomalies
                                mean_val = np.mean(values)
                                std_val = np.std(values)
                                
                                if std_val > 0:
                                    z_scores = [(abs(val - mean_val) / std_val) for val in values]
                                    max_z_score = max(z_scores)
                                    max_z_index = z_scores.index(max_z_score)
                                    
                                    if max_z_score > 2.5:  # Significant anomaly
                                        anomaly = AnomalyDetection(
                                            detection_id=f"anomaly_{investigation.investigation_id}_{metric_name}",
                                            investigation_id=investigation.investigation_id,
                                            metric_name=f"{source}_{metric_name}",
                                            anomaly_type='spike' if values[max_z_index] > mean_val else 'drop',
                                            severity=float(max_z_score),
                                            confidence=min(0.95, max_z_score / 5.0),
                                            timestamp=datetime.utcnow(),
                                            details={
                                                'anomaly_value': values[max_z_index],
                                                'mean_value': mean_val,
                                                'std_value': std_val,
                                                'z_score': max_z_score,
                                                'position': max_z_index
                                            }
                                        )
                                        anomalies.append(anomaly)
                                        
                                        # Store anomaly detection
                                        self.anomaly_detections[anomaly.detection_id] = anomaly
                                        await self._store_anomaly_detection(anomaly)
            
            return anomalies
            
        except Exception as e:
            self.logger.error(f"Error detecting anomalies: {e}")
            return []
    
    async def _identify_root_causes(self, investigation: RCAInvestigation, data: Dict[str, Any],
                                  correlation_analysis: CorrelationAnalysis, 
                                  anomalies: List[AnomalyDetection]) -> List[Dict[str, Any]]:
        """Identify potential root causes"""
        try:
            root_causes = []
            
            # Analyze anomalies
            for anomaly in anomalies:
                if anomaly.severity > 3.0:  # High severity anomalies
                    root_causes.append({
                        'type': 'anomaly',
                        'metric': anomaly.metric_name,
                        'description': f"Significant {anomaly.anomaly_type} detected in {anomaly.metric_name}",
                        'severity': anomaly.severity,
                        'confidence': anomaly.confidence,
                        'details': anomaly.details
                    })
            
            # Analyze correlations
            for correlation in correlation_analysis.significant_correlations:
                if correlation['strength'] == 'strong':
                    root_causes.append({
                        'type': 'correlation',
                        'metrics': [correlation['metric1'], correlation['metric2']],
                        'description': f"Strong correlation between {correlation['metric1']} and {correlation['metric2']}",
                        'severity': abs(correlation['correlation']),
                        'confidence': 0.8,
                        'details': correlation
                    })
            
            # Analyze system metrics
            if 'system_metrics' in data:
                system_data = data['system_metrics']
                for metric_name, metric_data in system_data.items():
                    if isinstance(metric_data, dict) and 'values' in metric_data:
                        values = metric_data['values']
                        if values:
                            avg_value = np.mean(values)
                            if avg_value > 90:  # High system usage
                                root_causes.append({
                                    'type': 'system',
                                    'metric': f"system_{metric_name}",
                                    'description': f"High {metric_name} usage detected",
                                    'severity': avg_value / 100.0,
                                    'confidence': 0.7,
                                    'details': {'average_value': avg_value}
                                })
            
            # Analyze deployments
            if 'deployments' in data:
                deployments = data['deployments'].get('recent_deployments', [])
                failed_deployments = [d for d in deployments if d.get('status') == 'failed']
                if failed_deployments:
                    root_causes.append({
                        'type': 'deployment',
                        'description': f"Recent failed deployments detected: {len(failed_deployments)}",
                        'severity': len(failed_deployments) / len(deployments) if deployments else 0,
                        'confidence': 0.8,
                        'details': {'failed_deployments': failed_deployments}
                    })
            
            return root_causes
            
        except Exception as e:
            self.logger.error(f"Error identifying root causes: {e}")
            return []
    
    async def _assess_impact(self, investigation: RCAInvestigation, data: Dict[str, Any],
                           root_causes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Assess the impact of identified issues"""
        try:
            impact_assessment = {
                'overall_impact': 'low',
                'impact_score': 0.0,
                'affected_metrics': [],
                'business_impact': 'minimal',
                'recovery_time': 'immediate',
                'details': {}
            }
            
            # Calculate overall impact score
            total_severity = 0.0
            total_confidence = 0.0
            affected_metrics = set()
            
            for root_cause in root_causes:
                total_severity += root_cause.get('severity', 0.0)
                total_confidence += root_cause.get('confidence', 0.0)
                
                if 'metric' in root_cause:
                    affected_metrics.add(root_cause['metric'])
                elif 'metrics' in root_cause:
                    affected_metrics.update(root_cause['metrics'])
            
            if root_causes:
                avg_severity = total_severity / len(root_causes)
                avg_confidence = total_confidence / len(root_causes)
                impact_score = avg_severity * avg_confidence
                
                impact_assessment['impact_score'] = impact_score
                impact_assessment['affected_metrics'] = list(affected_metrics)
                
                # Determine impact level
                if impact_score > 0.8:
                    impact_assessment['overall_impact'] = 'critical'
                    impact_assessment['business_impact'] = 'severe'
                    impact_assessment['recovery_time'] = 'days'
                elif impact_score > 0.6:
                    impact_assessment['overall_impact'] = 'high'
                    impact_assessment['business_impact'] = 'significant'
                    impact_assessment['recovery_time'] = 'hours'
                elif impact_score > 0.4:
                    impact_assessment['overall_impact'] = 'medium'
                    impact_assessment['business_impact'] = 'moderate'
                    impact_assessment['recovery_time'] = 'hours'
                else:
                    impact_assessment['overall_impact'] = 'low'
                    impact_assessment['business_impact'] = 'minimal'
                    impact_assessment['recovery_time'] = 'immediate'
            
            return impact_assessment
            
        except Exception as e:
            self.logger.error(f"Error assessing impact: {e}")
            return {}
    
    async def _generate_recommendations(self, investigation: RCAInvestigation,
                                      root_causes: List[Dict[str, Any]], 
                                      impact_assessment: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on findings"""
        try:
            recommendations = []
            
            for root_cause in root_causes:
                root_cause_type = root_cause.get('type')
                
                if root_cause_type == 'anomaly':
                    recommendations.append(f"Investigate the anomaly in {root_cause.get('metric', 'unknown metric')}")
                    recommendations.append("Consider implementing anomaly detection alerts")
                
                elif root_cause_type == 'correlation':
                    metrics = root_cause.get('metrics', [])
                    recommendations.append(f"Investigate the relationship between {metrics[0]} and {metrics[1]}")
                    recommendations.append("Consider if this correlation indicates a causal relationship")
                
                elif root_cause_type == 'system':
                    recommendations.append(f"Monitor {root_cause.get('metric', 'system metric')} more closely")
                    recommendations.append("Consider scaling resources if usage remains high")
                
                elif root_cause_type == 'deployment':
                    recommendations.append("Review recent deployment failures")
                    recommendations.append("Implement deployment rollback procedures")
                    recommendations.append("Add deployment health checks")
            
            # Add general recommendations based on impact
            if impact_assessment.get('overall_impact') in ['high', 'critical']:
                recommendations.append("Implement immediate monitoring and alerting")
                recommendations.append("Consider rolling back recent changes")
                recommendations.append("Schedule incident review meeting")
            
            if not recommendations:
                recommendations.append("Continue monitoring the situation")
                recommendations.append("Review system logs for additional insights")
            
            return recommendations
            
        except Exception as e:
            self.logger.error(f"Error generating recommendations: {e}")
            return ["Continue monitoring the situation"]
    
    async def get_investigation_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get summary of RCA investigations"""
        try:
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            
            summary = {
                'total_investigations': 0,
                'completed_investigations': 0,
                'failed_investigations': 0,
                'in_progress_investigations': 0,
                'investigations_by_severity': {},
                'recent_investigations': [],
                'avg_investigation_time': 0.0
            }
            
            investigation_times = []
            
            for investigation in self.investigations.values():
                if investigation.created_at >= cutoff_time:
                    summary['total_investigations'] += 1
                    
                    # Count by status
                    if investigation.status == RCAStatus.COMPLETED:
                        summary['completed_investigations'] += 1
                    elif investigation.status == RCAStatus.FAILED:
                        summary['failed_investigations'] += 1
                    elif investigation.status == RCAStatus.IN_PROGRESS:
                        summary['in_progress_investigations'] += 1
                    
                    # Count by severity
                    severity = investigation.severity.value
                    summary['investigations_by_severity'][severity] = summary['investigations_by_severity'].get(severity, 0) + 1
                    
                    # Calculate investigation time
                    if investigation.completed_at and investigation.started_at:
                        duration = (investigation.completed_at - investigation.started_at).total_seconds()
                        investigation_times.append(duration)
                    
                    # Add to recent investigations
                    summary['recent_investigations'].append({
                        'investigation_id': investigation.investigation_id,
                        'title': investigation.title,
                        'status': investigation.status.value,
                        'severity': investigation.severity.value,
                        'created_at': investigation.created_at.isoformat(),
                        'root_causes_count': len(investigation.root_causes or [])
                    })
            
            # Calculate average investigation time
            if investigation_times:
                summary['avg_investigation_time'] = np.mean(investigation_times)
            
            # Sort recent investigations by creation time
            summary['recent_investigations'].sort(key=lambda x: x['created_at'], reverse=True)
            summary['recent_investigations'] = summary['recent_investigations'][:10]  # Keep only 10 most recent
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting investigation summary: {e}")
            return {}
    
    async def _store_rca_trigger(self, trigger: RCATrigger):
        """Store RCA trigger in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"rca_trigger:{trigger.trigger_id}",
                json.dumps({
                    'trigger_id': trigger.trigger_id,
                    'name': trigger.name,
                    'description': trigger.description,
                    'trigger_conditions': trigger.trigger_conditions,
                    'severity': trigger.severity.value,
                    'enabled': trigger.enabled,
                    'auto_trigger': trigger.auto_trigger
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing RCA trigger: {e}")
    
    async def _store_investigation(self, investigation: RCAInvestigation):
        """Store investigation in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"rca_investigation:{investigation.investigation_id}",
                json.dumps({
                    'investigation_id': investigation.investigation_id,
                    'trigger_id': investigation.trigger_id,
                    'title': investigation.title,
                    'description': investigation.description,
                    'status': investigation.status.value,
                    'severity': investigation.severity.value,
                    'created_at': investigation.created_at.isoformat(),
                    'started_at': investigation.started_at.isoformat() if investigation.started_at else None,
                    'completed_at': investigation.completed_at.isoformat() if investigation.completed_at else None,
                    'assigned_to': investigation.assigned_to,
                    'findings': investigation.findings,
                    'root_causes': investigation.root_causes,
                    'recommendations': investigation.recommendations,
                    'impact_assessment': investigation.impact_assessment
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing investigation: {e}")
    
    async def _update_investigation(self, investigation: RCAInvestigation):
        """Update investigation in Redis"""
        try:
            await self._store_investigation(investigation)
        except Exception as e:
            self.logger.error(f"Error updating investigation: {e}")
    
    async def _store_correlation_analysis(self, analysis: CorrelationAnalysis):
        """Store correlation analysis in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"correlation_analysis:{analysis.analysis_id}",
                json.dumps({
                    'analysis_id': analysis.analysis_id,
                    'investigation_id': analysis.investigation_id,
                    'significant_correlations': analysis.significant_correlations,
                    'timestamp': analysis.timestamp.isoformat()
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing correlation analysis: {e}")
    
    async def _store_anomaly_detection(self, anomaly: AnomalyDetection):
        """Store anomaly detection in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"anomaly_detection:{anomaly.detection_id}",
                json.dumps({
                    'detection_id': anomaly.detection_id,
                    'investigation_id': anomaly.investigation_id,
                    'metric_name': anomaly.metric_name,
                    'anomaly_type': anomaly.anomaly_type,
                    'severity': anomaly.severity,
                    'confidence': anomaly.confidence,
                    'timestamp': anomaly.timestamp.isoformat(),
                    'details': anomaly.details
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing anomaly detection: {e}")
    
    async def start_rca_monitoring(self):
        """Start continuous RCA monitoring"""
        self.running = True
        self.logger.info("Starting RCA monitoring")
        
        while self.running:
            try:
                # Monitor for new triggers
                # Check investigation progress
                # Clean up completed investigations
                self.logger.debug("RCA monitoring active")
                
                # Wait before next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in RCA monitoring: {e}")
                await asyncio.sleep(60)
    
    async def stop_rca_monitoring(self):
        """Stop RCA monitoring"""
        self.running = False
        self.logger.info("Stopping RCA monitoring")
        
        # Cancel all investigation tasks
        for task in self.investigation_tasks.values():
            task.cancel()
        self.investigation_tasks.clear()
