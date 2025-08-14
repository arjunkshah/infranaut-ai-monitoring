import asyncio
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score
import redis.asyncio as aioredis

@dataclass
class PerformanceMetrics:
    """Model performance metrics"""
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    auc_score: float
    latency_ms: float
    throughput_rps: float
    timestamp: datetime

@dataclass
class ModelConfig:
    """Configuration for model performance tracking"""
    model_id: str
    model_name: str
    model_version: str
    model_type: str  # 'classification', 'regression', 'clustering'
    target_metrics: Dict[str, float]  # target values for each metric
    alert_thresholds: Dict[str, float]  # thresholds for alerts
    enabled: bool = True

@dataclass
class PerformanceAlert:
    """Performance alert"""
    model_id: str
    metric_name: str
    current_value: float
    threshold_value: float
    alert_type: str  # 'degradation', 'improvement', 'threshold_breach'
    severity: str  # 'info', 'warning', 'error', 'critical'
    timestamp: datetime
    details: Dict[str, Any]

class ModelPerformanceTracker:
    """
    Comprehensive model performance tracking system
    Monitors accuracy, precision, recall, F1-score, latency, and throughput
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.models: Dict[str, ModelConfig] = {}
        self.performance_history: Dict[str, List[PerformanceMetrics]] = {}
        self.alerts: Dict[str, List[PerformanceAlert]] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
    async def add_model_config(self, config: ModelConfig) -> bool:
        """Add a model configuration for performance tracking"""
        try:
            self.models[config.model_id] = config
            self.logger.info(f"Added performance tracking for model: {config.model_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add model config: {e}")
            return False
    
    async def record_prediction(self, model_id: str, y_true: List[Any], y_pred: List[Any], 
                              latency_ms: float, timestamp: Optional[datetime] = None) -> bool:
        """Record model prediction and calculate performance metrics"""
        try:
            if model_id not in self.models:
                raise ValueError(f"No model config found for: {model_id}")
            
            if not timestamp:
                timestamp = datetime.utcnow()
            
            # Calculate performance metrics
            metrics = await self._calculate_metrics(model_id, y_true, y_pred, latency_ms)
            metrics.timestamp = timestamp
            
            # Store metrics
            await self._store_performance_metrics(model_id, metrics)
            
            # Check for alerts
            await self._check_performance_alerts(model_id, metrics)
            
            # Update performance history
            if model_id not in self.performance_history:
                self.performance_history[model_id] = []
            self.performance_history[model_id].append(metrics)
            
            # Keep only recent history
            if len(self.performance_history[model_id]) > 1000:
                self.performance_history[model_id] = self.performance_history[model_id][-1000:]
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error recording prediction for {model_id}: {e}")
            return False
    
    async def _calculate_metrics(self, model_id: str, y_true: List[Any], y_pred: List[Any], 
                               latency_ms: float) -> PerformanceMetrics:
        """Calculate performance metrics"""
        try:
            model_config = self.models[model_id]
            
            if model_config.model_type == 'classification':
                # Classification metrics
                accuracy = accuracy_score(y_true, y_pred)
                precision = precision_score(y_true, y_pred, average='weighted', zero_division=0)
                recall = recall_score(y_true, y_pred, average='weighted', zero_division=0)
                f1 = f1_score(y_true, y_pred, average='weighted', zero_division=0)
                
                # AUC score (for binary classification)
                try:
                    if len(set(y_true)) == 2:  # Binary classification
                        auc = roc_auc_score(y_true, y_pred)
                    else:
                        auc = 0.0  # Multi-class AUC not implemented
                except:
                    auc = 0.0
                    
            elif model_config.model_type == 'regression':
                # Regression metrics
                mse = np.mean((np.array(y_true) - np.array(y_pred)) ** 2)
                rmse = np.sqrt(mse)
                mae = np.mean(np.abs(np.array(y_true) - np.array(y_pred)))
                
                # Convert to 0-1 scale for consistency
                max_val = max(max(y_true), max(y_pred))
                min_val = min(min(y_true), min(y_pred))
                if max_val != min_val:
                    accuracy = 1 - (rmse / (max_val - min_val))
                    precision = 1 - (mae / (max_val - min_val))
                    recall = accuracy  # For regression, use accuracy as recall
                    f1 = accuracy  # For regression, use accuracy as F1
                    auc = accuracy  # For regression, use accuracy as AUC
                else:
                    accuracy = precision = recall = f1 = auc = 1.0
                    
            else:  # clustering or other
                # Simple accuracy-like metric
                accuracy = precision = recall = f1 = auc = 0.5
            
            # Calculate throughput (requests per second)
            throughput_rps = 1000 / latency_ms if latency_ms > 0 else 0
            
            return PerformanceMetrics(
                accuracy=float(accuracy),
                precision=float(precision),
                recall=float(recall),
                f1_score=float(f1),
                auc_score=float(auc),
                latency_ms=float(latency_ms),
                throughput_rps=float(throughput_rps),
                timestamp=datetime.utcnow()
            )
            
        except Exception as e:
            self.logger.error(f"Error calculating metrics: {e}")
            # Return default metrics
            return PerformanceMetrics(
                accuracy=0.0,
                precision=0.0,
                recall=0.0,
                f1_score=0.0,
                auc_score=0.0,
                latency_ms=float(latency_ms),
                throughput_rps=0.0,
                timestamp=datetime.utcnow()
            )
    
    async def _store_performance_metrics(self, model_id: str, metrics: PerformanceMetrics):
        """Store performance metrics in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store metrics
            await redis.lpush(
                f"performance_metrics:{model_id}",
                json.dumps({
                    'model_id': model_id,
                    'accuracy': metrics.accuracy,
                    'precision': metrics.precision,
                    'recall': metrics.recall,
                    'f1_score': metrics.f1_score,
                    'auc_score': metrics.auc_score,
                    'latency_ms': metrics.latency_ms,
                    'throughput_rps': metrics.throughput_rps,
                    'timestamp': metrics.timestamp.isoformat()
                })
            )
            
            # Keep only recent metrics
            await redis.ltrim(f"performance_metrics:{model_id}", 0, 999)
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing performance metrics: {e}")
    
    async def _check_performance_alerts(self, model_id: str, metrics: PerformanceMetrics):
        """Check for performance alerts"""
        try:
            model_config = self.models[model_id]
            
            for metric_name, threshold in model_config.alert_thresholds.items():
                current_value = getattr(metrics, metric_name, 0.0)
                
                # Determine alert type based on metric
                if metric_name in ['latency_ms']:
                    # Higher is worse for latency
                    if current_value > threshold:
                        alert = PerformanceAlert(
                            model_id=model_id,
                            metric_name=metric_name,
                            current_value=current_value,
                            threshold_value=threshold,
                            alert_type='threshold_breach',
                            severity='warning',
                            timestamp=datetime.utcnow(),
                            details={'message': f'Latency exceeded threshold: {current_value}ms > {threshold}ms'}
                        )
                        await self._store_alert(alert)
                else:
                    # Lower is worse for accuracy, precision, recall, f1_score
                    if current_value < threshold:
                        alert = PerformanceAlert(
                            model_id=model_id,
                            metric_name=metric_name,
                            current_value=current_value,
                            threshold_value=threshold,
                            alert_type='threshold_breach',
                            severity='warning',
                            timestamp=datetime.utcnow(),
                            details={'message': f'{metric_name} below threshold: {current_value} < {threshold}'}
                        )
                        await self._store_alert(alert)
            
            # Check for significant degradation
            await self._check_degradation(model_id, metrics)
            
        except Exception as e:
            self.logger.error(f"Error checking performance alerts: {e}")
    
    async def _check_degradation(self, model_id: str, current_metrics: PerformanceMetrics):
        """Check for significant performance degradation"""
        try:
            if model_id not in self.performance_history or len(self.performance_history[model_id]) < 10:
                return
            
            # Get recent metrics (last 10 predictions)
            recent_metrics = self.performance_history[model_id][-10:]
            
            # Calculate average of recent metrics
            avg_accuracy = np.mean([m.accuracy for m in recent_metrics[:-1]])  # Exclude current
            avg_latency = np.mean([m.latency_ms for m in recent_metrics[:-1]])
            
            # Check for significant degradation
            accuracy_degradation = (avg_accuracy - current_metrics.accuracy) / avg_accuracy if avg_accuracy > 0 else 0
            latency_degradation = (current_metrics.latency_ms - avg_latency) / avg_latency if avg_latency > 0 else 0
            
            if accuracy_degradation > 0.1:  # 10% degradation
                alert = PerformanceAlert(
                    model_id=model_id,
                    metric_name='accuracy',
                    current_value=current_metrics.accuracy,
                    threshold_value=avg_accuracy,
                    alert_type='degradation',
                    severity='error',
                    timestamp=datetime.utcnow(),
                    details={
                        'message': f'Significant accuracy degradation detected: {accuracy_degradation:.2%}',
                        'degradation_pct': accuracy_degradation
                    }
                )
                await self._store_alert(alert)
            
            if latency_degradation > 0.2:  # 20% degradation
                alert = PerformanceAlert(
                    model_id=model_id,
                    metric_name='latency',
                    current_value=current_metrics.latency_ms,
                    threshold_value=avg_latency,
                    alert_type='degradation',
                    severity='error',
                    timestamp=datetime.utcnow(),
                    details={
                        'message': f'Significant latency degradation detected: {latency_degradation:.2%}',
                        'degradation_pct': latency_degradation
                    }
                )
                await self._store_alert(alert)
                
        except Exception as e:
            self.logger.error(f"Error checking degradation: {e}")
    
    async def _store_alert(self, alert: PerformanceAlert):
        """Store performance alert"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store alert
            await redis.lpush(
                f"performance_alerts:{alert.model_id}",
                json.dumps({
                    'model_id': alert.model_id,
                    'metric_name': alert.metric_name,
                    'current_value': alert.current_value,
                    'threshold_value': alert.threshold_value,
                    'alert_type': alert.alert_type,
                    'severity': alert.severity,
                    'timestamp': alert.timestamp.isoformat(),
                    'details': alert.details
                })
            )
            
            # Keep only recent alerts
            await redis.ltrim(f"performance_alerts:{alert.model_id}", 0, 999)
            
            # Update alerts history
            if alert.model_id not in self.alerts:
                self.alerts[alert.model_id] = []
            self.alerts[alert.model_id].append(alert)
            
            # Keep only recent alerts history
            if len(self.alerts[alert.model_id]) > 1000:
                self.alerts[alert.model_id] = self.alerts[alert.model_id][-1000:]
            
            await redis.close()
            
            # Log alert
            self.logger.warning(f"Performance alert for {alert.model_id}: {alert.details.get('message', 'Unknown alert')}")
            
        except Exception as e:
            self.logger.error(f"Error storing alert: {e}")
    
    async def get_performance_summary(self, model_id: str, hours: int = 24) -> Dict[str, Any]:
        """Get performance summary for a model"""
        try:
            if model_id not in self.performance_history:
                return {}
            
            # Get metrics from the specified time period
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            recent_metrics = [
                m for m in self.performance_history[model_id]
                if m.timestamp >= cutoff_time
            ]
            
            if not recent_metrics:
                return {}
            
            # Calculate summary statistics
            summary = {
                'model_id': model_id,
                'time_period_hours': hours,
                'total_predictions': len(recent_metrics),
                'avg_accuracy': float(np.mean([m.accuracy for m in recent_metrics])),
                'avg_precision': float(np.mean([m.precision for m in recent_metrics])),
                'avg_recall': float(np.mean([m.recall for m in recent_metrics])),
                'avg_f1_score': float(np.mean([m.f1_score for m in recent_metrics])),
                'avg_auc_score': float(np.mean([m.auc_score for m in recent_metrics])),
                'avg_latency_ms': float(np.mean([m.latency_ms for m in recent_metrics])),
                'avg_throughput_rps': float(np.mean([m.throughput_rps for m in recent_metrics])),
                'min_accuracy': float(np.min([m.accuracy for m in recent_metrics])),
                'max_accuracy': float(np.max([m.accuracy for m in recent_metrics])),
                'min_latency_ms': float(np.min([m.latency_ms for m in recent_metrics])),
                'max_latency_ms': float(np.max([m.latency_ms for m in recent_metrics])),
                'last_updated': recent_metrics[-1].timestamp.isoformat()
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting performance summary: {e}")
            return {}
    
    async def get_performance_trends(self, model_id: str, hours: int = 24) -> Dict[str, Any]:
        """Get performance trends for a model"""
        try:
            if model_id not in self.performance_history:
                return {}
            
            # Get metrics from the specified time period
            cutoff_time = datetime.utcnow() - timedelta(hours=hours)
            recent_metrics = [
                m for m in self.performance_history[model_id]
                if m.timestamp >= cutoff_time
            ]
            
            if len(recent_metrics) < 2:
                return {}
            
            # Calculate trends (slope of linear regression)
            timestamps = [(m.timestamp - cutoff_time).total_seconds() for m in recent_metrics]
            
            trends = {}
            for metric_name in ['accuracy', 'precision', 'recall', 'f1_score', 'latency_ms']:
                values = [getattr(m, metric_name) for m in recent_metrics]
                
                # Calculate trend (positive = improving, negative = degrading)
                if len(values) > 1:
                    slope = np.polyfit(timestamps, values, 1)[0]
                    trends[f'{metric_name}_trend'] = float(slope)
                    trends[f'{metric_name}_trend_direction'] = 'improving' if slope > 0 else 'degrading'
                else:
                    trends[f'{metric_name}_trend'] = 0.0
                    trends[f'{metric_name}_trend_direction'] = 'stable'
            
            return trends
            
        except Exception as e:
            self.logger.error(f"Error getting performance trends: {e}")
            return {}
    
    async def get_alerts_summary(self, model_id: str, hours: int = 24) -> Dict[str, Any]:
        """Get alerts summary for a model"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            alerts = await redis.lrange(f"performance_alerts:{model_id}", 0, 999)
            await redis.close()
            
            if not alerts:
                return {}
            
            # Parse alerts
            parsed_alerts = []
            for alert_str in alerts:
                try:
                    alert_data = json.loads(alert_str)
                    alert_time = datetime.fromisoformat(alert_data['timestamp'])
                    if alert_time >= datetime.utcnow() - timedelta(hours=hours):
                        parsed_alerts.append(alert_data)
                except:
                    continue
            
            # Calculate summary
            summary = {
                'total_alerts': len(parsed_alerts),
                'alerts_by_severity': {},
                'alerts_by_type': {},
                'alerts_by_metric': {}
            }
            
            for alert in parsed_alerts:
                # Count by severity
                severity = alert['severity']
                summary['alerts_by_severity'][severity] = summary['alerts_by_severity'].get(severity, 0) + 1
                
                # Count by type
                alert_type = alert['alert_type']
                summary['alerts_by_type'][alert_type] = summary['alerts_by_type'].get(alert_type, 0) + 1
                
                # Count by metric
                metric = alert['metric_name']
                summary['alerts_by_metric'][metric] = summary['alerts_by_metric'].get(metric, 0) + 1
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting alerts summary: {e}")
            return {}
    
    async def get_comprehensive_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of all models"""
        try:
            summary = {
                'total_models': len(self.models),
                'active_models': sum(1 for model in self.models.values() if model.enabled),
                'model_details': {},
                'overall_performance': {
                    'avg_accuracy': 0.0,
                    'avg_latency_ms': 0.0,
                    'total_predictions': 0,
                    'total_alerts': 0
                }
            }
            
            total_accuracy = 0.0
            total_latency = 0.0
            total_predictions = 0
            total_alerts = 0
            
            for model_id, model_config in self.models.items():
                # Get performance summary
                perf_summary = await self.get_performance_summary(model_id, hours=24)
                alerts_summary = await self.get_alerts_summary(model_id, hours=24)
                
                summary['model_details'][model_id] = {
                    'model_name': model_config.model_name,
                    'model_version': model_config.model_version,
                    'model_type': model_config.model_type,
                    'enabled': model_config.enabled,
                    'performance_summary': perf_summary,
                    'alerts_summary': alerts_summary
                }
                
                if perf_summary:
                    total_accuracy += perf_summary.get('avg_accuracy', 0.0)
                    total_latency += perf_summary.get('avg_latency_ms', 0.0)
                    total_predictions += perf_summary.get('total_predictions', 0)
                
                if alerts_summary:
                    total_alerts += alerts_summary.get('total_alerts', 0)
            
            # Calculate overall averages
            if len(self.models) > 0:
                summary['overall_performance']['avg_accuracy'] = total_accuracy / len(self.models)
                summary['overall_performance']['avg_latency_ms'] = total_latency / len(self.models)
                summary['overall_performance']['total_predictions'] = total_predictions
                summary['overall_performance']['total_alerts'] = total_alerts
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive summary: {e}")
            return {}
    
    async def start_performance_monitoring(self):
        """Start continuous performance monitoring"""
        self.running = True
        self.logger.info("Starting performance monitoring")
        
        while self.running:
            try:
                # Monitor performance for all active models
                for model_id, model_config in self.models.items():
                    if not model_config.enabled:
                        continue
                    
                    # Get recent performance data and check for issues
                    # This would typically involve checking actual model predictions
                    # For now, we'll just log that monitoring is active
                    self.logger.debug(f"Monitoring performance for model: {model_id}")
                
                # Wait before next check
                await asyncio.sleep(60)  # Check every minute
                
            except Exception as e:
                self.logger.error(f"Error in performance monitoring: {e}")
                await asyncio.sleep(60)
    
    async def stop_performance_monitoring(self):
        """Stop performance monitoring"""
        self.running = False
        self.logger.info("Stopping performance monitoring")
