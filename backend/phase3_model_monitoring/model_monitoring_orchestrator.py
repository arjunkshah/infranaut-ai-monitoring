import asyncio
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import redis.asyncio as aioredis

from services.performance_tracker import ModelPerformanceTracker, ModelConfig, PerformanceMetrics
from services.lifecycle_manager import ModelLifecycleManager, ModelVersion, ModelStatus, Deployment, DeploymentStatus, RetrainingTrigger
from services.ab_testing import ABTestingSystem, ABTestConfig, ABTestResult

@dataclass
class MonitoringConfig:
    """Configuration for model monitoring"""
    model_id: str
    performance_tracking_enabled: bool = True
    lifecycle_management_enabled: bool = True
    ab_testing_enabled: bool = False
    monitoring_interval: int = 300  # seconds
    alert_thresholds: Dict[str, float] = None

@dataclass
class MonitoringResult:
    """Result of model monitoring"""
    model_id: str
    timestamp: datetime
    performance_metrics: Optional[PerformanceMetrics]
    lifecycle_status: Optional[Dict[str, Any]]
    ab_test_results: Optional[ABTestResult]
    alerts: List[Dict[str, Any]]
    processing_time: float
    success: bool
    errors: List[str]

class ModelMonitoringOrchestrator:
    """
    Main orchestrator for Phase 3 model monitoring
    Coordinates performance tracking, lifecycle management, and A/B testing
    """
    
    def __init__( self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.configs: Dict[str, MonitoringConfig] = {}
        self.performance_tracker = ModelPerformanceTracker(redis_url)
        self.lifecycle_manager = ModelLifecycleManager(redis_url)
        self.ab_testing = ABTestingSystem(redis_url)
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Monitoring history
        self.monitoring_history: Dict[str, List[MonitoringResult]] = {}
        
    async def add_monitoring_config(self, config: MonitoringConfig) -> bool:
        """Add a model monitoring configuration"""
        try:
            self.configs[config.model_id] = config
            self.logger.info(f"Added monitoring config for model: {config.model_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add monitoring config: {e}")
            return False
    
    async def setup_performance_tracking(self, model_id: str, model_config: ModelConfig) -> bool:
        """Setup performance tracking for a model"""
        try:
            success = await self.performance_tracker.add_model_config(model_config)
            if success:
                self.logger.info(f"Setup performance tracking for model: {model_id}")
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to setup performance tracking: {e}")
            return False
    
    async def setup_lifecycle_management(self, model_id: str, model_version: ModelVersion) -> bool:
        """Setup lifecycle management for a model"""
        try:
            success = await self.lifecycle_manager.register_model_version(model_version)
            if success:
                self.logger.info(f"Setup lifecycle management for model: {model_id}")
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to setup lifecycle management: {e}")
            return False
    
    async def setup_ab_testing(self, model_id: str, ab_test_config: ABTestConfig) -> bool:
        """Setup A/B testing for a model"""
        try:
            success = await self.ab_testing.create_ab_test(ab_test_config)
            if success:
                self.logger.info(f"Setup A/B testing for model: {model_id}")
            return success
            
        except Exception as e:
            self.logger.error(f"Failed to setup A/B testing: {e}")
            return False
    
    async def record_model_prediction(self, model_id: str, y_true: List[Any], y_pred: List[Any], 
                                    latency_ms: float, user_id: Optional[str] = None) -> bool:
        """Record model prediction for monitoring"""
        try:
            if model_id not in self.configs:
                raise ValueError(f"No monitoring config found for model: {model_id}")
            
            config = self.configs[model_id]
            success = True
            
            # Record performance metrics
            if config.performance_tracking_enabled:
                perf_success = await self.performance_tracker.record_prediction(
                    model_id, y_true, y_pred, latency_ms
                )
                if not perf_success:
                    success = False
            
            # Record A/B test results if applicable
            if config.ab_testing_enabled and user_id:
                # Find active A/B test for this model
                active_tests = [test for test in self.ab_testing.tests.values() 
                              if test.model_a_id == model_id or test.model_b_id == model_id]
                
                for test in active_tests:
                    ab_success = await self.ab_testing.record_prediction_result(
                        test.test_id, user_id, y_true, y_pred, latency_ms
                    )
                    if not ab_success:
                        success = False
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error recording model prediction: {e}")
            return False
    
    async def monitor_model(self, model_id: str) -> MonitoringResult:
        """Monitor a single model"""
        start_time = datetime.utcnow()
        errors = []
        alerts = []
        
        try:
            if model_id not in self.configs:
                raise ValueError(f"No monitoring config found for model: {model_id}")
            
            config = self.configs[model_id]
            
            performance_metrics = None
            lifecycle_status = None
            ab_test_results = None
            
            # Monitor performance
            if config.performance_tracking_enabled:
                try:
                    # Get recent performance summary
                    perf_summary = await self.performance_tracker.get_performance_summary(model_id, hours=24)
                    if perf_summary:
                        # Check for performance alerts
                        if config.alert_thresholds:
                            for metric, threshold in config.alert_thresholds.items():
                                current_value = perf_summary.get(f'avg_{metric}', 0.0)
                                if metric == 'latency_ms':
                                    if current_value > threshold:
                                        alerts.append({
                                            'type': 'performance_threshold',
                                            'metric': metric,
                                            'current_value': current_value,
                                            'threshold': threshold,
                                            'severity': 'warning'
                                        })
                                else:
                                    if current_value < threshold:
                                        alerts.append({
                                            'type': 'performance_threshold',
                                            'metric': metric,
                                            'current_value': current_value,
                                            'threshold': threshold,
                                            'severity': 'warning'
                                        })
                except Exception as e:
                    errors.append(f"Performance monitoring error: {e}")
            
            # Monitor lifecycle
            if config.lifecycle_management_enabled:
                try:
                    # Get model lineage
                    lineage = await self.lifecycle_manager.get_model_lineage(model_id)
                    if lineage:
                        lifecycle_status = {
                            'total_versions': lineage.get('total_versions', 0),
                            'current_production': lineage.get('current_production'),
                            'current_staging': lineage.get('current_staging'),
                            'latest_version': lineage['versions'][-1]['version'] if lineage['versions'] else None
                        }
                        
                        # Check for retraining triggers
                        if model_id in self.performance_tracker.performance_history:
                            recent_metrics = self.performance_tracker.performance_history[model_id][-10:]
                            if recent_metrics:
                                current_metrics = {
                                    'accuracy': np.mean([m.accuracy for m in recent_metrics]),
                                    'latency_ms': np.mean([m.latency_ms for m in recent_metrics])
                                }
                                
                                triggered_triggers = await self.lifecycle_manager.check_retraining_triggers(
                                    model_id, current_metrics
                                )
                                
                                if triggered_triggers:
                                    alerts.append({
                                        'type': 'retraining_trigger',
                                        'triggered_triggers': triggered_triggers,
                                        'severity': 'info'
                                    })
                except Exception as e:
                    errors.append(f"Lifecycle monitoring error: {e}")
            
            # Monitor A/B tests
            if config.ab_testing_enabled:
                try:
                    # Find active A/B tests for this model
                    active_tests = [test for test in self.ab_testing.tests.values() 
                                  if test.model_a_id == model_id or test.model_b_id == model_id]
                    
                    for test in active_tests:
                        # Analyze A/B test if enough data
                        test_summary = await self.ab_testing.get_test_summary(test.test_id)
                        if test_summary.get('traffic_with_results', 0) >= test.min_sample_size:
                            ab_result = await self.ab_testing.analyze_ab_test(test.test_id)
                            if ab_result:
                                ab_test_results = ab_result
                                
                                # Check for significant results
                                if ab_result.statistical_significance:
                                    alerts.append({
                                        'type': 'ab_test_significant',
                                        'test_id': test.test_id,
                                        'winner': ab_result.winner,
                                        'p_value': ab_result.p_value,
                                        'severity': 'info'
                                    })
                except Exception as e:
                    errors.append(f"A/B testing monitoring error: {e}")
            
            # Create monitoring result
            result = MonitoringResult(
                model_id=model_id,
                timestamp=start_time,
                performance_metrics=performance_metrics,
                lifecycle_status=lifecycle_status,
                ab_test_results=ab_test_results,
                alerts=alerts,
                processing_time=(datetime.utcnow() - start_time).total_seconds(),
                success=len(errors) == 0,
                errors=errors
            )
            
            await self._store_monitoring_result(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error monitoring model {model_id}: {e}")
            return MonitoringResult(
                model_id=model_id,
                timestamp=start_time,
                performance_metrics=None,
                lifecycle_status=None,
                ab_test_results=None,
                alerts=[],
                processing_time=(datetime.utcnow() - start_time).total_seconds(),
                success=False,
                errors=[str(e)]
            )
    
    async def _store_monitoring_result(self, result: MonitoringResult):
        """Store monitoring result"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store result
            await redis.lpush(
                f"monitoring_results:{result.model_id}",
                json.dumps({
                    'model_id': result.model_id,
                    'timestamp': result.timestamp.isoformat(),
                    'alerts_count': len(result.alerts),
                    'processing_time': result.processing_time,
                    'success': result.success,
                    'errors': result.errors
                })
            )
            
            # Keep only recent results
            await redis.ltrim(f"monitoring_results:{result.model_id}", 0, 999)
            
            # Update monitoring history
            if result.model_id not in self.monitoring_history:
                self.monitoring_history[result.model_id] = []
            self.monitoring_history[result.model_id].append(result)
            
            # Keep only recent history
            if len(self.monitoring_history[result.model_id]) > 1000:
                self.monitoring_history[result.model_id] = self.monitoring_history[result.model_id][-1000:]
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing monitoring result: {e}")
    
    async def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get summary of all monitoring activities"""
        try:
            summary = {
                'total_models': len(self.configs),
                'active_models': sum(1 for config in self.configs.values() if config.performance_tracking_enabled or config.lifecycle_management_enabled),
                'total_monitoring_runs': 0,
                'successful_runs': 0,
                'failed_runs': 0,
                'avg_processing_time': 0.0,
                'total_alerts': 0,
                'model_details': {}
            }
            
            processing_times = []
            total_alerts = 0
            
            for model_id, config in self.configs.items():
                redis = await aioredis.from_url(self.redis_url)
                results = await redis.lrange(f"monitoring_results:{model_id}", 0, 999)
                await redis.close()
                
                if results:
                    parsed_results = [json.loads(result) for result in results]
                    total_runs = len(parsed_results)
                    successful_runs = sum(1 for result in parsed_results if result['success'])
                    failed_runs = total_runs - successful_runs
                    avg_time = np.mean([result['processing_time'] for result in parsed_results])
                    model_alerts = sum([result['alerts_count'] for result in parsed_results])
                    
                    summary['model_details'][model_id] = {
                        'performance_tracking_enabled': config.performance_tracking_enabled,
                        'lifecycle_management_enabled': config.lifecycle_management_enabled,
                        'ab_testing_enabled': config.ab_testing_enabled,
                        'total_runs': total_runs,
                        'successful_runs': successful_runs,
                        'failed_runs': failed_runs,
                        'avg_processing_time': avg_time,
                        'total_alerts': model_alerts,
                        'last_monitored': parsed_results[0]['timestamp'] if parsed_results else None
                    }
                    
                    summary['total_monitoring_runs'] += total_runs
                    summary['successful_runs'] += successful_runs
                    summary['failed_runs'] += failed_runs
                    processing_times.extend([result['processing_time'] for result in parsed_results])
                    total_alerts += model_alerts
            
            if processing_times:
                summary['avg_processing_time'] = np.mean(processing_times)
            
            summary['total_alerts'] = total_alerts
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting monitoring summary: {e}")
            return {}
    
    async def get_comprehensive_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of all Phase 3 activities"""
        try:
            summary = {
                'monitoring_summary': await self.get_monitoring_summary(),
                'performance_summary': await self.performance_tracker.get_comprehensive_summary(),
                'lifecycle_summary': await self.lifecycle_manager.get_deployment_summary(),
                'ab_testing_summary': await self.ab_testing.get_comprehensive_summary(),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive summary: {e}")
            return {}
    
    async def start_model_monitoring(self):
        """Start the model monitoring pipeline"""
        self.running = True
        self.logger.info("Starting model monitoring pipeline")
        
        # Start individual components
        await self.performance_tracker.start_performance_monitoring()
        await self.lifecycle_manager.start_lifecycle_monitoring()
        await self.ab_testing.start_ab_testing_monitoring()
        
        while self.running:
            try:
                # Monitor each configured model
                for model_id, config in self.configs.items():
                    try:
                        result = await self.monitor_model(model_id)
                        
                        if not result.success:
                            self.logger.error(f"Monitoring failed for {model_id}: {result.errors}")
                        else:
                            if result.alerts:
                                self.logger.warning(f"Alerts for {model_id}: {len(result.alerts)} alerts")
                            else:
                                self.logger.info(f"Monitoring completed for {model_id} in {result.processing_time:.2f}s")
                            
                    except Exception as e:
                        self.logger.error(f"Error monitoring model {model_id}: {e}")
                
                # Wait before next monitoring cycle
                min_interval = min(config.monitoring_interval for config in self.configs.values())
                await asyncio.sleep(min_interval)
                
            except Exception as e:
                self.logger.error(f"Error in model monitoring pipeline: {e}")
                await asyncio.sleep(60)
    
    async def stop_model_monitoring(self):
        """Stop the model monitoring pipeline"""
        self.running = False
        self.logger.info("Stopping model monitoring pipeline")
        
        # Stop individual components
        await self.performance_tracker.stop_performance_monitoring()
        await self.lifecycle_manager.stop_lifecycle_monitoring()
        await self.ab_testing.stop_ab_testing_monitoring()
