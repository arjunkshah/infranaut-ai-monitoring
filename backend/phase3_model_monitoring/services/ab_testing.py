import asyncio
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from scipy import stats
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
import redis.asyncio as aioredis

@dataclass
class ABTestConfig:
    """A/B test configuration"""
    test_id: str
    test_name: str
    model_a_id: str
    model_b_id: str
    traffic_split: float  # percentage of traffic to model B (0.0 to 1.0)
    primary_metric: str  # 'accuracy', 'precision', 'recall', 'f1_score', 'latency'
    significance_level: float = 0.05
    min_sample_size: int = 1000
    max_duration_days: int = 30
    enabled: bool = True

@dataclass
class ABTestResult:
    """A/B test result"""
    test_id: str
    model_a_metrics: Dict[str, float]
    model_b_metrics: Dict[str, float]
    statistical_significance: bool
    p_value: float
    effect_size: float
    winner: Optional[str]  # 'A', 'B', or None if not significant
    confidence_interval: Tuple[float, float]
    sample_size_a: int
    sample_size_b: int
    timestamp: datetime

@dataclass
class ABTestTraffic:
    """A/B test traffic allocation"""
    test_id: str
    user_id: str
    assigned_model: str  # 'A' or 'B'
    timestamp: datetime
    prediction_result: Optional[Dict[str, Any]] = None

class ABTestingSystem:
    """
    Comprehensive A/B testing system for model comparisons
    Provides statistical significance testing and performance analysis
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.tests: Dict[str, ABTestConfig] = {}
        self.traffic: Dict[str, List[ABTestTraffic]] = {}
        self.results: Dict[str, List[ABTestResult]] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
    async def create_ab_test(self, config: ABTestConfig) -> bool:
        """Create a new A/B test"""
        try:
            self.tests[config.test_id] = config
            await self._store_test_config(config)
            
            self.logger.info(f"Created A/B test: {config.test_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create A/B test: {e}")
            return False
    
    async def assign_traffic(self, test_id: str, user_id: str) -> str:
        """Assign traffic to model A or B based on test configuration"""
        try:
            if test_id not in self.tests:
                raise ValueError(f"A/B test not found: {test_id}")
            
            test_config = self.tests[test_id]
            if not test_config.enabled:
                return 'A'  # Default to model A if test is disabled
            
            # Use user_id hash for consistent assignment
            user_hash = hash(user_id) % 100
            traffic_split_percentage = test_config.traffic_split * 100
            
            if user_hash < traffic_split_percentage:
                assigned_model = 'B'
            else:
                assigned_model = 'A'
            
            # Record traffic assignment
            traffic = ABTestTraffic(
                test_id=test_id,
                user_id=user_id,
                assigned_model=assigned_model,
                timestamp=datetime.utcnow()
            )
            
            await self._store_traffic(traffic)
            
            return assigned_model
            
        except Exception as e:
            self.logger.error(f"Error assigning traffic: {e}")
            return 'A'  # Default to model A on error
    
    async def record_prediction_result(self, test_id: str, user_id: str, 
                                     y_true: List[Any], y_pred: List[Any], 
                                     latency_ms: float) -> bool:
        """Record prediction result for A/B test analysis"""
        try:
            # Find the traffic assignment
            traffic = await self._get_traffic(test_id, user_id)
            if not traffic:
                return False
            
            # Calculate metrics
            metrics = self._calculate_metrics(y_true, y_pred, latency_ms)
            
            # Update traffic with prediction result
            traffic.prediction_result = {
                'y_true': y_true,
                'y_pred': y_pred,
                'metrics': metrics,
                'timestamp': datetime.utcnow().isoformat()
            }
            
            await self._update_traffic(traffic)
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error recording prediction result: {e}")
            return False
    
    async def analyze_ab_test(self, test_id: str) -> Optional[ABTestResult]:
        """Analyze A/B test results with statistical significance testing"""
        try:
            if test_id not in self.tests:
                raise ValueError(f"A/B test not found: {test_id}")
            
            test_config = self.tests[test_id]
            
            # Get all traffic for this test
            all_traffic = await self._get_test_traffic(test_id)
            
            # Filter traffic with prediction results
            traffic_with_results = [t for t in all_traffic if t.prediction_result]
            
            if len(traffic_with_results) < test_config.min_sample_size:
                self.logger.warning(f"Insufficient sample size for test {test_id}: {len(traffic_with_results)} < {test_config.min_sample_size}")
                return None
            
            # Separate results by model
            model_a_results = [t for t in traffic_with_results if t.assigned_model == 'A']
            model_b_results = [t for t in traffic_with_results if t.assigned_model == 'B']
            
            if len(model_a_results) < 100 or len(model_b_results) < 100:
                self.logger.warning(f"Insufficient samples per model for test {test_id}")
                return None
            
            # Calculate metrics for each model
            model_a_metrics = self._aggregate_metrics([t.prediction_result['metrics'] for t in model_a_results])
            model_b_metrics = self._aggregate_metrics([t.prediction_result['metrics'] for t in model_b_results])
            
            # Perform statistical significance test
            primary_metric = test_config.primary_metric
            metric_a_values = [t.prediction_result['metrics'][primary_metric] for t in model_a_results]
            metric_b_values = [t.prediction_result['metrics'][primary_metric] for t in model_b_results]
            
            # T-test for statistical significance
            t_stat, p_value = stats.ttest_ind(metric_a_values, metric_b_values)
            
            # Calculate effect size (Cohen's d)
            pooled_std = np.sqrt(((len(metric_a_values) - 1) * np.var(metric_a_values, ddof=1) + 
                                 (len(metric_b_values) - 1) * np.var(metric_b_values, ddof=1)) / 
                                (len(metric_a_values) + len(metric_b_values) - 2))
            effect_size = (np.mean(metric_b_values) - np.mean(metric_a_values)) / pooled_std if pooled_std > 0 else 0
            
            # Determine winner
            statistical_significance = p_value < test_config.significance_level
            winner = None
            
            if statistical_significance:
                if np.mean(metric_b_values) > np.mean(metric_a_values):
                    winner = 'B'
                else:
                    winner = 'A'
            
            # Calculate confidence interval
            mean_diff = np.mean(metric_b_values) - np.mean(metric_a_values)
            se_diff = np.sqrt(np.var(metric_a_values) / len(metric_a_values) + np.var(metric_b_values) / len(metric_b_values))
            ci_lower = mean_diff - 1.96 * se_diff
            ci_upper = mean_diff + 1.96 * se_diff
            
            # Create result
            result = ABTestResult(
                test_id=test_id,
                model_a_metrics=model_a_metrics,
                model_b_metrics=model_b_metrics,
                statistical_significance=statistical_significance,
                p_value=p_value,
                effect_size=effect_size,
                winner=winner,
                confidence_interval=(ci_lower, ci_upper),
                sample_size_a=len(model_a_results),
                sample_size_b=len(model_b_results),
                timestamp=datetime.utcnow()
            )
            
            # Store result
            await self._store_test_result(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error analyzing A/B test: {e}")
            return None
    
    def _calculate_metrics(self, y_true: List[Any], y_pred: List[Any], latency_ms: float) -> Dict[str, float]:
        """Calculate performance metrics"""
        try:
            # Classification metrics
            accuracy = accuracy_score(y_true, y_pred)
            precision = precision_score(y_true, y_pred, average='weighted', zero_division=0)
            recall = recall_score(y_true, y_pred, average='weighted', zero_division=0)
            f1 = f1_score(y_true, y_pred, average='weighted', zero_division=0)
            
            return {
                'accuracy': float(accuracy),
                'precision': float(precision),
                'recall': float(recall),
                'f1_score': float(f1),
                'latency_ms': float(latency_ms)
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating metrics: {e}")
            return {
                'accuracy': 0.0,
                'precision': 0.0,
                'recall': 0.0,
                'f1_score': 0.0,
                'latency_ms': float(latency_ms)
            }
    
    def _aggregate_metrics(self, metrics_list: List[Dict[str, float]]) -> Dict[str, float]:
        """Aggregate metrics across multiple predictions"""
        try:
            aggregated = {}
            
            for metric_name in ['accuracy', 'precision', 'recall', 'f1_score', 'latency_ms']:
                values = [m[metric_name] for m in metrics_list if metric_name in m]
                if values:
                    aggregated[metric_name] = float(np.mean(values))
                else:
                    aggregated[metric_name] = 0.0
            
            return aggregated
            
        except Exception as e:
            self.logger.error(f"Error aggregating metrics: {e}")
            return {}
    
    async def get_test_summary(self, test_id: str) -> Dict[str, Any]:
        """Get summary of A/B test"""
        try:
            if test_id not in self.tests:
                return {}
            
            test_config = self.tests[test_id]
            
            # Get recent result
            recent_result = await self._get_latest_test_result(test_id)
            
            # Get traffic statistics
            all_traffic = await self._get_test_traffic(test_id)
            traffic_with_results = [t for t in all_traffic if t.prediction_result]
            
            summary = {
                'test_id': test_id,
                'test_name': test_config.test_name,
                'model_a_id': test_config.model_a_id,
                'model_b_id': test_config.model_b_id,
                'traffic_split': test_config.traffic_split,
                'primary_metric': test_config.primary_metric,
                'enabled': test_config.enabled,
                'total_traffic': len(all_traffic),
                'traffic_with_results': len(traffic_with_results),
                'traffic_a_count': len([t for t in all_traffic if t.assigned_model == 'A']),
                'traffic_b_count': len([t for t in all_traffic if t.assigned_model == 'B']),
                'latest_result': recent_result
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting test summary: {e}")
            return {}
    
    async def get_comprehensive_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of all A/B tests"""
        try:
            summary = {
                'total_tests': len(self.tests),
                'active_tests': sum(1 for test in self.tests.values() if test.enabled),
                'test_details': {},
                'overall_statistics': {
                    'total_traffic': 0,
                    'significant_results': 0,
                    'model_a_wins': 0,
                    'model_b_wins': 0
                }
            }
            
            total_traffic = 0
            significant_results = 0
            model_a_wins = 0
            model_b_wins = 0
            
            for test_id, test_config in self.tests.items():
                test_summary = await self.get_test_summary(test_id)
                summary['test_details'][test_id] = test_summary
                
                total_traffic += test_summary.get('total_traffic', 0)
                
                latest_result = test_summary.get('latest_result')
                if latest_result:
                    if latest_result.statistical_significance:
                        significant_results += 1
                        if latest_result.winner == 'A':
                            model_a_wins += 1
                        elif latest_result.winner == 'B':
                            model_b_wins += 1
            
            summary['overall_statistics']['total_traffic'] = total_traffic
            summary['overall_statistics']['significant_results'] = significant_results
            summary['overall_statistics']['model_a_wins'] = model_a_wins
            summary['overall_statistics']['model_b_wins'] = model_b_wins
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive summary: {e}")
            return {}
    
    async def _store_test_config(self, config: ABTestConfig):
        """Store test configuration in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"ab_test_config:{config.test_id}",
                json.dumps({
                    'test_id': config.test_id,
                    'test_name': config.test_name,
                    'model_a_id': config.model_a_id,
                    'model_b_id': config.model_b_id,
                    'traffic_split': config.traffic_split,
                    'primary_metric': config.primary_metric,
                    'significance_level': config.significance_level,
                    'min_sample_size': config.min_sample_size,
                    'max_duration_days': config.max_duration_days,
                    'enabled': config.enabled
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing test config: {e}")
    
    async def _store_traffic(self, traffic: ABTestTraffic):
        """Store traffic assignment in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.lpush(
                f"ab_test_traffic:{traffic.test_id}",
                json.dumps({
                    'test_id': traffic.test_id,
                    'user_id': traffic.user_id,
                    'assigned_model': traffic.assigned_model,
                    'timestamp': traffic.timestamp.isoformat(),
                    'prediction_result': traffic.prediction_result
                })
            )
            
            # Keep only recent traffic
            await redis.ltrim(f"ab_test_traffic:{traffic.test_id}", 0, 9999)
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing traffic: {e}")
    
    async def _store_test_result(self, result: ABTestResult):
        """Store test result in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.lpush(
                f"ab_test_results:{result.test_id}",
                json.dumps({
                    'test_id': result.test_id,
                    'model_a_metrics': result.model_a_metrics,
                    'model_b_metrics': result.model_b_metrics,
                    'statistical_significance': result.statistical_significance,
                    'p_value': result.p_value,
                    'effect_size': result.effect_size,
                    'winner': result.winner,
                    'confidence_interval': result.confidence_interval,
                    'sample_size_a': result.sample_size_a,
                    'sample_size_b': result.sample_size_b,
                    'timestamp': result.timestamp.isoformat()
                })
            )
            
            # Keep only recent results
            await redis.ltrim(f"ab_test_results:{result.test_id}", 0, 99)
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing test result: {e}")
    
    async def _get_traffic(self, test_id: str, user_id: str) -> Optional[ABTestTraffic]:
        """Get traffic assignment for a user"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            traffic_data = await redis.lrange(f"ab_test_traffic:{test_id}", 0, -1)
            await redis.close()
            
            for data_str in traffic_data:
                data = json.loads(data_str)
                if data['user_id'] == user_id:
                    return ABTestTraffic(
                        test_id=data['test_id'],
                        user_id=data['user_id'],
                        assigned_model=data['assigned_model'],
                        timestamp=datetime.fromisoformat(data['timestamp']),
                        prediction_result=data.get('prediction_result')
                    )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting traffic: {e}")
            return None
    
    async def _get_test_traffic(self, test_id: str) -> List[ABTestTraffic]:
        """Get all traffic for a test"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            traffic_data = await redis.lrange(f"ab_test_traffic:{test_id}", 0, -1)
            await redis.close()
            
            traffic = []
            for data_str in traffic_data:
                data = json.loads(data_str)
                traffic.append(ABTestTraffic(
                    test_id=data['test_id'],
                    user_id=data['user_id'],
                    assigned_model=data['assigned_model'],
                    timestamp=datetime.fromisoformat(data['timestamp']),
                    prediction_result=data.get('prediction_result')
                ))
            
            return traffic
            
        except Exception as e:
            self.logger.error(f"Error getting test traffic: {e}")
            return []
    
    async def _get_latest_test_result(self, test_id: str) -> Optional[ABTestResult]:
        """Get the latest test result"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            result_data = await redis.lindex(f"ab_test_results:{test_id}", 0)
            await redis.close()
            
            if result_data:
                data = json.loads(result_data)
                return ABTestResult(
                    test_id=data['test_id'],
                    model_a_metrics=data['model_a_metrics'],
                    model_b_metrics=data['model_b_metrics'],
                    statistical_significance=data['statistical_significance'],
                    p_value=data['p_value'],
                    effect_size=data['effect_size'],
                    winner=data['winner'],
                    confidence_interval=tuple(data['confidence_interval']),
                    sample_size_a=data['sample_size_a'],
                    sample_size_b=data['sample_size_b'],
                    timestamp=datetime.fromisoformat(data['timestamp'])
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting latest test result: {e}")
            return None
    
    async def _update_traffic(self, traffic: ABTestTraffic):
        """Update traffic with prediction result"""
        try:
            # Remove old traffic record
            redis = await aioredis.from_url(self.redis_url)
            await redis.lrem(f"ab_test_traffic:{traffic.test_id}", 0, json.dumps({
                'test_id': traffic.test_id,
                'user_id': traffic.user_id,
                'assigned_model': traffic.assigned_model,
                'timestamp': traffic.timestamp.isoformat(),
                'prediction_result': None
            }))
            
            # Add updated traffic record
            await self._store_traffic(traffic)
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error updating traffic: {e}")
    
    async def start_ab_testing_monitoring(self):
        """Start continuous A/B testing monitoring"""
        self.running = True
        self.logger.info("Starting A/B testing monitoring")
        
        while self.running:
            try:
                # Monitor active A/B tests
                for test_id, test_config in self.tests.items():
                    if not test_config.enabled:
                        continue
                    
                    # Check if test should be analyzed
                    # This would typically involve checking sample sizes and test duration
                    self.logger.debug(f"Monitoring A/B test: {test_id}")
                
                # Wait before next check
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in A/B testing monitoring: {e}")
                await asyncio.sleep(60)
    
    async def stop_ab_testing_monitoring(self):
        """Stop A/B testing monitoring"""
        self.running = False
        self.logger.info("Stopping A/B testing monitoring")
