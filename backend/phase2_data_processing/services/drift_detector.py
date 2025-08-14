import asyncio
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from scipy import stats
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.ensemble import IsolationForest
import redis.asyncio as aioredis

@dataclass
class DriftConfig:
    """Configuration for drift detection"""
    feature_name: str
    detection_method: str  # 'statistical', 'ml', 'distribution'
    threshold: float = 0.05
    window_size: int = 1000
    min_samples: int = 100
    update_frequency: int = 3600  # seconds

@dataclass
class DriftResult:
    """Result of drift detection"""
    feature_name: str
    drift_detected: bool
    drift_score: float
    confidence: float
    method: str
    timestamp: datetime
    details: Dict[str, Any]

class DataDriftDetector:
    """
    Comprehensive data drift detection system
    Detects statistical drift, concept drift, and distribution changes
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.configs: Dict[str, DriftConfig] = {}
        self.baseline_data: Dict[str, np.ndarray] = {}
        self.drift_history: Dict[str, List[DriftResult]] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Initialize ML models for concept drift
        self.isolation_forest = IsolationForest(contamination=0.1, random_state=42)
        self.scaler = StandardScaler()
        self.pca = PCA(n_components=0.95)
        
    async def add_drift_config(self, config: DriftConfig) -> bool:
        """Add a drift detection configuration"""
        try:
            self.configs[config.feature_name] = config
            self.logger.info(f"Added drift config for feature: {config.feature_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add drift config: {e}")
            return False
    
    async def set_baseline_data(self, feature_name: str, data: List[float]) -> bool:
        """Set baseline data for drift detection"""
        try:
            if len(data) < self.configs[feature_name].min_samples:
                raise ValueError(f"Insufficient baseline data: {len(data)} < {self.configs[feature_name].min_samples}")
            
            self.baseline_data[feature_name] = np.array(data)
            
            # Store baseline in Redis
            redis = await aioredis.from_url(self.redis_url)
            await redis.set(
                f"drift_baseline:{feature_name}",
                json.dumps({
                    'data': data,
                    'timestamp': datetime.utcnow().isoformat(),
                    'statistics': self._calculate_statistics(data)
                })
            )
            await redis.close()
            
            self.logger.info(f"Set baseline for {feature_name} with {len(data)} samples")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to set baseline for {feature_name}: {e}")
            return False
    
    async def detect_drift(self, feature_name: str, current_data: List[float]) -> DriftResult:
        """Detect drift for a specific feature"""
        try:
            if feature_name not in self.configs:
                raise ValueError(f"No drift config found for feature: {feature_name}")
            
            if feature_name not in self.baseline_data:
                raise ValueError(f"No baseline data found for feature: {feature_name}")
            
            config = self.configs[feature_name]
            baseline = self.baseline_data[feature_name]
            current = np.array(current_data)
            
            if len(current) < config.min_samples:
                return DriftResult(
                    feature_name=feature_name,
                    drift_detected=False,
                    drift_score=0.0,
                    confidence=0.0,
                    method=config.detection_method,
                    timestamp=datetime.utcnow(),
                    details={'error': 'Insufficient current data'}
                )
            
            # Apply detection method
            if config.detection_method == 'statistical':
                result = await self._statistical_drift_detection(baseline, current, config)
            elif config.detection_method == 'ml':
                result = await self._ml_drift_detection(baseline, current, config)
            elif config.detection_method == 'distribution':
                result = await self._distribution_drift_detection(baseline, current, config)
            else:
                raise ValueError(f"Unknown detection method: {config.detection_method}")
            
            # Store result
            await self._store_drift_result(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error detecting drift for {feature_name}: {e}")
            return DriftResult(
                feature_name=feature_name,
                drift_detected=False,
                drift_score=0.0,
                confidence=0.0,
                method='error',
                timestamp=datetime.utcnow(),
                details={'error': str(e)}
            )
    
    async def _statistical_drift_detection(self, baseline: np.ndarray, current: np.ndarray, 
                                         config: DriftConfig) -> DriftResult:
        """Statistical drift detection using hypothesis testing"""
        try:
            # Kolmogorov-Smirnov test
            ks_statistic, ks_pvalue = stats.ks_2samp(baseline, current)
            
            # Mann-Whitney U test
            mw_statistic, mw_pvalue = stats.mannwhitneyu(baseline, current, alternative='two-sided')
            
            # T-test (if data is approximately normal)
            t_statistic, t_pvalue = stats.ttest_ind(baseline, current)
            
            # Calculate drift score (average of p-values)
            p_values = [ks_pvalue, mw_pvalue, t_pvalue]
            drift_score = 1 - np.mean(p_values)
            
            # Determine if drift is detected
            drift_detected = drift_score > config.threshold
            
            # Calculate confidence
            confidence = 1 - np.std(p_values)
            
            return DriftResult(
                feature_name=config.feature_name,
                drift_detected=drift_detected,
                drift_score=drift_score,
                confidence=confidence,
                method='statistical',
                timestamp=datetime.utcnow(),
                details={
                    'ks_statistic': ks_statistic,
                    'ks_pvalue': ks_pvalue,
                    'mw_statistic': mw_statistic,
                    'mw_pvalue': mw_pvalue,
                    't_statistic': t_statistic,
                    't_pvalue': t_pvalue,
                    'threshold': config.threshold
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error in statistical drift detection: {e}")
            raise
    
    async def _ml_drift_detection(self, baseline: np.ndarray, current: np.ndarray, 
                                config: DriftConfig) -> DriftResult:
        """ML-based drift detection using isolation forest"""
        try:
            # Prepare data
            baseline_2d = baseline.reshape(-1, 1)
            current_2d = current.reshape(-1, 1)
            
            # Fit isolation forest on baseline data
            self.isolation_forest.fit(baseline_2d)
            
            # Predict anomalies in current data
            current_scores = self.isolation_forest.decision_function(current_2d)
            baseline_scores = self.isolation_forest.decision_function(baseline_2d)
            
            # Calculate drift score based on score distribution difference
            current_mean = np.mean(current_scores)
            baseline_mean = np.mean(baseline_scores)
            current_std = np.std(current_scores)
            baseline_std = np.std(baseline_scores)
            
            # Drift score based on mean and std differences
            mean_diff = abs(current_mean - baseline_mean) / (baseline_std + 1e-8)
            std_diff = abs(current_std - baseline_std) / (baseline_std + 1e-8)
            drift_score = (mean_diff + std_diff) / 2
            
            # Normalize drift score to [0, 1]
            drift_score = min(drift_score / 2, 1.0)
            
            drift_detected = drift_score > config.threshold
            
            # Calculate confidence based on score consistency
            confidence = 1 - np.std(current_scores)
            
            return DriftResult(
                feature_name=config.feature_name,
                drift_detected=drift_detected,
                drift_score=drift_score,
                confidence=confidence,
                method='ml',
                timestamp=datetime.utcnow(),
                details={
                    'current_mean_score': current_mean,
                    'baseline_mean_score': baseline_mean,
                    'current_std_score': current_std,
                    'baseline_std_score': baseline_std,
                    'mean_diff': mean_diff,
                    'std_diff': std_diff,
                    'threshold': config.threshold
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error in ML drift detection: {e}")
            raise
    
    async def _distribution_drift_detection(self, baseline: np.ndarray, current: np.ndarray, 
                                          config: DriftConfig) -> DriftResult:
        """Distribution-based drift detection"""
        try:
            # Calculate distribution statistics
            baseline_stats = self._calculate_statistics(baseline)
            current_stats = self._calculate_statistics(current)
            
            # Calculate distribution differences
            mean_diff = abs(current_stats['mean'] - baseline_stats['mean']) / (baseline_stats['std'] + 1e-8)
            std_diff = abs(current_stats['std'] - baseline_stats['std']) / (baseline_stats['std'] + 1e-8)
            skew_diff = abs(current_stats['skew'] - baseline_stats['skew'])
            kurt_diff = abs(current_stats['kurtosis'] - baseline_stats['kurtosis'])
            
            # Calculate drift score
            drift_score = (mean_diff + std_diff + skew_diff + kurt_diff) / 4
            drift_score = min(drift_score, 1.0)
            
            drift_detected = drift_score > config.threshold
            
            # Calculate confidence based on sample sizes
            confidence = min(len(current) / len(baseline), 1.0)
            
            return DriftResult(
                feature_name=config.feature_name,
                drift_detected=drift_detected,
                drift_score=drift_score,
                confidence=confidence,
                method='distribution',
                timestamp=datetime.utcnow(),
                details={
                    'baseline_stats': baseline_stats,
                    'current_stats': current_stats,
                    'mean_diff': mean_diff,
                    'std_diff': std_diff,
                    'skew_diff': skew_diff,
                    'kurt_diff': kurt_diff,
                    'threshold': config.threshold
                }
            )
            
        except Exception as e:
            self.logger.error(f"Error in distribution drift detection: {e}")
            raise
    
    def _calculate_statistics(self, data: np.ndarray) -> Dict[str, float]:
        """Calculate statistical measures for data"""
        return {
            'mean': float(np.mean(data)),
            'std': float(np.std(data)),
            'median': float(np.median(data)),
            'min': float(np.min(data)),
            'max': float(np.max(data)),
            'skew': float(stats.skew(data)),
            'kurtosis': float(stats.kurtosis(data)),
            'q25': float(np.percentile(data, 25)),
            'q75': float(np.percentile(data, 75))
        }
    
    async def _store_drift_result(self, result: DriftResult):
        """Store drift detection result"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store result
            await redis.lpush(
                f"drift_results:{result.feature_name}",
                json.dumps({
                    'feature_name': result.feature_name,
                    'drift_detected': result.drift_detected,
                    'drift_score': result.drift_score,
                    'confidence': result.confidence,
                    'method': result.method,
                    'timestamp': result.timestamp.isoformat(),
                    'details': result.details
                })
            )
            
            # Keep only recent results
            await redis.ltrim(f"drift_results:{result.feature_name}", 0, 999)
            
            # Update drift history
            if result.feature_name not in self.drift_history:
                self.drift_history[result.feature_name] = []
            self.drift_history[result.feature_name].append(result)
            
            # Keep only recent history
            if len(self.drift_history[result.feature_name]) > 1000:
                self.drift_history[result.feature_name] = self.drift_history[result.feature_name][-1000:]
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing drift result: {e}")
    
    async def get_drift_history(self, feature_name: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get drift detection history for a feature"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            results = await redis.lrange(f"drift_results:{feature_name}", 0, limit - 1)
            await redis.close()
            
            return [json.loads(result) for result in results]
            
        except Exception as e:
            self.logger.error(f"Error getting drift history: {e}")
            return []
    
    async def get_drift_summary(self) -> Dict[str, Any]:
        """Get summary of all drift detections"""
        try:
            summary = {
                'total_features': len(self.configs),
                'features_with_drift': 0,
                'total_drift_events': 0,
                'drift_rate': 0.0,
                'feature_details': {}
            }
            
            for feature_name in self.configs:
                history = await self.get_drift_history(feature_name, limit=1000)
                drift_events = sum(1 for result in history if result['drift_detected'])
                
                summary['feature_details'][feature_name] = {
                    'total_checks': len(history),
                    'drift_events': drift_events,
                    'drift_rate': drift_events / len(history) if history else 0.0,
                    'last_check': history[0]['timestamp'] if history else None
                }
                
                summary['total_drift_events'] += drift_events
                if drift_events > 0:
                    summary['features_with_drift'] += 1
            
            if summary['total_features'] > 0:
                summary['drift_rate'] = summary['features_with_drift'] / summary['total_features']
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting drift summary: {e}")
            return {}
    
    async def start_drift_monitoring(self):
        """Start continuous drift monitoring"""
        self.running = True
        self.logger.info("Starting drift monitoring")
        
        while self.running:
            try:
                for feature_name, config in self.configs.items():
                    # Get recent data for drift detection
                    redis = await aioredis.from_url(self.redis_url)
                    recent_data = await redis.lrange(f"data_queue:{feature_name}", 0, config.window_size - 1)
                    await redis.close()
                    
                    if len(recent_data) >= config.min_samples:
                        # Parse data
                        current_data = []
                        for data_str in recent_data:
                            try:
                                data = json.loads(data_str)
                                if feature_name in data.get('data', {}):
                                    current_data.append(data['data'][feature_name])
                            except:
                                continue
                        
                        if len(current_data) >= config.min_samples:
                            # Detect drift
                            result = await self.detect_drift(feature_name, current_data)
                            
                            if result.drift_detected:
                                self.logger.warning(f"Drift detected in {feature_name}: score={result.drift_score:.3f}")
                
                # Wait before next check
                await asyncio.sleep(min(config.update_frequency for config in self.configs.values()))
                
            except Exception as e:
                self.logger.error(f"Error in drift monitoring: {e}")
                await asyncio.sleep(60)
    
    async def stop_drift_monitoring(self):
        """Stop drift monitoring"""
        self.running = False
        self.logger.info("Stopping drift monitoring")
