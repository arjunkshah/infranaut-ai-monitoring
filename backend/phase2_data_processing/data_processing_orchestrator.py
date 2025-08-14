import asyncio
import json
import logging
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import redis.asyncio as aioredis

from services.drift_detector import DataDriftDetector, DriftConfig
from services.quality_monitor import DataQualityMonitor, QualityRule
from services.feature_engineering import FeatureEngineeringPipeline, FeatureConfig

@dataclass
class ProcessingConfig:
    """Configuration for data processing pipeline"""
    dataset_name: str
    drift_detection_enabled: bool = True
    quality_monitoring_enabled: bool = True
    feature_engineering_enabled: bool = True
    processing_interval: int = 300  # seconds
    batch_size: int = 1000

@dataclass
class ProcessingResult:
    """Result of data processing"""
    dataset_name: str
    timestamp: datetime
    drift_results: List[Any]
    quality_results: List[Any]
    feature_results: Dict[str, Any]
    processing_time: float
    success: bool
    errors: List[str]

class DataProcessingOrchestrator:
    """
    Main orchestrator for Phase 2 data processing
    Coordinates drift detection, quality monitoring, and feature engineering
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.configs: Dict[str, ProcessingConfig] = {}
        self.drift_detector = DataDriftDetector(redis_url)
        self.quality_monitor = DataQualityMonitor(redis_url)
        self.feature_engineer = FeatureEngineeringPipeline(redis_url)
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Processing history
        self.processing_history: Dict[str, List[ProcessingResult]] = {}
        
    async def add_processing_config(self, config: ProcessingConfig) -> bool:
        """Add a data processing configuration"""
        try:
            self.configs[config.dataset_name] = config
            self.logger.info(f"Added processing config for dataset: {config.dataset_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add processing config: {e}")
            return False
    
    async def setup_drift_detection(self, dataset_name: str, features: List[str]) -> bool:
        """Setup drift detection for dataset features"""
        try:
            for feature in features:
                config = DriftConfig(
                    feature_name=feature,
                    detection_method='statistical',
                    threshold=0.05,
                    window_size=1000,
                    min_samples=100
                )
                await self.drift_detector.add_drift_config(config)
                
                # Set baseline data (would come from historical data)
                baseline_data = np.random.normal(0, 1, 1000).tolist()
                await self.drift_detector.set_baseline_data(feature, baseline_data)
            
            self.logger.info(f"Setup drift detection for {len(features)} features in {dataset_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup drift detection: {e}")
            return False
    
    async def setup_quality_monitoring(self, dataset_name: str, schema: Dict[str, Any]) -> bool:
        """Setup quality monitoring for dataset"""
        try:
            # Set schema
            await self.quality_monitor.set_schema(dataset_name, schema)
            
            # Add quality rules
            rules = [
                QualityRule(
                    rule_id=f"{dataset_name}_missing_values",
                    rule_type='missing',
                    field_name='*',  # Apply to all fields
                    parameters={'max_missing_pct': 0.1},
                    severity='warning'
                ),
                QualityRule(
                    rule_id=f"{dataset_name}_outliers",
                    rule_type='outlier',
                    field_name='*',
                    parameters={'method': 'iqr', 'max_outlier_pct': 0.05},
                    severity='warning'
                ),
                QualityRule(
                    rule_id=f"{dataset_name}_duplicates",
                    rule_type='duplicate',
                    field_name='*',
                    parameters={'fields': ['id'], 'max_duplicate_pct': 0.01},
                    severity='error'
                ),
                QualityRule(
                    rule_id=f"{dataset_name}_schema",
                    rule_type='schema',
                    field_name='*',
                    parameters={'max_violation_pct': 0.05},
                    severity='error'
                )
            ]
            
            for rule in rules:
                await self.quality_monitor.add_quality_rule(rule)
            
            self.logger.info(f"Setup quality monitoring for {dataset_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup quality monitoring: {e}")
            return False
    
    async def setup_feature_engineering(self, dataset_name: str, features: List[Dict[str, Any]]) -> bool:
        """Setup feature engineering for dataset"""
        try:
            for feature_spec in features:
                config = FeatureConfig(
                    feature_name=feature_spec['name'],
                    feature_type=feature_spec['type'],
                    source_fields=feature_spec['source_fields'],
                    transformation=feature_spec.get('transformation', 'none'),
                    parameters=feature_spec.get('parameters', {})
                )
                await self.feature_engineer.add_feature_config(config)
            
            self.logger.info(f"Setup feature engineering for {len(features)} features in {dataset_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to setup feature engineering: {e}")
            return False
    
    async def process_dataset(self, dataset_name: str) -> ProcessingResult:
        """Process a single dataset"""
        start_time = datetime.utcnow()
        errors = []
        
        try:
            if dataset_name not in self.configs:
                raise ValueError(f"No processing config found for dataset: {dataset_name}")
            
            config = self.configs[dataset_name]
            
            # Get data from Redis
            redis = await aioredis.from_url(self.redis_url)
            data = await redis.lrange(f"data_queue:{dataset_name}", 0, config.batch_size - 1)
            await redis.close()
            
            if not data:
                return ProcessingResult(
                    dataset_name=dataset_name,
                    timestamp=start_time,
                    drift_results=[],
                    quality_results=[],
                    feature_results={},
                    processing_time=(datetime.utcnow() - start_time).total_seconds(),
                    success=True,
                    errors=['No data to process']
                )
            
            # Parse data
            parsed_data = []
            for data_str in data:
                try:
                    parsed_data.append(json.loads(data_str))
                except:
                    continue
            
            drift_results = []
            quality_results = []
            feature_results = {}
            
            # Run drift detection
            if config.drift_detection_enabled:
                try:
                    for feature_name in self.drift_detector.configs:
                        if feature_name in parsed_data[0] if parsed_data else False:
                            feature_values = [record.get(feature_name) for record in parsed_data if record.get(feature_name) is not None]
                            if len(feature_values) >= 100:
                                result = await self.drift_detector.detect_drift(feature_name, feature_values)
                                drift_results.append(result)
                except Exception as e:
                    errors.append(f"Drift detection error: {e}")
            
            # Run quality monitoring
            if config.quality_monitoring_enabled:
                try:
                    quality_results = await self.quality_monitor.check_data_quality(dataset_name, parsed_data)
                except Exception as e:
                    errors.append(f"Quality monitoring error: {e}")
            
            # Run feature engineering
            if config.feature_engineering_enabled:
                try:
                    feature_results = await self.feature_engineer.engineer_features(parsed_data)
                except Exception as e:
                    errors.append(f"Feature engineering error: {e}")
            
            # Store processing result
            result = ProcessingResult(
                dataset_name=dataset_name,
                timestamp=start_time,
                drift_results=drift_results,
                quality_results=quality_results,
                feature_results=feature_results,
                processing_time=(datetime.utcnow() - start_time).total_seconds(),
                success=len(errors) == 0,
                errors=errors
            )
            
            await self._store_processing_result(result)
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error processing dataset {dataset_name}: {e}")
            return ProcessingResult(
                dataset_name=dataset_name,
                timestamp=start_time,
                drift_results=[],
                quality_results=[],
                feature_results={},
                processing_time=(datetime.utcnow() - start_time).total_seconds(),
                success=False,
                errors=[str(e)]
            )
    
    async def _store_processing_result(self, result: ProcessingResult):
        """Store processing result"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store result
            await redis.lpush(
                f"processing_results:{result.dataset_name}",
                json.dumps({
                    'dataset_name': result.dataset_name,
                    'timestamp': result.timestamp.isoformat(),
                    'drift_results_count': len(result.drift_results),
                    'quality_results_count': len(result.quality_results),
                    'feature_results_count': len(result.feature_results),
                    'processing_time': result.processing_time,
                    'success': result.success,
                    'errors': result.errors
                })
            )
            
            # Keep only recent results
            await redis.ltrim(f"processing_results:{result.dataset_name}", 0, 999)
            
            # Update processing history
            if result.dataset_name not in self.processing_history:
                self.processing_history[result.dataset_name] = []
            self.processing_history[result.dataset_name].append(result)
            
            # Keep only recent history
            if len(self.processing_history[result.dataset_name]) > 1000:
                self.processing_history[result.dataset_name] = self.processing_history[result.dataset_name][-1000:]
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing processing result: {e}")
    
    async def get_processing_summary(self) -> Dict[str, Any]:
        """Get summary of all processing activities"""
        try:
            summary = {
                'total_datasets': len(self.configs),
                'active_datasets': sum(1 for config in self.configs.values() if config.drift_detection_enabled or config.quality_monitoring_enabled or config.feature_engineering_enabled),
                'total_processing_runs': 0,
                'successful_runs': 0,
                'failed_runs': 0,
                'avg_processing_time': 0.0,
                'dataset_details': {}
            }
            
            processing_times = []
            
            for dataset_name, config in self.configs.items():
                redis = await aioredis.from_url(self.redis_url)
                results = await redis.lrange(f"processing_results:{dataset_name}", 0, 999)
                await redis.close()
                
                if results:
                    parsed_results = [json.loads(result) for result in results]
                    total_runs = len(parsed_results)
                    successful_runs = sum(1 for result in parsed_results if result['success'])
                    failed_runs = total_runs - successful_runs
                    avg_time = np.mean([result['processing_time'] for result in parsed_results])
                    
                    summary['dataset_details'][dataset_name] = {
                        'drift_detection_enabled': config.drift_detection_enabled,
                        'quality_monitoring_enabled': config.quality_monitoring_enabled,
                        'feature_engineering_enabled': config.feature_engineering_enabled,
                        'total_runs': total_runs,
                        'successful_runs': successful_runs,
                        'failed_runs': failed_runs,
                        'avg_processing_time': avg_time,
                        'last_processed': parsed_results[0]['timestamp'] if parsed_results else None
                    }
                    
                    summary['total_processing_runs'] += total_runs
                    summary['successful_runs'] += successful_runs
                    summary['failed_runs'] += failed_runs
                    processing_times.extend([result['processing_time'] for result in parsed_results])
            
            if processing_times:
                summary['avg_processing_time'] = np.mean(processing_times)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting processing summary: {e}")
            return {}
    
    async def start_processing_pipeline(self):
        """Start the data processing pipeline"""
        self.running = True
        self.logger.info("Starting data processing pipeline")
        
        # Start individual components
        await self.drift_detector.start_drift_monitoring()
        await self.quality_monitor.start_quality_monitoring()
        await self.feature_engineer.start_feature_engineering()
        
        while self.running:
            try:
                # Process each configured dataset
                for dataset_name, config in self.configs.items():
                    try:
                        result = await self.process_dataset(dataset_name)
                        
                        if not result.success:
                            self.logger.error(f"Processing failed for {dataset_name}: {result.errors}")
                        else:
                            self.logger.info(f"Processing completed for {dataset_name} in {result.processing_time:.2f}s")
                            
                    except Exception as e:
                        self.logger.error(f"Error processing dataset {dataset_name}: {e}")
                
                # Wait before next processing cycle
                min_interval = min(config.processing_interval for config in self.configs.values())
                await asyncio.sleep(min_interval)
                
            except Exception as e:
                self.logger.error(f"Error in processing pipeline: {e}")
                await asyncio.sleep(60)
    
    async def stop_processing_pipeline(self):
        """Stop the data processing pipeline"""
        self.running = False
        self.logger.info("Stopping data processing pipeline")
        
        # Stop individual components
        await self.drift_detector.stop_drift_monitoring()
        await self.quality_monitor.stop_quality_monitoring()
        await self.feature_engineer.stop_feature_engineering()
    
    async def get_comprehensive_summary(self) -> Dict[str, Any]:
        """Get comprehensive summary of all Phase 2 activities"""
        try:
            summary = {
                'processing_summary': await self.get_processing_summary(),
                'drift_summary': await self.drift_detector.get_drift_summary(),
                'quality_summary': await self.quality_monitor.get_quality_summary(),
                'feature_summary': await self.feature_engineer.get_feature_summary(),
                'timestamp': datetime.utcnow().isoformat()
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting comprehensive summary: {e}")
            return {}
