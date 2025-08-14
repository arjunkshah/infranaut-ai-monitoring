import asyncio
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Callable
from dataclasses import dataclass
from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import PCA
import redis.asyncio as aioredis

@dataclass
class FeatureConfig:
    """Configuration for feature engineering"""
    feature_name: str
    feature_type: str  # 'numeric', 'categorical', 'text', 'datetime', 'derived'
    source_fields: List[str]
    transformation: str  # 'none', 'scale', 'encode', 'extract', 'aggregate', 'custom'
    parameters: Dict[str, Any]
    enabled: bool = True

@dataclass
class FeatureResult:
    """Result of feature engineering"""
    feature_name: str
    feature_type: str
    values: List[Any]
    metadata: Dict[str, Any]
    timestamp: datetime
    quality_score: float

class FeatureEngineeringPipeline:
    """
    Comprehensive feature engineering pipeline
    Creates, transforms, and monitors features automatically
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.features: Dict[str, FeatureConfig] = {}
        self.transformers: Dict[str, Any] = {}
        self.feature_history: Dict[str, List[FeatureResult]] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Initialize transformers
        self._init_transformers()
        
    def _init_transformers(self):
        """Initialize feature transformers"""
        self.transformers = {
            'standard_scaler': StandardScaler(),
            'minmax_scaler': MinMaxScaler(),
            'label_encoder': LabelEncoder(),
            'tfidf_vectorizer': TfidfVectorizer(max_features=100),
            'pca': PCA(n_components=0.95)
        }
    
    async def add_feature_config(self, config: FeatureConfig) -> bool:
        """Add a feature engineering configuration"""
        try:
            self.features[config.feature_name] = config
            self.logger.info(f"Added feature config: {config.feature_name}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add feature config: {e}")
            return False
    
    async def engineer_features(self, data: List[Dict[str, Any]]) -> Dict[str, FeatureResult]:
        """Engineer features from raw data"""
        try:
            results = {}
            
            for feature_name, config in self.features.items():
                if not config.enabled:
                    continue
                
                try:
                    if config.feature_type == 'numeric':
                        result = await self._engineer_numeric_feature(config, data)
                    elif config.feature_type == 'categorical':
                        result = await self._engineer_categorical_feature(config, data)
                    elif config.feature_type == 'text':
                        result = await self._engineer_text_feature(config, data)
                    elif config.feature_type == 'datetime':
                        result = await self._engineer_datetime_feature(config, data)
                    elif config.feature_type == 'derived':
                        result = await self._engineer_derived_feature(config, data)
                    else:
                        self.logger.warning(f"Unknown feature type: {config.feature_type}")
                        continue
                    
                    results[feature_name] = result
                    await self._store_feature_result(result)
                    
                except Exception as e:
                    self.logger.error(f"Error engineering feature {feature_name}: {e}")
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error in feature engineering: {e}")
            return {}
    
    async def _engineer_numeric_feature(self, config: FeatureConfig, data: List[Dict[str, Any]]) -> FeatureResult:
        """Engineer numeric features"""
        try:
            # Extract source values
            source_values = []
            for record in data:
                value = self._extract_source_value(record, config.source_fields)
                if value is not None:
                    try:
                        source_values.append(float(value))
                    except (ValueError, TypeError):
                        continue
            
            if not source_values:
                return FeatureResult(
                    feature_name=config.feature_name,
                    feature_type='numeric',
                    values=[],
                    metadata={'error': 'No valid numeric values'},
                    timestamp=datetime.utcnow(),
                    quality_score=0.0
                )
            
            # Apply transformation
            if config.transformation == 'none':
                transformed_values = source_values
            elif config.transformation == 'scale':
                transformed_values = await self._apply_scaling(config, source_values)
            elif config.transformation == 'aggregate':
                transformed_values = await self._apply_aggregation(config, source_values)
            else:
                transformed_values = source_values
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(transformed_values)
            
            return FeatureResult(
                feature_name=config.feature_name,
                feature_type='numeric',
                values=transformed_values,
                metadata={
                    'source_count': len(source_values),
                    'transformation': config.transformation,
                    'statistics': self._calculate_statistics(transformed_values)
                },
                timestamp=datetime.utcnow(),
                quality_score=quality_score
            )
            
        except Exception as e:
            self.logger.error(f"Error engineering numeric feature: {e}")
            raise
    
    async def _engineer_categorical_feature(self, config: FeatureConfig, data: List[Dict[str, Any]]) -> FeatureResult:
        """Engineer categorical features"""
        try:
            # Extract source values
            source_values = []
            for record in data:
                value = self._extract_source_value(record, config.source_fields)
                if value is not None:
                    source_values.append(str(value))
            
            if not source_values:
                return FeatureResult(
                    feature_name=config.feature_name,
                    feature_type='categorical',
                    values=[],
                    metadata={'error': 'No valid categorical values'},
                    timestamp=datetime.utcnow(),
                    quality_score=0.0
                )
            
            # Apply transformation
            if config.transformation == 'none':
                transformed_values = source_values
            elif config.transformation == 'encode':
                transformed_values = await self._apply_encoding(config, source_values)
            else:
                transformed_values = source_values
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(transformed_values)
            
            return FeatureResult(
                feature_name=config.feature_name,
                feature_type='categorical',
                values=transformed_values,
                metadata={
                    'source_count': len(source_values),
                    'transformation': config.transformation,
                    'unique_values': len(set(source_values)),
                    'value_counts': dict(pd.Series(source_values).value_counts().head(10))
                },
                timestamp=datetime.utcnow(),
                quality_score=quality_score
            )
            
        except Exception as e:
            self.logger.error(f"Error engineering categorical feature: {e}")
            raise
    
    async def _engineer_text_feature(self, config: FeatureConfig, data: List[Dict[str, Any]]) -> FeatureResult:
        """Engineer text features"""
        try:
            # Extract source values
            source_values = []
            for record in data:
                value = self._extract_source_value(record, config.source_fields)
                if value is not None:
                    source_values.append(str(value))
            
            if not source_values:
                return FeatureResult(
                    feature_name=config.feature_name,
                    feature_type='text',
                    values=[],
                    metadata={'error': 'No valid text values'},
                    timestamp=datetime.utcnow(),
                    quality_score=0.0
                )
            
            # Apply transformation
            if config.transformation == 'none':
                transformed_values = source_values
            elif config.transformation == 'extract':
                transformed_values = await self._apply_text_extraction(config, source_values)
            else:
                transformed_values = source_values
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(transformed_values)
            
            return FeatureResult(
                feature_name=config.feature_name,
                feature_type='text',
                values=transformed_values,
                metadata={
                    'source_count': len(source_values),
                    'transformation': config.transformation,
                    'avg_length': np.mean([len(str(v)) for v in source_values]),
                    'max_length': max([len(str(v)) for v in source_values])
                },
                timestamp=datetime.utcnow(),
                quality_score=quality_score
            )
            
        except Exception as e:
            self.logger.error(f"Error engineering text feature: {e}")
            raise
    
    async def _engineer_datetime_feature(self, config: FeatureConfig, data: List[Dict[str, Any]]) -> FeatureResult:
        """Engineer datetime features"""
        try:
            # Extract source values
            source_values = []
            for record in data:
                value = self._extract_source_value(record, config.source_fields)
                if value is not None:
                    try:
                        from dateutil import parser
                        parsed_date = parser.parse(str(value))
                        source_values.append(parsed_date)
                    except:
                        continue
            
            if not source_values:
                return FeatureResult(
                    feature_name=config.feature_name,
                    feature_type='datetime',
                    values=[],
                    metadata={'error': 'No valid datetime values'},
                    timestamp=datetime.utcnow(),
                    quality_score=0.0
                )
            
            # Apply transformation
            if config.transformation == 'none':
                transformed_values = [d.isoformat() for d in source_values]
            elif config.transformation == 'extract':
                transformed_values = await self._apply_datetime_extraction(config, source_values)
            else:
                transformed_values = [d.isoformat() for d in source_values]
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(transformed_values)
            
            return FeatureResult(
                feature_name=config.feature_name,
                feature_type='datetime',
                values=transformed_values,
                metadata={
                    'source_count': len(source_values),
                    'transformation': config.transformation,
                    'date_range': {
                        'min': min(source_values).isoformat(),
                        'max': max(source_values).isoformat()
                    }
                },
                timestamp=datetime.utcnow(),
                quality_score=quality_score
            )
            
        except Exception as e:
            self.logger.error(f"Error engineering datetime feature: {e}")
            raise
    
    async def _engineer_derived_feature(self, config: FeatureConfig, data: List[Dict[str, Any]]) -> FeatureResult:
        """Engineer derived features"""
        try:
            # Extract source values for all source fields
            source_data = {}
            for field in config.source_fields:
                source_data[field] = []
                for record in data:
                    value = record.get(field)
                    source_data[field].append(value)
            
            # Apply custom transformation
            if config.transformation == 'custom':
                transformed_values = await self._apply_custom_transformation(config, source_data)
            else:
                transformed_values = await self._apply_derived_transformation(config, source_data)
            
            # Calculate quality score
            quality_score = self._calculate_quality_score(transformed_values)
            
            return FeatureResult(
                feature_name=config.feature_name,
                feature_type='derived',
                values=transformed_values,
                metadata={
                    'source_fields': config.source_fields,
                    'transformation': config.transformation,
                    'source_count': len(data)
                },
                timestamp=datetime.utcnow(),
                quality_score=quality_score
            )
            
        except Exception as e:
            self.logger.error(f"Error engineering derived feature: {e}")
            raise
    
    def _extract_source_value(self, record: Dict[str, Any], source_fields: List[str]) -> Any:
        """Extract value from source fields"""
        for field in source_fields:
            if field in record:
                return record[field]
        return None
    
    async def _apply_scaling(self, config: FeatureConfig, values: List[float]) -> List[float]:
        """Apply scaling transformation"""
        try:
            scaler_type = config.parameters.get('scaler_type', 'standard')
            
            if scaler_type == 'standard':
                scaler = self.transformers['standard_scaler']
            elif scaler_type == 'minmax':
                scaler = self.transformers['minmax_scaler']
            else:
                return values
            
            # Fit and transform
            values_array = np.array(values).reshape(-1, 1)
            scaled_values = scaler.fit_transform(values_array)
            
            return scaled_values.flatten().tolist()
            
        except Exception as e:
            self.logger.error(f"Error applying scaling: {e}")
            return values
    
    async def _apply_aggregation(self, config: FeatureConfig, values: List[float]) -> List[float]:
        """Apply aggregation transformation"""
        try:
            agg_type = config.parameters.get('agg_type', 'mean')
            window_size = config.parameters.get('window_size', 10)
            
            if len(values) < window_size:
                return values
            
            aggregated_values = []
            for i in range(len(values)):
                start_idx = max(0, i - window_size + 1)
                window_values = values[start_idx:i + 1]
                
                if agg_type == 'mean':
                    agg_value = np.mean(window_values)
                elif agg_type == 'sum':
                    agg_value = np.sum(window_values)
                elif agg_type == 'max':
                    agg_value = np.max(window_values)
                elif agg_type == 'min':
                    agg_value = np.min(window_values)
                else:
                    agg_value = window_values[-1]  # Default to last value
                
                aggregated_values.append(agg_value)
            
            return aggregated_values
            
        except Exception as e:
            self.logger.error(f"Error applying aggregation: {e}")
            return values
    
    async def _apply_encoding(self, config: FeatureConfig, values: List[str]) -> List[int]:
        """Apply encoding transformation"""
        try:
            encoder = self.transformers['label_encoder']
            encoded_values = encoder.fit_transform(values)
            return encoded_values.tolist()
            
        except Exception as e:
            self.logger.error(f"Error applying encoding: {e}")
            return list(range(len(values)))
    
    async def _apply_text_extraction(self, config: FeatureConfig, values: List[str]) -> List[float]:
        """Apply text feature extraction"""
        try:
            extraction_type = config.parameters.get('extraction_type', 'tfidf')
            
            if extraction_type == 'tfidf':
                vectorizer = self.transformers['tfidf_vectorizer']
                features = vectorizer.fit_transform(values)
                
                # Use first component as feature value
                feature_values = features.toarray()[:, 0].tolist()
                return feature_values
            else:
                # Simple length-based feature
                return [len(str(v)) for v in values]
            
        except Exception as e:
            self.logger.error(f"Error applying text extraction: {e}")
            return [len(str(v)) for v in values]
    
    async def _apply_datetime_extraction(self, config: FeatureConfig, values: List[datetime]) -> List[float]:
        """Apply datetime feature extraction"""
        try:
            extraction_type = config.parameters.get('extraction_type', 'timestamp')
            
            if extraction_type == 'timestamp':
                return [d.timestamp() for d in values]
            elif extraction_type == 'hour':
                return [d.hour for d in values]
            elif extraction_type == 'day_of_week':
                return [d.weekday() for d in values]
            elif extraction_type == 'month':
                return [d.month for d in values]
            else:
                return [d.timestamp() for d in values]
            
        except Exception as e:
            self.logger.error(f"Error applying datetime extraction: {e}")
            return [d.timestamp() for d in values]
    
    async def _apply_custom_transformation(self, config: FeatureConfig, source_data: Dict[str, List[Any]]) -> List[Any]:
        """Apply custom transformation"""
        try:
            # This would typically involve loading and executing custom functions
            # For now, return a simple combination
            custom_func = config.parameters.get('custom_function')
            if custom_func:
                # Execute custom function (would need proper sandboxing in production)
                return [f"custom_{i}" for i in range(len(next(iter(source_data.values()))))]
            else:
                return await self._apply_derived_transformation(config, source_data)
            
        except Exception as e:
            self.logger.error(f"Error applying custom transformation: {e}")
            return []
    
    async def _apply_derived_transformation(self, config: FeatureConfig, source_data: Dict[str, List[Any]]) -> List[Any]:
        """Apply derived transformation"""
        try:
            operation = config.parameters.get('operation', 'concat')
            
            if operation == 'concat':
                # Concatenate string values
                result = []
                for i in range(len(next(iter(source_data.values())))):
                    parts = []
                    for field, values in source_data.items():
                        if i < len(values) and values[i] is not None:
                            parts.append(str(values[i]))
                    result.append('_'.join(parts))
                return result
            
            elif operation == 'sum':
                # Sum numeric values
                result = []
                for i in range(len(next(iter(source_data.values())))):
                    total = 0
                    for field, values in source_data.items():
                        if i < len(values) and values[i] is not None:
                            try:
                                total += float(values[i])
                            except (ValueError, TypeError):
                                continue
                    result.append(total)
                return result
            
            else:
                return []
            
        except Exception as e:
            self.logger.error(f"Error applying derived transformation: {e}")
            return []
    
    def _calculate_quality_score(self, values: List[Any]) -> float:
        """Calculate quality score for feature values"""
        try:
            if not values:
                return 0.0
            
            # Calculate various quality metrics
            completeness = 1.0 - (values.count(None) / len(values))
            
            # For numeric values, check for reasonable range
            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v))
                except (ValueError, TypeError):
                    continue
            
            if numeric_values:
                # Check for outliers (simple IQR method)
                q1 = np.percentile(numeric_values, 25)
                q3 = np.percentile(numeric_values, 75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                outlier_count = sum(1 for v in numeric_values if v < lower_bound or v > upper_bound)
                outlier_ratio = outlier_count / len(numeric_values)
                quality_score = completeness * (1 - outlier_ratio)
            else:
                quality_score = completeness
            
            return max(0.0, min(1.0, quality_score))
            
        except Exception as e:
            self.logger.error(f"Error calculating quality score: {e}")
            return 0.5
    
    def _calculate_statistics(self, values: List[float]) -> Dict[str, float]:
        """Calculate statistics for numeric values"""
        try:
            numeric_values = []
            for v in values:
                try:
                    numeric_values.append(float(v))
                except (ValueError, TypeError):
                    continue
            
            if not numeric_values:
                return {}
            
            return {
                'mean': float(np.mean(numeric_values)),
                'std': float(np.std(numeric_values)),
                'min': float(np.min(numeric_values)),
                'max': float(np.max(numeric_values)),
                'median': float(np.median(numeric_values))
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating statistics: {e}")
            return {}
    
    async def _store_feature_result(self, result: FeatureResult):
        """Store feature engineering result"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store result
            await redis.lpush(
                f"feature_results:{result.feature_name}",
                json.dumps({
                    'feature_name': result.feature_name,
                    'feature_type': result.feature_type,
                    'values': result.values[:100],  # Store only first 100 values
                    'metadata': result.metadata,
                    'timestamp': result.timestamp.isoformat(),
                    'quality_score': result.quality_score
                })
            )
            
            # Keep only recent results
            await redis.ltrim(f"feature_results:{result.feature_name}", 0, 999)
            
            # Update feature history
            if result.feature_name not in self.feature_history:
                self.feature_history[result.feature_name] = []
            self.feature_history[result.feature_name].append(result)
            
            # Keep only recent history
            if len(self.feature_history[result.feature_name]) > 1000:
                self.feature_history[result.feature_name] = self.feature_history[result.feature_name][-1000:]
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing feature result: {e}")
    
    async def get_feature_summary(self) -> Dict[str, Any]:
        """Get summary of all engineered features"""
        try:
            summary = {
                'total_features': len(self.features),
                'active_features': sum(1 for feature in self.features.values() if feature.enabled),
                'feature_types': {},
                'overall_quality_score': 0.0,
                'feature_details': {}
            }
            
            quality_scores = []
            
            for feature_name, feature in self.features.items():
                redis = await aioredis.from_url(self.redis_url)
                results = await redis.lrange(f"feature_results:{feature_name}", 0, 999)
                await redis.close()
                
                if results:
                    parsed_results = [json.loads(result) for result in results]
                    avg_quality = np.mean([result['quality_score'] for result in parsed_results])
                    quality_scores.append(avg_quality)
                    
                    summary['feature_details'][feature_name] = {
                        'feature_type': feature.feature_type,
                        'transformation': feature.transformation,
                        'enabled': feature.enabled,
                        'avg_quality_score': avg_quality,
                        'last_engineered': parsed_results[0]['timestamp'] if parsed_results else None
                    }
                
                # Count feature types
                feature_type = feature.feature_type
                summary['feature_types'][feature_type] = summary['feature_types'].get(feature_type, 0) + 1
            
            if quality_scores:
                summary['overall_quality_score'] = np.mean(quality_scores)
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting feature summary: {e}")
            return {}
    
    async def start_feature_engineering(self):
        """Start continuous feature engineering"""
        self.running = True
        self.logger.info("Starting feature engineering")
        
        while self.running:
            try:
                # Get data from Redis queues and engineer features
                redis = await aioredis.from_url(self.redis_url)
                
                # Get all dataset keys
                keys = await redis.keys("data_queue:*")
                
                for key in keys:
                    dataset_name = key.decode().replace("data_queue:", "")
                    data = await redis.lrange(key, 0, 999)  # Get recent data
                    
                    if data:
                        # Parse data
                        parsed_data = []
                        for data_str in data:
                            try:
                                parsed_data.append(json.loads(data_str))
                            except:
                                continue
                        
                        if parsed_data:
                            # Engineer features
                            results = await self.engineer_features(parsed_data)
                            
                            # Log low quality features
                            for feature_name, result in results.items():
                                if result.quality_score < 0.5:
                                    self.logger.warning(f"Low quality feature {feature_name}: score={result.quality_score:.3f}")
                
                await redis.close()
                
                # Wait before next check
                await asyncio.sleep(600)  # Check every 10 minutes
                
            except Exception as e:
                self.logger.error(f"Error in feature engineering: {e}")
                await asyncio.sleep(60)
    
    async def stop_feature_engineering(self):
        """Stop feature engineering"""
        self.running = False
        self.logger.info("Stopping feature engineering")
