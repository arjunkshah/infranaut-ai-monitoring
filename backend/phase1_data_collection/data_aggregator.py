import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from collections import defaultdict, Counter
import asyncio
import redis.asyncio as aioredis

class DataAggregator:
    """
    Data aggregation and transformation for AI model monitoring
    Processes raw data into meaningful features and metrics
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.logger = logging.getLogger(__name__)
        self.aggregation_rules: Dict[str, List[Dict]] = {}
        self.feature_engineering_pipelines: Dict[str, List[Dict]] = {}
        self.window_configs: Dict[str, Dict] = {}
        
    def add_aggregation_rule(self, data_type: str, rule: Dict[str, Any]):
        """Add an aggregation rule for a data type"""
        if data_type not in self.aggregation_rules:
            self.aggregation_rules[data_type] = []
        self.aggregation_rules[data_type].append(rule)
        self.logger.info(f"Added aggregation rule for {data_type}")
    
    def add_feature_engineering_pipeline(self, data_type: str, pipeline: List[Dict[str, Any]]):
        """Add a feature engineering pipeline for a data type"""
        self.feature_engineering_pipelines[data_type] = pipeline
        self.logger.info(f"Added feature engineering pipeline for {data_type}")
    
    def set_window_config(self, data_type: str, config: Dict[str, Any]):
        """Set windowing configuration for time-based aggregations"""
        self.window_configs[data_type] = config
        self.logger.info(f"Set window config for {data_type}")
    
    async def aggregate_data(self, data_batch: List[Dict[str, Any]], data_type: str) -> Dict[str, Any]:
        """Aggregate data according to configured rules"""
        if not data_batch:
            return {}
        
        # Convert to DataFrame for easier processing
        df = pd.DataFrame(data_batch)
        
        # Apply feature engineering
        df = self._apply_feature_engineering(df, data_type)
        
        # Apply aggregation rules
        aggregated_data = self._apply_aggregation_rules(df, data_type)
        
        # Apply windowing if configured
        if data_type in self.window_configs:
            aggregated_data = self._apply_windowing(df, data_type, aggregated_data)
        
        # Store aggregated data
        await self._store_aggregated_data(data_type, aggregated_data)
        
        return aggregated_data
    
    def _apply_feature_engineering(self, df: pd.DataFrame, data_type: str) -> pd.DataFrame:
        """Apply feature engineering transformations"""
        if data_type not in self.feature_engineering_pipelines:
            return df
        
        pipeline = self.feature_engineering_pipelines[data_type]
        
        for step in pipeline:
            step_type = step.get('type')
            
            if step_type == 'extract_datetime':
                df = self._extract_datetime_features(df, step)
            elif step_type == 'create_categorical':
                df = self._create_categorical_features(df, step)
            elif step_type == 'calculate_rolling':
                df = self._calculate_rolling_features(df, step)
            elif step_type == 'normalize':
                df = self._normalize_features(df, step)
            elif step_type == 'encode_categorical':
                df = self._encode_categorical_features(df, step)
            elif step_type == 'create_interaction':
                df = self._create_interaction_features(df, step)
            elif step_type == 'custom_function':
                df = self._apply_custom_function(df, step)
        
        return df
    
    def _extract_datetime_features(self, df: pd.DataFrame, step: Dict[str, Any]) -> pd.DataFrame:
        """Extract datetime features from timestamp columns"""
        column = step.get('column')
        if column not in df.columns:
            return df
        
        df[f'{column}_year'] = pd.to_datetime(df[column]).dt.year
        df[f'{column}_month'] = pd.to_datetime(df[column]).dt.month
        df[f'{column}_day'] = pd.to_datetime(df[column]).dt.day
        df[f'{column}_hour'] = pd.to_datetime(df[column]).dt.hour
        df[f'{column}_dayofweek'] = pd.to_datetime(df[column]).dt.dayofweek
        df[f'{column}_is_weekend'] = pd.to_datetime(df[column]).dt.dayofweek.isin([5, 6])
        
        return df
    
    def _create_categorical_features(self, df: pd.DataFrame, step: Dict[str, Any]) -> pd.DataFrame:
        """Create categorical features from continuous variables"""
        column = step.get('column')
        bins = step.get('bins', 10)
        labels = step.get('labels')
        
        if column not in df.columns:
            return df
        
        new_column = step.get('new_column', f'{column}_binned')
        df[new_column] = pd.cut(df[column], bins=bins, labels=labels)
        
        return df
    
    def _calculate_rolling_features(self, df: pd.DataFrame, step: Dict[str, Any]) -> pd.DataFrame:
        """Calculate rolling window features"""
        column = step.get('column')
        window = step.get('window', 5)
        functions = step.get('functions', ['mean', 'std'])
        
        if column not in df.columns:
            return df
        
        for func in functions:
            new_column = f'{column}_rolling_{func}_{window}'
            if func == 'mean':
                df[new_column] = df[column].rolling(window=window).mean()
            elif func == 'std':
                df[new_column] = df[column].rolling(window=window).std()
            elif func == 'min':
                df[new_column] = df[column].rolling(window=window).min()
            elif func == 'max':
                df[new_column] = df[column].rolling(window=window).max()
        
        return df
    
    def _normalize_features(self, df: pd.DataFrame, step: Dict[str, Any]) -> pd.DataFrame:
        """Normalize features using various methods"""
        columns = step.get('columns', [])
        method = step.get('method', 'z_score')
        
        for column in columns:
            if column not in df.columns:
                continue
            
            if method == 'z_score':
                df[f'{column}_normalized'] = (df[column] - df[column].mean()) / df[column].std()
            elif method == 'min_max':
                df[f'{column}_normalized'] = (df[column] - df[column].min()) / (df[column].max() - df[column].min())
            elif method == 'robust':
                df[f'{column}_normalized'] = (df[column] - df[column].median()) / (df[column].quantile(0.75) - df[column].quantile(0.25))
        
        return df
    
    def _encode_categorical_features(self, df: pd.DataFrame, step: Dict[str, Any]) -> pd.DataFrame:
        """Encode categorical features"""
        columns = step.get('columns', [])
        method = step.get('method', 'one_hot')
        
        for column in columns:
            if column not in df.columns:
                continue
            
            if method == 'one_hot':
                dummies = pd.get_dummies(df[column], prefix=column)
                df = pd.concat([df, dummies], axis=1)
            elif method == 'label':
                df[f'{column}_encoded'] = df[column].astype('category').cat.codes
            elif method == 'frequency':
                value_counts = df[column].value_counts()
                df[f'{column}_freq'] = df[column].map(value_counts)
        
        return df
    
    def _create_interaction_features(self, df: pd.DataFrame, step: Dict[str, Any]) -> pd.DataFrame:
        """Create interaction features between columns"""
        columns = step.get('columns', [])
        interaction_type = step.get('interaction_type', 'multiply')
        
        if len(columns) < 2:
            return df
        
        col1, col2 = columns[0], columns[1]
        if col1 not in df.columns or col2 not in df.columns:
            return df
        
        new_column = step.get('new_column', f'{col1}_{col2}_interaction')
        
        if interaction_type == 'multiply':
            df[new_column] = df[col1] * df[col2]
        elif interaction_type == 'divide':
            df[new_column] = df[col1] / df[col2].replace(0, np.nan)
        elif interaction_type == 'add':
            df[new_column] = df[col1] + df[col2]
        elif interaction_type == 'subtract':
            df[new_column] = df[col1] - df[col2]
        
        return df
    
    def _apply_custom_function(self, df: pd.DataFrame, step: Dict[str, Any]) -> pd.DataFrame:
        """Apply a custom function to create features"""
        # This would be implemented based on specific requirements
        # Could use eval() or exec() for dynamic function execution
        return df
    
    def _apply_aggregation_rules(self, df: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """Apply aggregation rules to the data"""
        if data_type not in self.aggregation_rules:
            return {}
        
        aggregated_data = {}
        rules = self.aggregation_rules[data_type]
        
        for rule in rules:
            rule_type = rule.get('type')
            column = rule.get('column')
            output_key = rule.get('output_key', f'{column}_{rule_type}')
            
            if column not in df.columns:
                continue
            
            if rule_type == 'count':
                aggregated_data[output_key] = len(df)
            elif rule_type == 'sum':
                aggregated_data[output_key] = df[column].sum()
            elif rule_type == 'mean':
                aggregated_data[output_key] = df[column].mean()
            elif rule_type == 'median':
                aggregated_data[output_key] = df[column].median()
            elif rule_type == 'std':
                aggregated_data[output_key] = df[column].std()
            elif rule_type == 'min':
                aggregated_data[output_key] = df[column].min()
            elif rule_type == 'max':
                aggregated_data[output_key] = df[column].max()
            elif rule_type == 'quantile':
                q = rule.get('quantile', 0.5)
                aggregated_data[output_key] = df[column].quantile(q)
            elif rule_type == 'unique_count':
                aggregated_data[output_key] = df[column].nunique()
            elif rule_type == 'most_common':
                aggregated_data[output_key] = df[column].mode().iloc[0] if not df[column].mode().empty else None
            elif rule_type == 'custom':
                # Custom aggregation function
                func = rule.get('function')
                if func:
                    aggregated_data[output_key] = func(df[column])
        
        return aggregated_data
    
    def _apply_windowing(self, df: pd.DataFrame, data_type: str, aggregated_data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply time-based windowing to aggregations"""
        config = self.window_configs[data_type]
        window_size = config.get('window_size', '1H')
        timestamp_column = config.get('timestamp_column', 'timestamp')
        
        if timestamp_column not in df.columns:
            return aggregated_data
        
        # Convert timestamp column to datetime
        df[timestamp_column] = pd.to_datetime(df[timestamp_column])
        
        # Create time-based windows
        df['window'] = df[timestamp_column].dt.floor(window_size)
        
        # Group by window and apply aggregations
        windowed_data = {}
        for window, group in df.groupby('window'):
            window_key = window.isoformat()
            windowed_data[window_key] = self._apply_aggregation_rules(group, data_type)
        
        aggregated_data['windowed_data'] = windowed_data
        return aggregated_data
    
    async def _store_aggregated_data(self, data_type: str, aggregated_data: Dict[str, Any]):
        """Store aggregated data in Redis for quick access"""
        redis = await aioredis.from_url(self.redis_url)
        
        # Store current aggregated data
        await redis.set(
            f"aggregated:{data_type}:current",
            json.dumps(aggregated_data),
            ex=3600  # Expire in 1 hour
        )
        
        # Store historical aggregated data
        timestamp = datetime.utcnow().isoformat()
        await redis.zadd(
            f"aggregated:{data_type}:history",
            {json.dumps({**aggregated_data, 'timestamp': timestamp}): datetime.utcnow().timestamp()}
        )
        
        # Keep only last 1000 historical records
        await redis.zremrangebyrank(f"aggregated:{data_type}:history", 0, -1001)
        
        await redis.close()
    
    async def get_aggregated_data(self, data_type: str, window: str = 'current') -> Dict[str, Any]:
        """Retrieve aggregated data"""
        redis = await aioredis.from_url(self.redis_url)
        
        if window == 'current':
            data = await redis.get(f"aggregated:{data_type}:current")
        else:
            # Get historical data for specific time range
            end_time = datetime.utcnow().timestamp()
            start_time = end_time - self._parse_time_window(window)
            data_list = await redis.zrangebyscore(
                f"aggregated:{data_type}:history",
                start_time,
                end_time
            )
            data = json.dumps({'historical_data': [json.loads(d) for d in data_list]})
        
        await redis.close()
        
        return json.loads(data) if data else {}
    
    def _parse_time_window(self, window: str) -> int:
        """Parse time window string to seconds"""
        if window.endswith('H'):
            return int(window[:-1]) * 3600
        elif window.endswith('D'):
            return int(window[:-1]) * 86400
        elif window.endswith('W'):
            return int(window[:-1]) * 604800
        elif window.endswith('M'):
            return int(window[:-1]) * 2592000
        else:
            return int(window)
    
    async def get_aggregation_summary(self, data_type: str) -> Dict[str, Any]:
        """Get summary of aggregated data"""
        current_data = await self.get_aggregated_data(data_type, 'current')
        historical_data = await self.get_aggregated_data(data_type, '24H')
        
        summary = {
            'data_type': data_type,
            'last_updated': datetime.utcnow().isoformat(),
            'current_metrics': current_data,
            'historical_trends': self._calculate_trends(historical_data.get('historical_data', [])),
            'data_quality': self._assess_data_quality(current_data)
        }
        
        return summary
    
    def _calculate_trends(self, historical_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate trends from historical data"""
        if not historical_data:
            return {}
        
        trends = {}
        for key in historical_data[0].keys():
            if key == 'timestamp':
                continue
            
            values = [d.get(key) for d in historical_data if d.get(key) is not None]
            if len(values) > 1:
                trends[key] = {
                    'trend': 'increasing' if values[-1] > values[0] else 'decreasing',
                    'change_percent': ((values[-1] - values[0]) / values[0] * 100) if values[0] != 0 else 0,
                    'volatility': np.std(values) if len(values) > 1 else 0
                }
        
        return trends
    
    def _assess_data_quality(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess the quality of aggregated data"""
        quality_metrics = {
            'completeness': 0,
            'consistency': 0,
            'timeliness': 0
        }
        
        # Calculate completeness (percentage of non-null values)
        total_fields = len(data)
        non_null_fields = sum(1 for v in data.values() if v is not None)
        quality_metrics['completeness'] = (non_null_fields / total_fields * 100) if total_fields > 0 else 0
        
        # Calculate consistency (check for expected data types)
        numeric_fields = sum(1 for v in data.values() if isinstance(v, (int, float)))
        quality_metrics['consistency'] = (numeric_fields / total_fields * 100) if total_fields > 0 else 0
        
        # Calculate timeliness (based on last update time)
        quality_metrics['timeliness'] = 100  # Assuming data is current
        
        return quality_metrics
