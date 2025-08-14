#!/usr/bin/env python3
"""
Comprehensive unit tests for Data Collection & Aggregation components
Tests all major functionality and edge cases
"""

import pytest
import asyncio
import json
import tempfile
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, List, Any

from data_collection import (
    DataIngestionPipeline,
    DataValidator,
    DataAggregator,
    StorageManager,
    MetadataManager,
    DataSource,
    DatasetMetadata,
    DataLineage,
    DataLineageType
)

# Test data
SAMPLE_MODEL_METRICS = [
    {
        "model_id": "model_classification_v1",
        "timestamp": "2024-01-15T10:30:00Z",
        "accuracy": 0.92,
        "latency_ms": 150,
        "throughput": 100.5,
        "error_rate": 0.08
    },
    {
        "model_id": "model_classification_v1",
        "timestamp": "2024-01-15T10:31:00Z",
        "accuracy": 0.89,
        "latency_ms": 180,
        "throughput": 95.2,
        "error_rate": 0.11
    },
    {
        "model_id": "model_classification_v1",
        "timestamp": "2024-01-15T10:32:00Z",
        "accuracy": 0.94,
        "latency_ms": 120,
        "throughput": 110.3,
        "error_rate": 0.06
    }
]

INVALID_MODEL_METRICS = [
    {
        "model_id": "invalid_model",
        "timestamp": "invalid_timestamp",
        "accuracy": 1.5,  # Invalid: > 1.0
        "latency_ms": -50,  # Invalid: negative
        "throughput": "not_a_number",  # Invalid: string
        "error_rate": 0.08
    }
]

class TestDataIngestionPipeline:
    """Test cases for DataIngestionPipeline"""
    
    @pytest.fixture
    async def pipeline(self):
        """Create a test pipeline instance"""
        pipeline = DataIngestionPipeline("redis://localhost:6379")
        yield pipeline
        await pipeline.stop_ingestion()
    
    @pytest.mark.asyncio
    async def test_add_data_source(self, pipeline):
        """Test adding a data source"""
        source = DataSource(
            source_id="test_api",
            source_type="api",
            connection_config={"url": "https://api.test.com"},
            data_format="json"
        )
        
        result = await pipeline.add_data_source(source)
        assert result is True
        assert "test_api" in pipeline.sources
    
    @pytest.mark.asyncio
    async def test_add_invalid_data_source(self, pipeline):
        """Test adding an invalid data source"""
        source = DataSource(
            source_id="invalid_source",
            source_type="invalid_type",
            connection_config={},
            data_format="json"
        )
        
        result = await pipeline.add_data_source(source)
        assert result is False
    
    @pytest.mark.asyncio
    async def test_process_batch(self, pipeline):
        """Test batch processing"""
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis.return_value = mock_redis_instance
            
            await pipeline._process_batch("test_source", SAMPLE_MODEL_METRICS)
            
            # Verify Redis operations were called
            assert mock_redis_instance.lpush.call_count == len(SAMPLE_MODEL_METRICS)
    
    @pytest.mark.asyncio
    async def test_get_data_queue_size(self, pipeline):
        """Test getting queue size"""
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis_instance.llen.return_value = 100
            mock_redis.return_value = mock_redis_instance
            
            size = await pipeline.get_data_queue_size("test_source")
            assert size == 100
    
    @pytest.mark.asyncio
    async def test_get_recent_data(self, pipeline):
        """Test getting recent data"""
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis_instance.lrange.return_value = [
                json.dumps({"data": item, "metadata": {}}) 
                for item in SAMPLE_MODEL_METRICS
            ]
            mock_redis.return_value = mock_redis_instance
            
            data = await pipeline.get_recent_data("test_source", limit=3)
            assert len(data) == 3
            assert all("data" in item for item in data)

class TestDataValidator:
    """Test cases for DataValidator"""
    
    @pytest.fixture
    def validator(self):
        """Create a test validator instance"""
        return DataValidator()
    
    def test_add_validation_schema(self, validator):
        """Test adding validation schema"""
        schema = {
            "type": "object",
            "properties": {
                "model_id": {"type": "string"},
                "accuracy": {"type": "number"}
            },
            "required": ["model_id"]
        }
        
        validator.add_validation_schema("test_type", schema)
        assert "test_type" in validator.validation_schemas
    
    def test_add_data_rules(self, validator):
        """Test adding data rules"""
        rules = [
            {"type": "required", "field": "model_id"},
            {"type": "min_value", "field": "accuracy", "value": 0.0}
        ]
        
        validator.add_data_rules("test_type", rules)
        assert "test_type" in validator.data_rules
        assert len(validator.data_rules["test_type"]) == 2
    
    def test_set_anomaly_thresholds(self, validator):
        """Test setting anomaly thresholds"""
        thresholds = {
            "accuracy": {
                "z_score_threshold": 2.0,
                "mean": 0.85,
                "std": 0.1
            }
        }
        
        validator.set_anomaly_thresholds("test_type", thresholds)
        assert "test_type" in validator.anomaly_thresholds
    
    def test_validate_data_success(self, validator):
        """Test successful data validation"""
        # Add schema and rules
        schema = {
            "type": "object",
            "properties": {
                "model_id": {"type": "string"},
                "accuracy": {"type": "number", "minimum": 0, "maximum": 1}
            },
            "required": ["model_id", "accuracy"]
        }
        validator.add_validation_schema("test_type", schema)
        
        rules = [
            {"type": "required", "field": "model_id"},
            {"type": "min_value", "field": "accuracy", "value": 0.0}
        ]
        validator.add_data_rules("test_type", rules)
        
        # Test valid data
        valid_data = {
            "model_id": "test_model",
            "accuracy": 0.95
        }
        
        is_valid, errors = validator.validate_data(valid_data, "test_type")
        assert is_valid is True
        assert len(errors) == 0
    
    def test_validate_data_failure(self, validator):
        """Test failed data validation"""
        # Add schema and rules
        schema = {
            "type": "object",
            "properties": {
                "model_id": {"type": "string"},
                "accuracy": {"type": "number", "minimum": 0, "maximum": 1}
            },
            "required": ["model_id", "accuracy"]
        }
        validator.add_validation_schema("test_type", schema)
        
        rules = [
            {"type": "required", "field": "model_id"},
            {"type": "min_value", "field": "accuracy", "value": 0.0}
        ]
        validator.add_data_rules("test_type", rules)
        
        # Test invalid data
        invalid_data = {
            "model_id": "test_model",
            "accuracy": 1.5  # Invalid: > 1.0
        }
        
        is_valid, errors = validator.validate_data(invalid_data, "test_type")
        assert is_valid is False
        assert len(errors) > 0
    
    def test_validate_batch(self, validator):
        """Test batch validation"""
        # Add schema and rules
        schema = {
            "type": "object",
            "properties": {
                "model_id": {"type": "string"},
                "accuracy": {"type": "number", "minimum": 0, "maximum": 1}
            },
            "required": ["model_id", "accuracy"]
        }
        validator.add_validation_schema("test_type", schema)
        
        # Test batch validation
        results = validator.validate_batch(SAMPLE_MODEL_METRICS, "test_type")
        
        assert results["total_records"] == len(SAMPLE_MODEL_METRICS)
        assert results["valid_records"] == len(SAMPLE_MODEL_METRICS)
        assert results["invalid_records"] == 0
    
    def test_get_data_quality_metrics(self, validator):
        """Test data quality metrics calculation"""
        metrics = validator.get_data_quality_metrics(SAMPLE_MODEL_METRICS, "test_type")
        
        assert metrics["total_records"] == len(SAMPLE_MODEL_METRICS)
        assert "missing_values" in metrics
        assert "numeric_stats" in metrics
        assert "categorical_stats" in metrics
    
    def test_create_validation_report(self, validator):
        """Test validation report creation"""
        validation_results = validator.validate_batch(SAMPLE_MODEL_METRICS, "test_type")
        quality_metrics = validator.get_data_quality_metrics(SAMPLE_MODEL_METRICS, "test_type")
        
        report = validator.create_validation_report(validation_results, quality_metrics)
        
        assert "validation_summary" in report
        assert "error_analysis" in report
        assert "data_quality_metrics" in report
        assert "recommendations" in report

class TestDataAggregator:
    """Test cases for DataAggregator"""
    
    @pytest.fixture
    async def aggregator(self):
        """Create a test aggregator instance"""
        return DataAggregator("redis://localhost:6379")
    
    def test_add_aggregation_rule(self, aggregator):
        """Test adding aggregation rule"""
        rule = {
            "type": "mean",
            "column": "accuracy",
            "output_key": "avg_accuracy"
        }
        
        aggregator.add_aggregation_rule("test_type", rule)
        assert "test_type" in aggregator.aggregation_rules
        assert len(aggregator.aggregation_rules["test_type"]) == 1
    
    def test_add_feature_engineering_pipeline(self, aggregator):
        """Test adding feature engineering pipeline"""
        pipeline = [
            {
                "type": "extract_datetime",
                "column": "timestamp"
            },
            {
                "type": "normalize",
                "columns": ["accuracy"],
                "method": "z_score"
            }
        ]
        
        aggregator.add_feature_engineering_pipeline("test_type", pipeline)
        assert "test_type" in aggregator.feature_engineering_pipelines
    
    def test_set_window_config(self, aggregator):
        """Test setting window configuration"""
        config = {
            "window_size": "1H",
            "timestamp_column": "timestamp"
        }
        
        aggregator.set_window_config("test_type", config)
        assert "test_type" in aggregator.window_configs
    
    @pytest.mark.asyncio
    async def test_aggregate_data(self, aggregator):
        """Test data aggregation"""
        # Add aggregation rules
        rules = [
            {"type": "mean", "column": "accuracy", "output_key": "avg_accuracy"},
            {"type": "std", "column": "accuracy", "output_key": "accuracy_std"},
            {"type": "count", "column": "model_id", "output_key": "record_count"}
        ]
        
        for rule in rules:
            aggregator.add_aggregation_rule("test_type", rule)
        
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis.return_value = mock_redis_instance
            
            result = await aggregator.aggregate_data(SAMPLE_MODEL_METRICS, "test_type")
            
            assert "avg_accuracy" in result
            assert "accuracy_std" in result
            assert "record_count" in result
            assert result["record_count"] == len(SAMPLE_MODEL_METRICS)
    
    @pytest.mark.asyncio
    async def test_get_aggregated_data(self, aggregator):
        """Test retrieving aggregated data"""
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis_instance.get.return_value = json.dumps({
                "avg_accuracy": 0.92,
                "record_count": 3
            })
            mock_redis.return_value = mock_redis_instance
            
            data = await aggregator.get_aggregated_data("test_type", "current")
            assert "avg_accuracy" in data
            assert data["avg_accuracy"] == 0.92
    
    @pytest.mark.asyncio
    async def test_get_aggregation_summary(self, aggregator):
        """Test getting aggregation summary"""
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis_instance.get.return_value = json.dumps({
                "avg_accuracy": 0.92,
                "record_count": 3
            })
            mock_redis_instance.zrange.return_value = []
            mock_redis.return_value = mock_redis_instance
            
            summary = await aggregator.get_aggregation_summary("test_type")
            
            assert "data_type" in summary
            assert "current_metrics" in summary
            assert "historical_trends" in summary
            assert "data_quality" in summary

class TestStorageManager:
    """Test cases for StorageManager"""
    
    @pytest.fixture
    def storage_config(self):
        """Create test storage configuration"""
        return {
            'redis_url': 'redis://localhost:6379',
            'postgresql': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_password'
            }
        }
    
    @pytest.fixture
    def storage_manager(self, storage_config):
        """Create a test storage manager instance"""
        with patch('psycopg2.connect'):
            return StorageManager(storage_config)
    
    def test_set_partition_strategy(self, storage_manager):
        """Test setting partition strategy"""
        strategy = {
            "type": "date",
            "field": "timestamp"
        }
        
        storage_manager.set_partition_strategy("test_type", strategy)
        assert "test_type" in storage_manager.partition_strategies
    
    @pytest.mark.asyncio
    async def test_store_data_hot(self, storage_manager):
        """Test storing data in hot storage"""
        data = {"test": "data"}
        
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis.return_value = mock_redis_instance
            
            result = await storage_manager.store_data(data, "test_type", "hot")
            assert result is True
    
    @pytest.mark.asyncio
    async def test_store_data_warm(self, storage_manager):
        """Test storing data in warm storage"""
        data = {"test": "data"}
        
        with patch('psycopg2.connect') as mock_connect:
            mock_conn = Mock()
            mock_cursor = Mock()
            mock_conn.cursor.return_value = mock_cursor
            mock_connect.return_value = mock_conn
            
            result = await storage_manager.store_data(data, "test_type", "warm")
            assert result is True
    
    @pytest.mark.asyncio
    async def test_retrieve_data_hot(self, storage_manager):
        """Test retrieving data from hot storage"""
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis_instance.zrangebyscore.return_value = []
            mock_redis.return_value = mock_redis_instance
            
            data = await storage_manager.retrieve_data("test_type", {}, "hot")
            assert isinstance(data, list)
    
    @pytest.mark.asyncio
    async def test_get_storage_stats(self, storage_manager):
        """Test getting storage statistics"""
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis_instance.info.return_value = {
                'used_memory': 1000,
                'connected_clients': 5,
                'total_commands_processed': 10000
            }
            mock_redis.return_value = mock_redis_instance
            
            stats = await storage_manager.get_storage_stats()
            assert "hot_storage" in stats
            assert "warm_storage" in stats
            assert "cold_storage" in stats

class TestMetadataManager:
    """Test cases for MetadataManager"""
    
    @pytest.fixture
    async def metadata_manager(self):
        """Create a test metadata manager instance"""
        return MetadataManager("redis://localhost:6379")
    
    @pytest.fixture
    def sample_dataset_metadata(self):
        """Create sample dataset metadata"""
        return DatasetMetadata(
            dataset_id="test_dataset_v1",
            name="Test Dataset",
            description="Test dataset for unit testing",
            version="1.0.0",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            schema={"test": "string"},
            statistics={"count": 0},
            tags=["test"],
            owner="test_user",
            data_source="test_source",
            data_quality_score=0.95,
            row_count=0,
            column_count=1,
            file_size_bytes=0,
            checksum="test_checksum"
        )
    
    @pytest.mark.asyncio
    async def test_register_dataset(self, metadata_manager, sample_dataset_metadata):
        """Test registering a dataset"""
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis.return_value = mock_redis_instance
            
            result = await metadata_manager.register_dataset(sample_dataset_metadata)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_update_dataset_metadata(self, metadata_manager, sample_dataset_metadata):
        """Test updating dataset metadata"""
        # First register the dataset
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis.return_value = mock_redis_instance
            
            await metadata_manager.register_dataset(sample_dataset_metadata)
            
            # Then update it
            updates = {"name": "Updated Test Dataset"}
            result = await metadata_manager.update_dataset_metadata(
                sample_dataset_metadata.dataset_id, 
                updates
            )
            assert result is True
    
    @pytest.mark.asyncio
    async def test_add_data_lineage(self, metadata_manager):
        """Test adding data lineage"""
        lineage = DataLineage(
            source_id="source_dataset",
            target_id="target_dataset",
            relationship_type=DataLineageType.DERIVED_FROM,
            transformation_details={"operation": "test"},
            timestamp=datetime.utcnow()
        )
        
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis.return_value = mock_redis_instance
            
            result = await metadata_manager.add_data_lineage(lineage)
            assert result is True
    
    @pytest.mark.asyncio
    async def test_get_dataset_metadata(self, metadata_manager, sample_dataset_metadata):
        """Test getting dataset metadata"""
        # First register the dataset
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis_instance.hgetall.return_value = {
                'dataset_id': sample_dataset_metadata.dataset_id,
                'name': sample_dataset_metadata.name,
                'description': sample_dataset_metadata.description,
                'version': sample_dataset_metadata.version,
                'created_at': sample_dataset_metadata.created_at.isoformat(),
                'updated_at': sample_dataset_metadata.updated_at.isoformat(),
                'schema': json.dumps(sample_dataset_metadata.schema),
                'statistics': json.dumps(sample_dataset_metadata.statistics),
                'tags': json.dumps(sample_dataset_metadata.tags),
                'owner': sample_dataset_metadata.owner,
                'data_source': sample_dataset_metadata.data_source,
                'data_quality_score': str(sample_dataset_metadata.data_quality_score),
                'row_count': str(sample_dataset_metadata.row_count),
                'column_count': str(sample_dataset_metadata.column_count),
                'file_size_bytes': str(sample_dataset_metadata.file_size_bytes),
                'checksum': sample_dataset_metadata.checksum
            }
            mock_redis.return_value = mock_redis_instance
            
            metadata = await metadata_manager.get_dataset_metadata(sample_dataset_metadata.dataset_id)
            assert metadata is not None
            assert metadata.dataset_id == sample_dataset_metadata.dataset_id
    
    @pytest.mark.asyncio
    async def test_calculate_dataset_checksum(self, metadata_manager):
        """Test calculating dataset checksum"""
        data = [{"id": 1, "value": "test"}, {"id": 2, "value": "test2"}]
        checksum = await metadata_manager.calculate_dataset_checksum(data)
        assert isinstance(checksum, str)
        assert len(checksum) == 64  # SHA-256 hash length
    
    @pytest.mark.asyncio
    async def test_validate_dataset_integrity(self, metadata_manager, sample_dataset_metadata):
        """Test dataset integrity validation"""
        # Register dataset first
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis.return_value = mock_redis_instance
            
            await metadata_manager.register_dataset(sample_dataset_metadata)
            
            # Test integrity validation
            is_valid = await metadata_manager.validate_dataset_integrity(
                sample_dataset_metadata.dataset_id,
                sample_dataset_metadata.checksum
            )
            assert is_valid is True

class TestIntegration:
    """Integration tests for the complete data collection pipeline"""
    
    @pytest.mark.asyncio
    async def test_complete_pipeline(self):
        """Test the complete data collection pipeline"""
        # Initialize all components
        redis_url = "redis://localhost:6379"
        
        ingestion_pipeline = DataIngestionPipeline(redis_url)
        validator = DataValidator()
        aggregator = DataAggregator(redis_url)
        
        storage_config = {
            'redis_url': redis_url,
            'postgresql': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_password'
            }
        }
        
        with patch('psycopg2.connect'):
            storage_manager = StorageManager(storage_config)
        
        metadata_manager = MetadataManager(redis_url)
        
        # Set up validation rules
        schema = {
            "type": "object",
            "properties": {
                "model_id": {"type": "string"},
                "accuracy": {"type": "number", "minimum": 0, "maximum": 1}
            },
            "required": ["model_id", "accuracy"]
        }
        validator.add_validation_schema("model_metrics", schema)
        
        # Set up aggregation rules
        aggregator.add_aggregation_rule("model_metrics", {
            "type": "mean",
            "column": "accuracy",
            "output_key": "avg_accuracy"
        })
        
        # Test the complete flow
        with patch('aioredis.from_url') as mock_redis:
            mock_redis_instance = AsyncMock()
            mock_redis.return_value = mock_redis_instance
            
            # 1. Validate data
            validation_results = validator.validate_batch(SAMPLE_MODEL_METRICS, "model_metrics")
            assert validation_results["valid_records"] == len(SAMPLE_MODEL_METRICS)
            
            # 2. Aggregate data
            aggregated_data = await aggregator.aggregate_data(SAMPLE_MODEL_METRICS, "model_metrics")
            assert "avg_accuracy" in aggregated_data
            
            # 3. Store data
            for record in SAMPLE_MODEL_METRICS:
                result = await storage_manager.store_data(record, "model_metrics", "hot")
                assert result is True
            
            # 4. Store aggregated data
            result = await storage_manager.store_data(aggregated_data, "model_metrics_aggregated", "warm")
            assert result is True

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])
