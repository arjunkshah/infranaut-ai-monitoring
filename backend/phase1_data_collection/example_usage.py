#!/usr/bin/env python3
"""
Example usage of the Data Collection & Aggregation components
Demonstrates how to set up and use the complete data collection pipeline
"""

import asyncio
import json
import logging
from datetime import datetime
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

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """Main example demonstrating the data collection pipeline"""
    
    # Initialize components
    redis_url = "redis://localhost:6379"
    
    # 1. Initialize Data Ingestion Pipeline
    ingestion_pipeline = DataIngestionPipeline(redis_url)
    
    # 2. Initialize Data Validator
    validator = DataValidator()
    
    # 3. Initialize Data Aggregator
    aggregator = DataAggregator(redis_url)
    
    # 4. Initialize Storage Manager
    storage_config = {
        'redis_url': redis_url,
        'postgresql': {
            'host': 'localhost',
            'port': 5432,
            'database': 'ai_monitoring',
            'user': 'postgres',
            'password': 'password'
        },
        'mongodb': {
            'connection_string': 'mongodb://localhost:27017',
            'database': 'ai_monitoring'
        },
        's3': {
            'access_key': 'your_access_key',
            'secret_key': 'your_secret_key',
            'region': 'us-east-1',
            'bucket': 'ai-monitoring-data'
        }
    }
    storage_manager = StorageManager(storage_config)
    
    # 5. Initialize Metadata Manager
    metadata_manager = MetadataManager(redis_url)
    
    # Example 1: Set up data sources
    await setup_data_sources(ingestion_pipeline)
    
    # Example 2: Configure validation rules
    setup_validation_rules(validator)
    
    # Example 3: Configure aggregation rules
    setup_aggregation_rules(aggregator)
    
    # Example 4: Configure storage partitioning
    setup_storage_partitioning(storage_manager)
    
    # Example 5: Register dataset metadata
    await register_dataset_metadata(metadata_manager)
    
    # Example 6: Process sample data
    await process_sample_data(
        ingestion_pipeline, 
        validator, 
        aggregator, 
        storage_manager, 
        metadata_manager
    )
    
    # Example 7: Query and analyze data
    await query_and_analyze_data(
        storage_manager, 
        metadata_manager, 
        aggregator
    )

async def setup_data_sources(ingestion_pipeline: DataIngestionPipeline):
    """Set up various data sources for ingestion"""
    logger.info("Setting up data sources...")
    
    # API data source
    api_source = DataSource(
        source_id="model_metrics_api",
        source_type="api",
        connection_config={
            "url": "https://api.example.com/model-metrics",
            "headers": {"Authorization": "Bearer token"}
        },
        data_format="json",
        batch_size=100,
        poll_interval=30
    )
    await ingestion_pipeline.add_data_source(api_source)
    
    # Kafka data source
    kafka_source = DataSource(
        source_id="model_predictions_kafka",
        source_type="kafka",
        connection_config={
            "bootstrap_servers": "localhost:9092",
            "topic": "model-predictions"
        },
        data_format="json",
        batch_size=1000,
        poll_interval=5
    )
    await ingestion_pipeline.add_data_source(kafka_source)
    
    # S3 data source
    s3_source = DataSource(
        source_id="batch_data_s3",
        source_type="s3",
        connection_config={
            "bucket": "ai-monitoring-data",
            "prefix": "batch-data/"
        },
        data_format="parquet",
        batch_size=500,
        poll_interval=300
    )
    await ingestion_pipeline.add_data_source(s3_source)

def setup_validation_rules(validator: DataValidator):
    """Set up validation rules for different data types"""
    logger.info("Setting up validation rules...")
    
    # Schema for model metrics
    metrics_schema = {
        "type": "object",
        "properties": {
            "model_id": {"type": "string"},
            "timestamp": {"type": "string", "format": "date-time"},
            "accuracy": {"type": "number", "minimum": 0, "maximum": 1},
            "latency_ms": {"type": "number", "minimum": 0},
            "throughput": {"type": "number", "minimum": 0},
            "error_rate": {"type": "number", "minimum": 0, "maximum": 1}
        },
        "required": ["model_id", "timestamp", "accuracy"]
    }
    validator.add_validation_schema("model_metrics", metrics_schema)
    
    # Custom validation rules
    metrics_rules = [
        {"type": "required", "field": "model_id"},
        {"type": "not_null", "field": "accuracy"},
        {"type": "min_value", "field": "accuracy", "value": 0.0},
        {"type": "max_value", "field": "accuracy", "value": 1.0},
        {"type": "min_value", "field": "latency_ms", "value": 0},
        {"type": "regex", "field": "model_id", "value": r"^model_[a-zA-Z0-9_]+$"}
    ]
    validator.add_data_rules("model_metrics", metrics_rules)
    
    # Anomaly detection thresholds
    anomaly_thresholds = {
        "accuracy": {
            "z_score_threshold": 2.0,
            "mean": 0.85,
            "std": 0.1
        },
        "latency_ms": {
            "min_value": 0,
            "max_value": 10000,
            "z_score_threshold": 3.0,
            "mean": 100,
            "std": 50
        }
    }
    validator.set_anomaly_thresholds("model_metrics", anomaly_thresholds)

def setup_aggregation_rules(aggregator: DataAggregator):
    """Set up aggregation rules for data processing"""
    logger.info("Setting up aggregation rules...")
    
    # Aggregation rules for model metrics
    metrics_aggregation_rules = [
        {"type": "mean", "column": "accuracy", "output_key": "avg_accuracy"},
        {"type": "std", "column": "accuracy", "output_key": "accuracy_std"},
        {"type": "mean", "column": "latency_ms", "output_key": "avg_latency"},
        {"type": "max", "column": "latency_ms", "output_key": "max_latency"},
        {"type": "sum", "column": "throughput", "output_key": "total_throughput"},
        {"type": "count", "column": "model_id", "output_key": "prediction_count"},
        {"type": "unique_count", "column": "model_id", "output_key": "unique_models"}
    ]
    
    for rule in metrics_aggregation_rules:
        aggregator.add_aggregation_rule("model_metrics", rule)
    
    # Feature engineering pipeline
    feature_pipeline = [
        {
            "type": "extract_datetime",
            "column": "timestamp"
        },
        {
            "type": "calculate_rolling",
            "column": "accuracy",
            "window": 10,
            "functions": ["mean", "std"]
        },
        {
            "type": "normalize",
            "columns": ["latency_ms", "throughput"],
            "method": "z_score"
        },
        {
            "type": "create_categorical",
            "column": "accuracy",
            "bins": 5,
            "labels": ["very_low", "low", "medium", "high", "very_high"]
        }
    ]
    aggregator.add_feature_engineering_pipeline("model_metrics", feature_pipeline)
    
    # Time-based windowing
    window_config = {
        "window_size": "1H",
        "timestamp_column": "timestamp"
    }
    aggregator.set_window_config("model_metrics", window_config)

def setup_storage_partitioning(storage_manager: StorageManager):
    """Set up storage partitioning strategies"""
    logger.info("Setting up storage partitioning...")
    
    # Date-based partitioning for model metrics
    date_partition_strategy = {
        "type": "date",
        "field": "timestamp"
    }
    storage_manager.set_partition_strategy("model_metrics", date_partition_strategy)
    
    # Hash-based partitioning for model predictions
    hash_partition_strategy = {
        "type": "hash",
        "field": "model_id",
        "num_partitions": 10
    }
    storage_manager.set_partition_strategy("model_predictions", hash_partition_strategy)

async def register_dataset_metadata(metadata_manager: MetadataManager):
    """Register dataset metadata"""
    logger.info("Registering dataset metadata...")
    
    # Create sample dataset metadata
    dataset_metadata = DatasetMetadata(
        dataset_id="model_metrics_v1",
        name="Model Performance Metrics",
        description="Real-time metrics for AI model performance monitoring",
        version="1.0.0",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        schema={
            "model_id": "string",
            "timestamp": "datetime",
            "accuracy": "float",
            "latency_ms": "integer",
            "throughput": "float",
            "error_rate": "float"
        },
        statistics={
            "total_records": 0,
            "avg_accuracy": 0.0,
            "avg_latency": 0.0
        },
        tags=["ai", "monitoring", "metrics", "performance"],
        owner="ai-team",
        data_source="model_metrics_api",
        data_quality_score=0.95,
        row_count=0,
        column_count=6,
        file_size_bytes=0,
        checksum=""
    )
    
    await metadata_manager.register_dataset(dataset_metadata)
    
    # Add data lineage
    lineage = DataLineage(
        source_id="model_metrics_api",
        target_id="model_metrics_v1",
        relationship_type=DataLineageType.DERIVED_FROM,
        transformation_details={
            "operation": "data_collection",
            "filters": {},
            "aggregations": []
        },
        timestamp=datetime.utcnow(),
        user_id="system"
    )
    
    await metadata_manager.add_data_lineage(lineage)

async def process_sample_data(
    ingestion_pipeline: DataIngestionPipeline,
    validator: DataValidator,
    aggregator: DataAggregator,
    storage_manager: StorageManager,
    metadata_manager: MetadataManager
):
    """Process sample data through the pipeline"""
    logger.info("Processing sample data...")
    
    # Sample model metrics data
    sample_data = [
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
    
    # 1. Validate data
    validation_results = validator.validate_batch(sample_data, "model_metrics")
    quality_metrics = validator.get_data_quality_metrics(sample_data, "model_metrics")
    validation_report = validator.create_validation_report(validation_results, quality_metrics)
    
    logger.info(f"Validation results: {validation_report['validation_summary']}")
    
    # 2. Aggregate data
    aggregated_data = await aggregator.aggregate_data(sample_data, "model_metrics")
    logger.info(f"Aggregated data: {aggregated_data}")
    
    # 3. Store data
    for record in sample_data:
        await storage_manager.store_data(record, "model_metrics", "hot")
    
    # Store aggregated data
    await storage_manager.store_data(aggregated_data, "model_metrics_aggregated", "warm")
    
    # 4. Update metadata
    await metadata_manager.update_dataset_metadata("model_metrics_v1", {
        "row_count": len(sample_data),
        "updated_at": datetime.utcnow(),
        "statistics": aggregated_data
    })

async def query_and_analyze_data(
    storage_manager: StorageManager,
    metadata_manager: MetadataManager,
    aggregator: DataAggregator
):
    """Query and analyze stored data"""
    logger.info("Querying and analyzing data...")
    
    # 1. Get dataset statistics
    stats = await metadata_manager.get_dataset_statistics("model_metrics_v1")
    logger.info(f"Dataset statistics: {stats}")
    
    # 2. Query recent data
    recent_data = await storage_manager.retrieve_data(
        "model_metrics",
        {
            "start_time": datetime.utcnow().timestamp() - 3600,  # Last hour
            "limit": 100
        },
        "hot"
    )
    logger.info(f"Retrieved {len(recent_data)} recent records")
    
    # 3. Get aggregated data summary
    aggregation_summary = await aggregator.get_aggregation_summary("model_metrics")
    logger.info(f"Aggregation summary: {aggregation_summary}")
    
    # 4. Get data lineage
    lineage = await metadata_manager.get_data_lineage("model_metrics_v1")
    logger.info(f"Data lineage: {len(lineage)} relationships")
    
    # 5. Search datasets
    search_results = await metadata_manager.search_datasets({
        "tags": ["ai", "monitoring"],
        "min_quality_score": 0.9
    })
    logger.info(f"Found {len(search_results)} matching datasets")

if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
