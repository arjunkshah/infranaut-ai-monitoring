#!/usr/bin/env python3
"""
Simplified test for Data Collection & Aggregation components
Tests core functionality without complex mocking
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import Dict, List, Any

# Add current directory to path
sys.path.insert(0, '.')

# Import modules directly
from data_validator import DataValidator
from ingestion_pipeline import DataSource
from metadata_manager import DatasetMetadata, DataLineage, DataLineageType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

def test_data_validator():
    """Test DataValidator functionality"""
    print("\n=== Testing DataValidator ===")
    
    validator = DataValidator()
    
    # Test 1: Add validation schema
    print("1. Adding validation schema...")
    schema = {
        "type": "object",
        "properties": {
            "model_id": {"type": "string"},
            "accuracy": {"type": "number", "minimum": 0, "maximum": 1}
        },
        "required": ["model_id", "accuracy"]
    }
    validator.add_validation_schema("model_metrics", schema)
    print("✅ Schema added successfully")
    
    # Test 2: Add data rules
    print("2. Adding data rules...")
    rules = [
        {"type": "required", "field": "model_id"},
        {"type": "min_value", "field": "accuracy", "value": 0.0},
        {"type": "max_value", "field": "accuracy", "value": 1.0}
    ]
    validator.add_data_rules("model_metrics", rules)
    print("✅ Rules added successfully")
    
    # Test 3: Validate valid data
    print("3. Testing valid data validation...")
    valid_data = {
        "model_id": "test_model",
        "accuracy": 0.95
    }
    is_valid, errors = validator.validate_data(valid_data, "model_metrics")
    print(f"   Valid data result: {is_valid}, Errors: {len(errors)}")
    assert is_valid, f"Valid data should pass validation: {errors}"
    print("✅ Valid data validation passed")
    
    # Test 4: Validate invalid data
    print("4. Testing invalid data validation...")
    invalid_data = {
        "model_id": "test_model",
        "accuracy": 1.5  # Invalid: > 1.0
    }
    is_valid, errors = validator.validate_data(invalid_data, "model_metrics")
    print(f"   Invalid data result: {is_valid}, Errors: {len(errors)}")
    assert not is_valid, "Invalid data should fail validation"
    print("✅ Invalid data validation passed")
    
    # Test 5: Batch validation
    print("5. Testing batch validation...")
    results = validator.validate_batch(SAMPLE_MODEL_METRICS, "model_metrics")
    print(f"   Batch results: {results['valid_records']}/{results['total_records']} valid")
    assert results["valid_records"] == len(SAMPLE_MODEL_METRICS), "All sample data should be valid"
    print("✅ Batch validation passed")
    
    # Test 6: Data quality metrics
    print("6. Testing data quality metrics...")
    metrics = validator.get_data_quality_metrics(SAMPLE_MODEL_METRICS, "model_metrics")
    print(f"   Quality metrics: {metrics['total_records']} records")
    assert metrics["total_records"] == len(SAMPLE_MODEL_METRICS), "Should have correct record count"
    print("✅ Data quality metrics passed")
    
    # Test 7: Validation report
    print("7. Testing validation report...")
    validation_results = validator.validate_batch(SAMPLE_MODEL_METRICS, "model_metrics")
    quality_metrics = validator.get_data_quality_metrics(SAMPLE_MODEL_METRICS, "model_metrics")
    report = validator.create_validation_report(validation_results, quality_metrics)
    print(f"   Report generated: {report['validation_summary']['validation_rate']:.1f}% validation rate")
    assert "validation_summary" in report, "Report should contain validation summary"
    print("✅ Validation report passed")

def test_data_source():
    """Test DataSource functionality"""
    print("\n=== Testing DataSource ===")
    
    # Test 1: Create API data source
    print("1. Creating API data source...")
    api_source = DataSource(
        source_id="test_api",
        source_type="api",
        connection_config={"url": "https://api.test.com"},
        data_format="json",
        batch_size=100,
        poll_interval=30
    )
    print(f"   Source ID: {api_source.source_id}")
    print(f"   Source Type: {api_source.source_type}")
    print(f"   Batch Size: {api_source.batch_size}")
    print("✅ API data source created")
    
    # Test 2: Create Kafka data source
    print("2. Creating Kafka data source...")
    kafka_source = DataSource(
        source_id="test_kafka",
        source_type="kafka",
        connection_config={
            "bootstrap_servers": "localhost:9092",
            "topic": "test-topic"
        },
        data_format="json",
        batch_size=1000,
        poll_interval=5
    )
    print(f"   Source ID: {kafka_source.source_id}")
    print(f"   Source Type: {kafka_source.source_type}")
    print("✅ Kafka data source created")

def test_dataset_metadata():
    """Test DatasetMetadata functionality"""
    print("\n=== Testing DatasetMetadata ===")
    
    # Test 1: Create dataset metadata
    print("1. Creating dataset metadata...")
    metadata = DatasetMetadata(
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
    print(f"   Dataset ID: {metadata.dataset_id}")
    print(f"   Name: {metadata.name}")
    print(f"   Version: {metadata.version}")
    print(f"   Owner: {metadata.owner}")
    print("✅ Dataset metadata created")

def test_data_lineage():
    """Test DataLineage functionality"""
    print("\n=== Testing DataLineage ===")
    
    # Test 1: Create data lineage
    print("1. Creating data lineage...")
    lineage = DataLineage(
        source_id="source_dataset",
        target_id="target_dataset",
        relationship_type=DataLineageType.DERIVED_FROM,
        transformation_details={"operation": "test"},
        timestamp=datetime.utcnow(),
        user_id="test_user"
    )
    print(f"   Source: {lineage.source_id}")
    print(f"   Target: {lineage.target_id}")
    print(f"   Relationship: {lineage.relationship_type}")
    print("✅ Data lineage created")

def test_integration():
    """Test integration between components"""
    print("\n=== Testing Integration ===")
    
    # Test 1: Complete workflow simulation
    print("1. Simulating complete workflow...")
    
    # Create validator
    validator = DataValidator()
    
    # Add validation rules
    schema = {
        "type": "object",
        "properties": {
            "model_id": {"type": "string"},
            "accuracy": {"type": "number", "minimum": 0, "maximum": 1}
        },
        "required": ["model_id", "accuracy"]
    }
    validator.add_validation_schema("model_metrics", schema)
    
    # Validate data
    validation_results = validator.validate_batch(SAMPLE_MODEL_METRICS, "model_metrics")
    quality_metrics = validator.get_data_quality_metrics(SAMPLE_MODEL_METRICS, "model_metrics")
    
    print(f"   Validation: {validation_results['valid_records']}/{validation_results['total_records']} valid")
    print(f"   Quality Score: {quality_metrics.get('total_records', 0)} records")
    
    # Create metadata
    metadata = DatasetMetadata(
        dataset_id="integration_test_v1",
        name="Integration Test Dataset",
        description="Dataset for integration testing",
        version="1.0.0",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        schema=schema,
        statistics=validation_results,
        tags=["integration", "test"],
        owner="test_user",
        data_source="test_source",
        data_quality_score=validation_results['valid_records'] / validation_results['total_records'],
        row_count=validation_results['total_records'],
        column_count=len(schema['properties']),
        file_size_bytes=0,
        checksum="integration_checksum"
    )
    
    print(f"   Dataset: {metadata.dataset_id}")
    print(f"   Quality Score: {metadata.data_quality_score:.2f}")
    
    # Create lineage
    lineage = DataLineage(
        source_id="raw_data",
        target_id=metadata.dataset_id,
        relationship_type=DataLineageType.TRANSFORMED_FROM,
        transformation_details={
            "operation": "validation_and_cleaning",
            "validation_rules": len(validator.validation_schemas.get("model_metrics", {}))
        },
        timestamp=datetime.utcnow(),
        user_id="test_user"
    )
    
    print(f"   Lineage: {lineage.source_id} -> {lineage.target_id}")
    print("✅ Integration test completed")

def main():
    """Run all tests"""
    print("🚀 Starting Data Collection & Aggregation Tests")
    print("=" * 50)
    
    try:
        test_data_validator()
        test_data_source()
        test_dataset_metadata()
        test_data_lineage()
        test_integration()
        
        print("\n" + "=" * 50)
        print("✅ All tests passed successfully!")
        print("🎉 Data Collection & Aggregation components are working correctly")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
