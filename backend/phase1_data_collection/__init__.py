# Data Collection & Aggregation Module
# Handles real-time data ingestion from multiple sources

from .ingestion_pipeline import DataIngestionPipeline, DataSource
from .data_validator import DataValidator
from .data_aggregator import DataAggregator
from .storage_manager import StorageManager
from .metadata_manager import MetadataManager, DatasetMetadata, DataLineage, DataLineageType

__all__ = [
    'DataIngestionPipeline',
    'DataSource',
    'DataValidator', 
    'DataAggregator',
    'StorageManager',
    'MetadataManager',
    'DatasetMetadata',
    'DataLineage',
    'DataLineageType'
]
