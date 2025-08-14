import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import hashlib
import asyncio
import redis.asyncio as aioredis
from dataclasses import dataclass, asdict
from enum import Enum

class DataLineageType(Enum):
    """Types of data lineage relationships"""
    DERIVED_FROM = "derived_from"
    TRANSFORMED_FROM = "transformed_from"
    AGGREGATED_FROM = "aggregated_from"
    FILTERED_FROM = "filtered_from"
    JOINED_WITH = "joined_with"

@dataclass
class DataLineage:
    """Data lineage information"""
    source_id: str
    target_id: str
    relationship_type: DataLineageType
    transformation_details: Dict[str, Any]
    timestamp: datetime
    user_id: Optional[str] = None

@dataclass
class DatasetMetadata:
    """Metadata for a dataset"""
    dataset_id: str
    name: str
    description: str
    version: str
    created_at: datetime
    updated_at: datetime
    schema: Dict[str, Any]
    statistics: Dict[str, Any]
    tags: List[str]
    owner: str
    data_source: str
    data_quality_score: float
    row_count: int
    column_count: int
    file_size_bytes: int
    checksum: str

class MetadataManager:
    """
    Metadata management and data lineage tracking for AI model monitoring
    Tracks data origins, transformations, and version control
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.logger = logging.getLogger(__name__)
        self.dataset_metadata: Dict[str, DatasetMetadata] = {}
        self.data_lineage: List[DataLineage] = []
        self.version_history: Dict[str, List[Dict]] = {}
        
    async def register_dataset(self, metadata: DatasetMetadata) -> bool:
        """Register a new dataset with metadata"""
        try:
            # Store metadata in Redis
            redis = await aioredis.from_url(self.redis_url)
            
            # Store dataset metadata
            await redis.hset(
                f"dataset_metadata:{metadata.dataset_id}",
                mapping=asdict(metadata)
            )
            
            # Add to dataset index
            await redis.sadd("datasets", metadata.dataset_id)
            
            # Store version history
            version_entry = {
                'version': metadata.version,
                'timestamp': metadata.created_at.isoformat(),
                'changes': 'Initial version',
                'checksum': metadata.checksum
            }
            
            await redis.lpush(
                f"dataset_versions:{metadata.dataset_id}",
                json.dumps(version_entry)
            )
            
            await redis.close()
            
            # Store in memory for quick access
            self.dataset_metadata[metadata.dataset_id] = metadata
            self.version_history[metadata.dataset_id] = [version_entry]
            
            self.logger.info(f"Registered dataset: {metadata.dataset_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to register dataset: {e}")
            return False
    
    async def update_dataset_metadata(self, dataset_id: str, updates: Dict[str, Any]) -> bool:
        """Update dataset metadata"""
        try:
            if dataset_id not in self.dataset_metadata:
                self.logger.error(f"Dataset not found: {dataset_id}")
                return False
            
            # Update metadata
            metadata = self.dataset_metadata[dataset_id]
            for key, value in updates.items():
                if hasattr(metadata, key):
                    setattr(metadata, key, value)
            
            metadata.updated_at = datetime.utcnow()
            
            # Store updated metadata
            redis = await aioredis.from_url(self.redis_url)
            await redis.hset(
                f"dataset_metadata:{dataset_id}",
                mapping=asdict(metadata)
            )
            
            # Add version entry
            version_entry = {
                'version': metadata.version,
                'timestamp': metadata.updated_at.isoformat(),
                'changes': json.dumps(updates),
                'checksum': metadata.checksum
            }
            
            await redis.lpush(
                f"dataset_versions:{dataset_id}",
                json.dumps(version_entry)
            )
            
            await redis.close()
            
            # Update memory cache
            self.dataset_metadata[dataset_id] = metadata
            self.version_history[dataset_id].insert(0, version_entry)
            
            self.logger.info(f"Updated dataset metadata: {dataset_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update dataset metadata: {e}")
            return False
    
    async def add_data_lineage(self, lineage: DataLineage) -> bool:
        """Add data lineage information"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store lineage information
            lineage_key = f"data_lineage:{lineage.source_id}:{lineage.target_id}"
            await redis.set(
                lineage_key,
                json.dumps(asdict(lineage)),
                ex=86400 * 30  # 30 days TTL
            )
            
            # Add to lineage index
            await redis.sadd(f"lineage_sources:{lineage.source_id}", lineage.target_id)
            await redis.sadd(f"lineage_targets:{lineage.target_id}", lineage.source_id)
            
            await redis.close()
            
            # Store in memory
            self.data_lineage.append(lineage)
            
            self.logger.info(f"Added data lineage: {lineage.source_id} -> {lineage.target_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add data lineage: {e}")
            return False
    
    async def get_dataset_metadata(self, dataset_id: str) -> Optional[DatasetMetadata]:
        """Get dataset metadata"""
        try:
            # Check memory cache first
            if dataset_id in self.dataset_metadata:
                return self.dataset_metadata[dataset_id]
            
            # Load from Redis
            redis = await aioredis.from_url(self.redis_url)
            metadata_dict = await redis.hgetall(f"dataset_metadata:{dataset_id}")
            await redis.close()
            
            if not metadata_dict:
                return None
            
            # Convert string values back to appropriate types
            metadata_dict['created_at'] = datetime.fromisoformat(metadata_dict['created_at'])
            metadata_dict['updated_at'] = datetime.fromisoformat(metadata_dict['updated_at'])
            metadata_dict['schema'] = json.loads(metadata_dict['schema'])
            metadata_dict['statistics'] = json.loads(metadata_dict['statistics'])
            metadata_dict['tags'] = json.loads(metadata_dict['tags'])
            metadata_dict['data_quality_score'] = float(metadata_dict['data_quality_score'])
            metadata_dict['row_count'] = int(metadata_dict['row_count'])
            metadata_dict['column_count'] = int(metadata_dict['column_count'])
            metadata_dict['file_size_bytes'] = int(metadata_dict['file_size_bytes'])
            
            metadata = DatasetMetadata(**metadata_dict)
            
            # Cache in memory
            self.dataset_metadata[dataset_id] = metadata
            
            return metadata
            
        except Exception as e:
            self.logger.error(f"Failed to get dataset metadata: {e}")
            return None
    
    async def get_data_lineage(self, dataset_id: str, direction: str = 'both') -> List[DataLineage]:
        """Get data lineage for a dataset"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            lineage_list = []
            
            if direction in ['both', 'downstream']:
                # Get downstream lineage (what this dataset was used to create)
                downstream_ids = await redis.smembers(f"lineage_sources:{dataset_id}")
                for target_id in downstream_ids:
                    lineage_key = f"data_lineage:{dataset_id}:{target_id.decode()}"
                    lineage_data = await redis.get(lineage_key)
                    if lineage_data:
                        lineage_dict = json.loads(lineage_data)
                        lineage_dict['timestamp'] = datetime.fromisoformat(lineage_dict['timestamp'])
                        lineage_dict['relationship_type'] = DataLineageType(lineage_dict['relationship_type'])
                        lineage_list.append(DataLineage(**lineage_dict))
            
            if direction in ['both', 'upstream']:
                # Get upstream lineage (what this dataset was created from)
                upstream_ids = await redis.smembers(f"lineage_targets:{dataset_id}")
                for source_id in upstream_ids:
                    lineage_key = f"data_lineage:{source_id.decode()}:{dataset_id}"
                    lineage_data = await redis.get(lineage_key)
                    if lineage_data:
                        lineage_dict = json.loads(lineage_data)
                        lineage_dict['timestamp'] = datetime.fromisoformat(lineage_dict['timestamp'])
                        lineage_dict['relationship_type'] = DataLineageType(lineage_dict['relationship_type'])
                        lineage_list.append(DataLineage(**lineage_dict))
            
            await redis.close()
            return lineage_list
            
        except Exception as e:
            self.logger.error(f"Failed to get data lineage: {e}")
            return []
    
    async def get_version_history(self, dataset_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get version history for a dataset"""
        try:
            # Check memory cache first
            if dataset_id in self.version_history:
                return self.version_history[dataset_id][:limit]
            
            # Load from Redis
            redis = await aioredis.from_url(self.redis_url)
            version_data = await redis.lrange(f"dataset_versions:{dataset_id}", 0, limit - 1)
            await redis.close()
            
            versions = []
            for version_str in version_data:
                version_dict = json.loads(version_str)
                versions.append(version_dict)
            
            # Cache in memory
            self.version_history[dataset_id] = versions
            
            return versions
            
        except Exception as e:
            self.logger.error(f"Failed to get version history: {e}")
            return []
    
    async def search_datasets(self, query: Dict[str, Any]) -> List[DatasetMetadata]:
        """Search datasets based on criteria"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            dataset_ids = await redis.smembers("datasets")
            await redis.close()
            
            matching_datasets = []
            
            for dataset_id in dataset_ids:
                metadata = await self.get_dataset_metadata(dataset_id.decode())
                if metadata and self._matches_query(metadata, query):
                    matching_datasets.append(metadata)
            
            return matching_datasets
            
        except Exception as e:
            self.logger.error(f"Failed to search datasets: {e}")
            return []
    
    def _matches_query(self, metadata: DatasetMetadata, query: Dict[str, Any]) -> bool:
        """Check if dataset metadata matches search query"""
        for key, value in query.items():
            if key == 'tags' and isinstance(value, list):
                if not all(tag in metadata.tags for tag in value):
                    return False
            elif key == 'owner' and metadata.owner != value:
                return False
            elif key == 'data_source' and metadata.data_source != value:
                return False
            elif key == 'min_quality_score' and metadata.data_quality_score < value:
                return False
            elif key == 'max_quality_score' and metadata.data_quality_score > value:
                return False
            elif key == 'min_row_count' and metadata.row_count < value:
                return False
            elif key == 'max_row_count' and metadata.row_count > value:
                return False
            elif key == 'created_after' and metadata.created_at < value:
                return False
            elif key == 'created_before' and metadata.created_at > value:
                return False
        
        return True
    
    async def calculate_dataset_checksum(self, data: List[Dict[str, Any]]) -> str:
        """Calculate checksum for dataset"""
        try:
            # Convert data to sorted JSON string for consistent checksum
            data_str = json.dumps(data, sort_keys=True)
            return hashlib.sha256(data_str.encode()).hexdigest()
        except Exception as e:
            self.logger.error(f"Failed to calculate checksum: {e}")
            return ""
    
    async def validate_dataset_integrity(self, dataset_id: str, current_checksum: str) -> bool:
        """Validate dataset integrity by comparing checksums"""
        try:
            metadata = await self.get_dataset_metadata(dataset_id)
            if not metadata:
                return False
            
            return metadata.checksum == current_checksum
            
        except Exception as e:
            self.logger.error(f"Failed to validate dataset integrity: {e}")
            return False
    
    async def get_dataset_statistics(self, dataset_id: str) -> Dict[str, Any]:
        """Get comprehensive statistics for a dataset"""
        try:
            metadata = await self.get_dataset_metadata(dataset_id)
            if not metadata:
                return {}
            
            lineage = await self.get_data_lineage(dataset_id)
            version_history = await self.get_version_history(dataset_id)
            
            stats = {
                'basic_info': {
                    'dataset_id': metadata.dataset_id,
                    'name': metadata.name,
                    'version': metadata.version,
                    'created_at': metadata.created_at.isoformat(),
                    'updated_at': metadata.updated_at.isoformat(),
                    'owner': metadata.owner,
                    'data_source': metadata.data_source
                },
                'data_quality': {
                    'quality_score': metadata.data_quality_score,
                    'row_count': metadata.row_count,
                    'column_count': metadata.column_count,
                    'file_size_bytes': metadata.file_size_bytes,
                    'checksum': metadata.checksum
                },
                'lineage_info': {
                    'upstream_datasets': len([l for l in lineage if l.target_id == dataset_id]),
                    'downstream_datasets': len([l for l in lineage if l.source_id == dataset_id]),
                    'total_lineage_relationships': len(lineage)
                },
                'version_info': {
                    'total_versions': len(version_history),
                    'latest_version': version_history[0]['version'] if version_history else None,
                    'last_updated': version_history[0]['timestamp'] if version_history else None
                },
                'schema_info': metadata.schema,
                'statistics': metadata.statistics,
                'tags': metadata.tags
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get dataset statistics: {e}")
            return {}
    
    async def export_metadata(self, dataset_ids: List[str] = None) -> Dict[str, Any]:
        """Export metadata for datasets"""
        try:
            if dataset_ids is None:
                # Export all datasets
                redis = await aioredis.from_url(self.redis_url)
                dataset_ids = [id.decode() for id in await redis.smembers("datasets")]
                await redis.close()
            
            export_data = {
                'export_timestamp': datetime.utcnow().isoformat(),
                'datasets': {},
                'lineage': [],
                'version_histories': {}
            }
            
            for dataset_id in dataset_ids:
                # Get dataset metadata
                metadata = await self.get_dataset_metadata(dataset_id)
                if metadata:
                    export_data['datasets'][dataset_id] = asdict(metadata)
                
                # Get lineage
                lineage = await self.get_data_lineage(dataset_id)
                export_data['lineage'].extend([asdict(l) for l in lineage])
                
                # Get version history
                version_history = await self.get_version_history(dataset_id)
                export_data['version_histories'][dataset_id] = version_history
            
            return export_data
            
        except Exception as e:
            self.logger.error(f"Failed to export metadata: {e}")
            return {}
    
    async def import_metadata(self, export_data: Dict[str, Any]) -> bool:
        """Import metadata from export data"""
        try:
            # Import datasets
            for dataset_id, metadata_dict in export_data.get('datasets', {}).items():
                metadata_dict['created_at'] = datetime.fromisoformat(metadata_dict['created_at'])
                metadata_dict['updated_at'] = datetime.fromisoformat(metadata_dict['updated_at'])
                metadata = DatasetMetadata(**metadata_dict)
                await self.register_dataset(metadata)
            
            # Import lineage
            for lineage_dict in export_data.get('lineage', []):
                lineage_dict['timestamp'] = datetime.fromisoformat(lineage_dict['timestamp'])
                lineage_dict['relationship_type'] = DataLineageType(lineage_dict['relationship_type'])
                lineage = DataLineage(**lineage_dict)
                await self.add_data_lineage(lineage)
            
            # Import version histories
            for dataset_id, version_history in export_data.get('version_histories', {}).items():
                redis = await aioredis.from_url(self.redis_url)
                for version_entry in version_history:
                    await redis.lpush(
                        f"dataset_versions:{dataset_id}",
                        json.dumps(version_entry)
                    )
                await redis.close()
            
            self.logger.info("Successfully imported metadata")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to import metadata: {e}")
            return False
    
    async def cleanup_old_metadata(self, retention_days: int = 90) -> int:
        """Clean up old metadata based on retention policy"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
            deleted_count = 0
            
            redis = await aioredis.from_url(self.redis_url)
            dataset_ids = await redis.smembers("datasets")
            
            for dataset_id in dataset_ids:
                dataset_id_str = dataset_id.decode()
                metadata = await self.get_dataset_metadata(dataset_id_str)
                
                if metadata and metadata.updated_at < cutoff_date:
                    # Delete dataset metadata
                    await redis.delete(f"dataset_metadata:{dataset_id_str}")
                    await redis.srem("datasets", dataset_id_str)
                    await redis.delete(f"dataset_versions:{dataset_id_str}")
                    
                    # Delete lineage information
                    await redis.delete(f"lineage_sources:{dataset_id_str}")
                    await redis.delete(f"lineage_targets:{dataset_id_str}")
                    
                    # Remove from memory cache
                    if dataset_id_str in self.dataset_metadata:
                        del self.dataset_metadata[dataset_id_str]
                    if dataset_id_str in self.version_history:
                        del self.version_history[dataset_id_str]
                    
                    deleted_count += 1
            
            await redis.close()
            
            self.logger.info(f"Cleaned up {deleted_count} old datasets")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old metadata: {e}")
            return 0
