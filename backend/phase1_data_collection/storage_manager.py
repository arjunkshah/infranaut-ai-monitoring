import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
import asyncio
import redis.asyncio as aioredis
import boto3
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
import sqlite3
import psycopg2
from pymongo import MongoClient
import pandas as pd
import numpy as np

class StorageManager:
    """
    Scalable data storage and retrieval for AI model monitoring
    Supports multiple storage backends and data partitioning
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.redis_url = config.get('redis_url', 'redis://localhost:6379')
        self.storage_backends: Dict[str, Any] = {}
        self.partition_strategies: Dict[str, Dict] = {}
        
        # Initialize storage backends
        self._initialize_storage_backends()
    
    def _initialize_storage_backends(self):
        """Initialize connections to storage backends"""
        try:
            # Redis for caching and real-time data
            self.storage_backends['redis'] = None  # Will be initialized on demand
            
            # PostgreSQL for structured data
            if 'postgresql' in self.config:
                self.storage_backends['postgresql'] = psycopg2.connect(
                    host=self.config['postgresql']['host'],
                    port=self.config['postgresql']['port'],
                    database=self.config['postgresql']['database'],
                    user=self.config['postgresql']['user'],
                    password=self.config['postgresql']['password']
                )
            
            # MongoDB for document storage
            if 'mongodb' in self.config:
                self.storage_backends['mongodb'] = MongoClient(
                    self.config['mongodb']['connection_string']
                )[self.config['mongodb']['database']]
            
            # AWS S3 for object storage
            if 's3' in self.config:
                self.storage_backends['s3'] = boto3.client(
                    's3',
                    aws_access_key_id=self.config['s3']['access_key'],
                    aws_secret_access_key=self.config['s3']['secret_key'],
                    region_name=self.config['s3']['region']
                )
            
            # Google Cloud Storage
            if 'gcs' in self.config:
                self.storage_backends['gcs'] = storage.Client.from_service_account_json(
                    self.config['gcs']['service_account_file']
                )
            
            # Azure Blob Storage
            if 'azure' in self.config:
                self.storage_backends['azure'] = BlobServiceClient.from_connection_string(
                    self.config['azure']['connection_string']
                )
            
            self.logger.info("Storage backends initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize storage backends: {e}")
    
    def set_partition_strategy(self, data_type: str, strategy: Dict[str, Any]):
        """Set partitioning strategy for a data type"""
        self.partition_strategies[data_type] = strategy
        self.logger.info(f"Set partition strategy for {data_type}")
    
    async def store_data(self, data: Dict[str, Any], data_type: str, storage_tier: str = 'hot') -> bool:
        """
        Store data in appropriate storage tier
        storage_tier: 'hot' (Redis), 'warm' (PostgreSQL/MongoDB), 'cold' (S3/GCS/Azure)
        """
        try:
            # Add metadata
            data_with_metadata = {
                'data': data,
                'metadata': {
                    'data_type': data_type,
                    'storage_tier': storage_tier,
                    'timestamp': datetime.utcnow().isoformat(),
                    'partition_key': self._get_partition_key(data, data_type)
                }
            }
            
            # Store in appropriate tier
            if storage_tier == 'hot':
                return await self._store_hot_data(data_with_metadata, data_type)
            elif storage_tier == 'warm':
                return await self._store_warm_data(data_with_metadata, data_type)
            elif storage_tier == 'cold':
                return await self._store_cold_data(data_with_metadata, data_type)
            else:
                self.logger.error(f"Unknown storage tier: {storage_tier}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to store data: {e}")
            return False
    
    async def _store_hot_data(self, data_with_metadata: Dict[str, Any], data_type: str) -> bool:
        """Store data in Redis (hot storage)"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store in Redis with TTL
            key = f"hot_data:{data_type}:{data_with_metadata['metadata']['timestamp']}"
            await redis.set(key, json.dumps(data_with_metadata), ex=3600)  # 1 hour TTL
            
            # Add to sorted set for time-based queries
            await redis.zadd(
                f"hot_data_index:{data_type}",
                {key: datetime.utcnow().timestamp()}
            )
            
            await redis.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store hot data: {e}")
            return False
    
    async def _store_warm_data(self, data_with_metadata: Dict[str, Any], data_type: str) -> bool:
        """Store data in PostgreSQL or MongoDB (warm storage)"""
        try:
            # Use PostgreSQL for structured data, MongoDB for document data
            if data_type in ['metrics', 'logs', 'traces']:
                return await self._store_in_postgresql(data_with_metadata, data_type)
            else:
                return await self._store_in_mongodb(data_with_metadata, data_type)
                
        except Exception as e:
            self.logger.error(f"Failed to store warm data: {e}")
            return False
    
    async def _store_in_postgresql(self, data_with_metadata: Dict[str, Any], data_type: str) -> bool:
        """Store data in PostgreSQL"""
        try:
            conn = self.storage_backends['postgresql']
            cursor = conn.cursor()
            
            # Create table if not exists
            table_name = f"{data_type}_data"
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    data JSONB,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    partition_key VARCHAR(255)
                )
            """)
            
            # Insert data
            cursor.execute(f"""
                INSERT INTO {table_name} (data, metadata, partition_key)
                VALUES (%s, %s, %s)
            """, (
                json.dumps(data_with_metadata['data']),
                json.dumps(data_with_metadata['metadata']),
                data_with_metadata['metadata']['partition_key']
            ))
            
            conn.commit()
            cursor.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store in PostgreSQL: {e}")
            return False
    
    async def _store_in_mongodb(self, data_with_metadata: Dict[str, Any], data_type: str) -> bool:
        """Store data in MongoDB"""
        try:
            collection = self.storage_backends['mongodb'][f"{data_type}_data"]
            collection.insert_one(data_with_metadata)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store in MongoDB: {e}")
            return False
    
    async def _store_cold_data(self, data_with_metadata: Dict[str, Any], data_type: str) -> bool:
        """Store data in object storage (cold storage)"""
        try:
            # Choose object storage backend
            if 's3' in self.storage_backends:
                return await self._store_in_s3(data_with_metadata, data_type)
            elif 'gcs' in self.storage_backends:
                return await self._store_in_gcs(data_with_metadata, data_type)
            elif 'azure' in self.storage_backends:
                return await self._store_in_azure(data_with_metadata, data_type)
            else:
                self.logger.error("No object storage backend configured")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to store cold data: {e}")
            return False
    
    async def _store_in_s3(self, data_with_metadata: Dict[str, Any], data_type: str) -> bool:
        """Store data in AWS S3"""
        try:
            s3_client = self.storage_backends['s3']
            bucket = self.config['s3']['bucket']
            
            # Create key with partitioning
            partition_key = data_with_metadata['metadata']['partition_key']
            key = f"cold_data/{data_type}/{partition_key}/{data_with_metadata['metadata']['timestamp']}.json"
            
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data_with_metadata),
                ContentType='application/json'
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store in S3: {e}")
            return False
    
    async def _store_in_gcs(self, data_with_metadata: Dict[str, Any], data_type: str) -> bool:
        """Store data in Google Cloud Storage"""
        try:
            gcs_client = self.storage_backends['gcs']
            bucket_name = self.config['gcs']['bucket']
            bucket = gcs_client.bucket(bucket_name)
            
            # Create blob with partitioning
            partition_key = data_with_metadata['metadata']['partition_key']
            blob_name = f"cold_data/{data_type}/{partition_key}/{data_with_metadata['metadata']['timestamp']}.json"
            blob = bucket.blob(blob_name)
            
            blob.upload_from_string(
                json.dumps(data_with_metadata),
                content_type='application/json'
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store in GCS: {e}")
            return False
    
    async def _store_in_azure(self, data_with_metadata: Dict[str, Any], data_type: str) -> bool:
        """Store data in Azure Blob Storage"""
        try:
            azure_client = self.storage_backends['azure']
            container_name = self.config['azure']['container']
            container_client = azure_client.get_container_client(container_name)
            
            # Create blob with partitioning
            partition_key = data_with_metadata['metadata']['partition_key']
            blob_name = f"cold_data/{data_type}/{partition_key}/{data_with_metadata['metadata']['timestamp']}.json"
            blob_client = container_client.get_blob_client(blob_name)
            
            blob_client.upload_blob(
                json.dumps(data_with_metadata),
                content_type='application/json'
            )
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store in Azure: {e}")
            return False
    
    def _get_partition_key(self, data: Dict[str, Any], data_type: str) -> str:
        """Generate partition key based on strategy"""
        if data_type not in self.partition_strategies:
            return datetime.utcnow().strftime('%Y-%m-%d')
        
        strategy = self.partition_strategies[data_type]
        strategy_type = strategy.get('type', 'date')
        
        if strategy_type == 'date':
            return datetime.utcnow().strftime('%Y-%m-%d')
        elif strategy_type == 'hour':
            return datetime.utcnow().strftime('%Y-%m-%d-%H')
        elif strategy_type == 'field':
            field = strategy.get('field')
            return str(data.get(field, 'unknown'))
        elif strategy_type == 'hash':
            field = strategy.get('field')
            value = str(data.get(field, ''))
            return str(hash(value) % strategy.get('num_partitions', 10))
        else:
            return datetime.utcnow().strftime('%Y-%m-%d')
    
    async def retrieve_data(self, data_type: str, filters: Dict[str, Any], storage_tier: str = 'hot') -> List[Dict[str, Any]]:
        """Retrieve data based on filters and storage tier"""
        try:
            if storage_tier == 'hot':
                return await self._retrieve_hot_data(data_type, filters)
            elif storage_tier == 'warm':
                return await self._retrieve_warm_data(data_type, filters)
            elif storage_tier == 'cold':
                return await self._retrieve_cold_data(data_type, filters)
            else:
                self.logger.error(f"Unknown storage tier: {storage_tier}")
                return []
                
        except Exception as e:
            self.logger.error(f"Failed to retrieve data: {e}")
            return []
    
    async def _retrieve_hot_data(self, data_type: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Get keys from sorted set
            start_time = filters.get('start_time', 0)
            end_time = filters.get('end_time', datetime.utcnow().timestamp())
            
            keys = await redis.zrangebyscore(
                f"hot_data_index:{data_type}",
                start_time,
                end_time
            )
            
            # Retrieve data for keys
            data = []
            for key in keys:
                value = await redis.get(key)
                if value:
                    data.append(json.loads(value))
            
            await redis.close()
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve hot data: {e}")
            return []
    
    async def _retrieve_warm_data(self, data_type: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from PostgreSQL or MongoDB"""
        try:
            if data_type in ['metrics', 'logs', 'traces']:
                return await self._retrieve_from_postgresql(data_type, filters)
            else:
                return await self._retrieve_from_mongodb(data_type, filters)
                
        except Exception as e:
            self.logger.error(f"Failed to retrieve warm data: {e}")
            return []
    
    async def _retrieve_from_postgresql(self, data_type: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from PostgreSQL"""
        try:
            conn = self.storage_backends['postgresql']
            cursor = conn.cursor()
            
            table_name = f"{data_type}_data"
            
            # Build query based on filters
            query = f"SELECT data, metadata FROM {table_name} WHERE 1=1"
            params = []
            
            if 'start_time' in filters:
                query += " AND created_at >= %s"
                params.append(filters['start_time'])
            
            if 'end_time' in filters:
                query += " AND created_at <= %s"
                params.append(filters['end_time'])
            
            if 'partition_key' in filters:
                query += " AND partition_key = %s"
                params.append(filters['partition_key'])
            
            query += " ORDER BY created_at DESC"
            
            if 'limit' in filters:
                query += f" LIMIT {filters['limit']}"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            data = []
            for row in rows:
                data.append({
                    'data': json.loads(row[0]),
                    'metadata': json.loads(row[1])
                })
            
            cursor.close()
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve from PostgreSQL: {e}")
            return []
    
    async def _retrieve_from_mongodb(self, data_type: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from MongoDB"""
        try:
            collection = self.storage_backends['mongodb'][f"{data_type}_data"]
            
            # Build query based on filters
            query = {}
            
            if 'start_time' in filters or 'end_time' in filters:
                query['metadata.timestamp'] = {}
                if 'start_time' in filters:
                    query['metadata.timestamp']['$gte'] = filters['start_time']
                if 'end_time' in filters:
                    query['metadata.timestamp']['$lte'] = filters['end_time']
            
            if 'partition_key' in filters:
                query['metadata.partition_key'] = filters['partition_key']
            
            # Execute query
            cursor = collection.find(query).sort('metadata.timestamp', -1)
            
            if 'limit' in filters:
                cursor = cursor.limit(filters['limit'])
            
            return list(cursor)
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve from MongoDB: {e}")
            return []
    
    async def _retrieve_cold_data(self, data_type: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from object storage"""
        try:
            # Choose object storage backend
            if 's3' in self.storage_backends:
                return await self._retrieve_from_s3(data_type, filters)
            elif 'gcs' in self.storage_backends:
                return await self._retrieve_from_gcs(data_type, filters)
            elif 'azure' in self.storage_backends:
                return await self._retrieve_from_azure(data_type, filters)
            else:
                self.logger.error("No object storage backend configured")
                return []
                
        except Exception as e:
            self.logger.error(f"Failed to retrieve cold data: {e}")
            return []
    
    async def _retrieve_from_s3(self, data_type: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from AWS S3"""
        try:
            s3_client = self.storage_backends['s3']
            bucket = self.config['s3']['bucket']
            
            # List objects based on filters
            prefix = f"cold_data/{data_type}/"
            if 'partition_key' in filters:
                prefix += f"{filters['partition_key']}/"
            
            response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            
            data = []
            for obj in response.get('Contents', []):
                # Download and parse object
                obj_response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                content = obj_response['Body'].read().decode('utf-8')
                data.append(json.loads(content))
            
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve from S3: {e}")
            return []
    
    async def _retrieve_from_gcs(self, data_type: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from Google Cloud Storage"""
        try:
            gcs_client = self.storage_backends['gcs']
            bucket_name = self.config['gcs']['bucket']
            bucket = gcs_client.bucket(bucket_name)
            
            # List blobs based on filters
            prefix = f"cold_data/{data_type}/"
            if 'partition_key' in filters:
                prefix += f"{filters['partition_key']}/"
            
            blobs = bucket.list_blobs(prefix=prefix)
            
            data = []
            for blob in blobs:
                # Download and parse blob
                content = blob.download_as_text()
                data.append(json.loads(content))
            
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve from GCS: {e}")
            return []
    
    async def _retrieve_from_azure(self, data_type: str, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Retrieve data from Azure Blob Storage"""
        try:
            azure_client = self.storage_backends['azure']
            container_name = self.config['azure']['container']
            container_client = azure_client.get_container_client(container_name)
            
            # List blobs based on filters
            prefix = f"cold_data/{data_type}/"
            if 'partition_key' in filters:
                prefix += f"{filters['partition_key']}/"
            
            blobs = container_client.list_blobs(name_starts_with=prefix)
            
            data = []
            for blob in blobs:
                # Download and parse blob
                blob_client = container_client.get_blob_client(blob.name)
                content = blob_client.download_blob().readall().decode('utf-8')
                data.append(json.loads(content))
            
            return data
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve from Azure: {e}")
            return []
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics across all backends"""
        stats = {
            'hot_storage': {},
            'warm_storage': {},
            'cold_storage': {},
            'total_records': 0
        }
        
        try:
            # Redis stats
            redis = await aioredis.from_url(self.redis_url)
            info = await redis.info()
            stats['hot_storage'] = {
                'used_memory': info.get('used_memory', 0),
                'connected_clients': info.get('connected_clients', 0),
                'total_commands_processed': info.get('total_commands_processed', 0)
            }
            await redis.close()
            
            # PostgreSQL stats
            if 'postgresql' in self.storage_backends:
                conn = self.storage_backends['postgresql']
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
                table_count = cursor.fetchone()[0]
                stats['warm_storage']['postgresql'] = {
                    'table_count': table_count,
                    'connection_status': 'connected'
                }
                cursor.close()
            
            # MongoDB stats
            if 'mongodb' in self.storage_backends:
                db = self.storage_backends['mongodb']
                stats['warm_storage']['mongodb'] = {
                    'collection_count': len(db.list_collection_names()),
                    'connection_status': 'connected'
                }
            
            # Object storage stats
            if 's3' in self.storage_backends:
                s3_client = self.storage_backends['s3']
                bucket = self.config['s3']['bucket']
                response = s3_client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                stats['cold_storage']['s3'] = {
                    'bucket': bucket,
                    'has_objects': 'Contents' in response
                }
            
        except Exception as e:
            self.logger.error(f"Failed to get storage stats: {e}")
        
        return stats
    
    async def cleanup_old_data(self, data_type: str, retention_days: int) -> int:
        """Clean up old data based on retention policy"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
            cutoff_timestamp = cutoff_date.timestamp()
            
            deleted_count = 0
            
            # Clean up hot storage (Redis)
            redis = await aioredis.from_url(self.redis_url)
            keys_to_delete = await redis.zrangebyscore(
                f"hot_data_index:{data_type}",
                0,
                cutoff_timestamp
            )
            
            if keys_to_delete:
                await redis.delete(*keys_to_delete)
                await redis.zremrangebyscore(
                    f"hot_data_index:{data_type}",
                    0,
                    cutoff_timestamp
                )
                deleted_count += len(keys_to_delete)
            
            await redis.close()
            
            # Clean up warm storage
            if 'postgresql' in self.storage_backends:
                conn = self.storage_backends['postgresql']
                cursor = conn.cursor()
                cursor.execute(
                    f"DELETE FROM {data_type}_data WHERE created_at < %s",
                    (cutoff_date,)
                )
                deleted_count += cursor.rowcount
                conn.commit()
                cursor.close()
            
            if 'mongodb' in self.storage_backends:
                collection = self.storage_backends['mongodb'][f"{data_type}_data"]
                result = collection.delete_many({
                    'metadata.timestamp': {'$lt': cutoff_date.isoformat()}
                })
                deleted_count += result.deleted_count
            
            self.logger.info(f"Cleaned up {deleted_count} old records for {data_type}")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old data: {e}")
            return 0
