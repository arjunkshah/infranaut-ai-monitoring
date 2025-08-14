import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import redis.asyncio as aioredis
import boto3
from google.cloud import storage
from azure.storage.blob import BlobServiceClient
import schedule
import time
import threading

@dataclass
class RetentionPolicy:
    """Configuration for data retention policy"""
    policy_id: str
    source_id: str
    hot_storage_days: int = 7  # Days to keep in Redis/hot storage
    warm_storage_days: int = 30  # Days to keep in PostgreSQL/warm storage
    cold_storage_days: int = 365  # Days to keep in S3/cold storage
    archival_enabled: bool = True
    cleanup_enabled: bool = True
    compression_enabled: bool = True

@dataclass
class StorageConfig:
    """Configuration for different storage tiers"""
    hot_storage: Dict[str, Any]  # Redis config
    warm_storage: Dict[str, Any]  # PostgreSQL config
    cold_storage: Dict[str, Any]  # S3/GCS/Azure config

class DataRetentionManager:
    """
    Manages data lifecycle across different storage tiers
    Implements automated retention policies and archival
    """
    
    def __init__(self, storage_config: StorageConfig, redis_url: str = "redis://localhost:6379"):
        self.storage_config = storage_config
        self.redis_url = redis_url
        self.policies: Dict[str, RetentionPolicy] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Initialize storage clients
        self._init_storage_clients()
        
    def _init_storage_clients(self):
        """Initialize connections to different storage tiers"""
        try:
            # Cold storage clients
            if 'aws' in self.storage_config.cold_storage:
                self.s3_client = boto3.client('s3')
            if 'gcp' in self.storage_config.cold_storage:
                self.gcs_client = storage.Client()
            if 'azure' in self.storage_config.cold_storage:
                self.azure_client = BlobServiceClient.from_connection_string(
                    self.storage_config.cold_storage['azure']['connection_string']
                )
                
            self.logger.info("Storage clients initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize storage clients: {e}")
    
    async def add_retention_policy(self, policy: RetentionPolicy) -> bool:
        """Add a new retention policy"""
        try:
            self.policies[policy.policy_id] = policy
            self.logger.info(f"Added retention policy: {policy.policy_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add retention policy {policy.policy_id}: {e}")
            return False
    
    async def start_retention_manager(self):
        """Start the retention management service"""
        self.running = True
        self.logger.info("Starting data retention manager")
        
        # Start background tasks
        tasks = [
            asyncio.create_task(self._archival_scheduler()),
            asyncio.create_task(self._cleanup_scheduler()),
            asyncio.create_task(self._policy_monitor())
        ]
        
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _archival_scheduler(self):
        """Schedule and execute archival tasks"""
        while self.running:
            try:
                for policy_id, policy in self.policies.items():
                    if policy.archival_enabled:
                        await self._archive_old_data(policy)
                
                # Run archival every hour
                await asyncio.sleep(3600)
                
            except Exception as e:
                self.logger.error(f"Error in archival scheduler: {e}")
                await asyncio.sleep(3600)
    
    async def _cleanup_scheduler(self):
        """Schedule and execute cleanup tasks"""
        while self.running:
            try:
                for policy_id, policy in self.policies.items():
                    if policy.cleanup_enabled:
                        await self._cleanup_expired_data(policy)
                
                # Run cleanup every 6 hours
                await asyncio.sleep(21600)
                
            except Exception as e:
                self.logger.error(f"Error in cleanup scheduler: {e}")
                await asyncio.sleep(21600)
    
    async def _policy_monitor(self):
        """Monitor retention policies and generate reports"""
        while self.running:
            try:
                await self._generate_retention_report()
                
                # Generate report daily
                await asyncio.sleep(86400)
                
            except Exception as e:
                self.logger.error(f"Error in policy monitor: {e}")
                await asyncio.sleep(86400)
    
    async def _archive_old_data(self, policy: RetentionPolicy):
        """Archive data from hot storage to warm/cold storage"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Get data older than hot storage retention period
            cutoff_time = datetime.utcnow() - timedelta(days=policy.hot_storage_days)
            
            # Get all data from Redis
            data_queue = await redis.lrange(f"data_queue:{policy.source_id}", 0, -1)
            
            archived_count = 0
            for data_str in data_queue:
                data = json.loads(data_str)
                data_timestamp = datetime.fromisoformat(data['metadata']['timestamp'])
                
                if data_timestamp < cutoff_time:
                    # Archive to warm storage (PostgreSQL)
                    await self._archive_to_warm_storage(policy, data)
                    
                    # Archive to cold storage if enabled
                    if policy.archival_enabled:
                        await self._archive_to_cold_storage(policy, data)
                    
                    # Remove from hot storage
                    await redis.lrem(f"data_queue:{policy.source_id}", 1, data_str)
                    archived_count += 1
            
            await redis.close()
            
            if archived_count > 0:
                self.logger.info(f"Archived {archived_count} records for policy {policy.policy_id}")
                
        except Exception as e:
            self.logger.error(f"Error archiving data for policy {policy.policy_id}: {e}")
    
    async def _archive_to_warm_storage(self, policy: RetentionPolicy, data: Dict[str, Any]):
        """Archive data to warm storage (PostgreSQL)"""
        try:
            # This would typically use an async PostgreSQL client
            # For now, we'll log the archival
            self.logger.debug(f"Archiving to warm storage: {data['metadata']['batch_id']}")
            
            # TODO: Implement PostgreSQL archival
            # await self.postgres_client.execute(
            #     "INSERT INTO archived_data (source_id, data, metadata, archived_at) VALUES (%s, %s, %s, %s)",
            #     policy.source_id, json.dumps(data['data']), json.dumps(data['metadata']), datetime.utcnow()
            # )
            
        except Exception as e:
            self.logger.error(f"Error archiving to warm storage: {e}")
    
    async def _archive_to_cold_storage(self, policy: RetentionPolicy, data: Dict[str, Any]):
        """Archive data to cold storage (S3/GCS/Azure)"""
        try:
            timestamp = datetime.utcnow()
            year_month = timestamp.strftime("%Y/%m")
            filename = f"{policy.source_id}/{year_month}/{data['metadata']['batch_id']}.json"
            
            # Compress data if enabled
            data_to_store = json.dumps(data)
            if policy.compression_enabled:
                import gzip
                data_to_store = gzip.compress(data_to_store.encode('utf-8'))
                filename += '.gz'
            
            # Upload to appropriate cloud storage
            if 'aws' in self.storage_config.cold_storage:
                await self._upload_to_s3(filename, data_to_store)
            elif 'gcp' in self.storage_config.cold_storage:
                await self._upload_to_gcs(filename, data_to_store)
            elif 'azure' in self.storage_config.cold_storage:
                await self._upload_to_azure(filename, data_to_store)
                
        except Exception as e:
            self.logger.error(f"Error archiving to cold storage: {e}")
    
    async def _upload_to_s3(self, filename: str, data: bytes):
        """Upload data to AWS S3"""
        bucket = self.storage_config.cold_storage['aws']['bucket']
        self.s3_client.put_object(
            Bucket=bucket,
            Key=filename,
            Body=data,
            ContentType='application/json'
        )
    
    async def _upload_to_gcs(self, filename: str, data: bytes):
        """Upload data to Google Cloud Storage"""
        bucket_name = self.storage_config.cold_storage['gcp']['bucket']
        bucket = self.gcs_client.bucket(bucket_name)
        blob = bucket.blob(filename)
        blob.upload_from_string(data, content_type='application/json')
    
    async def _upload_to_azure(self, filename: str, data: bytes):
        """Upload data to Azure Blob Storage"""
        container_name = self.storage_config.cold_storage['azure']['container']
        container_client = self.azure_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(filename)
        blob_client.upload_blob(data, overwrite=True)
    
    async def _cleanup_expired_data(self, policy: RetentionPolicy):
        """Clean up data that has exceeded retention periods"""
        try:
            # Clean up warm storage (data older than warm_storage_days)
            warm_cutoff = datetime.utcnow() - timedelta(days=policy.warm_storage_days)
            await self._cleanup_warm_storage(policy, warm_cutoff)
            
            # Clean up cold storage (data older than cold_storage_days)
            cold_cutoff = datetime.utcnow() - timedelta(days=policy.cold_storage_days)
            await self._cleanup_cold_storage(policy, cold_cutoff)
            
        except Exception as e:
            self.logger.error(f"Error cleaning up expired data for policy {policy.policy_id}: {e}")
    
    async def _cleanup_warm_storage(self, policy: RetentionPolicy, cutoff_time: datetime):
        """Clean up expired data from warm storage"""
        try:
            # TODO: Implement PostgreSQL cleanup
            # await self.postgres_client.execute(
            #     "DELETE FROM archived_data WHERE source_id = %s AND archived_at < %s",
            #     policy.source_id, cutoff_time
            # )
            self.logger.debug(f"Cleaned up warm storage for policy {policy.policy_id}")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up warm storage: {e}")
    
    async def _cleanup_cold_storage(self, policy: RetentionPolicy, cutoff_time: datetime):
        """Clean up expired data from cold storage"""
        try:
            # Delete files older than cutoff time
            if 'aws' in self.storage_config.cold_storage:
                await self._cleanup_s3(policy, cutoff_time)
            elif 'gcp' in self.storage_config.cold_storage:
                await self._cleanup_gcs(policy, cutoff_time)
            elif 'azure' in self.storage_config.cold_storage:
                await self._cleanup_azure(policy, cutoff_time)
                
        except Exception as e:
            self.logger.error(f"Error cleaning up cold storage: {e}")
    
    async def _cleanup_s3(self, policy: RetentionPolicy, cutoff_time: datetime):
        """Clean up expired data from S3"""
        bucket = self.storage_config.cold_storage['aws']['bucket']
        prefix = f"{policy.source_id}/"
        
        response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get('Contents', []):
            if obj['LastModified'].replace(tzinfo=None) < cutoff_time:
                self.s3_client.delete_object(Bucket=bucket, Key=obj['Key'])
    
    async def _cleanup_gcs(self, policy: RetentionPolicy, cutoff_time: datetime):
        """Clean up expired data from GCS"""
        bucket_name = self.storage_config.cold_storage['gcp']['bucket']
        bucket = self.gcs_client.bucket(bucket_name)
        prefix = f"{policy.source_id}/"
        
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            if blob.time_created.replace(tzinfo=None) < cutoff_time:
                blob.delete()
    
    async def _cleanup_azure(self, policy: RetentionPolicy, cutoff_time: datetime):
        """Clean up expired data from Azure"""
        container_name = self.storage_config.cold_storage['azure']['container']
        container_client = self.azure_client.get_container_client(container_name)
        prefix = f"{policy.source_id}/"
        
        blobs = container_client.list_blobs(name_starts_with=prefix)
        for blob in blobs:
            if blob.last_modified.replace(tzinfo=None) < cutoff_time:
                container_client.delete_blob(blob.name)
    
    async def _generate_retention_report(self):
        """Generate retention policy compliance report"""
        try:
            report = {
                'timestamp': datetime.utcnow().isoformat(),
                'policies': {}
            }
            
            for policy_id, policy in self.policies.items():
                policy_report = await self._get_policy_stats(policy)
                report['policies'][policy_id] = policy_report
            
            # Store report in Redis
            redis = await aioredis.from_url(self.redis_url)
            await redis.set('retention_report', json.dumps(report), ex=86400)  # 24 hours
            await redis.close()
            
            self.logger.info("Generated retention policy compliance report")
            
        except Exception as e:
            self.logger.error(f"Error generating retention report: {e}")
    
    async def _get_policy_stats(self, policy: RetentionPolicy) -> Dict[str, Any]:
        """Get statistics for a retention policy"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Get current queue size
            queue_size = await redis.llen(f"data_queue:{policy.source_id}")
            
            # Get data age distribution
            data_queue = await redis.lrange(f"data_queue:{policy.source_id}", 0, -1)
            age_distribution = {'hot': 0, 'warm': 0, 'cold': 0}
            
            now = datetime.utcnow()
            for data_str in data_queue:
                data = json.loads(data_str)
                data_timestamp = datetime.fromisoformat(data['metadata']['timestamp'])
                age_days = (now - data_timestamp).days
                
                if age_days <= policy.hot_storage_days:
                    age_distribution['hot'] += 1
                elif age_days <= policy.warm_storage_days:
                    age_distribution['warm'] += 1
                else:
                    age_distribution['cold'] += 1
            
            await redis.close()
            
            return {
                'queue_size': queue_size,
                'age_distribution': age_distribution,
                'policy_config': {
                    'hot_storage_days': policy.hot_storage_days,
                    'warm_storage_days': policy.warm_storage_days,
                    'cold_storage_days': policy.cold_storage_days,
                    'archival_enabled': policy.archival_enabled,
                    'cleanup_enabled': policy.cleanup_enabled
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting policy stats: {e}")
            return {}
    
    async def get_retention_report(self) -> Dict[str, Any]:
        """Get the latest retention report"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            report_str = await redis.get('retention_report')
            await redis.close()
            
            if report_str:
                return json.loads(report_str)
            else:
                return {'error': 'No retention report available'}
                
        except Exception as e:
            self.logger.error(f"Error getting retention report: {e}")
            return {'error': str(e)}
    
    async def stop_retention_manager(self):
        """Stop the retention management service"""
        self.running = False
        self.logger.info("Stopping data retention manager")
