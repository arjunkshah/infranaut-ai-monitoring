import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import aiohttp
import redis.asyncio as aioredis
from kafka import KafkaProducer, KafkaConsumer
import boto3
from google.cloud import pubsub_v1
import azure.eventhub

@dataclass
class DataSource:
    """Configuration for a data source"""
    source_id: str
    source_type: str  # 'api', 'database', 'kafka', 'pubsub', 's3', 'eventhub'
    connection_config: Dict[str, Any]
    data_format: str  # 'json', 'csv', 'avro', 'parquet'
    batch_size: int = 1000
    poll_interval: int = 5  # seconds

class DataIngestionPipeline:
    """
    Real-time data ingestion pipeline for AI model monitoring
    Supports multiple data sources and formats
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.sources: Dict[str, DataSource] = {}
        self.producers: Dict[str, Any] = {}
        self.consumers: Dict[str, Any] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
    async def add_data_source(self, source: DataSource) -> bool:
        """Add a new data source to the pipeline"""
        try:
            self.sources[source.source_id] = source
            await self._initialize_source(source)
            self.logger.info(f"Added data source: {source.source_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add data source {source.source_id}: {e}")
            return False
    
    async def _initialize_source(self, source: DataSource):
        """Initialize connection to a data source"""
        if source.source_type == 'kafka':
            self.producers[source.source_id] = KafkaProducer(
                bootstrap_servers=source.connection_config['bootstrap_servers'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
        elif source.source_type == 'pubsub':
            self.producers[source.source_id] = pubsub_v1.PublisherClient()
        elif source.source_type == 'eventhub':
            self.producers[source.source_id] = azure.eventhub.EventHubProducerClient.from_connection_string(
                source.connection_config['connection_string']
            )
        elif source.source_type == 's3':
            self.producers[source.source_id] = boto3.client('s3')
    
    async def start_ingestion(self):
        """Start the data ingestion pipeline"""
        self.running = True
        self.logger.info("Starting data ingestion pipeline")
        
        # Start ingestion tasks for each source
        tasks = []
        for source_id, source in self.sources.items():
            task = asyncio.create_task(self._ingest_from_source(source))
            tasks.append(task)
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
    
    async def _ingest_from_source(self, source: DataSource):
        """Ingest data from a specific source"""
        self.logger.info(f"Starting ingestion from {source.source_id}")
        
        while self.running:
            try:
                if source.source_type == 'api':
                    await self._ingest_from_api(source)
                elif source.source_type == 'database':
                    await self._ingest_from_database(source)
                elif source.source_type == 'kafka':
                    await self._ingest_from_kafka(source)
                elif source.source_type == 'pubsub':
                    await self._ingest_from_pubsub(source)
                elif source.source_type == 's3':
                    await self._ingest_from_s3(source)
                elif source.source_type == 'eventhub':
                    await self._ingest_from_eventhub(source)
                
                await asyncio.sleep(source.poll_interval)
                
            except Exception as e:
                self.logger.error(f"Error ingesting from {source.source_id}: {e}")
                await asyncio.sleep(source.poll_interval * 2)  # Back off on error
    
    async def _ingest_from_api(self, source: DataSource):
        """Ingest data from REST API"""
        async with aiohttp.ClientSession() as session:
            async with session.get(source.connection_config['url']) as response:
                if response.status == 200:
                    data = await response.json()
                    await self._process_batch(source.source_id, data)
    
    async def _ingest_from_database(self, source: DataSource):
        """Ingest data from database"""
        # Implementation depends on database type (PostgreSQL, MySQL, etc.)
        # This is a placeholder for database-specific logic
        pass
    
    async def _ingest_from_kafka(self, source: DataSource):
        """Ingest data from Kafka topic"""
        consumer = KafkaConsumer(
            source.connection_config['topic'],
            bootstrap_servers=source.connection_config['bootstrap_servers'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        batch = []
        for message in consumer:
            if not self.running:
                break
                
            batch.append(message.value)
            if len(batch) >= source.batch_size:
                await self._process_batch(source.source_id, batch)
                batch = []
    
    async def _ingest_from_pubsub(self, source: DataSource):
        """Ingest data from Google Cloud Pub/Sub"""
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(
            source.connection_config['project_id'],
            source.connection_config['subscription_id']
        )
        
        def callback(message):
            data = json.loads(message.data.decode('utf-8'))
            asyncio.create_task(self._process_batch(source.source_id, [data]))
            message.ack()
        
        subscriber.subscribe(subscription_path, callback=callback)
    
    async def _ingest_from_s3(self, source: DataSource):
        """Ingest data from AWS S3"""
        s3_client = self.producers[source.source_id]
        bucket = source.connection_config['bucket']
        prefix = source.connection_config.get('prefix', '')
        
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        for obj in response.get('Contents', []):
            if not self.running:
                break
                
            file_data = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
            # Process file based on format (JSON, CSV, etc.)
            await self._process_batch(source.source_id, [file_data])
    
    async def _ingest_from_eventhub(self, source: DataSource):
        """Ingest data from Azure Event Hub"""
        consumer = azure.eventhub.EventHubConsumerClient.from_connection_string(
            source.connection_config['connection_string'],
            consumer_group=source.connection_config['consumer_group']
        )
        
        async with consumer:
            async for event_data in consumer.receive():
                if not self.running:
                    break
                    
                data = json.loads(event_data.body_as_str())
                await self._process_batch(source.source_id, [data])
    
    async def _process_batch(self, source_id: str, data_batch: List[Dict[str, Any]]):
        """Process a batch of data from a source"""
        try:
            # Add metadata
            processed_batch = []
            for data in data_batch:
                processed_data = {
                    'data': data,
                    'metadata': {
                        'source_id': source_id,
                        'timestamp': datetime.utcnow().isoformat(),
                        'batch_id': f"{source_id}_{datetime.utcnow().timestamp()}"
                    }
                }
                processed_batch.append(processed_data)
            
            # Store in Redis for immediate access
            redis = await aioredis.from_url(self.redis_url)
            for processed_data in processed_batch:
                await redis.lpush(
                    f"data_queue:{source_id}",
                    json.dumps(processed_data)
                )
            
            # Log batch processing
            self.logger.info(f"Processed batch of {len(processed_batch)} records from {source_id}")
            
        except Exception as e:
            self.logger.error(f"Error processing batch from {source_id}: {e}")
    
    async def stop_ingestion(self):
        """Stop the data ingestion pipeline"""
        self.running = False
        self.logger.info("Stopping data ingestion pipeline")
        
        # Close all connections
        for producer in self.producers.values():
            if hasattr(producer, 'close'):
                producer.close()
        
        # Close Redis connection
        redis = await aioredis.from_url(self.redis_url)
        await redis.close()
    
    async def get_data_queue_size(self, source_id: str) -> int:
        """Get the current size of the data queue for a source"""
        redis = await aioredis.from_url(self.redis_url)
        size = await redis.llen(f"data_queue:{source_id}")
        await redis.close()
        return size
    
    async def get_recent_data(self, source_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent data from a specific source"""
        redis = await aioredis.from_url(self.redis_url)
        data = await redis.lrange(f"data_queue:{source_id}", 0, limit - 1)
        await redis.close()
        
        return [json.loads(item) for item in data]
