import asyncio
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
import websockets
import aiohttp
import redis.asyncio as aioredis
from kafka import KafkaProducer, KafkaConsumer
import asyncio_mqtt as mqtt
from google.cloud import pubsub_v1
import azure.eventhub
import boto3
from collections import deque
import time

@dataclass
class StreamingConfig:
    """Configuration for real-time streaming"""
    stream_id: str
    source_type: str  # 'websocket', 'kafka', 'pubsub', 'eventhub', 'mqtt'
    connection_config: Dict[str, Any]
    processing_config: Dict[str, Any]
    window_size: int = 1000  # Number of events in sliding window
    window_duration: int = 60  # Window duration in seconds
    batch_size: int = 100
    enable_analytics: bool = True

@dataclass
class StreamEvent:
    """Represents a streaming event"""
    event_id: str
    timestamp: datetime
    source: str
    data: Dict[str, Any]
    metadata: Dict[str, Any]

class RealTimeStreamingPipeline:
    """
    Real-time streaming pipeline with advanced processing capabilities
    Supports multiple streaming sources and real-time analytics
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.streams: Dict[str, StreamingConfig] = {}
        self.event_queues: Dict[str, deque] = {}
        self.processing_tasks: Dict[str, asyncio.Task] = {}
        self.analytics_tasks: Dict[str, asyncio.Task] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Real-time metrics
        self.metrics = {
            'total_events': 0,
            'events_per_second': 0,
            'processing_latency_ms': 0,
            'error_rate': 0
        }
        
    async def add_stream(self, config: StreamingConfig) -> bool:
        """Add a new streaming source"""
        try:
            self.streams[config.stream_id] = config
            self.event_queues[config.stream_id] = deque(maxlen=config.window_size)
            
            # Start processing task
            self.processing_tasks[config.stream_id] = asyncio.create_task(
                self._process_stream(config)
            )
            
            # Start analytics task if enabled
            if config.enable_analytics:
                self.analytics_tasks[config.stream_id] = asyncio.create_task(
                    self._stream_analytics(config)
                )
            
            self.logger.info(f"Added streaming source: {config.stream_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add streaming source {config.stream_id}: {e}")
            return False
    
    async def _process_stream(self, config: StreamingConfig):
        """Process events from a streaming source"""
        try:
            if config.source_type == 'websocket':
                await self._process_websocket_stream(config)
            elif config.source_type == 'kafka':
                await self._process_kafka_stream(config)
            elif config.source_type == 'pubsub':
                await self._process_pubsub_stream(config)
            elif config.source_type == 'eventhub':
                await self._process_eventhub_stream(config)
            elif config.source_type == 'mqtt':
                await self._process_mqtt_stream(config)
                
        except Exception as e:
            self.logger.error(f"Error processing stream {config.stream_id}: {e}")
    
    async def _process_websocket_stream(self, config: StreamingConfig):
        """Process WebSocket stream"""
        uri = config.connection_config['uri']
        
        while self.running:
            try:
                async with websockets.connect(uri) as websocket:
                    self.logger.info(f"Connected to WebSocket: {uri}")
                    
                    async for message in websocket:
                        if not self.running:
                            break
                        
                        # Parse message
                        try:
                            data = json.loads(message)
                            event = StreamEvent(
                                event_id=f"ws_{int(time.time() * 1000)}",
                                timestamp=datetime.utcnow(),
                                source=config.stream_id,
                                data=data,
                                metadata={'source_type': 'websocket', 'uri': uri}
                            )
                            
                            await self._handle_stream_event(config, event)
                            
                        except json.JSONDecodeError as e:
                            self.logger.error(f"Failed to parse WebSocket message: {e}")
                            
            except Exception as e:
                self.logger.error(f"WebSocket connection error: {e}")
                await asyncio.sleep(5)  # Reconnect delay
    
    async def _process_kafka_stream(self, config: StreamingConfig):
        """Process Kafka stream"""
        consumer = KafkaConsumer(
            config.connection_config['topic'],
            bootstrap_servers=config.connection_config['bootstrap_servers'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            enable_auto_commit=True
        )
        
        try:
            for message in consumer:
                if not self.running:
                    break
                
                event = StreamEvent(
                    event_id=f"kafka_{message.offset}",
                    timestamp=datetime.utcnow(),
                    source=config.stream_id,
                    data=message.value,
                    metadata={
                        'source_type': 'kafka',
                        'topic': message.topic,
                        'partition': message.partition,
                        'offset': message.offset
                    }
                )
                
                await self._handle_stream_event(config, event)
                
        except Exception as e:
            self.logger.error(f"Kafka stream error: {e}")
        finally:
            consumer.close()
    
    async def _process_pubsub_stream(self, config: StreamingConfig):
        """Process Google Cloud Pub/Sub stream"""
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(
            config.connection_config['project_id'],
            config.connection_config['subscription_id']
        )
        
        def callback(message):
            try:
                data = json.loads(message.data.decode('utf-8'))
                event = StreamEvent(
                    event_id=f"pubsub_{message.message_id}",
                    timestamp=datetime.utcnow(),
                    source=config.stream_id,
                    data=data,
                    metadata={
                        'source_type': 'pubsub',
                        'message_id': message.message_id,
                        'publish_time': message.publish_time.isoformat()
                    }
                )
                
                asyncio.create_task(self._handle_stream_event(config, event))
                message.ack()
                
            except Exception as e:
                self.logger.error(f"Pub/Sub message processing error: {e}")
                message.nack()
        
        try:
            subscription = subscriber.subscribe(subscription_path, callback=callback)
            await subscription.wait_for_exit()
        except Exception as e:
            self.logger.error(f"Pub/Sub stream error: {e}")
    
    async def _process_eventhub_stream(self, config: StreamingConfig):
        """Process Azure Event Hub stream"""
        consumer = azure.eventhub.EventHubConsumerClient.from_connection_string(
            config.connection_config['connection_string'],
            consumer_group=config.connection_config['consumer_group']
        )
        
        try:
            async with consumer:
                async for event_data in consumer.receive():
                    if not self.running:
                        break
                    
                    try:
                        data = json.loads(event_data.body_as_str())
                        event = StreamEvent(
                            event_id=f"eventhub_{event_data.sequence_number}",
                            timestamp=datetime.utcnow(),
                            source=config.stream_id,
                            data=data,
                            metadata={
                                'source_type': 'eventhub',
                                'sequence_number': event_data.sequence_number,
                                'offset': event_data.offset
                            }
                        )
                        
                        await self._handle_stream_event(config, event)
                        
                    except Exception as e:
                        self.logger.error(f"Event Hub message processing error: {e}")
                        
        except Exception as e:
            self.logger.error(f"Event Hub stream error: {e}")
    
    async def _process_mqtt_stream(self, config: StreamingConfig):
        """Process MQTT stream"""
        client = mqtt.Client(config.connection_config['hostname'])
        
        try:
            async with client:
                await client.subscribe(config.connection_config['topic'])
                
                async for message in client.messages:
                    if not self.running:
                        break
                    
                    try:
                        data = json.loads(message.payload.decode())
                        event = StreamEvent(
                            event_id=f"mqtt_{int(time.time() * 1000)}",
                            timestamp=datetime.utcnow(),
                            source=config.stream_id,
                            data=data,
                            metadata={
                                'source_type': 'mqtt',
                                'topic': message.topic,
                                'qos': message.qos
                            }
                        )
                        
                        await self._handle_stream_event(config, event)
                        
                    except Exception as e:
                        self.logger.error(f"MQTT message processing error: {e}")
                        
        except Exception as e:
            self.logger.error(f"MQTT stream error: {e}")
    
    async def _handle_stream_event(self, config: StreamingConfig, event: StreamEvent):
        """Handle a streaming event"""
        start_time = time.time()
        
        try:
            # Add to event queue
            self.event_queues[config.stream_id].append(event)
            
            # Update metrics
            self.metrics['total_events'] += 1
            
            # Process in batches
            if len(self.event_queues[config.stream_id]) >= config.batch_size:
                await self._process_batch(config)
            
            # Store in Redis for real-time access
            redis = await aioredis.from_url(self.redis_url)
            await redis.lpush(
                f"stream_queue:{config.stream_id}",
                json.dumps({
                    'event_id': event.event_id,
                    'timestamp': event.timestamp.isoformat(),
                    'source': event.source,
                    'data': event.data,
                    'metadata': event.metadata
                })
            )
            
            # Publish to real-time channels
            await redis.publish(
                f"stream:events:{config.stream_id}",
                json.dumps({
                    'event_id': event.event_id,
                    'timestamp': event.timestamp.isoformat(),
                    'data': event.data
                })
            )
            
            await redis.close()
            
            # Calculate processing latency
            processing_time = (time.time() - start_time) * 1000
            self.metrics['processing_latency_ms'] = processing_time
            
            self.logger.debug(f"Processed event {event.event_id} in {processing_time:.2f}ms")
            
        except Exception as e:
            self.logger.error(f"Error handling stream event: {e}")
            self.metrics['error_rate'] += 1
    
    async def _process_batch(self, config: StreamingConfig):
        """Process a batch of events"""
        try:
            batch = list(self.event_queues[config.stream_id])
            self.event_queues[config.stream_id].clear()
            
            # Apply processing configuration
            processed_batch = await self._apply_processing_config(config, batch)
            
            # Store processed batch
            redis = await aioredis.from_url(self.redis_url)
            await redis.lpush(
                f"processed_batch:{config.stream_id}",
                json.dumps({
                    'batch_id': f"batch_{int(time.time() * 1000)}",
                    'timestamp': datetime.utcnow().isoformat(),
                    'events': [{
                        'event_id': event.event_id,
                        'data': event.data,
                        'metadata': event.metadata
                    } for event in processed_batch]
                })
            )
            await redis.close()
            
            self.logger.info(f"Processed batch of {len(processed_batch)} events for {config.stream_id}")
            
        except Exception as e:
            self.logger.error(f"Error processing batch: {e}")
    
    async def _apply_processing_config(self, config: StreamingConfig, events: List[StreamEvent]) -> List[StreamEvent]:
        """Apply processing configuration to events"""
        processed_events = events
        
        # Apply filters
        if 'filters' in config.processing_config:
            for filter_config in config.processing_config['filters']:
                processed_events = await self._apply_filter(processed_events, filter_config)
        
        # Apply transformations
        if 'transformations' in config.processing_config:
            for transform_config in config.processing_config['transformations']:
                processed_events = await self._apply_transformation(processed_events, transform_config)
        
        # Apply aggregations
        if 'aggregations' in config.processing_config:
            for agg_config in config.processing_config['aggregations']:
                processed_events = await self._apply_aggregation(processed_events, agg_config)
        
        return processed_events
    
    async def _apply_filter(self, events: List[StreamEvent], filter_config: Dict[str, Any]) -> List[StreamEvent]:
        """Apply filter to events"""
        filtered_events = []
        
        for event in events:
            if self._evaluate_filter_condition(event, filter_config):
                filtered_events.append(event)
        
        return filtered_events
    
    def _evaluate_filter_condition(self, event: StreamEvent, filter_config: Dict[str, Any]) -> bool:
        """Evaluate filter condition for an event"""
        field = filter_config.get('field')
        operator = filter_config.get('operator')
        value = filter_config.get('value')
        
        if field not in event.data:
            return False
        
        event_value = event.data[field]
        
        if operator == 'equals':
            return event_value == value
        elif operator == 'not_equals':
            return event_value != value
        elif operator == 'greater_than':
            return event_value > value
        elif operator == 'less_than':
            return event_value < value
        elif operator == 'contains':
            return value in str(event_value)
        elif operator == 'regex':
            import re
            return bool(re.search(value, str(event_value)))
        
        return True
    
    async def _apply_transformation(self, events: List[StreamEvent], transform_config: Dict[str, Any]) -> List[StreamEvent]:
        """Apply transformation to events"""
        transform_type = transform_config.get('type')
        
        for event in events:
            if transform_type == 'field_mapping':
                event.data = self._map_fields(event.data, transform_config.get('mapping', {}))
            elif transform_type == 'field_renaming':
                event.data = self._rename_fields(event.data, transform_config.get('renaming', {}))
            elif transform_type == 'field_removal':
                event.data = self._remove_fields(event.data, transform_config.get('fields', []))
            elif transform_type == 'custom_function':
                event.data = await self._apply_custom_function(event.data, transform_config.get('function'))
        
        return events
    
    def _map_fields(self, data: Dict[str, Any], mapping: Dict[str, str]) -> Dict[str, Any]:
        """Map fields in data"""
        mapped_data = {}
        for old_key, new_key in mapping.items():
            if old_key in data:
                mapped_data[new_key] = data[old_key]
        return mapped_data
    
    def _rename_fields(self, data: Dict[str, Any], renaming: Dict[str, str]) -> Dict[str, Any]:
        """Rename fields in data"""
        renamed_data = data.copy()
        for old_name, new_name in renaming.items():
            if old_name in renamed_data:
                renamed_data[new_name] = renamed_data.pop(old_name)
        return renamed_data
    
    def _remove_fields(self, data: Dict[str, Any], fields: List[str]) -> Dict[str, Any]:
        """Remove fields from data"""
        cleaned_data = data.copy()
        for field in fields:
            cleaned_data.pop(field, None)
        return cleaned_data
    
    async def _apply_custom_function(self, data: Dict[str, Any], function_config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply custom function to data"""
        # This would typically involve loading and executing custom functions
        # For now, we'll return the data as-is
        return data
    
    async def _apply_aggregation(self, events: List[StreamEvent], agg_config: Dict[str, Any]) -> List[StreamEvent]:
        """Apply aggregation to events"""
        agg_type = agg_config.get('type')
        field = agg_config.get('field')
        window_size = agg_config.get('window_size', 100)
        
        if len(events) < window_size:
            return events
        
        # Simple aggregations
        values = [event.data.get(field, 0) for event in events[-window_size:]]
        
        if agg_type == 'sum':
            agg_value = sum(values)
        elif agg_type == 'average':
            agg_value = sum(values) / len(values)
        elif agg_type == 'min':
            agg_value = min(values)
        elif agg_type == 'max':
            agg_value = max(values)
        elif agg_type == 'count':
            agg_value = len(values)
        else:
            agg_value = 0
        
        # Create aggregation event
        agg_event = StreamEvent(
            event_id=f"agg_{int(time.time() * 1000)}",
            timestamp=datetime.utcnow(),
            source=events[0].source,
            data={f"{field}_{agg_type}": agg_value},
            metadata={
                'aggregation_type': agg_type,
                'field': field,
                'window_size': window_size,
                'event_count': len(events)
            }
        )
        
        return [agg_event]
    
    async def _stream_analytics(self, config: StreamingConfig):
        """Perform real-time analytics on stream"""
        while self.running:
            try:
                # Calculate events per second
                current_time = time.time()
                events_in_window = len(self.event_queues[config.stream_id])
                
                # Update metrics
                self.metrics['events_per_second'] = events_in_window / config.window_duration
                
                # Store analytics in Redis
                redis = await aioredis.from_url(self.redis_url)
                await redis.hset(
                    f"stream_analytics:{config.stream_id}",
                    mapping={
                        'events_per_second': str(self.metrics['events_per_second']),
                        'queue_size': str(events_in_window),
                        'processing_latency_ms': str(self.metrics['processing_latency_ms']),
                        'error_rate': str(self.metrics['error_rate']),
                        'timestamp': datetime.utcnow().isoformat()
                    }
                )
                await redis.close()
                
                # Analytics interval
                await asyncio.sleep(5)
                
            except Exception as e:
                self.logger.error(f"Error in stream analytics: {e}")
                await asyncio.sleep(5)
    
    async def get_stream_metrics(self, stream_id: str) -> Dict[str, Any]:
        """Get metrics for a specific stream"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            metrics = await redis.hgetall(f"stream_analytics:{stream_id}")
            await redis.close()
            
            return {
                'stream_id': stream_id,
                'queue_size': len(self.event_queues.get(stream_id, [])),
                'metrics': metrics
            }
            
        except Exception as e:
            self.logger.error(f"Error getting stream metrics: {e}")
            return {'error': str(e)}
    
    async def get_recent_events(self, stream_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent events from a stream"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            events = await redis.lrange(f"stream_queue:{stream_id}", 0, limit - 1)
            await redis.close()
            
            return [json.loads(event) for event in events]
            
        except Exception as e:
            self.logger.error(f"Error getting recent events: {e}")
            return []
    
    async def start_streaming(self):
        """Start the streaming pipeline"""
        self.running = True
        self.logger.info("Starting real-time streaming pipeline")
    
    async def stop_streaming(self):
        """Stop the streaming pipeline"""
        self.running = False
        self.logger.info("Stopping real-time streaming pipeline")
        
        # Cancel all tasks
        for task in self.processing_tasks.values():
            task.cancel()
        
        for task in self.analytics_tasks.values():
            task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self.processing_tasks.values(), *self.analytics_tasks.values(), 
                           return_exceptions=True)
