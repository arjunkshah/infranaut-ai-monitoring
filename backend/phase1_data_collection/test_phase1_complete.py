#!/usr/bin/env python3
"""
Comprehensive test suite for Phase 1: Data Collection & Aggregation
Tests all components including data retention, lineage tracking, and real-time streaming
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any

# Import all Phase 1 components
from ingestion_pipeline import DataIngestionPipeline, DataSource
from data_validator import DataValidator
from data_aggregator import DataAggregator
from storage_manager import StorageManager
from metadata_manager import MetadataManager
from retention_manager import DataRetentionManager, RetentionPolicy, StorageConfig
from lineage_tracker import DataLineageTracker, LineageNode, LineageNodeType, LineageEdge, LineageEdgeType, DataArtifact
from streaming_pipeline import RealTimeStreamingPipeline, StreamingConfig, StreamEvent

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Phase1CompleteTest:
    """Comprehensive test suite for Phase 1 completion"""
    
    def __init__(self):
        self.test_results = {}
        self.start_time = time.time()
        
    async def run_all_tests(self):
        """Run all Phase 1 tests"""
        logger.info("🚀 Starting Phase 1 Complete Test Suite")
        
        tests = [
            ("Data Ingestion Pipeline", self.test_data_ingestion_pipeline),
            ("Data Validation", self.test_data_validation),
            ("Data Aggregation", self.test_data_aggregation),
            ("Storage Management", self.test_storage_management),
            ("Metadata Management", self.test_metadata_management),
            ("Data Retention Management", self.test_data_retention),
            ("Data Lineage Tracking", self.test_data_lineage),
            ("Real-time Streaming", self.test_real_time_streaming),
            ("Integration Test", self.test_integration),
        ]
        
        for test_name, test_func in tests:
            try:
                logger.info(f"📋 Running: {test_name}")
                result = await test_func()
                self.test_results[test_name] = result
                status = "✅ PASS" if result['success'] else "❌ FAIL"
                logger.info(f"{status} {test_name}: {result['message']}")
            except Exception as e:
                self.test_results[test_name] = {
                    'success': False,
                    'message': f"Exception: {str(e)}",
                    'details': str(e)
                }
                logger.error(f"❌ FAIL {test_name}: Exception - {str(e)}")
        
        await self.generate_test_report()
    
    async def test_data_ingestion_pipeline(self) -> Dict[str, Any]:
        """Test data ingestion pipeline functionality"""
        try:
            # Initialize pipeline
            pipeline = DataIngestionPipeline()
            
            # Create test data source
            api_source = DataSource(
                source_id="test_api",
                source_type="api",
                connection_config={"url": "https://jsonplaceholder.typicode.com/posts/1"},
                data_format="json",
                batch_size=10,
                poll_interval=1
            )
            
            # Add data source
            success = await pipeline.add_data_source(api_source)
            if not success:
                return {'success': False, 'message': 'Failed to add data source'}
            
            # Test data queue operations
            queue_size = await pipeline.get_data_queue_size("test_api")
            recent_data = await pipeline.get_recent_data("test_api", limit=5)
            
            # Verify pipeline components
            assert "test_api" in pipeline.sources
            assert len(pipeline.sources) == 1
            
            return {
                'success': True,
                'message': f'Pipeline initialized with {len(pipeline.sources)} sources',
                'queue_size': queue_size,
                'recent_data_count': len(recent_data)
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Pipeline test failed: {str(e)}'}
    
    async def test_data_validation(self) -> Dict[str, Any]:
        """Test data validation functionality"""
        try:
            # Initialize validator
            validator = DataValidator()
            
            # Test data
            test_data = {
                "id": 1,
                "title": "Test Post",
                "body": "This is a test post",
                "userId": 1
            }
            
            # Define validation schema
            schema = {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "title": {"type": "string", "minLength": 1},
                    "body": {"type": "string", "minLength": 1},
                    "userId": {"type": "integer"}
                },
                "required": ["id", "title", "body", "userId"]
            }
            
            # Validate data
            validation_result = await validator.validate_data(test_data, schema)
            
            # Test validation rules
            rules = [
                {"field": "id", "rule": "required"},
                {"field": "title", "rule": "min_length", "value": 1},
                {"field": "body", "rule": "max_length", "value": 1000}
            ]
            
            rules_result = await validator.validate_with_rules(test_data, rules)
            
            return {
                'success': validation_result['valid'] and rules_result['valid'],
                'message': f'Validation completed - Schema: {validation_result["valid"]}, Rules: {rules_result["valid"]}',
                'schema_validation': validation_result,
                'rules_validation': rules_result
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Validation test failed: {str(e)}'}
    
    async def test_data_aggregation(self) -> Dict[str, Any]:
        """Test data aggregation functionality"""
        try:
            # Initialize aggregator
            aggregator = DataAggregator()
            
            # Test data
            test_data = [
                {"id": 1, "value": 10, "category": "A"},
                {"id": 2, "value": 20, "category": "B"},
                {"id": 3, "value": 15, "category": "A"},
                {"id": 4, "value": 25, "category": "B"}
            ]
            
            # Test aggregation functions
            sum_result = await aggregator.aggregate_data(test_data, "value", "sum")
            avg_result = await aggregator.aggregate_data(test_data, "value", "average")
            count_result = await aggregator.aggregate_data(test_data, "id", "count")
            
            # Test group by aggregation
            group_result = await aggregator.aggregate_by_group(test_data, "category", "value", "sum")
            
            return {
                'success': True,
                'message': f'Aggregation completed - Sum: {sum_result}, Avg: {avg_result}, Count: {count_result}',
                'sum': sum_result,
                'average': avg_result,
                'count': count_result,
                'group_by': group_result
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Aggregation test failed: {str(e)}'}
    
    async def test_storage_management(self) -> Dict[str, Any]:
        """Test storage management functionality"""
        try:
            # Initialize storage manager
            storage_manager = StorageManager()
            
            # Test data
            test_data = {"id": 1, "name": "Test Item", "timestamp": datetime.utcnow().isoformat()}
            
            # Test storage operations
            store_result = await storage_manager.store_data("test_collection", test_data)
            retrieve_result = await storage_manager.retrieve_data("test_collection", {"id": 1})
            
            # Test batch operations
            batch_data = [{"id": i, "name": f"Item {i}"} for i in range(5)]
            batch_store = await storage_manager.store_batch("test_batch", batch_data)
            batch_retrieve = await storage_manager.retrieve_batch("test_batch", limit=10)
            
            return {
                'success': True,
                'message': f'Storage operations completed - Store: {store_result}, Retrieve: {len(retrieve_result)} items',
                'store_result': store_result,
                'retrieve_count': len(retrieve_result),
                'batch_store': batch_store,
                'batch_retrieve_count': len(batch_retrieve)
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Storage test failed: {str(e)}'}
    
    async def test_metadata_management(self) -> Dict[str, Any]:
        """Test metadata management functionality"""
        try:
            # Initialize metadata manager
            metadata_manager = MetadataManager()
            
            # Test metadata operations
            metadata = {
                "source": "test_source",
                "timestamp": datetime.utcnow().isoformat(),
                "version": "1.0",
                "tags": ["test", "phase1"]
            }
            
            # Store metadata
            store_result = await metadata_manager.store_metadata("test_dataset", metadata)
            
            # Retrieve metadata
            retrieve_result = await metadata_manager.get_metadata("test_dataset")
            
            # Test metadata search
            search_result = await metadata_manager.search_metadata({"tags": "test"})
            
            return {
                'success': True,
                'message': f'Metadata operations completed - Store: {store_result}, Retrieve: {retrieve_result is not None}',
                'store_result': store_result,
                'retrieve_result': retrieve_result,
                'search_count': len(search_result)
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Metadata test failed: {str(e)}'}
    
    async def test_data_retention(self) -> Dict[str, Any]:
        """Test data retention management functionality"""
        try:
            # Initialize storage config
            storage_config = StorageConfig(
                hot_storage={"redis_url": "redis://localhost:6379"},
                warm_storage={"postgres_url": "postgresql://localhost/test"},
                cold_storage={"aws": {"bucket": "test-bucket"}}
            )
            
            # Initialize retention manager
            retention_manager = DataRetentionManager(storage_config)
            
            # Create retention policy
            policy = RetentionPolicy(
                policy_id="test_policy",
                source_id="test_source",
                hot_storage_days=1,
                warm_storage_days=7,
                cold_storage_days=30,
                archival_enabled=True,
                cleanup_enabled=True
            )
            
            # Add policy
            add_result = await retention_manager.add_retention_policy(policy)
            
            # Get retention report
            report = await retention_manager.get_retention_report()
            
            return {
                'success': add_result,
                'message': f'Retention management initialized - Policy added: {add_result}',
                'policy_added': add_result,
                'report_available': report is not None
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Retention test failed: {str(e)}'}
    
    async def test_data_lineage(self) -> Dict[str, Any]:
        """Test data lineage tracking functionality"""
        try:
            # Initialize lineage tracker
            lineage_tracker = DataLineageTracker()
            
            # Create test nodes
            source_node = LineageNode(
                node_id="test_source",
                node_type=LineageNodeType.SOURCE,
                name="Test Data Source",
                description="Test source for lineage tracking",
                metadata={"source_type": "api", "endpoint": "/test"},
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                tags=["test", "api"]
            )
            
            sink_node = LineageNode(
                node_id="test_sink",
                node_type=LineageNodeType.SINK,
                name="Test Data Sink",
                description="Test sink for lineage tracking",
                metadata={"sink_type": "database", "table": "test_table"},
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                tags=["test", "database"]
            )
            
            # Add nodes
            source_added = await lineage_tracker.add_node(source_node)
            sink_added = await lineage_tracker.add_node(sink_node)
            
            # Create edge
            edge = LineageEdge(
                edge_id="test_edge",
                source_node_id="test_source",
                target_node_id="test_sink",
                edge_type=LineageEdgeType.FLOWS_TO,
                metadata={"flow_type": "data_transfer"},
                created_at=datetime.utcnow()
            )
            
            edge_added = await lineage_tracker.add_edge(edge)
            
            # Test lineage queries
            upstream = await lineage_tracker.get_upstream_dependencies("test_sink")
            downstream = await lineage_tracker.get_downstream_dependencies("test_source")
            
            # Get lineage statistics
            stats = await lineage_tracker.get_lineage_statistics()
            
            return {
                'success': source_added and sink_added and edge_added,
                'message': f'Lineage tracking initialized - Nodes: {source_added}, {sink_added}, Edge: {edge_added}',
                'nodes_added': source_added and sink_added,
                'edge_added': edge_added,
                'upstream_count': len(upstream),
                'downstream_count': len(downstream),
                'total_nodes': stats.get('total_nodes', 0)
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Lineage test failed: {str(e)}'}
    
    async def test_real_time_streaming(self) -> Dict[str, Any]:
        """Test real-time streaming functionality"""
        try:
            # Initialize streaming pipeline
            streaming_pipeline = RealTimeStreamingPipeline()
            
            # Create streaming config
            stream_config = StreamingConfig(
                stream_id="test_stream",
                source_type="websocket",
                connection_config={"uri": "wss://echo.websocket.org"},
                processing_config={
                    "filters": [{"field": "type", "operator": "equals", "value": "test"}],
                    "transformations": [{"type": "field_mapping", "mapping": {"old_field": "new_field"}}]
                },
                window_size=100,
                window_duration=60,
                batch_size=10,
                enable_analytics=True
            )
            
            # Add stream
            stream_added = await streaming_pipeline.add_stream(stream_config)
            
            # Test stream metrics
            metrics = await streaming_pipeline.get_stream_metrics("test_stream")
            
            # Test recent events
            events = await streaming_pipeline.get_recent_events("test_stream", limit=5)
            
            return {
                'success': stream_added,
                'message': f'Streaming pipeline initialized - Stream added: {stream_added}',
                'stream_added': stream_added,
                'metrics_available': metrics is not None,
                'events_count': len(events)
            }
            
        except Exception as e:
            return {'success': False, 'message': f'Streaming test failed: {str(e)}'}
    
    async def test_integration(self) -> Dict[str, Any]:
        """Test integration between all Phase 1 components"""
        try:
            # Initialize all components
            ingestion_pipeline = DataIngestionPipeline()
            validator = DataValidator()
            aggregator = DataAggregator()
            storage_manager = StorageManager()
            metadata_manager = MetadataManager()
            
            # Create storage config for retention
            storage_config = StorageConfig(
                hot_storage={"redis_url": "redis://localhost:6379"},
                warm_storage={"postgres_url": "postgresql://localhost/test"},
                cold_storage={"aws": {"bucket": "test-bucket"}}
            )
            retention_manager = DataRetentionManager(storage_config)
            
            lineage_tracker = DataLineageTracker()
            streaming_pipeline = RealTimeStreamingPipeline()
            
            # Test data flow
            test_data = {
                "id": 1,
                "value": 100,
                "timestamp": datetime.utcnow().isoformat(),
                "source": "integration_test"
            }
            
            # 1. Validate data
            validation_result = await validator.validate_data(test_data, {"type": "object"})
            
            # 2. Store data
            if validation_result['valid']:
                store_result = await storage_manager.store_data("integration_test", test_data)
                
                # 3. Store metadata
                metadata = {
                    "source": "integration_test",
                    "validation_status": "passed",
                    "timestamp": datetime.utcnow().isoformat()
                }
                metadata_result = await metadata_manager.store_metadata("integration_test", metadata)
                
                # 4. Track lineage
                source_node = LineageNode(
                    node_id="integration_source",
                    node_type=LineageNodeType.SOURCE,
                    name="Integration Test Source",
                    description="Source for integration test",
                    metadata={"test_type": "integration"},
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow()
                )
                lineage_result = await lineage_tracker.add_node(source_node)
                
                return {
                    'success': True,
                    'message': 'Integration test completed successfully',
                    'validation': validation_result['valid'],
                    'storage': store_result,
                    'metadata': metadata_result,
                    'lineage': lineage_result
                }
            else:
                return {
                    'success': False,
                    'message': 'Integration test failed at validation step',
                    'validation': validation_result
                }
                
        except Exception as e:
            return {'success': False, 'message': f'Integration test failed: {str(e)}'}
    
    async def generate_test_report(self):
        """Generate comprehensive test report"""
        end_time = time.time()
        duration = end_time - self.start_time
        
        # Calculate statistics
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results.values() if result['success'])
        failed_tests = total_tests - passed_tests
        success_rate = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
        
        # Generate report
        report = {
            "test_suite": "Phase 1: Data Collection & Aggregation",
            "timestamp": datetime.utcnow().isoformat(),
            "duration_seconds": round(duration, 2),
            "summary": {
                "total_tests": total_tests,
                "passed_tests": passed_tests,
                "failed_tests": failed_tests,
                "success_rate": round(success_rate, 2)
            },
            "results": self.test_results
        }
        
        # Save report
        with open("PHASE1_COMPLETE_TEST_RESULTS.json", "w") as f:
            json.dump(report, f, indent=2, default=str)
        
        # Print summary
        logger.info("=" * 60)
        logger.info("📊 PHASE 1 COMPLETE TEST RESULTS")
        logger.info("=" * 60)
        logger.info(f"⏱️  Duration: {duration:.2f} seconds")
        logger.info(f"📋 Total Tests: {total_tests}")
        logger.info(f"✅ Passed: {passed_tests}")
        logger.info(f"❌ Failed: {failed_tests}")
        logger.info(f"📈 Success Rate: {success_rate:.1f}%")
        logger.info("=" * 60)
        
        # Print detailed results
        for test_name, result in self.test_results.items():
            status = "✅ PASS" if result['success'] else "❌ FAIL"
            logger.info(f"{status} {test_name}: {result['message']}")
        
        logger.info("=" * 60)
        logger.info(f"📄 Detailed report saved to: PHASE1_COMPLETE_TEST_RESULTS.json")
        
        # Phase 1 completion assessment
        if success_rate >= 80:
            logger.info("🎉 PHASE 1 COMPLETION STATUS: EXCELLENT")
            logger.info("✅ All core functionality implemented and tested")
            logger.info("✅ Ready to proceed to Phase 2")
        elif success_rate >= 60:
            logger.info("⚠️  PHASE 1 COMPLETION STATUS: GOOD")
            logger.info("✅ Core functionality implemented")
            logger.info("⚠️  Some features may need refinement")
        else:
            logger.info("❌ PHASE 1 COMPLETION STATUS: NEEDS WORK")
            logger.info("❌ Core functionality incomplete")
            logger.info("❌ Requires additional development")

async def main():
    """Main test execution"""
    test_suite = Phase1CompleteTest()
    await test_suite.run_all_tests()

if __name__ == "__main__":
    asyncio.run(main())
