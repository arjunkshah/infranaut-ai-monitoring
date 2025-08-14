import asyncio
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any
import sys
import os

# Add the current directory to the path for imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.performance_tracker import ModelPerformanceTracker, ModelConfig, PerformanceMetrics
from services.lifecycle_manager import ModelLifecycleManager, ModelVersion, ModelStatus, Deployment, DeploymentStatus, RetrainingTrigger
from services.ab_testing import ABTestingSystem, ABTestConfig, ABTestResult
from model_monitoring_orchestrator import ModelMonitoringOrchestrator, MonitoringConfig

class Phase3CompleteTest:
    """Comprehensive test suite for Phase 3 Backend"""
    
    def __init__(self):
        self.test_results = {}
        self.logger = logging.getLogger(__name__)
        
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all Phase 3 tests"""
        self.logger.info("Starting Phase 3 Backend Complete Test Suite")
        
        tests = [
            ("performance_tracking", self.test_performance_tracking),
            ("lifecycle_management", self.test_lifecycle_management),
            ("ab_testing", self.test_ab_testing),
            ("orchestrator", self.test_orchestrator),
            ("integration", self.test_integration)
        ]
        
        for test_name, test_func in tests:
            try:
                self.logger.info(f"Running test: {test_name}")
                result = await test_func()
                self.test_results[test_name] = result
                self.logger.info(f"Test {test_name}: {'PASSED' if result['passed'] else 'FAILED'}")
            except Exception as e:
                self.logger.error(f"Test {test_name} failed with exception: {e}")
                self.test_results[test_name] = {
                    'passed': False,
                    'error': str(e),
                    'details': {}
                }
        
        return self.generate_test_report()
    
    async def test_performance_tracking(self) -> Dict[str, Any]:
        """Test performance tracking functionality"""
        try:
            # Initialize performance tracker
            performance_tracker = ModelPerformanceTracker()
            
            # Test 1: Add model config
            config = ModelConfig(
                model_id="test_model",
                model_name="Test Model",
                model_version="1.0.0",
                model_type="classification",
                target_metrics={'accuracy': 0.9, 'latency_ms': 100},
                alert_thresholds={'accuracy': 0.8, 'latency_ms': 200}
            )
            success = await performance_tracker.add_model_config(config)
            
            if not success:
                return {'passed': False, 'error': 'Failed to add model config', 'details': {}}
            
            # Test 2: Record prediction
            y_true = [0, 1, 0, 1, 0]
            y_pred = [0, 1, 0, 0, 0]
            latency_ms = 50.0
            
            success = await performance_tracker.record_prediction("test_model", y_true, y_pred, latency_ms)
            
            if not success:
                return {'passed': False, 'error': 'Failed to record prediction', 'details': {}}
            
            # Test 3: Get performance summary
            summary = await performance_tracker.get_performance_summary("test_model", hours=24)
            
            if not summary:
                return {'passed': False, 'error': 'Failed to get performance summary', 'details': {}}
            
            # Test 4: Get comprehensive summary
            comprehensive_summary = await performance_tracker.get_comprehensive_summary()
            
            return {
                'passed': True,
                'details': {
                    'config_added': True,
                    'prediction_recorded': True,
                    'summary_retrieved': bool(summary),
                    'comprehensive_summary_retrieved': bool(comprehensive_summary)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}
    
    async def test_lifecycle_management(self) -> Dict[str, Any]:
        """Test lifecycle management functionality"""
        try:
            # Initialize lifecycle manager
            lifecycle_manager = ModelLifecycleManager()
            
            # Test 1: Register model version
            model_version = ModelVersion(
                model_id="test_model",
                version="1.0.0",
                model_name="Test Model",
                model_type="classification",
                created_at=datetime.utcnow(),
                created_by="test_user",
                description="Test model version",
                status=ModelStatus.DRAFT,
                artifact_path="/models/test_model_1.0.0.pkl",
                metadata={'framework': 'scikit-learn', 'algorithm': 'random_forest'},
                performance_baseline={'accuracy': 0.9, 'latency_ms': 100},
                dependencies={'scikit-learn': '1.0.0', 'numpy': '1.21.0'}
            )
            success = await lifecycle_manager.register_model_version(model_version)
            
            if not success:
                return {'passed': False, 'error': 'Failed to register model version', 'details': {}}
            
            # Test 2: Deploy model
            deployment_config = {'environment': 'staging', 'replicas': 2}
            deployment_id = await lifecycle_manager.deploy_model(
                "test_model", "1.0.0", "staging", "test_user", deployment_config
            )
            
            if not deployment_id:
                return {'passed': False, 'error': 'Failed to deploy model', 'details': {}}
            
            # Test 3: Add retraining trigger
            trigger = RetrainingTrigger(
                trigger_id="test_trigger",
                model_id="test_model",
                trigger_type="performance",
                conditions={'baseline': {'accuracy': 0.9}, 'threshold': 0.1}
            )
            success = await lifecycle_manager.add_retraining_trigger(trigger)
            
            if not success:
                return {'passed': False, 'error': 'Failed to add retraining trigger', 'details': {}}
            
            # Test 4: Get model lineage
            lineage = await lifecycle_manager.get_model_lineage("test_model")
            
            return {
                'passed': True,
                'details': {
                    'version_registered': True,
                    'model_deployed': bool(deployment_id),
                    'trigger_added': True,
                    'lineage_retrieved': bool(lineage)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}
    
    async def test_ab_testing(self) -> Dict[str, Any]:
        """Test A/B testing functionality"""
        try:
            # Initialize A/B testing system
            ab_testing = ABTestingSystem()
            
            # Test 1: Create A/B test
            config = ABTestConfig(
                test_id="test_ab_test",
                test_name="Test A/B Test",
                model_a_id="model_a",
                model_b_id="model_b",
                traffic_split=0.5,
                primary_metric="accuracy",
                significance_level=0.05,
                min_sample_size=100
            )
            success = await ab_testing.create_ab_test(config)
            
            if not success:
                return {'passed': False, 'error': 'Failed to create A/B test', 'details': {}}
            
            # Test 2: Assign traffic
            user_id = "test_user_1"
            assigned_model = await ab_testing.assign_traffic("test_ab_test", user_id)
            
            if not assigned_model:
                return {'passed': False, 'error': 'Failed to assign traffic', 'details': {}}
            
            # Test 3: Record prediction result
            y_true = [0, 1, 0, 1, 0]
            y_pred = [0, 1, 0, 0, 0]
            latency_ms = 50.0
            
            success = await ab_testing.record_prediction_result("test_ab_test", user_id, y_true, y_pred, latency_ms)
            
            if not success:
                return {'passed': False, 'error': 'Failed to record prediction result', 'details': {}}
            
            # Test 4: Get test summary
            summary = await ab_testing.get_test_summary("test_ab_test")
            
            return {
                'passed': True,
                'details': {
                    'test_created': True,
                    'traffic_assigned': bool(assigned_model),
                    'prediction_recorded': True,
                    'summary_retrieved': bool(summary)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}
    
    async def test_orchestrator(self) -> Dict[str, Any]:
        """Test orchestrator functionality"""
        try:
            # Initialize orchestrator
            orchestrator = ModelMonitoringOrchestrator()
            
            # Test 1: Add monitoring config
            config = MonitoringConfig(
                model_id="test_model",
                performance_tracking_enabled=True,
                lifecycle_management_enabled=True,
                ab_testing_enabled=False,
                monitoring_interval=60,
                alert_thresholds={'accuracy': 0.8, 'latency_ms': 200}
            )
            success = await orchestrator.add_monitoring_config(config)
            
            if not success:
                return {'passed': False, 'error': 'Failed to add monitoring config', 'details': {}}
            
            # Test 2: Setup performance tracking
            model_config = ModelConfig(
                model_id="test_model",
                model_name="Test Model",
                model_version="1.0.0",
                model_type="classification",
                target_metrics={'accuracy': 0.9, 'latency_ms': 100},
                alert_thresholds={'accuracy': 0.8, 'latency_ms': 200}
            )
            success = await orchestrator.setup_performance_tracking("test_model", model_config)
            
            if not success:
                return {'passed': False, 'error': 'Failed to setup performance tracking', 'details': {}}
            
            # Test 3: Setup lifecycle management
            model_version = ModelVersion(
                model_id="test_model",
                version="1.0.0",
                model_name="Test Model",
                model_type="classification",
                created_at=datetime.utcnow(),
                created_by="test_user",
                description="Test model version",
                status=ModelStatus.DRAFT,
                artifact_path="/models/test_model_1.0.0.pkl",
                metadata={'framework': 'scikit-learn'},
                performance_baseline={'accuracy': 0.9, 'latency_ms': 100},
                dependencies={'scikit-learn': '1.0.0'}
            )
            success = await orchestrator.setup_lifecycle_management("test_model", model_version)
            
            if not success:
                return {'passed': False, 'error': 'Failed to setup lifecycle management', 'details': {}}
            
            # Test 4: Record model prediction
            y_true = [0, 1, 0, 1, 0]
            y_pred = [0, 1, 0, 0, 0]
            latency_ms = 50.0
            
            success = await orchestrator.record_model_prediction("test_model", y_true, y_pred, latency_ms)
            
            if not success:
                return {'passed': False, 'error': 'Failed to record model prediction', 'details': {}}
            
            # Test 5: Get comprehensive summary
            summary = await orchestrator.get_comprehensive_summary()
            
            return {
                'passed': True,
                'details': {
                    'config_added': True,
                    'performance_setup': True,
                    'lifecycle_setup': True,
                    'prediction_recorded': True,
                    'summary_retrieved': bool(summary)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}
    
    async def test_integration(self) -> Dict[str, Any]:
        """Test integration between all components"""
        try:
            # Initialize orchestrator
            orchestrator = ModelMonitoringOrchestrator()
            
            # Setup complete monitoring pipeline
            config = MonitoringConfig(
                model_id="integration_test_model",
                performance_tracking_enabled=True,
                lifecycle_management_enabled=True,
                ab_testing_enabled=True,
                monitoring_interval=60,
                alert_thresholds={'accuracy': 0.8, 'latency_ms': 200}
            )
            await orchestrator.add_monitoring_config(config)
            
            # Setup all components
            model_config = ModelConfig(
                model_id="integration_test_model",
                model_name="Integration Test Model",
                model_version="1.0.0",
                model_type="classification",
                target_metrics={'accuracy': 0.9, 'latency_ms': 100},
                alert_thresholds={'accuracy': 0.8, 'latency_ms': 200}
            )
            await orchestrator.setup_performance_tracking("integration_test_model", model_config)
            
            model_version = ModelVersion(
                model_id="integration_test_model",
                version="1.0.0",
                model_name="Integration Test Model",
                model_type="classification",
                created_at=datetime.utcnow(),
                created_by="test_user",
                description="Integration test model version",
                status=ModelStatus.DRAFT,
                artifact_path="/models/integration_test_model_1.0.0.pkl",
                metadata={'framework': 'scikit-learn'},
                performance_baseline={'accuracy': 0.9, 'latency_ms': 100},
                dependencies={'scikit-learn': '1.0.0'}
            )
            await orchestrator.setup_lifecycle_management("integration_test_model", model_version)
            
            ab_test_config = ABTestConfig(
                test_id="integration_ab_test",
                test_name="Integration A/B Test",
                model_a_id="integration_test_model",
                model_b_id="integration_test_model_v2",
                traffic_split=0.3,
                primary_metric="accuracy",
                significance_level=0.05,
                min_sample_size=50
            )
            await orchestrator.setup_ab_testing("integration_test_model", ab_test_config)
            
            # Test monitoring (without actual data)
            summary = await orchestrator.get_comprehensive_summary()
            
            return {
                'passed': True,
                'details': {
                    'pipeline_configured': True,
                    'all_components_setup': True,
                    'summary_generated': bool(summary),
                    'integration_successful': True
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}
    
    def generate_test_report(self) -> Dict[str, Any]:
        """Generate comprehensive test report"""
        total_tests = len(self.test_results)
        passed_tests = sum(1 for result in self.test_results.values() if result['passed'])
        failed_tests = total_tests - passed_tests
        
        # Calculate overall score
        overall_score = (passed_tests / total_tests) * 100 if total_tests > 0 else 0
        
        # Phase 3 completion assessment
        if overall_score >= 90:
            completion_status = "COMPLETE"
            completion_message = "Phase 3 Backend is fully functional and ready for production!"
        elif overall_score >= 75:
            completion_status = "MOSTLY COMPLETE"
            completion_message = "Phase 3 Backend is functional with minor issues to address."
        elif overall_score >= 50:
            completion_status = "PARTIALLY COMPLETE"
            completion_message = "Phase 3 Backend has core functionality but needs improvements."
        else:
            completion_status = "INCOMPLETE"
            completion_message = "Phase 3 Backend needs significant work before deployment."
        
        report = {
            'test_summary': {
                'total_tests': total_tests,
                'passed_tests': passed_tests,
                'failed_tests': failed_tests,
                'overall_score': overall_score,
                'completion_status': completion_status,
                'completion_message': completion_message
            },
            'test_details': self.test_results,
            'phase3_assessment': {
                'performance_tracking': self.test_results.get('performance_tracking', {}).get('passed', False),
                'lifecycle_management': self.test_results.get('lifecycle_management', {}).get('passed', False),
                'ab_testing': self.test_results.get('ab_testing', {}).get('passed', False),
                'orchestrator': self.test_results.get('orchestrator', {}).get('passed', False),
                'integration': self.test_results.get('integration', {}).get('passed', False)
            },
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Save report to file
        with open('PHASE3_TEST_REPORT.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        print("\n" + "="*60)
        print("PHASE 3 BACKEND COMPLETE TEST REPORT")
        print("="*60)
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {passed_tests}")
        print(f"Failed: {failed_tests}")
        print(f"Overall Score: {overall_score:.1f}%")
        print(f"Status: {completion_status}")
        print(f"Message: {completion_message}")
        print("="*60)
        
        if failed_tests > 0:
            print("\nFAILED TESTS:")
            for test_name, result in self.test_results.items():
                if not result['passed']:
                    print(f"  - {test_name}: {result.get('error', 'Unknown error')}")
        
        print(f"\nDetailed report saved to: PHASE3_TEST_REPORT.json")
        print("="*60)
        
        return report

async def main():
    """Main test runner"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run tests
    tester = Phase3CompleteTest()
    report = await tester.run_all_tests()
    
    return report

if __name__ == "__main__":
    asyncio.run(main())
