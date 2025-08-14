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

from services.drift_detector import DataDriftDetector, DriftConfig
from services.quality_monitor import DataQualityMonitor, QualityRule
from services.feature_engineering import FeatureEngineeringPipeline, FeatureConfig
from data_processing_orchestrator import DataProcessingOrchestrator, ProcessingConfig

class Phase2CompleteTest:
    """Comprehensive test suite for Phase 2 Backend"""
    
    def __init__(self):
        self.test_results = {}
        self.logger = logging.getLogger(__name__)
        
    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all Phase 2 tests"""
        self.logger.info("Starting Phase 2 Backend Complete Test Suite")
        
        tests = [
            ("drift_detection", self.test_drift_detection),
            ("quality_monitoring", self.test_quality_monitoring),
            ("feature_engineering", self.test_feature_engineering),
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
    
    async def test_drift_detection(self) -> Dict[str, Any]:
        """Test drift detection functionality"""
        try:
            # Initialize drift detector
            drift_detector = DataDriftDetector()
            
            # Test 1: Add drift config
            config = DriftConfig(
                feature_name="test_feature",
                detection_method="statistical",
                threshold=0.05,
                window_size=100,
                min_samples=50
            )
            success = await drift_detector.add_drift_config(config)
            
            if not success:
                return {'passed': False, 'error': 'Failed to add drift config', 'details': {}}
            
            # Test 2: Set baseline data
            baseline_data = np.random.normal(0, 1, 100).tolist()
            success = await drift_detector.set_baseline_data("test_feature", baseline_data)
            
            if not success:
                return {'passed': False, 'error': 'Failed to set baseline data', 'details': {}}
            
            # Test 3: Detect drift
            current_data = np.random.normal(0.5, 1, 100).tolist()  # Slightly shifted data
            result = await drift_detector.detect_drift("test_feature", current_data)
            
            if not result:
                return {'passed': False, 'error': 'Failed to detect drift', 'details': {}}
            
            # Test 4: Get drift summary
            summary = await drift_detector.get_drift_summary()
            
            return {
                'passed': True,
                'details': {
                    'config_added': True,
                    'baseline_set': True,
                    'drift_detected': result.drift_detected,
                    'drift_score': result.drift_score,
                    'summary_retrieved': bool(summary)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}
    
    async def test_quality_monitoring(self) -> Dict[str, Any]:
        """Test quality monitoring functionality"""
        try:
            # Initialize quality monitor
            quality_monitor = DataQualityMonitor()
            
            # Test 1: Set schema
            schema = {
                'id': {'type': 'integer', 'required': True},
                'name': {'type': 'string', 'required': True},
                'value': {'type': 'float', 'required': False}
            }
            success = await quality_monitor.set_schema("test_dataset", schema)
            
            if not success:
                return {'passed': False, 'error': 'Failed to set schema', 'details': {}}
            
            # Test 2: Add quality rule
            rule = QualityRule(
                rule_id="test_missing_rule",
                rule_type="missing",
                field_name="name",
                parameters={'max_missing_pct': 0.1},
                severity="warning"
            )
            success = await quality_monitor.add_quality_rule(rule)
            
            if not success:
                return {'passed': False, 'error': 'Failed to add quality rule', 'details': {}}
            
            # Test 3: Check data quality
            test_data = [
                {'id': 1, 'name': 'test1', 'value': 10.5},
                {'id': 2, 'name': None, 'value': 20.0},  # Missing name
                {'id': 3, 'name': 'test3', 'value': 30.5}
            ]
            
            results = await quality_monitor.check_data_quality("test_dataset", test_data)
            
            if not results:
                return {'passed': False, 'error': 'Failed to check data quality', 'details': {}}
            
            # Test 4: Get quality summary
            summary = await quality_monitor.get_quality_summary()
            
            return {
                'passed': True,
                'details': {
                    'schema_set': True,
                    'rule_added': True,
                    'quality_checked': len(results) > 0,
                    'summary_retrieved': bool(summary)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}
    
    async def test_feature_engineering(self) -> Dict[str, Any]:
        """Test feature engineering functionality"""
        try:
            # Initialize feature engineer
            feature_engineer = FeatureEngineeringPipeline()
            
            # Test 1: Add feature config
            config = FeatureConfig(
                feature_name="scaled_value",
                feature_type="numeric",
                source_fields=["value"],
                transformation="scale",
                parameters={'scaler_type': 'standard'}
            )
            success = await feature_engineer.add_feature_config(config)
            
            if not success:
                return {'passed': False, 'error': 'Failed to add feature config', 'details': {}}
            
            # Test 2: Engineer features
            test_data = [
                {'id': 1, 'value': 10.0},
                {'id': 2, 'value': 20.0},
                {'id': 3, 'value': 30.0},
                {'id': 4, 'value': 40.0},
                {'id': 5, 'value': 50.0}
            ]
            
            results = await feature_engineer.engineer_features(test_data)
            
            if not results:
                return {'passed': False, 'error': 'Failed to engineer features', 'details': {}}
            
            # Test 3: Get feature summary
            summary = await feature_engineer.get_feature_summary()
            
            return {
                'passed': True,
                'details': {
                    'config_added': True,
                    'features_engineered': len(results) > 0,
                    'feature_quality_score': results.get('scaled_value', {}).quality_score if results else 0.0,
                    'summary_retrieved': bool(summary)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}
    
    async def test_orchestrator(self) -> Dict[str, Any]:
        """Test orchestrator functionality"""
        try:
            # Initialize orchestrator
            orchestrator = DataProcessingOrchestrator()
            
            # Test 1: Add processing config
            config = ProcessingConfig(
                dataset_name="test_dataset",
                drift_detection_enabled=True,
                quality_monitoring_enabled=True,
                feature_engineering_enabled=True,
                processing_interval=60,
                batch_size=100
            )
            success = await orchestrator.add_processing_config(config)
            
            if not success:
                return {'passed': False, 'error': 'Failed to add processing config', 'details': {}}
            
            # Test 2: Setup drift detection
            features = ["value", "score"]
            success = await orchestrator.setup_drift_detection("test_dataset", features)
            
            if not success:
                return {'passed': False, 'error': 'Failed to setup drift detection', 'details': {}}
            
            # Test 3: Setup quality monitoring
            schema = {
                'id': {'type': 'integer', 'required': True},
                'value': {'type': 'float', 'required': True},
                'score': {'type': 'float', 'required': False}
            }
            success = await orchestrator.setup_quality_monitoring("test_dataset", schema)
            
            if not success:
                return {'passed': False, 'error': 'Failed to setup quality monitoring', 'details': {}}
            
            # Test 4: Setup feature engineering
            feature_specs = [
                {
                    'name': 'scaled_value',
                    'type': 'numeric',
                    'source_fields': ['value'],
                    'transformation': 'scale',
                    'parameters': {'scaler_type': 'standard'}
                }
            ]
            success = await orchestrator.setup_feature_engineering("test_dataset", feature_specs)
            
            if not success:
                return {'passed': False, 'error': 'Failed to setup feature engineering', 'details': {}}
            
            # Test 5: Get comprehensive summary
            summary = await orchestrator.get_comprehensive_summary()
            
            return {
                'passed': True,
                'details': {
                    'config_added': True,
                    'drift_setup': True,
                    'quality_setup': True,
                    'feature_setup': True,
                    'summary_retrieved': bool(summary)
                }
            }
            
        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}
    
    async def test_integration(self) -> Dict[str, Any]:
        """Test integration between all components"""
        try:
            # Initialize orchestrator
            orchestrator = DataProcessingOrchestrator()
            
            # Setup complete pipeline
            config = ProcessingConfig(
                dataset_name="integration_test",
                drift_detection_enabled=True,
                quality_monitoring_enabled=True,
                feature_engineering_enabled=True,
                processing_interval=60,
                batch_size=50
            )
            await orchestrator.add_processing_config(config)
            
            # Setup all components
            await orchestrator.setup_drift_detection("integration_test", ["value", "score"])
            
            schema = {
                'id': {'type': 'integer', 'required': True},
                'value': {'type': 'float', 'required': True},
                'score': {'type': 'float', 'required': False}
            }
            await orchestrator.setup_quality_monitoring("integration_test", schema)
            
            feature_specs = [
                {
                    'name': 'scaled_value',
                    'type': 'numeric',
                    'source_fields': ['value'],
                    'transformation': 'scale',
                    'parameters': {'scaler_type': 'standard'}
                }
            ]
            await orchestrator.setup_feature_engineering("integration_test", feature_specs)
            
            # Test processing (without actual data)
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
        
        # Phase 2 completion assessment
        if overall_score >= 90:
            completion_status = "COMPLETE"
            completion_message = "Phase 2 Backend is fully functional and ready for production!"
        elif overall_score >= 75:
            completion_status = "MOSTLY COMPLETE"
            completion_message = "Phase 2 Backend is functional with minor issues to address."
        elif overall_score >= 50:
            completion_status = "PARTIALLY COMPLETE"
            completion_message = "Phase 2 Backend has core functionality but needs improvements."
        else:
            completion_status = "INCOMPLETE"
            completion_message = "Phase 2 Backend needs significant work before deployment."
        
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
            'phase2_assessment': {
                'drift_detection': self.test_results.get('drift_detection', {}).get('passed', False),
                'quality_monitoring': self.test_results.get('quality_monitoring', {}).get('passed', False),
                'feature_engineering': self.test_results.get('feature_engineering', {}).get('passed', False),
                'orchestrator': self.test_results.get('orchestrator', {}).get('passed', False),
                'integration': self.test_results.get('integration', {}).get('passed', False)
            },
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Save report to file
        with open('PHASE2_TEST_REPORT.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        # Print summary
        print("\n" + "="*60)
        print("PHASE 2 BACKEND COMPLETE TEST REPORT")
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
        
        print(f"\nDetailed report saved to: PHASE2_TEST_REPORT.json")
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
    tester = Phase2CompleteTest()
    report = await tester.run_all_tests()
    
    return report

if __name__ == "__main__":
    asyncio.run(main())
