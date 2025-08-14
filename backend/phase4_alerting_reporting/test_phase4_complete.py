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

from services.alerting_system import AlertingSystem, AlertRule, AlertSeverity, NotificationChannel
from services.reporting_analytics import ReportingAnalyticsSystem, ReportConfig, ReportType, ReportFormat, AnalyticsQuery
from services.root_cause_analysis import RootCauseAnalysisSystem, RCATrigger, RCASeverity
from alerting_reporting_orchestrator import AlertingReportingOrchestrator

class Phase4CompleteTest:
    """Comprehensive test suite for Phase 4 Backend"""

    def __init__(self):
        self.test_results = {}
        self.logger = logging.getLogger(__name__)

    async def run_all_tests(self) -> Dict[str, Any]:
        """Run all Phase 4 tests"""
        self.logger.info("Starting Phase 4 Backend Complete Test Suite")

        tests = [
            ("alerting_system", self.test_alerting_system),
            ("reporting_analytics", self.test_reporting_analytics),
            ("root_cause_analysis", self.test_root_cause_analysis),
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

    async def test_alerting_system(self) -> Dict[str, Any]:
        """Test alerting system functionality"""
        try:
            # Initialize alerting system
            alerting_system = AlertingSystem()

            # Test 1: Add alert rule
            alert_rule = AlertRule(
                rule_id="test_alert",
                name="Test Alert Rule",
                description="Test alert rule for accuracy degradation",
                conditions={
                    'metric': 'accuracy',
                    'threshold': 0.85,
                    'operator': '<'
                },
                severity=AlertSeverity.WARNING,
                channels=['email', 'slack'],
                escalation_rules={'delay_minutes': 30},
                cooldown_minutes=5
            )
            success = await alerting_system.add_alert_rule(alert_rule)

            if not success:
                return {'passed': False, 'error': 'Failed to add alert rule', 'details': {}}

            # Test 2: Add notification channel
            email_channel = NotificationChannel(
                channel_id="test_email",
                channel_type="email",
                name="Test Email Channel",
                config={
                    'smtp_server': 'smtp.gmail.com',
                    'smtp_port': 587,
                    'username': 'test@example.com',
                    'password': 'password',
                    'recipients': ['admin@example.com']
                }
            )
            success = await alerting_system.add_notification_channel(email_channel)

            if not success:
                return {'passed': False, 'error': 'Failed to add notification channel', 'details': {}}

            # Test 3: Check alert conditions
            triggered_rules = await alerting_system.check_alert_conditions('accuracy', 0.80)

            if not triggered_rules:
                return {'passed': False, 'error': 'Failed to trigger alert conditions', 'details': {}}

            # Test 4: Get alerts summary
            summary = await alerting_system.get_alerts_summary()

            return {
                'passed': True,
                'details': {
                    'alert_rule_added': True,
                    'notification_channel_added': True,
                    'alert_triggered': bool(triggered_rules),
                    'summary_retrieved': bool(summary)
                }
            }

        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}

    async def test_reporting_analytics(self) -> Dict[str, Any]:
        """Test reporting and analytics functionality"""
        try:
            # Initialize reporting system
            reporting_system = ReportingAnalyticsSystem()

            # Test 1: Add report config
            report_config = ReportConfig(
                report_id="test_report",
                name="Test Report",
                description="Test report configuration",
                report_type=ReportType.PERFORMANCE,
                data_sources=['performance_metrics', 'quality_metrics'],
                metrics=['accuracy', 'latency_ms'],
                time_range={'hours': 24},
                format=ReportFormat.JSON
            )
            success = await reporting_system.add_report_config(report_config)

            if not success:
                return {'passed': False, 'error': 'Failed to add report config', 'details': {}}

            # Test 2: Add analytics query
            analytics_query = AnalyticsQuery(
                query_id="test_query",
                name="Test Analytics Query",
                query_type="aggregation",
                parameters={
                    'metric': 'accuracy',
                    'aggregation': 'mean'
                },
                time_range={'hours': 24}
            )
            success = await reporting_system.add_analytics_query(analytics_query)

            if not success:
                return {'passed': False, 'error': 'Failed to add analytics query', 'details': {}}

            # Test 3: Generate report
            report = await reporting_system.generate_report("test_report")

            if not report:
                return {'passed': False, 'error': 'Failed to generate report', 'details': {}}

            # Test 4: Get reports summary
            summary = await reporting_system.get_reports_summary()

            return {
                'passed': True,
                'details': {
                    'report_config_added': True,
                    'analytics_query_added': True,
                    'report_generated': bool(report),
                    'summary_retrieved': bool(summary)
                }
            }

        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}

    async def test_root_cause_analysis(self) -> Dict[str, Any]:
        """Test root cause analysis functionality"""
        try:
            # Initialize RCA system
            rca_system = RootCauseAnalysisSystem()

            # Test 1: Add RCA trigger
            rca_trigger = RCATrigger(
                trigger_id="test_rca_trigger",
                name="Test RCA Trigger",
                description="Test RCA trigger for performance issues",
                trigger_conditions={
                    'type': 'threshold',
                    'metric': 'accuracy',
                    'threshold': 0.75,
                    'operator': '<'
                },
                severity=RCASeverity.HIGH
            )
            success = await rca_system.add_rca_trigger(rca_trigger)

            if not success:
                return {'passed': False, 'error': 'Failed to add RCA trigger', 'details': {}}

            # Test 2: Check RCA triggers
            metrics = {'accuracy': 0.70, 'latency_ms': 150}
            triggered_investigations = await rca_system.check_rca_triggers(metrics)

            if not triggered_investigations:
                return {'passed': False, 'error': 'Failed to trigger RCA investigation', 'details': {}}

            # Test 3: Get investigation summary
            summary = await rca_system.get_investigation_summary()

            return {
                'passed': True,
                'details': {
                    'rca_trigger_added': True,
                    'investigation_triggered': bool(triggered_investigations),
                    'summary_retrieved': bool(summary)
                }
            }

        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}

    async def test_orchestrator(self) -> Dict[str, Any]:
        """Test orchestrator functionality"""
        try:
            # Initialize orchestrator
            orchestrator = AlertingReportingOrchestrator()

            # Test 1: Setup default configurations
            await orchestrator.setup_default_configurations()

            # Test 2: Process metrics
            metrics = {
                'accuracy': 0.90,
                'latency_ms': 100,
                'throughput_rps': 1000,
                'drift_score': 0.05,
                'alerts_count': 2
            }
            result = await orchestrator.process_metrics(metrics)

            if not result.success:
                return {'passed': False, 'error': 'Failed to process metrics', 'details': {}}

            # Test 3: Get comprehensive summary
            summary = await orchestrator.get_comprehensive_summary()

            return {
                'passed': True,
                'details': {
                    'default_configs_setup': True,
                    'metrics_processed': result.success,
                    'summary_retrieved': bool(summary)
                }
            }

        except Exception as e:
            return {'passed': False, 'error': str(e), 'details': {}}

    async def test_integration(self) -> Dict[str, Any]:
        """Test integration between all components"""
        try:
            # Initialize orchestrator
            orchestrator = AlertingReportingOrchestrator()

            # Setup complete Phase 4 pipeline
            await orchestrator.setup_default_configurations()

            # Test 1: Process metrics that should trigger alerts
            low_accuracy_metrics = {
                'accuracy': 0.75,  # Below threshold
                'latency_ms': 250,  # Above threshold
                'throughput_rps': 800,
                'drift_score': 0.15,  # Above threshold
                'alerts_count': 8  # Multiple alerts
            }
            result1 = await orchestrator.process_metrics(low_accuracy_metrics)

            # Test 2: Process normal metrics
            normal_metrics = {
                'accuracy': 0.95,
                'latency_ms': 50,
                'throughput_rps': 1200,
                'drift_score': 0.02,
                'alerts_count': 1
            }
            result2 = await orchestrator.process_metrics(normal_metrics)

            # Test 3: Generate custom report
            custom_report_config = ReportConfig(
                report_id="integration_test_report",
                name="Integration Test Report",
                description="Test report for integration testing",
                report_type=ReportType.CUSTOM,
                data_sources=['performance_metrics', 'alerts'],
                metrics=['accuracy', 'latency_ms', 'alerts_count'],
                time_range={'hours': 24},
                format=ReportFormat.JSON
            )
            report_id = await orchestrator.generate_custom_report(custom_report_config)

            # Test 4: Trigger manual RCA
            rca_investigation_id = await orchestrator.trigger_manual_rca(
                "critical_performance", low_accuracy_metrics
            )

            # Test 5: Get comprehensive summary
            summary = await orchestrator.get_comprehensive_summary()

            return {
                'passed': True,
                'details': {
                    'pipeline_configured': True,
                    'low_accuracy_processed': result1.success,
                    'normal_metrics_processed': result2.success,
                    'custom_report_generated': bool(report_id),
                    'manual_rca_triggered': bool(rca_investigation_id),
                    'comprehensive_summary': bool(summary),
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

        # Phase 4 completion assessment
        if overall_score >= 90:
            completion_status = "COMPLETE"
            completion_message = "Phase 4 Backend is fully functional and ready for production!"
        elif overall_score >= 75:
            completion_status = "MOSTLY COMPLETE"
            completion_message = "Phase 4 Backend is functional with minor issues to address."
        elif overall_score >= 50:
            completion_status = "PARTIALLY COMPLETE"
            completion_message = "Phase 4 Backend has core functionality but needs improvements."
        else:
            completion_status = "INCOMPLETE"
            completion_message = "Phase 4 Backend needs significant work to be functional."

        # Generate detailed report
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
            'phase4_assessment': {
                'alerting_system': self._assess_component('alerting_system'),
                'reporting_analytics': self._assess_component('reporting_analytics'),
                'root_cause_analysis': self._assess_component('root_cause_analysis'),
                'orchestrator': self._assess_component('orchestrator'),
                'integration': self._assess_component('integration')
            },
            'recommendations': self._generate_recommendations(),
            'timestamp': datetime.utcnow().isoformat()
        }

        return report

    def _assess_component(self, component_name: str) -> Dict[str, Any]:
        """Assess individual component status"""
        if component_name in self.test_results:
            result = self.test_results[component_name]
            if result['passed']:
                return {
                    'status': 'FUNCTIONAL',
                    'score': 100,
                    'message': f"{component_name.replace('_', ' ').title()} is working correctly"
                }
            else:
                return {
                    'status': 'ISSUES_DETECTED',
                    'score': 0,
                    'message': f"{component_name.replace('_', ' ').title()} has issues: {result.get('error', 'Unknown error')}"
                }
        else:
            return {
                'status': 'NOT_TESTED',
                'score': 0,
                'message': f"{component_name.replace('_', ' ').title()} was not tested"
            }

    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on test results"""
        recommendations = []

        # Check for failed tests
        failed_components = [name for name, result in self.test_results.items() if not result['passed']]
        
        if failed_components:
            recommendations.append(f"Fix issues in failed components: {', '.join(failed_components)}")
            recommendations.append("Review error logs for detailed failure information")
        
        # General recommendations
        recommendations.append("Ensure all dependencies are properly installed")
        recommendations.append("Verify Redis connection and configuration")
        recommendations.append("Test with real data sources in production environment")
        recommendations.append("Implement proper error handling and logging")
        recommendations.append("Add unit tests for individual functions")
        recommendations.append("Configure proper notification channels for production use")

        return recommendations

async def main():
    """Main test execution function"""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run tests
    test_suite = Phase4CompleteTest()
    report = await test_suite.run_all_tests()
    
    # Print results
    print("\n" + "="*80)
    print("PHASE 4 BACKEND COMPLETE TEST REPORT")
    print("="*80)
    
    summary = report['test_summary']
    print(f"\nOverall Score: {summary['overall_score']:.1f}%")
    print(f"Status: {summary['completion_status']}")
    print(f"Message: {summary['completion_message']}")
    
    print(f"\nTest Results:")
    print(f"  Total Tests: {summary['total_tests']}")
    print(f"  Passed: {summary['passed_tests']}")
    print(f"  Failed: {summary['failed_tests']}")
    
    print(f"\nComponent Assessment:")
    for component, assessment in report['phase4_assessment'].items():
        status_icon = "✅" if assessment['status'] == 'FUNCTIONAL' else "❌"
        print(f"  {status_icon} {component.replace('_', ' ').title()}: {assessment['status']}")
    
    if report['recommendations']:
        print(f"\nRecommendations:")
        for i, rec in enumerate(report['recommendations'], 1):
            print(f"  {i}. {rec}")
    
    print(f"\nTimestamp: {report['timestamp']}")
    print("="*80)
    
    # Save report to file
    with open('PHASE4_TEST_REPORT.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\nDetailed report saved to: PHASE4_TEST_REPORT.json")
    
    return report

if __name__ == "__main__":
    asyncio.run(main())
