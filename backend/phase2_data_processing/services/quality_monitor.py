import asyncio
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from scipy import stats
from sklearn.ensemble import IsolationForest
import redis.asyncio as aioredis

@dataclass
class QualityRule:
    """Configuration for a data quality rule"""
    rule_id: str
    rule_type: str  # 'missing', 'outlier', 'duplicate', 'schema', 'range', 'format'
    field_name: str
    parameters: Dict[str, Any]
    severity: str = 'warning'  # 'info', 'warning', 'error', 'critical'
    enabled: bool = True

@dataclass
class QualityResult:
    """Result of quality check"""
    rule_id: str
    field_name: str
    rule_type: str
    passed: bool
    score: float
    issues: List[Dict[str, Any]]
    timestamp: datetime
    severity: str

class DataQualityMonitor:
    """
    Comprehensive data quality monitoring system
    Detects missing values, outliers, duplicates, and schema changes
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.rules: Dict[str, QualityRule] = {}
        self.schemas: Dict[str, Dict[str, Any]] = {}
        self.quality_history: Dict[str, List[QualityResult]] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
        # Initialize outlier detection
        self.isolation_forest = IsolationForest(contamination=0.1, random_state=42)
        
    async def add_quality_rule(self, rule: QualityRule) -> bool:
        """Add a data quality rule"""
        try:
            self.rules[rule.rule_id] = rule
            self.logger.info(f"Added quality rule: {rule.rule_id}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to add quality rule: {e}")
            return False
    
    async def set_schema(self, dataset_name: str, schema: Dict[str, Any]) -> bool:
        """Set expected schema for a dataset"""
        try:
            self.schemas[dataset_name] = schema
            
            # Store schema in Redis
            redis = await aioredis.from_url(self.redis_url)
            await redis.set(
                f"quality_schema:{dataset_name}",
                json.dumps({
                    'schema': schema,
                    'timestamp': datetime.utcnow().isoformat()
                })
            )
            await redis.close()
            
            self.logger.info(f"Set schema for dataset: {dataset_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to set schema for {dataset_name}: {e}")
            return False
    
    async def check_data_quality(self, dataset_name: str, data: List[Dict[str, Any]]) -> List[QualityResult]:
        """Check data quality for a dataset"""
        try:
            results = []
            
            for rule in self.rules.values():
                if not rule.enabled:
                    continue
                
                if rule.rule_type == 'missing':
                    result = await self._check_missing_values(rule, data)
                elif rule.rule_type == 'outlier':
                    result = await self._check_outliers(rule, data)
                elif rule.rule_type == 'duplicate':
                    result = await self._check_duplicates(rule, data)
                elif rule.rule_type == 'schema':
                    result = await self._check_schema(rule, dataset_name, data)
                elif rule.rule_type == 'range':
                    result = await self._check_range(rule, data)
                elif rule.rule_type == 'format':
                    result = await self._check_format(rule, data)
                else:
                    self.logger.warning(f"Unknown rule type: {rule.rule_type}")
                    continue
                
                results.append(result)
                await self._store_quality_result(result)
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error checking data quality: {e}")
            return []
    
    async def _check_missing_values(self, rule: QualityRule, data: List[Dict[str, Any]]) -> QualityResult:
        """Check for missing values"""
        try:
            field_name = rule.field_name
            max_missing_pct = rule.parameters.get('max_missing_pct', 0.1)
            
            total_records = len(data)
            missing_count = sum(1 for record in data 
                              if field_name not in record or record[field_name] is None)
            
            missing_pct = missing_count / total_records if total_records > 0 else 0
            passed = missing_pct <= max_missing_pct
            score = 1 - missing_pct
            
            issues = []
            if not passed:
                issues.append({
                    'type': 'missing_values',
                    'missing_count': missing_count,
                    'missing_pct': missing_pct,
                    'max_allowed': max_missing_pct
                })
            
            return QualityResult(
                rule_id=rule.rule_id,
                field_name=field_name,
                rule_type='missing',
                passed=passed,
                score=score,
                issues=issues,
                timestamp=datetime.utcnow(),
                severity=rule.severity
            )
            
        except Exception as e:
            self.logger.error(f"Error checking missing values: {e}")
            raise
    
    async def _check_outliers(self, rule: QualityRule, data: List[Dict[str, Any]]) -> QualityResult:
        """Check for outliers using statistical methods"""
        try:
            field_name = rule.field_name
            method = rule.parameters.get('method', 'iqr')  # 'iqr', 'zscore', 'isolation_forest'
            
            # Extract numeric values
            values = []
            for record in data:
                if field_name in record and record[field_name] is not None:
                    try:
                        value = float(record[field_name])
                        values.append(value)
                    except (ValueError, TypeError):
                        continue
            
            if len(values) < 10:
                return QualityResult(
                    rule_id=rule.rule_id,
                    field_name=field_name,
                    rule_type='outlier',
                    passed=True,
                    score=1.0,
                    issues=[],
                    timestamp=datetime.utcnow(),
                    severity=rule.severity
                )
            
            values = np.array(values)
            outliers = []
            
            if method == 'iqr':
                outliers = self._detect_outliers_iqr(values)
            elif method == 'zscore':
                outliers = self._detect_outliers_zscore(values)
            elif method == 'isolation_forest':
                outliers = self._detect_outliers_isolation_forest(values)
            
            outlier_pct = len(outliers) / len(values) if values.size > 0 else 0
            max_outlier_pct = rule.parameters.get('max_outlier_pct', 0.05)
            passed = outlier_pct <= max_outlier_pct
            score = 1 - outlier_pct
            
            issues = []
            if not passed:
                issues.append({
                    'type': 'outliers',
                    'outlier_count': len(outliers),
                    'outlier_pct': outlier_pct,
                    'max_allowed': max_outlier_pct,
                    'method': method
                })
            
            return QualityResult(
                rule_id=rule.rule_id,
                field_name=field_name,
                rule_type='outlier',
                passed=passed,
                score=score,
                issues=issues,
                timestamp=datetime.utcnow(),
                severity=rule.severity
            )
            
        except Exception as e:
            self.logger.error(f"Error checking outliers: {e}")
            raise
    
    def _detect_outliers_iqr(self, values: np.ndarray) -> List[int]:
        """Detect outliers using IQR method"""
        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outliers = np.where((values < lower_bound) | (values > upper_bound))[0]
        return outliers.tolist()
    
    def _detect_outliers_zscore(self, values: np.ndarray) -> List[int]:
        """Detect outliers using Z-score method"""
        z_scores = np.abs(stats.zscore(values))
        threshold = 3
        outliers = np.where(z_scores > threshold)[0]
        return outliers.tolist()
    
    def _detect_outliers_isolation_forest(self, values: np.ndarray) -> List[int]:
        """Detect outliers using isolation forest"""
        try:
            values_2d = values.reshape(-1, 1)
            predictions = self.isolation_forest.fit_predict(values_2d)
            outliers = np.where(predictions == -1)[0]
            return outliers.tolist()
        except Exception:
            # Fallback to IQR method
            return self._detect_outliers_iqr(values)
    
    async def _check_duplicates(self, rule: QualityRule, data: List[Dict[str, Any]]) -> QualityResult:
        """Check for duplicate records"""
        try:
            fields = rule.parameters.get('fields', [rule.field_name])
            max_duplicate_pct = rule.parameters.get('max_duplicate_pct', 0.1)
            
            # Create composite keys for duplicate detection
            keys = []
            for record in data:
                key_parts = []
                for field in fields:
                    if field in record:
                        key_parts.append(str(record[field]))
                    else:
                        key_parts.append('None')
                keys.append('|'.join(key_parts))
            
            # Count duplicates
            from collections import Counter
            key_counts = Counter(keys)
            duplicate_count = sum(count - 1 for count in key_counts.values() if count > 1)
            
            total_records = len(data)
            duplicate_pct = duplicate_count / total_records if total_records > 0 else 0
            passed = duplicate_pct <= max_duplicate_pct
            score = 1 - duplicate_pct
            
            issues = []
            if not passed:
                issues.append({
                    'type': 'duplicates',
                    'duplicate_count': duplicate_count,
                    'duplicate_pct': duplicate_pct,
                    'max_allowed': max_duplicate_pct,
                    'fields': fields
                })
            
            return QualityResult(
                rule_id=rule.rule_id,
                field_name=rule.field_name,
                rule_type='duplicate',
                passed=passed,
                score=score,
                issues=issues,
                timestamp=datetime.utcnow(),
                severity=rule.severity
            )
            
        except Exception as e:
            self.logger.error(f"Error checking duplicates: {e}")
            raise
    
    async def _check_schema(self, rule: QualityRule, dataset_name: str, data: List[Dict[str, Any]]) -> QualityResult:
        """Check schema compliance"""
        try:
            if dataset_name not in self.schemas:
                return QualityResult(
                    rule_id=rule.rule_id,
                    field_name=rule.field_name,
                    rule_type='schema',
                    passed=True,
                    score=1.0,
                    issues=[],
                    timestamp=datetime.utcnow(),
                    severity=rule.severity
                )
            
            expected_schema = self.schemas[dataset_name]
            schema_violations = []
            
            for i, record in enumerate(data):
                # Check for unexpected fields
                for field in record:
                    if field not in expected_schema:
                        schema_violations.append({
                            'record_index': i,
                            'field': field,
                            'issue': 'unexpected_field'
                        })
                
                # Check for missing required fields
                for field, field_schema in expected_schema.items():
                    if field_schema.get('required', False) and field not in record:
                        schema_violations.append({
                            'record_index': i,
                            'field': field,
                            'issue': 'missing_required_field'
                        })
                    
                    # Check data type
                    if field in record and 'type' in field_schema:
                        expected_type = field_schema['type']
                        actual_value = record[field]
                        
                        if not self._check_data_type(actual_value, expected_type):
                            schema_violations.append({
                                'record_index': i,
                                'field': field,
                                'issue': 'type_mismatch',
                                'expected_type': expected_type,
                                'actual_value': str(actual_value)
                            })
            
            total_records = len(data)
            violation_pct = len(schema_violations) / total_records if total_records > 0 else 0
            max_violation_pct = rule.parameters.get('max_violation_pct', 0.05)
            passed = violation_pct <= max_violation_pct
            score = 1 - violation_pct
            
            issues = []
            if not passed:
                issues.append({
                    'type': 'schema_violations',
                    'violation_count': len(schema_violations),
                    'violation_pct': violation_pct,
                    'max_allowed': max_violation_pct,
                    'violations': schema_violations[:10]  # Limit to first 10 violations
                })
            
            return QualityResult(
                rule_id=rule.rule_id,
                field_name=rule.field_name,
                rule_type='schema',
                passed=passed,
                score=score,
                issues=issues,
                timestamp=datetime.utcnow(),
                severity=rule.severity
            )
            
        except Exception as e:
            self.logger.error(f"Error checking schema: {e}")
            raise
    
    def _check_data_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected type"""
        if expected_type == 'string':
            return isinstance(value, str)
        elif expected_type == 'integer':
            return isinstance(value, int) or (isinstance(value, str) and value.isdigit())
        elif expected_type == 'float':
            return isinstance(value, (int, float)) or (isinstance(value, str) and self._is_float(value))
        elif expected_type == 'boolean':
            return isinstance(value, bool) or value in ['true', 'false', 'True', 'False', 1, 0]
        elif expected_type == 'datetime':
            return isinstance(value, str) and self._is_datetime(value)
        else:
            return True  # Unknown type, assume valid
    
    def _is_float(self, value: str) -> bool:
        """Check if string can be converted to float"""
        try:
            float(value)
            return True
        except ValueError:
            return False
    
    def _is_datetime(self, value: str) -> bool:
        """Check if string can be parsed as datetime"""
        try:
            from dateutil import parser
            parser.parse(value)
            return True
        except:
            return False
    
    async def _check_range(self, rule: QualityRule, data: List[Dict[str, Any]]) -> QualityResult:
        """Check if values are within expected range"""
        try:
            field_name = rule.field_name
            min_value = rule.parameters.get('min_value')
            max_value = rule.parameters.get('max_value')
            
            out_of_range_count = 0
            total_values = 0
            
            for record in data:
                if field_name in record and record[field_name] is not None:
                    try:
                        value = float(record[field_name])
                        total_values += 1
                        
                        if min_value is not None and value < min_value:
                            out_of_range_count += 1
                        elif max_value is not None and value > max_value:
                            out_of_range_count += 1
                    except (ValueError, TypeError):
                        continue
            
            out_of_range_pct = out_of_range_count / total_values if total_values > 0 else 0
            max_out_of_range_pct = rule.parameters.get('max_out_of_range_pct', 0.05)
            passed = out_of_range_pct <= max_out_of_range_pct
            score = 1 - out_of_range_pct
            
            issues = []
            if not passed:
                issues.append({
                    'type': 'out_of_range',
                    'out_of_range_count': out_of_range_count,
                    'out_of_range_pct': out_of_range_pct,
                    'max_allowed': max_out_of_range_pct,
                    'min_value': min_value,
                    'max_value': max_value
                })
            
            return QualityResult(
                rule_id=rule.rule_id,
                field_name=field_name,
                rule_type='range',
                passed=passed,
                score=score,
                issues=issues,
                timestamp=datetime.utcnow(),
                severity=rule.severity
            )
            
        except Exception as e:
            self.logger.error(f"Error checking range: {e}")
            raise
    
    async def _check_format(self, rule: QualityRule, data: List[Dict[str, Any]]) -> QualityResult:
        """Check if values match expected format"""
        try:
            field_name = rule.field_name
            pattern = rule.parameters.get('pattern')
            format_type = rule.parameters.get('format_type', 'regex')
            
            if not pattern:
                return QualityResult(
                    rule_id=rule.rule_id,
                    field_name=field_name,
                    rule_type='format',
                    passed=True,
                    score=1.0,
                    issues=[],
                    timestamp=datetime.utcnow(),
                    severity=rule.severity
                )
            
            format_violations = 0
            total_values = 0
            
            for record in data:
                if field_name in record and record[field_name] is not None:
                    value = str(record[field_name])
                    total_values += 1
                    
                    if format_type == 'regex':
                        import re
                        if not re.match(pattern, value):
                            format_violations += 1
                    elif format_type == 'email':
                        import re
                        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                        if not re.match(email_pattern, value):
                            format_violations += 1
                    elif format_type == 'url':
                        import re
                        url_pattern = r'^https?://(?:[-\w.])+(?:[:\d]+)?(?:/(?:[\w/_.])*(?:\?(?:[\w&=%.])*)?(?:#(?:[\w.])*)?)?$'
                        if not re.match(url_pattern, value):
                            format_violations += 1
            
            format_violation_pct = format_violations / total_values if total_values > 0 else 0
            max_violation_pct = rule.parameters.get('max_violation_pct', 0.05)
            passed = format_violation_pct <= max_violation_pct
            score = 1 - format_violation_pct
            
            issues = []
            if not passed:
                issues.append({
                    'type': 'format_violations',
                    'violation_count': format_violations,
                    'violation_pct': format_violation_pct,
                    'max_allowed': max_violation_pct,
                    'format_type': format_type,
                    'pattern': pattern
                })
            
            return QualityResult(
                rule_id=rule.rule_id,
                field_name=field_name,
                rule_type='format',
                passed=passed,
                score=score,
                issues=issues,
                timestamp=datetime.utcnow(),
                severity=rule.severity
            )
            
        except Exception as e:
            self.logger.error(f"Error checking format: {e}")
            raise
    
    async def _store_quality_result(self, result: QualityResult):
        """Store quality check result"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            # Store result
            await redis.lpush(
                f"quality_results:{result.rule_id}",
                json.dumps({
                    'rule_id': result.rule_id,
                    'field_name': result.field_name,
                    'rule_type': result.rule_type,
                    'passed': result.passed,
                    'score': result.score,
                    'issues': result.issues,
                    'timestamp': result.timestamp.isoformat(),
                    'severity': result.severity
                })
            )
            
            # Keep only recent results
            await redis.ltrim(f"quality_results:{result.rule_id}", 0, 999)
            
            # Update quality history
            if result.rule_id not in self.quality_history:
                self.quality_history[result.rule_id] = []
            self.quality_history[result.rule_id].append(result)
            
            # Keep only recent history
            if len(self.quality_history[result.rule_id]) > 1000:
                self.quality_history[result.rule_id] = self.quality_history[result.rule_id][-1000:]
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing quality result: {e}")
    
    async def get_quality_summary(self) -> Dict[str, Any]:
        """Get summary of all quality checks"""
        try:
            summary = {
                'total_rules': len(self.rules),
                'active_rules': sum(1 for rule in self.rules.values() if rule.enabled),
                'total_checks': 0,
                'passed_checks': 0,
                'failed_checks': 0,
                'overall_score': 0.0,
                'rule_details': {}
            }
            
            for rule_id, rule in self.rules.items():
                redis = await aioredis.from_url(self.redis_url)
                results = await redis.lrange(f"quality_results:{rule_id}", 0, 999)
                await redis.close()
                
                if results:
                    parsed_results = [json.loads(result) for result in results]
                    total_checks = len(parsed_results)
                    passed_checks = sum(1 for result in parsed_results if result['passed'])
                    failed_checks = total_checks - passed_checks
                    avg_score = np.mean([result['score'] for result in parsed_results])
                    
                    summary['rule_details'][rule_id] = {
                        'field_name': rule.field_name,
                        'rule_type': rule.rule_type,
                        'severity': rule.severity,
                        'enabled': rule.enabled,
                        'total_checks': total_checks,
                        'passed_checks': passed_checks,
                        'failed_checks': failed_checks,
                        'avg_score': avg_score,
                        'last_check': parsed_results[0]['timestamp'] if parsed_results else None
                    }
                    
                    summary['total_checks'] += total_checks
                    summary['passed_checks'] += passed_checks
                    summary['failed_checks'] += failed_checks
            
            if summary['total_checks'] > 0:
                summary['overall_score'] = summary['passed_checks'] / summary['total_checks']
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting quality summary: {e}")
            return {}
    
    async def start_quality_monitoring(self):
        """Start continuous quality monitoring"""
        self.running = True
        self.logger.info("Starting quality monitoring")
        
        while self.running:
            try:
                # Get data from Redis queues and check quality
                redis = await aioredis.from_url(self.redis_url)
                
                # Get all dataset keys
                keys = await redis.keys("data_queue:*")
                
                for key in keys:
                    dataset_name = key.decode().replace("data_queue:", "")
                    data = await redis.lrange(key, 0, 999)  # Get recent data
                    
                    if data:
                        # Parse data
                        parsed_data = []
                        for data_str in data:
                            try:
                                parsed_data.append(json.loads(data_str))
                            except:
                                continue
                        
                        if parsed_data:
                            # Check quality
                            results = await self.check_data_quality(dataset_name, parsed_data)
                            
                            # Log critical issues
                            for result in results:
                                if not result.passed and result.severity in ['error', 'critical']:
                                    self.logger.error(f"Quality issue in {dataset_name}: {result.rule_id} - {result.issues}")
                
                await redis.close()
                
                # Wait before next check
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in quality monitoring: {e}")
                await asyncio.sleep(60)
    
    async def stop_quality_monitoring(self):
        """Stop quality monitoring"""
        self.running = False
        self.logger.info("Stopping quality monitoring")
