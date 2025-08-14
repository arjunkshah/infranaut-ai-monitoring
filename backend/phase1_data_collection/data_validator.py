import json
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import pandas as pd
import numpy as np
from jsonschema import validate, ValidationError
import re

class DataValidator:
    """
    Data validation and quality assurance for AI model monitoring
    Validates format, range, consistency, and detects anomalies
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.validation_schemas: Dict[str, Dict] = {}
        self.data_rules: Dict[str, List[Dict]] = {}
        self.anomaly_thresholds: Dict[str, Dict] = {}
        
    def add_validation_schema(self, data_type: str, schema: Dict[str, Any]):
        """Add a JSON schema for data validation"""
        self.validation_schemas[data_type] = schema
        self.logger.info(f"Added validation schema for {data_type}")
    
    def add_data_rules(self, data_type: str, rules: List[Dict[str, Any]]):
        """Add custom validation rules for a data type"""
        self.data_rules[data_type] = rules
        self.logger.info(f"Added {len(rules)} validation rules for {data_type}")
    
    def set_anomaly_thresholds(self, data_type: str, thresholds: Dict[str, Any]):
        """Set anomaly detection thresholds for a data type"""
        self.anomaly_thresholds[data_type] = thresholds
        self.logger.info(f"Set anomaly thresholds for {data_type}")
    
    def validate_data(self, data: Any, data_type: str) -> Tuple[bool, List[str]]:
        """
        Validate data against schema and rules
        Returns (is_valid, list_of_errors)
        """
        errors = []
        
        # Schema validation
        if data_type in self.validation_schemas:
            try:
                validate(instance=data, schema=self.validation_schemas[data_type])
            except ValidationError as e:
                errors.append(f"Schema validation failed: {e.message}")
        
        # Custom rules validation
        if data_type in self.data_rules:
            rule_errors = self._validate_custom_rules(data, data_type)
            errors.extend(rule_errors)
        
        # Anomaly detection
        if data_type in self.anomaly_thresholds:
            anomaly_errors = self._detect_anomalies(data, data_type)
            errors.extend(anomaly_errors)
        
        is_valid = len(errors) == 0
        return is_valid, errors
    
    def _validate_custom_rules(self, data: Any, data_type: str) -> List[str]:
        """Validate data against custom rules"""
        errors = []
        rules = self.data_rules.get(data_type, [])
        
        for rule in rules:
            rule_type = rule.get('type')
            field = rule.get('field')
            value = rule.get('value')
            
            if rule_type == 'required':
                if not self._field_exists(data, field):
                    errors.append(f"Required field '{field}' is missing")
            
            elif rule_type == 'not_null':
                if self._field_exists(data, field) and self._get_field_value(data, field) is None:
                    errors.append(f"Field '{field}' cannot be null")
            
            elif rule_type == 'min_length':
                field_value = self._get_field_value(data, field)
                if field_value and len(str(field_value)) < value:
                    errors.append(f"Field '{field}' must be at least {value} characters")
            
            elif rule_type == 'max_length':
                field_value = self._get_field_value(data, field)
                if field_value and len(str(field_value)) > value:
                    errors.append(f"Field '{field}' must be at most {value} characters")
            
            elif rule_type == 'min_value':
                field_value = self._get_field_value(data, field)
                if field_value is not None and field_value < value:
                    errors.append(f"Field '{field}' must be at least {value}")
            
            elif rule_type == 'max_value':
                field_value = self._get_field_value(data, field)
                if field_value is not None and field_value > value:
                    errors.append(f"Field '{field}' must be at most {value}")
            
            elif rule_type == 'regex':
                field_value = self._get_field_value(data, field)
                if field_value and not re.match(value, str(field_value)):
                    errors.append(f"Field '{field}' does not match pattern {value}")
            
            elif rule_type == 'enum':
                field_value = self._get_field_value(data, field)
                if field_value not in value:
                    errors.append(f"Field '{field}' must be one of {value}")
            
            elif rule_type == 'data_type':
                field_value = self._get_field_value(data, field)
                if field_value is not None:
                    expected_type = value
                    if expected_type == 'number' and not isinstance(field_value, (int, float)):
                        errors.append(f"Field '{field}' must be a number")
                    elif expected_type == 'string' and not isinstance(field_value, str):
                        errors.append(f"Field '{field}' must be a string")
                    elif expected_type == 'boolean' and not isinstance(field_value, bool):
                        errors.append(f"Field '{field}' must be a boolean")
                    elif expected_type == 'array' and not isinstance(field_value, list):
                        errors.append(f"Field '{field}' must be an array")
                    elif expected_type == 'object' and not isinstance(field_value, dict):
                        errors.append(f"Field '{field}' must be an object")
        
        return errors
    
    def _detect_anomalies(self, data: Any, data_type: str) -> List[str]:
        """Detect anomalies in data based on thresholds"""
        errors = []
        thresholds = self.anomaly_thresholds.get(data_type, {})
        
        for field, threshold_config in thresholds.items():
            field_value = self._get_field_value(data, field)
            if field_value is None:
                continue
            
            # Statistical anomaly detection
            if 'z_score_threshold' in threshold_config:
                z_score = self._calculate_z_score(field_value, threshold_config.get('mean', 0), threshold_config.get('std', 1))
                if abs(z_score) > threshold_config['z_score_threshold']:
                    errors.append(f"Anomaly detected in field '{field}': z-score {z_score:.2f}")
            
            # Range-based anomaly detection
            if 'min_value' in threshold_config and field_value < threshold_config['min_value']:
                errors.append(f"Anomaly detected in field '{field}': value {field_value} below minimum {threshold_config['min_value']}")
            
            if 'max_value' in threshold_config and field_value > threshold_config['max_value']:
                errors.append(f"Anomaly detected in field '{field}': value {field_value} above maximum {threshold_config['max_value']}")
            
            # Pattern-based anomaly detection
            if 'pattern' in threshold_config:
                if not re.match(threshold_config['pattern'], str(field_value)):
                    errors.append(f"Anomaly detected in field '{field}': value does not match expected pattern")
        
        return errors
    
    def _field_exists(self, data: Any, field: str) -> bool:
        """Check if a field exists in the data"""
        try:
            if isinstance(data, dict):
                return field in data
            elif isinstance(data, list) and field.isdigit():
                return int(field) < len(data)
            return False
        except:
            return False
    
    def _get_field_value(self, data: Any, field: str) -> Any:
        """Get the value of a field from the data"""
        try:
            if isinstance(data, dict):
                return data.get(field)
            elif isinstance(data, list) and field.isdigit():
                return data[int(field)]
            return None
        except:
            return None
    
    def _calculate_z_score(self, value: float, mean: float, std: float) -> float:
        """Calculate z-score for anomaly detection"""
        if std == 0:
            return 0
        return (value - mean) / std
    
    def validate_batch(self, data_batch: List[Dict[str, Any]], data_type: str) -> Dict[str, Any]:
        """Validate a batch of data records"""
        validation_results = {
            'total_records': len(data_batch),
            'valid_records': 0,
            'invalid_records': 0,
            'errors': [],
            'error_summary': {}
        }
        
        for i, record in enumerate(data_batch):
            is_valid, errors = self.validate_data(record, data_type)
            
            if is_valid:
                validation_results['valid_records'] += 1
            else:
                validation_results['invalid_records'] += 1
                validation_results['errors'].append({
                    'record_index': i,
                    'record_id': record.get('id', f'record_{i}'),
                    'errors': errors
                })
                
                # Count error types
                for error in errors:
                    error_type = error.split(':')[0] if ':' in error else 'Unknown'
                    validation_results['error_summary'][error_type] = validation_results['error_summary'].get(error_type, 0) + 1
        
        return validation_results
    
    def get_data_quality_metrics(self, data_batch: List[Dict[str, Any]], data_type: str) -> Dict[str, Any]:
        """Calculate data quality metrics for a batch"""
        if not data_batch:
            return {}
        
        df = pd.DataFrame(data_batch)
        metrics = {
            'total_records': len(df),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_records': df.duplicated().sum(),
            'data_types': df.dtypes.to_dict(),
            'numeric_stats': {},
            'categorical_stats': {}
        }
        
        # Calculate statistics for numeric columns
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        for col in numeric_columns:
            metrics['numeric_stats'][col] = {
                'mean': df[col].mean(),
                'std': df[col].std(),
                'min': df[col].min(),
                'max': df[col].max(),
                'median': df[col].median()
            }
        
        # Calculate statistics for categorical columns
        categorical_columns = df.select_dtypes(include=['object']).columns
        for col in categorical_columns:
            metrics['categorical_stats'][col] = {
                'unique_values': df[col].nunique(),
                'most_common': df[col].mode().iloc[0] if not df[col].mode().empty else None,
                'missing_percentage': (df[col].isnull().sum() / len(df)) * 100
            }
        
        return metrics
    
    def create_validation_report(self, validation_results: Dict[str, Any], quality_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Create a comprehensive validation report"""
        report = {
            'timestamp': datetime.utcnow().isoformat(),
            'validation_summary': {
                'total_records': validation_results['total_records'],
                'valid_records': validation_results['valid_records'],
                'invalid_records': validation_results['invalid_records'],
                'validation_rate': validation_results['valid_records'] / validation_results['total_records'] * 100 if validation_results['total_records'] > 0 else 0
            },
            'error_analysis': {
                'total_errors': len(validation_results['errors']),
                'error_types': validation_results['error_summary'],
                'top_errors': sorted(validation_results['error_summary'].items(), key=lambda x: x[1], reverse=True)[:5]
            },
            'data_quality_metrics': quality_metrics,
            'recommendations': self._generate_recommendations(validation_results, quality_metrics)
        }
        
        return report
    
    def _generate_recommendations(self, validation_results: Dict[str, Any], quality_metrics: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on validation results and quality metrics"""
        recommendations = []
        
        # Validation rate recommendations
        validation_rate = validation_results['valid_records'] / validation_results['total_records'] * 100 if validation_results['total_records'] > 0 else 0
        if validation_rate < 90:
            recommendations.append(f"Low validation rate ({validation_rate:.1f}%). Review data quality and validation rules.")
        
        # Missing values recommendations
        if 'missing_values' in quality_metrics:
            for field, missing_count in quality_metrics['missing_values'].items():
                missing_percentage = (missing_count / quality_metrics['total_records']) * 100
                if missing_percentage > 20:
                    recommendations.append(f"High missing values in field '{field}' ({missing_percentage:.1f}%). Consider data imputation or source investigation.")
        
        # Duplicate records recommendations
        if quality_metrics.get('duplicate_records', 0) > 0:
            recommendations.append(f"Found {quality_metrics['duplicate_records']} duplicate records. Consider deduplication strategy.")
        
        return recommendations
