import os
import json
import sys
from datetime import datetime
from typing import Dict, List, Any

def verify_phase4_structure() -> Dict[str, Any]:
    """Verify Phase 4 backend structure and files"""
    
    verification_result = {
        'phase': 'Phase 4 Backend',
        'timestamp': datetime.utcnow().isoformat(),
        'overall_status': 'PENDING',
        'files_checked': [],
        'missing_files': [],
        'file_sizes': {},
        'requirements_check': {},
        'structure_score': 0,
        'recommendations': []
    }
    
    # Expected files for Phase 4
    expected_files = [
        'services/alerting_system.py',
        'services/reporting_analytics.py', 
        'services/root_cause_analysis.py',
        'alerting_reporting_orchestrator.py',
        'requirements.txt',
        'test_phase4_complete.py',
        'verify_phase4_structure.py'
    ]
    
    # Check each expected file
    files_found = 0
    total_files = len(expected_files)
    
    for file_path in expected_files:
        if os.path.exists(file_path):
            files_found += 1
            file_size = os.path.getsize(file_path)
            verification_result['files_checked'].append({
                'file': file_path,
                'status': 'FOUND',
                'size_bytes': file_size,
                'size_kb': round(file_size / 1024, 2)
            })
            verification_result['file_sizes'][file_path] = file_size
            
            # Check if file has content
            if file_size == 0:
                verification_result['recommendations'].append(f"File {file_path} is empty")
        else:
            verification_result['missing_files'].append(file_path)
            verification_result['files_checked'].append({
                'file': file_path,
                'status': 'MISSING',
                'size_bytes': 0,
                'size_kb': 0
            })
    
    # Check requirements.txt content
    if os.path.exists('requirements.txt'):
        try:
            with open('requirements.txt', 'r') as f:
                requirements_content = f.read()
                requirements_lines = requirements_content.strip().split('\n')
                
                verification_result['requirements_check'] = {
                    'file_exists': True,
                    'total_lines': len(requirements_lines),
                    'non_empty_lines': len([line for line in requirements_lines if line.strip() and not line.startswith('#')]),
                    'has_core_dependencies': any('numpy' in line for line in requirements_lines),
                    'has_visualization': any('matplotlib' in line for line in requirements_lines),
                    'has_alerting': any('requests' in line for line in requirements_lines),
                    'has_reporting': any('pandas' in line for line in requirements_lines)
                }
        except Exception as e:
            verification_result['requirements_check'] = {
                'file_exists': True,
                'error': str(e)
            }
    else:
        verification_result['requirements_check'] = {
            'file_exists': False
        }
    
    # Calculate structure score
    structure_score = (files_found / total_files) * 100
    verification_result['structure_score'] = structure_score
    
    # Determine overall status
    if structure_score >= 90:
        verification_result['overall_status'] = 'COMPLETE'
        verification_result['status_message'] = 'Phase 4 Backend structure is complete and ready for testing!'
    elif structure_score >= 75:
        verification_result['overall_status'] = 'MOSTLY_COMPLETE'
        verification_result['status_message'] = 'Phase 4 Backend structure is mostly complete with minor issues.'
    elif structure_score >= 50:
        verification_result['overall_status'] = 'PARTIALLY_COMPLETE'
        verification_result['status_message'] = 'Phase 4 Backend structure is partially complete but needs work.'
    else:
        verification_result['overall_status'] = 'INCOMPLETE'
        verification_result['status_message'] = 'Phase 4 Backend structure is incomplete and needs significant work.'
    
    # Generate recommendations
    if verification_result['missing_files']:
        verification_result['recommendations'].append(f"Missing files: {', '.join(verification_result['missing_files'])}")
    
    if structure_score < 100:
        verification_result['recommendations'].append("Complete all missing files to achieve 100% structure score")
    
    # Check for minimum file sizes (indicating proper content)
    min_file_sizes = {
        'services/alerting_system.py': 5000,  # Should be substantial
        'services/reporting_analytics.py': 5000,
        'services/root_cause_analysis.py': 5000,
        'alerting_reporting_orchestrator.py': 3000,
        'test_phase4_complete.py': 2000
    }
    
    for file_path, min_size in min_file_sizes.items():
        if file_path in verification_result['file_sizes']:
            if verification_result['file_sizes'][file_path] < min_size:
                verification_result['recommendations'].append(f"File {file_path} seems too small - may need more content")
    
    # Add general recommendations
    verification_result['recommendations'].extend([
        "Run the test suite to verify functionality",
        "Check that all imports are working correctly",
        "Verify Redis connection for testing",
        "Ensure all dependencies are installed"
    ])
    
    return verification_result

def check_code_quality() -> Dict[str, Any]:
    """Basic code quality checks"""
    
    quality_result = {
        'timestamp': datetime.utcnow().isoformat(),
        'quality_score': 0,
        'issues_found': [],
        'files_analyzed': 0
    }
    
    # Files to analyze
    python_files = [
        'services/alerting_system.py',
        'services/reporting_analytics.py',
        'services/root_cause_analysis.py',
        'alerting_reporting_orchestrator.py',
        'test_phase4_complete.py'
    ]
    
    total_issues = 0
    files_analyzed = 0
    
    for file_path in python_files:
        if os.path.exists(file_path):
            files_analyzed += 1
            file_issues = []
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    lines = content.split('\n')
                    
                    # Basic quality checks
                    if len(lines) < 10:
                        file_issues.append("File seems too short")
                    
                    # Check for imports
                    if 'import' not in content and 'from' not in content:
                        file_issues.append("No imports found")
                    
                    # Check for classes
                    if 'class ' not in content:
                        file_issues.append("No classes found")
                    
                    # Check for async functions
                    if 'async def' not in content:
                        file_issues.append("No async functions found")
                    
                    # Check for docstrings
                    if '"""' not in content and "'''" not in content:
                        file_issues.append("No docstrings found")
                    
                    if file_issues:
                        quality_result['issues_found'].append({
                            'file': file_path,
                            'issues': file_issues
                        })
                        total_issues += len(file_issues)
                        
            except Exception as e:
                quality_result['issues_found'].append({
                    'file': file_path,
                    'issues': [f"Error reading file: {str(e)}"]
                })
                total_issues += 1
    
    quality_result['files_analyzed'] = files_analyzed
    
    # Calculate quality score (inverse of issues)
    if files_analyzed > 0:
        avg_issues_per_file = total_issues / files_analyzed
        quality_score = max(0, 100 - (avg_issues_per_file * 20))  # 5 issues per file = 0 score
        quality_result['quality_score'] = round(quality_score, 1)
    
    return quality_result

def main():
    """Main verification function"""
    
    print("="*80)
    print("PHASE 4 BACKEND STRUCTURE VERIFICATION")
    print("="*80)
    
    # Run structure verification
    structure_result = verify_phase4_structure()
    
    print(f"\nStructure Verification Results:")
    print(f"Overall Status: {structure_result['overall_status']}")
    print(f"Structure Score: {structure_result['structure_score']:.1f}%")
    print(f"Files Found: {len([f for f in structure_result['files_checked'] if f['status'] == 'FOUND'])}/{len(structure_result['files_checked'])}")
    print(f"Status Message: {structure_result['status_message']}")
    
    print(f"\nFiles Checked:")
    for file_check in structure_result['files_checked']:
        status_icon = "✅" if file_check['status'] == 'FOUND' else "❌"
        print(f"  {status_icon} {file_check['file']} ({file_check['size_kb']} KB)")
    
    if structure_result['missing_files']:
        print(f"\nMissing Files:")
        for missing_file in structure_result['missing_files']:
            print(f"  ❌ {missing_file}")
    
    # Run quality check
    quality_result = check_code_quality()
    
    print(f"\nCode Quality Results:")
    print(f"Quality Score: {quality_result['quality_score']:.1f}%")
    print(f"Files Analyzed: {quality_result['files_analyzed']}")
    print(f"Total Issues Found: {len(quality_result['issues_found'])}")
    
    if quality_result['issues_found']:
        print(f"\nQuality Issues:")
        for issue_group in quality_result['issues_found']:
            print(f"  📁 {issue_group['file']}:")
            for issue in issue_group['issues']:
                print(f"    ⚠️  {issue}")
    
    # Requirements check
    req_check = structure_result['requirements_check']
    if req_check.get('file_exists'):
        print(f"\nRequirements.txt Analysis:")
        print(f"  Total Lines: {req_check.get('total_lines', 'N/A')}")
        print(f"  Non-empty Lines: {req_check.get('non_empty_lines', 'N/A')}")
        print(f"  Has Core Dependencies: {'✅' if req_check.get('has_core_dependencies') else '❌'}")
        print(f"  Has Visualization: {'✅' if req_check.get('has_visualization') else '❌'}")
        print(f"  Has Alerting: {'✅' if req_check.get('has_alerting') else '❌'}")
        print(f"  Has Reporting: {'✅' if req_check.get('has_reporting') else '❌'}")
    else:
        print(f"\nRequirements.txt: ❌ NOT FOUND")
    
    # Recommendations
    if structure_result['recommendations']:
        print(f"\nRecommendations:")
        for i, rec in enumerate(structure_result['recommendations'], 1):
            print(f"  {i}. {rec}")
    
    print(f"\nTimestamp: {structure_result['timestamp']}")
    print("="*80)
    
    # Save verification report
    verification_report = {
        'structure_verification': structure_result,
        'quality_check': quality_result,
        'summary': {
            'structure_score': structure_result['structure_score'],
            'quality_score': quality_result['quality_score'],
            'overall_status': structure_result['overall_status'],
            'total_files_expected': len(structure_result['files_checked']),
            'files_found': len([f for f in structure_result['files_checked'] if f['status'] == 'FOUND'])
        }
    }
    
    with open('PHASE4_VERIFICATION_REPORT.json', 'w') as f:
        json.dump(verification_report, f, indent=2)
    
    print(f"\nDetailed verification report saved to: PHASE4_VERIFICATION_REPORT.json")
    
    return verification_report

if __name__ == "__main__":
    main()
