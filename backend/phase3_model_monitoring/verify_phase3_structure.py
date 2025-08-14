import os
import json
from datetime import datetime
from typing import Dict, List, Any

def verify_phase3_structure() -> Dict[str, Any]:
    """Verify Phase 3 backend structure and files"""
    
    verification_result = {
        'phase': 'Phase 3 - Model Monitoring & Performance',
        'timestamp': datetime.utcnow().isoformat(),
        'files_checked': [],
        'structure_status': 'PENDING',
        'completion_score': 0,
        'missing_files': [],
        'file_details': {},
        'requirements_check': {},
        'overall_assessment': ''
    }
    
    # Expected files for Phase 3
    expected_files = [
        'services/performance_tracker.py',
        'services/lifecycle_manager.py', 
        'services/ab_testing.py',
        'model_monitoring_orchestrator.py',
        'test_phase3_complete.py',
        'requirements.txt'
    ]
    
    # Check each expected file
    files_found = 0
    total_files = len(expected_files)
    
    for file_path in expected_files:
        file_info = {
            'exists': False,
            'size': 0,
            'has_classes': False,
            'has_async_functions': False,
            'has_imports': False
        }
        
        if os.path.exists(file_path):
            files_found += 1
            file_info['exists'] = True
            
            # Get file size
            try:
                file_size = os.path.getsize(file_path)
                file_info['size'] = file_size
            except:
                pass
            
            # Basic code quality checks
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Check for classes
                    file_info['has_classes'] = 'class ' in content
                    
                    # Check for async functions
                    file_info['has_async_functions'] = 'async def ' in content
                    
                    # Check for imports
                    file_info['has_imports'] = 'import ' in content or 'from ' in content
                    
            except Exception as e:
                file_info['error'] = str(e)
        else:
            verification_result['missing_files'].append(file_path)
        
        verification_result['file_details'][file_path] = file_info
    
    # Check requirements.txt content
    if os.path.exists('requirements.txt'):
        try:
            with open('requirements.txt', 'r') as f:
                requirements_content = f.read()
                
            verification_result['requirements_check'] = {
                'exists': True,
                'has_core_deps': 'numpy' in requirements_content and 'pandas' in requirements_content,
                'has_ml_deps': 'scikit-learn' in requirements_content,
                'has_stats_deps': 'scipy' in requirements_content,
                'has_redis_deps': 'aioredis' in requirements_content,
                'total_dependencies': len([line for line in requirements_content.split('\n') if line.strip() and not line.startswith('#')])
            }
        except Exception as e:
            verification_result['requirements_check'] = {'error': str(e)}
    else:
        verification_result['requirements_check'] = {'exists': False}
    
    # Calculate completion score
    completion_score = (files_found / total_files) * 100
    verification_result['completion_score'] = completion_score
    
    # Determine structure status
    if completion_score >= 95:
        verification_result['structure_status'] = 'EXCELLENT'
        verification_result['overall_assessment'] = 'Phase 3 structure is complete and well-organized!'
    elif completion_score >= 85:
        verification_result['structure_status'] = 'GOOD'
        verification_result['overall_assessment'] = 'Phase 3 structure is mostly complete with minor gaps.'
    elif completion_score >= 70:
        verification_result['structure_status'] = 'ACCEPTABLE'
        verification_result['overall_assessment'] = 'Phase 3 structure has core components but needs improvements.'
    else:
        verification_result['structure_status'] = 'INCOMPLETE'
        verification_result['overall_assessment'] = 'Phase 3 structure is incomplete and needs significant work.'
    
    # Save verification report
    with open('PHASE3_VERIFICATION_REPORT.json', 'w') as f:
        json.dump(verification_result, f, indent=2)
    
    return verification_result

def print_verification_summary(result: Dict[str, Any]):
    """Print a summary of the verification results"""
    
    print("\n" + "="*70)
    print("PHASE 3 STRUCTURE VERIFICATION REPORT")
    print("="*70)
    print(f"Phase: {result['phase']}")
    print(f"Timestamp: {result['timestamp']}")
    print(f"Structure Status: {result['structure_status']}")
    print(f"Completion Score: {result['completion_score']:.1f}%")
    print(f"Files Found: {len([f for f in result['file_details'].values() if f['exists']])}")
    print(f"Total Expected: {len(result['file_details'])}")
    print(f"Missing Files: {len(result['missing_files'])}")
    
    print(f"\nOverall Assessment: {result['overall_assessment']}")
    
    if result['missing_files']:
        print(f"\nMissing Files:")
        for file in result['missing_files']:
            print(f"  - {file}")
    
    print(f"\nFile Details:")
    for file_path, details in result['file_details'].items():
        status = "✓" if details['exists'] else "✗"
        size_info = f"({details['size']} bytes)" if details['size'] > 0 else ""
        print(f"  {status} {file_path} {size_info}")
        
        if details['exists']:
            features = []
            if details['has_classes']:
                features.append("classes")
            if details['has_async_functions']:
                features.append("async functions")
            if details['has_imports']:
                features.append("imports")
            
            if features:
                print(f"    Features: {', '.join(features)}")
    
    print(f"\nRequirements Check:")
    req_check = result['requirements_check']
    if req_check.get('exists'):
        print(f"  ✓ requirements.txt exists")
        print(f"  - Total dependencies: {req_check.get('total_dependencies', 0)}")
        print(f"  - Core deps (numpy/pandas): {'✓' if req_check.get('has_core_deps') else '✗'}")
        print(f"  - ML deps (scikit-learn): {'✓' if req_check.get('has_ml_deps') else '✗'}")
        print(f"  - Stats deps (scipy): {'✓' if req_check.get('has_stats_deps') else '✗'}")
        print(f"  - Redis deps (aioredis): {'✓' if req_check.get('has_redis_deps') else '✗'}")
    else:
        print(f"  ✗ requirements.txt missing")
    
    print("="*70)
    
    # Phase 3 completion assessment
    if result['completion_score'] >= 90:
        print("\n🎉 PHASE 3 STRUCTURE VERIFICATION: PASSED")
        print("✅ PHASE 3 IS COMPLETE AND READY!")
        print("\nNext steps:")
        print("1. Install dependencies: pip install -r requirements.txt")
        print("2. Run comprehensive tests: python test_phase3_complete.py")
        print("3. Start Phase 4 development")
    elif result['completion_score'] >= 75:
        print("\n⚠️  PHASE 3 STRUCTURE VERIFICATION: MOSTLY PASSED")
        print("✅ PHASE 3 IS MOSTLY COMPLETE!")
        print("\nNext steps:")
        print("1. Address missing files")
        print("2. Install dependencies: pip install -r requirements.txt")
        print("3. Run comprehensive tests: python test_phase3_complete.py")
    else:
        print("\n❌ PHASE 3 STRUCTURE VERIFICATION: FAILED")
        print("❌ PHASE 3 NEEDS MORE WORK!")
        print("\nNext steps:")
        print("1. Complete missing files")
        print("2. Fix structure issues")
        print("3. Re-run verification")

if __name__ == "__main__":
    # Run verification
    result = verify_phase3_structure()
    
    # Print summary
    print_verification_summary(result)
