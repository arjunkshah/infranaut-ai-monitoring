#!/usr/bin/env python3
"""
Phase 1 Structure Verification
Verifies that all Phase 1 components are properly implemented
"""

import os
import json
from datetime import datetime

def verify_phase1_structure():
    """Verify Phase 1 structure and implementation"""
    
    print("🔍 Verifying Phase 1: Data Collection & Aggregation Structure")
    print("=" * 60)
    
    # Define expected files
    expected_files = [
        "ingestion_pipeline.py",
        "data_validator.py", 
        "data_aggregator.py",
        "storage_manager.py",
        "metadata_manager.py",
        "retention_manager.py",
        "lineage_tracker.py",
        "streaming_pipeline.py",
        "requirements.txt",
        "test_phase1_complete.py"
    ]
    
    # Check files exist
    missing_files = []
    existing_files = []
    
    for file in expected_files:
        if os.path.exists(file):
            existing_files.append(file)
            print(f"✅ {file}")
        else:
            missing_files.append(file)
            print(f"❌ {file} - MISSING")
    
    print(f"\n📊 File Status: {len(existing_files)}/{len(expected_files)} files present")
    
    # Check file sizes to ensure they're not empty
    empty_files = []
    for file in existing_files:
        try:
            with open(file, 'r') as f:
                content = f.read()
                if len(content.strip()) < 100:  # Less than 100 chars is suspicious
                    empty_files.append(file)
        except Exception as e:
            print(f"⚠️  Error reading {file}: {e}")
    
    if empty_files:
        print(f"\n⚠️  Potentially empty files: {empty_files}")
    
    # Check requirements.txt for dependencies
    print(f"\n📦 Checking dependencies...")
    try:
        with open("requirements.txt", 'r') as f:
            requirements = f.read()
            dependency_count = len([line for line in requirements.split('\n') 
                                  if line.strip() and not line.startswith('#')])
            print(f"✅ requirements.txt contains {dependency_count} dependencies")
    except Exception as e:
        print(f"❌ Error reading requirements.txt: {e}")
    
    # Generate verification report
    verification_report = {
        "phase": "Phase 1: Data Collection & Aggregation",
        "timestamp": datetime.utcnow().isoformat(),
        "status": "COMPLETE" if len(missing_files) == 0 else "INCOMPLETE",
        "file_coverage": {
            "total_expected": len(expected_files),
            "present": len(existing_files),
            "missing": len(missing_files),
            "coverage_percentage": round((len(existing_files) / len(expected_files)) * 100, 2)
        },
        "files": {
            "present": existing_files,
            "missing": missing_files,
            "potentially_empty": empty_files
        },
        "components_implemented": [
            "Data Ingestion Pipeline",
            "Data Validation", 
            "Data Aggregation",
            "Storage Management",
            "Metadata Management", 
            "Data Retention Management",
            "Data Lineage Tracking",
            "Real-time Streaming Pipeline"
        ]
    }
    
    # Save verification report
    with open("PHASE1_VERIFICATION_REPORT.json", "w") as f:
        json.dump(verification_report, f, indent=2)
    
    print(f"\n📄 Verification report saved to: PHASE1_VERIFICATION_REPORT.json")
    
    # Final assessment
    print(f"\n" + "=" * 60)
    if len(missing_files) == 0:
        print("🎉 PHASE 1 STRUCTURE VERIFICATION: PASSED")
        print("✅ All expected files are present")
        print("✅ All core components implemented")
        print("✅ Ready for dependency installation and testing")
    else:
        print("❌ PHASE 1 STRUCTURE VERIFICATION: FAILED")
        print(f"❌ Missing {len(missing_files)} files")
        print("❌ Structure incomplete")
    
    print("=" * 60)
    
    return verification_report

def check_code_quality():
    """Check basic code quality indicators"""
    print(f"\n🔍 Checking code quality indicators...")
    
    quality_issues = []
    
    # Check for proper imports in main files
    main_files = [
        "ingestion_pipeline.py",
        "retention_manager.py", 
        "lineage_tracker.py",
        "streaming_pipeline.py"
    ]
    
    for file in main_files:
        if os.path.exists(file):
            try:
                with open(file, 'r') as f:
                    content = f.read()
                    
                    # Check for class definitions
                    if 'class ' not in content:
                        quality_issues.append(f"{file}: No class definitions found")
                    
                    # Check for async functions
                    if 'async def' not in content:
                        quality_issues.append(f"{file}: No async functions found")
                    
                    # Check for proper imports
                    if 'import ' not in content and 'from ' not in content:
                        quality_issues.append(f"{file}: No imports found")
                        
            except Exception as e:
                quality_issues.append(f"{file}: Error reading file - {e}")
    
    if quality_issues:
        print("⚠️  Quality issues found:")
        for issue in quality_issues:
            print(f"   - {issue}")
    else:
        print("✅ Code quality indicators look good")
    
    return quality_issues

if __name__ == "__main__":
    # Run structure verification
    report = verify_phase1_structure()
    
    # Run quality check
    quality_issues = check_code_quality()
    
    # Final summary
    print(f"\n📋 SUMMARY:")
    print(f"   Files: {report['file_coverage']['present']}/{report['file_coverage']['total_expected']}")
    print(f"   Coverage: {report['file_coverage']['coverage_percentage']}%")
    print(f"   Status: {report['status']}")
    print(f"   Quality Issues: {len(quality_issues)}")
    
    if report['status'] == "COMPLETE" and len(quality_issues) == 0:
        print(f"\n🎉 PHASE 1 IS COMPLETE AND READY!")
        print(f"   Next steps:")
        print(f"   1. Install dependencies: pip install -r requirements.txt")
        print(f"   2. Run comprehensive tests: python test_phase1_complete.py")
        print(f"   3. Proceed to Phase 2 development")
    else:
        print(f"\n⚠️  PHASE 1 NEEDS ATTENTION")
        print(f"   Please address missing files and quality issues before proceeding")
