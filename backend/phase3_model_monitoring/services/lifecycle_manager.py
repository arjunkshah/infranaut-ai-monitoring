import asyncio
import json
import logging
import hashlib
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import redis.asyncio as aioredis
import numpy as np

class ModelStatus(Enum):
    """Model deployment status"""
    DRAFT = "draft"
    TRAINING = "training"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    DEPRECATED = "deprecated"
    FAILED = "failed"

class DeploymentStatus(Enum):
    """Deployment status"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"

@dataclass
class ModelVersion:
    """Model version information"""
    model_id: str
    version: str
    model_name: str
    model_type: str
    created_at: datetime
    created_by: str
    description: str
    status: ModelStatus
    artifact_path: str
    metadata: Dict[str, Any]
    performance_baseline: Dict[str, float]
    dependencies: Dict[str, str]

@dataclass
class Deployment:
    """Model deployment information"""
    deployment_id: str
    model_id: str
    version: str
    environment: str  # 'staging', 'production'
    status: DeploymentStatus
    deployed_at: datetime
    deployed_by: str
    deployment_config: Dict[str, Any]
    rollback_version: Optional[str] = None
    rollback_reason: Optional[str] = None

@dataclass
class RetrainingTrigger:
    """Retraining trigger configuration"""
    trigger_id: str
    model_id: str
    trigger_type: str  # 'performance', 'data_drift', 'scheduled', 'manual'
    conditions: Dict[str, Any]
    enabled: bool = True
    last_triggered: Optional[datetime] = None

class ModelLifecycleManager:
    """
    Comprehensive model lifecycle management system
    Handles versioning, deployment tracking, retraining triggers, and rollback capabilities
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.models: Dict[str, List[ModelVersion]] = {}
        self.deployments: Dict[str, List[Deployment]] = {}
        self.retraining_triggers: Dict[str, RetrainingTrigger] = {}
        self.running = False
        self.logger = logging.getLogger(__name__)
        
    async def register_model_version(self, model_version: ModelVersion) -> bool:
        """Register a new model version"""
        try:
            if model_version.model_id not in self.models:
                self.models[model_version.model_id] = []
            
            # Check if version already exists
            existing_versions = [v.version for v in self.models[model_version.model_id]]
            if model_version.version in existing_versions:
                raise ValueError(f"Version {model_version.version} already exists for model {model_version.model_id}")
            
            # Add version
            self.models[model_version.model_id].append(model_version)
            
            # Store in Redis
            await self._store_model_version(model_version)
            
            self.logger.info(f"Registered model version: {model_version.model_id}:{model_version.version}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to register model version: {e}")
            return False
    
    async def deploy_model(self, model_id: str, version: str, environment: str, 
                          deployed_by: str, deployment_config: Dict[str, Any]) -> str:
        """Deploy a model version to an environment"""
        try:
            # Find model version
            model_version = await self._get_model_version(model_id, version)
            if not model_version:
                raise ValueError(f"Model version not found: {model_id}:{version}")
            
            # Create deployment
            deployment_id = f"{model_id}_{version}_{environment}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
            deployment = Deployment(
                deployment_id=deployment_id,
                model_id=model_id,
                version=version,
                environment=environment,
                status=DeploymentStatus.IN_PROGRESS,
                deployed_at=datetime.utcnow(),
                deployed_by=deployed_by,
                deployment_config=deployment_config
            )
            
            # Store deployment
            await self._store_deployment(deployment)
            
            # Update model version status
            model_version.status = ModelStatus.STAGING if environment == 'staging' else ModelStatus.PRODUCTION
            await self._update_model_version_status(model_id, version, model_version.status)
            
            # Simulate deployment process
            await self._simulate_deployment(deployment)
            
            self.logger.info(f"Deployed model: {model_id}:{version} to {environment}")
            return deployment_id
            
        except Exception as e:
            self.logger.error(f"Failed to deploy model: {e}")
            raise
    
    async def rollback_deployment(self, deployment_id: str, reason: str, rolled_back_by: str) -> bool:
        """Rollback a deployment"""
        try:
            # Get deployment
            deployment = await self._get_deployment(deployment_id)
            if not deployment:
                raise ValueError(f"Deployment not found: {deployment_id}")
            
            # Check if rollback version exists
            if not deployment.rollback_version:
                raise ValueError(f"No rollback version specified for deployment: {deployment_id}")
            
            # Create rollback deployment
            rollback_deployment = Deployment(
                deployment_id=f"{deployment.model_id}_{deployment.rollback_version}_{deployment.environment}_rollback_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                model_id=deployment.model_id,
                version=deployment.rollback_version,
                environment=deployment.environment,
                status=DeploymentStatus.IN_PROGRESS,
                deployed_at=datetime.utcnow(),
                deployed_by=rolled_back_by,
                deployment_config=deployment.deployment_config,
                rollback_version=deployment.version,
                rollback_reason=reason
            )
            
            # Update original deployment status
            deployment.status = DeploymentStatus.ROLLED_BACK
            await self._update_deployment_status(deployment_id, DeploymentStatus.ROLLED_BACK)
            
            # Store rollback deployment
            await self._store_deployment(rollback_deployment)
            
            # Simulate rollback process
            await self._simulate_deployment(rollback_deployment)
            
            self.logger.info(f"Rolled back deployment: {deployment_id} to version {deployment.rollback_version}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to rollback deployment: {e}")
            return False
    
    async def add_retraining_trigger(self, trigger: RetrainingTrigger) -> bool:
        """Add a retraining trigger"""
        try:
            self.retraining_triggers[trigger.trigger_id] = trigger
            await self._store_retraining_trigger(trigger)
            
            self.logger.info(f"Added retraining trigger: {trigger.trigger_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add retraining trigger: {e}")
            return False
    
    async def check_retraining_triggers(self, model_id: str, current_metrics: Dict[str, Any]) -> List[str]:
        """Check if any retraining triggers should be activated"""
        try:
            triggered_triggers = []
            
            for trigger_id, trigger in self.retraining_triggers.items():
                if trigger.model_id != model_id or not trigger.enabled:
                    continue
                
                should_trigger = False
                
                if trigger.trigger_type == 'performance':
                    # Check performance degradation
                    baseline = trigger.conditions.get('baseline', {})
                    threshold = trigger.conditions.get('threshold', 0.1)
                    
                    for metric, baseline_value in baseline.items():
                        current_value = current_metrics.get(metric, 0.0)
                        degradation = (baseline_value - current_value) / baseline_value if baseline_value > 0 else 0
                        
                        if degradation > threshold:
                            should_trigger = True
                            break
                
                elif trigger.trigger_type == 'data_drift':
                    # Check data drift
                    drift_score = current_metrics.get('drift_score', 0.0)
                    threshold = trigger.conditions.get('drift_threshold', 0.05)
                    
                    if drift_score > threshold:
                        should_trigger = True
                
                elif trigger.trigger_type == 'scheduled':
                    # Check scheduled retraining
                    last_triggered = trigger.last_triggered
                    interval_days = trigger.conditions.get('interval_days', 30)
                    
                    if not last_triggered or (datetime.utcnow() - last_triggered).days >= interval_days:
                        should_trigger = True
                
                if should_trigger:
                    triggered_triggers.append(trigger_id)
                    trigger.last_triggered = datetime.utcnow()
                    await self._update_retraining_trigger(trigger)
                    
                    self.logger.info(f"Retraining trigger activated: {trigger_id}")
            
            return triggered_triggers
            
        except Exception as e:
            self.logger.error(f"Error checking retraining triggers: {e}")
            return []
    
    async def get_model_lineage(self, model_id: str) -> Dict[str, Any]:
        """Get model lineage information"""
        try:
            if model_id not in self.models:
                return {}
            
            versions = self.models[model_id]
            deployments = await self._get_model_deployments(model_id)
            
            lineage = {
                'model_id': model_id,
                'total_versions': len(versions),
                'versions': [],
                'deployments': [],
                'current_production': None,
                'current_staging': None
            }
            
            # Add version information
            for version in sorted(versions, key=lambda v: v.created_at):
                lineage['versions'].append({
                    'version': version.version,
                    'status': version.status.value,
                    'created_at': version.created_at.isoformat(),
                    'created_by': version.created_by,
                    'description': version.description,
                    'performance_baseline': version.performance_baseline
                })
            
            # Add deployment information
            for deployment in deployments:
                lineage['deployments'].append({
                    'deployment_id': deployment.deployment_id,
                    'version': deployment.version,
                    'environment': deployment.environment,
                    'status': deployment.status.value,
                    'deployed_at': deployment.deployed_at.isoformat(),
                    'deployed_by': deployment.deployed_by
                })
                
                # Track current deployments
                if deployment.status == DeploymentStatus.SUCCESS:
                    if deployment.environment == 'production':
                        lineage['current_production'] = deployment.version
                    elif deployment.environment == 'staging':
                        lineage['current_staging'] = deployment.version
            
            return lineage
            
        except Exception as e:
            self.logger.error(f"Error getting model lineage: {e}")
            return {}
    
    async def get_deployment_summary(self) -> Dict[str, Any]:
        """Get deployment summary"""
        try:
            summary = {
                'total_deployments': 0,
                'successful_deployments': 0,
                'failed_deployments': 0,
                'rolled_back_deployments': 0,
                'deployments_by_environment': {},
                'recent_deployments': []
            }
            
            # Get all deployments from Redis
            redis = await aioredis.from_url(self.redis_url)
            deployment_keys = await redis.keys("deployment:*")
            
            for key in deployment_keys:
                deployment_data = await redis.get(key)
                if deployment_data:
                    deployment = json.loads(deployment_data)
                    
                    summary['total_deployments'] += 1
                    
                    # Count by status
                    status = deployment['status']
                    if status == DeploymentStatus.SUCCESS.value:
                        summary['successful_deployments'] += 1
                    elif status == DeploymentStatus.FAILED.value:
                        summary['failed_deployments'] += 1
                    elif status == DeploymentStatus.ROLLED_BACK.value:
                        summary['rolled_back_deployments'] += 1
                    
                    # Count by environment
                    environment = deployment['environment']
                    summary['deployments_by_environment'][environment] = summary['deployments_by_environment'].get(environment, 0) + 1
                    
                    # Add to recent deployments
                    summary['recent_deployments'].append({
                        'deployment_id': deployment['deployment_id'],
                        'model_id': deployment['model_id'],
                        'version': deployment['version'],
                        'environment': deployment['environment'],
                        'status': deployment['status'],
                        'deployed_at': deployment['deployed_at']
                    })
            
            await redis.close()
            
            # Sort recent deployments by date
            summary['recent_deployments'].sort(key=lambda x: x['deployed_at'], reverse=True)
            summary['recent_deployments'] = summary['recent_deployments'][:10]  # Keep only 10 most recent
            
            return summary
            
        except Exception as e:
            self.logger.error(f"Error getting deployment summary: {e}")
            return {}
    
    async def _store_model_version(self, model_version: ModelVersion):
        """Store model version in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"model_version:{model_version.model_id}:{model_version.version}",
                json.dumps({
                    'model_id': model_version.model_id,
                    'version': model_version.version,
                    'model_name': model_version.model_name,
                    'model_type': model_version.model_type,
                    'created_at': model_version.created_at.isoformat(),
                    'created_by': model_version.created_by,
                    'description': model_version.description,
                    'status': model_version.status.value,
                    'artifact_path': model_version.artifact_path,
                    'metadata': model_version.metadata,
                    'performance_baseline': model_version.performance_baseline,
                    'dependencies': model_version.dependencies
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing model version: {e}")
    
    async def _store_deployment(self, deployment: Deployment):
        """Store deployment in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"deployment:{deployment.deployment_id}",
                json.dumps({
                    'deployment_id': deployment.deployment_id,
                    'model_id': deployment.model_id,
                    'version': deployment.version,
                    'environment': deployment.environment,
                    'status': deployment.status.value,
                    'deployed_at': deployment.deployed_at.isoformat(),
                    'deployed_by': deployment.deployed_by,
                    'deployment_config': deployment.deployment_config,
                    'rollback_version': deployment.rollback_version,
                    'rollback_reason': deployment.rollback_reason
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing deployment: {e}")
    
    async def _store_retraining_trigger(self, trigger: RetrainingTrigger):
        """Store retraining trigger in Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            
            await redis.set(
                f"retraining_trigger:{trigger.trigger_id}",
                json.dumps({
                    'trigger_id': trigger.trigger_id,
                    'model_id': trigger.model_id,
                    'trigger_type': trigger.trigger_type,
                    'conditions': trigger.conditions,
                    'enabled': trigger.enabled,
                    'last_triggered': trigger.last_triggered.isoformat() if trigger.last_triggered else None
                })
            )
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error storing retraining trigger: {e}")
    
    async def _get_model_version(self, model_id: str, version: str) -> Optional[ModelVersion]:
        """Get model version from Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            data = await redis.get(f"model_version:{model_id}:{version}")
            await redis.close()
            
            if data:
                version_data = json.loads(data)
                return ModelVersion(
                    model_id=version_data['model_id'],
                    version=version_data['version'],
                    model_name=version_data['model_name'],
                    model_type=version_data['model_type'],
                    created_at=datetime.fromisoformat(version_data['created_at']),
                    created_by=version_data['created_by'],
                    description=version_data['description'],
                    status=ModelStatus(version_data['status']),
                    artifact_path=version_data['artifact_path'],
                    metadata=version_data['metadata'],
                    performance_baseline=version_data['performance_baseline'],
                    dependencies=version_data['dependencies']
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting model version: {e}")
            return None
    
    async def _get_deployment(self, deployment_id: str) -> Optional[Deployment]:
        """Get deployment from Redis"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            data = await redis.get(f"deployment:{deployment_id}")
            await redis.close()
            
            if data:
                deployment_data = json.loads(data)
                return Deployment(
                    deployment_id=deployment_data['deployment_id'],
                    model_id=deployment_data['model_id'],
                    version=deployment_data['version'],
                    environment=deployment_data['environment'],
                    status=DeploymentStatus(deployment_data['status']),
                    deployed_at=datetime.fromisoformat(deployment_data['deployed_at']),
                    deployed_by=deployment_data['deployed_by'],
                    deployment_config=deployment_data['deployment_config'],
                    rollback_version=deployment_data.get('rollback_version'),
                    rollback_reason=deployment_data.get('rollback_reason')
                )
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting deployment: {e}")
            return None
    
    async def _get_model_deployments(self, model_id: str) -> List[Deployment]:
        """Get all deployments for a model"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            deployment_keys = await redis.keys(f"deployment:*")
            
            deployments = []
            for key in deployment_keys:
                data = await redis.get(key)
                if data:
                    deployment_data = json.loads(data)
                    if deployment_data['model_id'] == model_id:
                        deployments.append(Deployment(
                            deployment_id=deployment_data['deployment_id'],
                            model_id=deployment_data['model_id'],
                            version=deployment_data['version'],
                            environment=deployment_data['environment'],
                            status=DeploymentStatus(deployment_data['status']),
                            deployed_at=datetime.fromisoformat(deployment_data['deployed_at']),
                            deployed_by=deployment_data['deployed_by'],
                            deployment_config=deployment_data['deployment_config'],
                            rollback_version=deployment_data.get('rollback_version'),
                            rollback_reason=deployment_data.get('rollback_reason')
                        ))
            
            await redis.close()
            return deployments
            
        except Exception as e:
            self.logger.error(f"Error getting model deployments: {e}")
            return []
    
    async def _update_model_version_status(self, model_id: str, version: str, status: ModelStatus):
        """Update model version status"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            key = f"model_version:{model_id}:{version}"
            
            data = await redis.get(key)
            if data:
                version_data = json.loads(data)
                version_data['status'] = status.value
                await redis.set(key, json.dumps(version_data))
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error updating model version status: {e}")
    
    async def _update_deployment_status(self, deployment_id: str, status: DeploymentStatus):
        """Update deployment status"""
        try:
            redis = await aioredis.from_url(self.redis_url)
            key = f"deployment:{deployment_id}"
            
            data = await redis.get(key)
            if data:
                deployment_data = json.loads(data)
                deployment_data['status'] = status.value
                await redis.set(key, json.dumps(deployment_data))
            
            await redis.close()
            
        except Exception as e:
            self.logger.error(f"Error updating deployment status: {e}")
    
    async def _update_retraining_trigger(self, trigger: RetrainingTrigger):
        """Update retraining trigger"""
        try:
            await self._store_retraining_trigger(trigger)
        except Exception as e:
            self.logger.error(f"Error updating retraining trigger: {e}")
    
    async def _simulate_deployment(self, deployment: Deployment):
        """Simulate deployment process"""
        try:
            # Simulate deployment time
            await asyncio.sleep(2)
            
            # Simulate success/failure
            success_rate = 0.9  # 90% success rate
            if np.random.random() < success_rate:
                deployment.status = DeploymentStatus.SUCCESS
                self.logger.info(f"Deployment successful: {deployment.deployment_id}")
            else:
                deployment.status = DeploymentStatus.FAILED
                self.logger.error(f"Deployment failed: {deployment.deployment_id}")
            
            # Update status
            await self._update_deployment_status(deployment.deployment_id, deployment.status)
            
        except Exception as e:
            self.logger.error(f"Error simulating deployment: {e}")
    
    async def start_lifecycle_monitoring(self):
        """Start continuous lifecycle monitoring"""
        self.running = True
        self.logger.info("Starting lifecycle monitoring")
        
        while self.running:
            try:
                # Monitor model versions and deployments
                # Check for expired models, failed deployments, etc.
                self.logger.debug("Lifecycle monitoring active")
                
                # Wait before next check
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                self.logger.error(f"Error in lifecycle monitoring: {e}")
                await asyncio.sleep(60)
    
    async def stop_lifecycle_monitoring(self):
        """Stop lifecycle monitoring"""
        self.running = False
        self.logger.info("Stopping lifecycle monitoring")
