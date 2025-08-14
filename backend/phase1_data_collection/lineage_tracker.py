import asyncio
import json
import logging
import hashlib
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Set
from dataclasses import dataclass, asdict
from enum import Enum
import redis.asyncio as aioredis
import networkx as nx
from uuid import uuid4

class LineageNodeType(Enum):
    """Types of nodes in the data lineage graph"""
    SOURCE = "source"
    TRANSFORMATION = "transformation"
    SINK = "sink"
    VALIDATION = "validation"
    AGGREGATION = "aggregation"

class LineageEdgeType(Enum):
    """Types of edges in the data lineage graph"""
    FLOWS_TO = "flows_to"
    DEPENDS_ON = "depends_on"
    VALIDATES = "validates"
    AGGREGATES = "aggregates"

@dataclass
class LineageNode:
    """Represents a node in the data lineage graph"""
    node_id: str
    node_type: LineageNodeType
    name: str
    description: str
    metadata: Dict[str, Any]
    created_at: datetime
    updated_at: datetime
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []

@dataclass
class LineageEdge:
    """Represents an edge in the data lineage graph"""
    edge_id: str
    source_node_id: str
    target_node_id: str
    edge_type: LineageEdgeType
    metadata: Dict[str, Any]
    created_at: datetime
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []

@dataclass
class DataArtifact:
    """Represents a data artifact in the lineage"""
    artifact_id: str
    name: str
    data_hash: str
    size_bytes: int
    format: str
    schema: Dict[str, Any]
    created_at: datetime
    source_node_id: str
    metadata: Dict[str, Any]

class DataLineageTracker:
    """
    Comprehensive data lineage tracking system
    Tracks data flow, transformations, and metadata throughout the pipeline
    """
    
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.graph = nx.DiGraph()
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: Dict[str, LineageEdge] = {}
        self.artifacts: Dict[str, DataArtifact] = {}
        self.logger = logging.getLogger(__name__)
        
    async def add_node(self, node: LineageNode) -> bool:
        """Add a node to the lineage graph"""
        try:
            self.nodes[node.node_id] = node
            self.graph.add_node(node.node_id, **asdict(node))
            
            # Store in Redis
            redis = await aioredis.from_url(self.redis_url)
            await redis.hset(
                f"lineage:node:{node.node_id}",
                mapping=asdict(node)
            )
            await redis.close()
            
            self.logger.info(f"Added lineage node: {node.node_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add lineage node {node.node_id}: {e}")
            return False
    
    async def add_edge(self, edge: LineageEdge) -> bool:
        """Add an edge to the lineage graph"""
        try:
            # Verify both nodes exist
            if edge.source_node_id not in self.nodes or edge.target_node_id not in self.nodes:
                raise ValueError(f"Source or target node not found: {edge.source_node_id} -> {edge.target_node_id}")
            
            self.edges[edge.edge_id] = edge
            self.graph.add_edge(
                edge.source_node_id,
                edge.target_node_id,
                **asdict(edge)
            )
            
            # Store in Redis
            redis = await aioredis.from_url(self.redis_url)
            await redis.hset(
                f"lineage:edge:{edge.edge_id}",
                mapping=asdict(edge)
            )
            await redis.close()
            
            self.logger.info(f"Added lineage edge: {edge.edge_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add lineage edge {edge.edge_id}: {e}")
            return False
    
    async def add_artifact(self, artifact: DataArtifact) -> bool:
        """Add a data artifact to the lineage"""
        try:
            self.artifacts[artifact.artifact_id] = artifact
            
            # Store in Redis
            redis = await aioredis.from_url(self.redis_url)
            await redis.hset(
                f"lineage:artifact:{artifact.artifact_id}",
                mapping=asdict(artifact)
            )
            await redis.close()
            
            self.logger.info(f"Added data artifact: {artifact.artifact_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to add artifact {artifact.artifact_id}: {e}")
            return False
    
    async def track_data_flow(self, source_id: str, target_id: str, data: Dict[str, Any], 
                            transformation_type: str = "copy") -> str:
        """Track data flow from source to target with transformation metadata"""
        try:
            # Create transformation node
            transformation_id = f"transformation_{uuid4().hex[:8]}"
            transformation_node = LineageNode(
                node_id=transformation_id,
                node_type=LineageNodeType.TRANSFORMATION,
                name=f"{transformation_type}_transformation",
                description=f"Data transformation: {transformation_type}",
                metadata={
                    'transformation_type': transformation_type,
                    'input_data_hash': self._calculate_data_hash(data),
                    'timestamp': datetime.utcnow().isoformat()
                },
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                tags=[transformation_type, 'data_flow']
            )
            
            await self.add_node(transformation_node)
            
            # Create edges
            source_edge = LineageEdge(
                edge_id=f"edge_{uuid4().hex[:8]}",
                source_node_id=source_id,
                target_node_id=transformation_id,
                edge_type=LineageEdgeType.FLOWS_TO,
                metadata={'flow_type': 'input'},
                created_at=datetime.utcnow()
            )
            
            target_edge = LineageEdge(
                edge_id=f"edge_{uuid4().hex[:8]}",
                source_node_id=transformation_id,
                target_node_id=target_id,
                edge_type=LineageEdgeType.FLOWS_TO,
                metadata={'flow_type': 'output'},
                created_at=datetime.utcnow()
            )
            
            await self.add_edge(source_edge)
            await self.add_edge(target_edge)
            
            return transformation_id
            
        except Exception as e:
            self.logger.error(f"Failed to track data flow: {e}")
            return None
    
    async def track_validation(self, data_id: str, validation_rules: List[Dict[str, Any]], 
                             validation_result: Dict[str, Any]) -> str:
        """Track data validation in the lineage"""
        try:
            validation_id = f"validation_{uuid4().hex[:8]}"
            validation_node = LineageNode(
                node_id=validation_id,
                node_type=LineageNodeType.VALIDATION,
                name="data_validation",
                description="Data quality validation",
                metadata={
                    'validation_rules': validation_rules,
                    'validation_result': validation_result,
                    'timestamp': datetime.utcnow().isoformat()
                },
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                tags=['validation', 'data_quality']
            )
            
            await self.add_node(validation_node)
            
            # Create validation edge
            validation_edge = LineageEdge(
                edge_id=f"edge_{uuid4().hex[:8]}",
                source_node_id=data_id,
                target_node_id=validation_id,
                edge_type=LineageEdgeType.VALIDATES,
                metadata={'validation_type': 'quality_check'},
                created_at=datetime.utcnow()
            )
            
            await self.add_edge(validation_edge)
            
            return validation_id
            
        except Exception as e:
            self.logger.error(f"Failed to track validation: {e}")
            return None
    
    async def track_aggregation(self, source_ids: List[str], target_id: str, 
                              aggregation_type: str, aggregation_config: Dict[str, Any]) -> str:
        """Track data aggregation in the lineage"""
        try:
            aggregation_id = f"aggregation_{uuid4().hex[:8]}"
            aggregation_node = LineageNode(
                node_id=aggregation_id,
                node_type=LineageNodeType.AGGREGATION,
                name=f"{aggregation_type}_aggregation",
                description=f"Data aggregation: {aggregation_type}",
                metadata={
                    'aggregation_type': aggregation_type,
                    'aggregation_config': aggregation_config,
                    'source_count': len(source_ids),
                    'timestamp': datetime.utcnow().isoformat()
                },
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                tags=['aggregation', aggregation_type]
            )
            
            await self.add_node(aggregation_node)
            
            # Create aggregation edges
            for source_id in source_ids:
                aggregation_edge = LineageEdge(
                    edge_id=f"edge_{uuid4().hex[:8]}",
                    source_node_id=source_id,
                    target_node_id=aggregation_id,
                    edge_type=LineageEdgeType.AGGREGATES,
                    metadata={'aggregation_type': aggregation_type},
                    created_at=datetime.utcnow()
                )
                await self.add_edge(aggregation_edge)
            
            # Create output edge
            output_edge = LineageEdge(
                edge_id=f"edge_{uuid4().hex[:8]}",
                source_node_id=aggregation_id,
                target_node_id=target_id,
                edge_type=LineageEdgeType.FLOWS_TO,
                metadata={'flow_type': 'aggregated_output'},
                created_at=datetime.utcnow()
            )
            await self.add_edge(output_edge)
            
            return aggregation_id
            
        except Exception as e:
            self.logger.error(f"Failed to track aggregation: {e}")
            return None
    
    def _calculate_data_hash(self, data: Dict[str, Any]) -> str:
        """Calculate hash of data for tracking changes"""
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()
    
    async def get_lineage_path(self, source_id: str, target_id: str) -> List[str]:
        """Get the lineage path between two nodes"""
        try:
            if nx.has_path(self.graph, source_id, target_id):
                path = nx.shortest_path(self.graph, source_id, target_id)
                return path
            else:
                return []
        except Exception as e:
            self.logger.error(f"Failed to get lineage path: {e}")
            return []
    
    async def get_upstream_dependencies(self, node_id: str) -> List[str]:
        """Get all upstream dependencies of a node"""
        try:
            if node_id in self.graph:
                predecessors = list(self.graph.predecessors(node_id))
                return predecessors
            else:
                return []
        except Exception as e:
            self.logger.error(f"Failed to get upstream dependencies: {e}")
            return []
    
    async def get_downstream_dependencies(self, node_id: str) -> List[str]:
        """Get all downstream dependencies of a node"""
        try:
            if node_id in self.graph:
                successors = list(self.graph.successors(node_id))
                return successors
            else:
                return []
        except Exception as e:
            self.logger.error(f"Failed to get downstream dependencies: {e}")
            return []
    
    async def get_node_lineage(self, node_id: str, max_depth: int = 3) -> Dict[str, Any]:
        """Get comprehensive lineage information for a node"""
        try:
            if node_id not in self.graph:
                return {'error': 'Node not found'}
            
            lineage_info = {
                'node': asdict(self.nodes.get(node_id, {})),
                'upstream': [],
                'downstream': [],
                'artifacts': []
            }
            
            # Get upstream nodes (limited depth)
            upstream_nodes = set()
            current_level = {node_id}
            
            for depth in range(max_depth):
                next_level = set()
                for node in current_level:
                    predecessors = list(self.graph.predecessors(node))
                    next_level.update(predecessors)
                upstream_nodes.update(next_level)
                current_level = next_level
                if not current_level:
                    break
            
            lineage_info['upstream'] = list(upstream_nodes)
            
            # Get downstream nodes (limited depth)
            downstream_nodes = set()
            current_level = {node_id}
            
            for depth in range(max_depth):
                next_level = set()
                for node in current_level:
                    successors = list(self.graph.successors(node))
                    next_level.update(successors)
                downstream_nodes.update(next_level)
                current_level = next_level
                if not current_level:
                    break
            
            lineage_info['downstream'] = list(downstream_nodes)
            
            # Get related artifacts
            for artifact_id, artifact in self.artifacts.items():
                if artifact.source_node_id == node_id:
                    lineage_info['artifacts'].append(asdict(artifact))
            
            return lineage_info
            
        except Exception as e:
            self.logger.error(f"Failed to get node lineage: {e}")
            return {'error': str(e)}
    
    async def export_lineage_graph(self, format: str = "json") -> str:
        """Export the lineage graph in various formats"""
        try:
            if format == "json":
                graph_data = {
                    'nodes': [asdict(node) for node in self.nodes.values()],
                    'edges': [asdict(edge) for edge in self.edges.values()],
                    'artifacts': [asdict(artifact) for artifact in self.artifacts.values()]
                }
                return json.dumps(graph_data, indent=2, default=str)
            
            elif format == "dot":
                return nx.drawing.nx_pydot.to_pydot(self.graph).to_string()
            
            elif format == "gml":
                return "\n".join(nx.generate_gml(self.graph))
            
            else:
                raise ValueError(f"Unsupported format: {format}")
                
        except Exception as e:
            self.logger.error(f"Failed to export lineage graph: {e}")
            return ""
    
    async def get_lineage_statistics(self) -> Dict[str, Any]:
        """Get statistics about the lineage graph"""
        try:
            stats = {
                'total_nodes': len(self.nodes),
                'total_edges': len(self.edges),
                'total_artifacts': len(self.artifacts),
                'node_types': {},
                'edge_types': {},
                'graph_density': nx.density(self.graph),
                'is_dag': nx.is_directed_acyclic_graph(self.graph),
                'connected_components': nx.number_strongly_connected_components(self.graph)
            }
            
            # Count node types
            for node in self.nodes.values():
                node_type = node.node_type.value
                stats['node_types'][node_type] = stats['node_types'].get(node_type, 0) + 1
            
            # Count edge types
            for edge in self.edges.values():
                edge_type = edge.edge_type.value
                stats['edge_types'][edge_type] = stats['edge_types'].get(edge_type, 0) + 1
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Failed to get lineage statistics: {e}")
            return {'error': str(e)}
    
    async def cleanup_old_lineage(self, days_to_keep: int = 30):
        """Clean up old lineage data"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
            
            # Clean up old nodes
            nodes_to_remove = []
            for node_id, node in self.nodes.items():
                if node.updated_at < cutoff_date:
                    nodes_to_remove.append(node_id)
            
            for node_id in nodes_to_remove:
                del self.nodes[node_id]
                self.graph.remove_node(node_id)
            
            # Clean up old edges
            edges_to_remove = []
            for edge_id, edge in self.edges.items():
                if edge.created_at < cutoff_date:
                    edges_to_remove.append(edge_id)
            
            for edge_id in edges_to_remove:
                del self.edges[edge_id]
                # Remove from graph (edges are stored as graph edges)
                # This is simplified - in practice you'd need to track edge IDs in the graph
            
            # Clean up old artifacts
            artifacts_to_remove = []
            for artifact_id, artifact in self.artifacts.items():
                if artifact.created_at < cutoff_date:
                    artifacts_to_remove.append(artifact_id)
            
            for artifact_id in artifacts_to_remove:
                del self.artifacts[artifact_id]
            
            self.logger.info(f"Cleaned up {len(nodes_to_remove)} nodes, {len(edges_to_remove)} edges, {len(artifacts_to_remove)} artifacts")
            
        except Exception as e:
            self.logger.error(f"Failed to cleanup old lineage: {e}")
