#!/usr/bin/env python3
"""
Data Lineage Tracking System
Tracks data flow, transformations, and dependencies across the ETL pipeline
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set
import networkx as nx
import pandas as pd
from dataclasses import dataclass, asdict
from pathlib import Path
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_lineage.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DataNode:
    """Represents a data node in the lineage graph"""
    name: str
    node_type: str  # 'source', 'staging', 'intermediate', 'mart', 'external'
    schema: str
    table: str
    description: str
    owner: str
    created_at: datetime
    updated_at: datetime
    columns: List[Dict[str, Any]]
    tags: List[str]
    metadata: Dict[str, Any]

@dataclass
class DataEdge:
    """Represents a relationship between data nodes"""
    source: str
    target: str
    relationship_type: str  # 'transforms', 'depends_on', 'feeds'
    transformation_logic: str
    created_at: datetime
    metadata: Dict[str, Any]

@dataclass
class DataTransformation:
    """Represents a data transformation"""
    name: str
    description: str
    input_tables: List[str]
    output_tables: List[str]
    transformation_type: str  # 'sql', 'python', 'dbt_model'
    logic: str
    owner: str
    created_at: datetime
    updated_at: datetime
    dependencies: List[str]
    metadata: Dict[str, Any]

class DataLineageTracker:
    """Comprehensive data lineage tracking system"""
    
    def __init__(self):
        self.lineage_dir = "data/lineage"
        self.graph = nx.DiGraph()
        self.nodes: Dict[str, DataNode] = {}
        self.edges: Dict[str, DataEdge] = {}
        self.transformations: Dict[str, DataTransformation] = {}
        
        os.makedirs(self.lineage_dir, exist_ok=True)
        
        # Load existing lineage data
        self._load_existing_lineage()
    
    def _load_existing_lineage(self):
        """Load existing lineage data from files"""
        try:
            # Load nodes
            nodes_file = os.path.join(self.lineage_dir, "nodes.json")
            if os.path.exists(nodes_file):
                with open(nodes_file, 'r') as f:
                    nodes_data = json.load(f)
                    for node_data in nodes_data:
                        node = DataNode(**node_data)
                        node.created_at = datetime.fromisoformat(node.created_at)
                        node.updated_at = datetime.fromisoformat(node.updated_at)
                        self.nodes[node.name] = node
                        self.graph.add_node(node.name, **asdict(node))
            
            # Load edges
            edges_file = os.path.join(self.lineage_dir, "edges.json")
            if os.path.exists(edges_file):
                with open(edges_file, 'r') as f:
                    edges_data = json.load(f)
                    for edge_data in edges_data:
                        edge = DataEdge(**edge_data)
                        edge.created_at = datetime.fromisoformat(edge.created_at)
                        self.edges[f"{edge.source}->{edge.target}"] = edge
                        self.graph.add_edge(edge.source, edge.target, **asdict(edge))
            
            # Load transformations
            transformations_file = os.path.join(self.lineage_dir, "transformations.json")
            if os.path.exists(transformations_file):
                with open(transformations_file, 'r') as f:
                    transformations_data = json.load(f)
                    for trans_data in transformations_data:
                        transformation = DataTransformation(**trans_data)
                        transformation.created_at = datetime.fromisoformat(transformation.created_at)
                        transformation.updated_at = datetime.fromisoformat(transformation.updated_at)
                        self.transformations[transformation.name] = transformation
            
            logger.info(f"Loaded {len(self.nodes)} nodes, {len(self.edges)} edges, {len(self.transformations)} transformations")
            
        except Exception as e:
            logger.error(f"Failed to load existing lineage data: {e}")
    
    def _save_lineage_data(self):
        """Save lineage data to files"""
        try:
            # Save nodes
            nodes_data = []
            for node in self.nodes.values():
                node_dict = asdict(node)
                node_dict['created_at'] = node.created_at.isoformat()
                node_dict['updated_at'] = node.updated_at.isoformat()
                nodes_data.append(node_dict)
            
            with open(os.path.join(self.lineage_dir, "nodes.json"), 'w') as f:
                json.dump(nodes_data, f, indent=2, default=str)
            
            # Save edges
            edges_data = []
            for edge in self.edges.values():
                edge_dict = asdict(edge)
                edge_dict['created_at'] = edge.created_at.isoformat()
                edges_data.append(edge_dict)
            
            with open(os.path.join(self.lineage_dir, "edges.json"), 'w') as f:
                json.dump(edges_data, f, indent=2, default=str)
            
            # Save transformations
            transformations_data = []
            for transformation in self.transformations.values():
                trans_dict = asdict(transformation)
                trans_dict['created_at'] = transformation.created_at.isoformat()
                trans_dict['updated_at'] = transformation.updated_at.isoformat()
                transformations_data.append(trans_dict)
            
            with open(os.path.join(self.lineage_dir, "transformations.json"), 'w') as f:
                json.dump(transformations_data, f, indent=2, default=str)
            
            logger.info("Lineage data saved successfully")
            
        except Exception as e:
            logger.error(f"Failed to save lineage data: {e}")
    
    def add_data_node(self, name: str, node_type: str, schema: str, table: str, 
                     description: str, owner: str, columns: List[Dict[str, Any]], 
                     tags: List[str] = None, metadata: Dict[str, Any] = None) -> DataNode:
        """Add a data node to the lineage graph"""
        try:
            now = datetime.now()
            
            node = DataNode(
                name=name,
                node_type=node_type,
                schema=schema,
                table=table,
                description=description,
                owner=owner,
                created_at=now,
                updated_at=now,
                columns=columns,
                tags=tags or [],
                metadata=metadata or {}
            )
            
            self.nodes[name] = node
            self.graph.add_node(name, **asdict(node))
            
            logger.info(f"Added data node: {name}")
            return node
            
        except Exception as e:
            logger.error(f"Failed to add data node {name}: {e}")
            raise
    
    def add_data_edge(self, source: str, target: str, relationship_type: str,
                     transformation_logic: str, metadata: Dict[str, Any] = None) -> DataEdge:
        """Add a relationship between data nodes"""
        try:
            if source not in self.nodes:
                raise ValueError(f"Source node {source} not found")
            if target not in self.nodes:
                raise ValueError(f"Target node {target} not found")
            
            edge_key = f"{source}->{target}"
            now = datetime.now()
            
            edge = DataEdge(
                source=source,
                target=target,
                relationship_type=relationship_type,
                transformation_logic=transformation_logic,
                created_at=now,
                metadata=metadata or {}
            )
            
            self.edges[edge_key] = edge
            self.graph.add_edge(source, target, **asdict(edge))
            
            logger.info(f"Added data edge: {source} -> {target}")
            return edge
            
        except Exception as e:
            logger.error(f"Failed to add data edge {source} -> {target}: {e}")
            raise
    
    def add_transformation(self, name: str, description: str, input_tables: List[str],
                          output_tables: List[str], transformation_type: str, logic: str,
                          owner: str, dependencies: List[str] = None, 
                          metadata: Dict[str, Any] = None) -> DataTransformation:
        """Add a data transformation"""
        try:
            now = datetime.now()
            
            transformation = DataTransformation(
                name=name,
                description=description,
                input_tables=input_tables,
                output_tables=output_tables,
                transformation_type=transformation_type,
                logic=logic,
                owner=owner,
                created_at=now,
                updated_at=now,
                dependencies=dependencies or [],
                metadata=metadata or {}
            )
            
            self.transformations[name] = transformation
            
            # Add edges for transformation
            for input_table in input_tables:
                for output_table in output_tables:
                    self.add_data_edge(
                        input_table, output_table, "transforms",
                        f"Transformation: {name}", {"transformation": name}
                    )
            
            logger.info(f"Added transformation: {name}")
            return transformation
            
        except Exception as e:
            logger.error(f"Failed to add transformation {name}: {e}")
            raise
    
    def get_upstream_lineage(self, node_name: str, max_depth: int = 5) -> Dict[str, Any]:
        """Get upstream lineage for a node"""
        try:
            if node_name not in self.nodes:
                return {"error": f"Node {node_name} not found"}
            
            upstream_nodes = []
            upstream_edges = []
            
            # Get all predecessors
            predecessors = list(nx.ancestors(self.graph, node_name))
            
            for pred in predecessors:
                if pred in self.nodes:
                    upstream_nodes.append(self.nodes[pred])
            
            # Get edges connecting upstream nodes
            for edge_key, edge in self.edges.items():
                if edge.target == node_name or edge.target in predecessors:
                    upstream_edges.append(edge)
            
            return {
                "node": self.nodes[node_name],
                "upstream_nodes": upstream_nodes,
                "upstream_edges": upstream_edges,
                "depth": len(predecessors),
                "max_depth": max_depth
            }
            
        except Exception as e:
            logger.error(f"Failed to get upstream lineage for {node_name}: {e}")
            return {"error": str(e)}
    
    def get_downstream_lineage(self, node_name: str, max_depth: int = 5) -> Dict[str, Any]:
        """Get downstream lineage for a node"""
        try:
            if node_name not in self.nodes:
                return {"error": f"Node {node_name} not found"}
            
            downstream_nodes = []
            downstream_edges = []
            
            # Get all successors
            successors = list(nx.descendants(self.graph, node_name))
            
            for succ in successors:
                if succ in self.nodes:
                    downstream_nodes.append(self.nodes[succ])
            
            # Get edges connecting downstream nodes
            for edge_key, edge in self.edges.items():
                if edge.source == node_name or edge.source in successors:
                    downstream_edges.append(edge)
            
            return {
                "node": self.nodes[node_name],
                "downstream_nodes": downstream_nodes,
                "downstream_edges": downstream_edges,
                "depth": len(successors),
                "max_depth": max_depth
            }
            
        except Exception as e:
            logger.error(f"Failed to get downstream lineage for {node_name}: {e}")
            return {"error": str(e)}
    
    def get_full_lineage(self, node_name: str) -> Dict[str, Any]:
        """Get complete lineage (upstream and downstream) for a node"""
        try:
            upstream = self.get_upstream_lineage(node_name)
            downstream = self.get_downstream_lineage(node_name)
            
            return {
                "node": self.nodes[node_name] if node_name in self.nodes else None,
                "upstream": upstream,
                "downstream": downstream,
                "total_nodes": len(upstream.get("upstream_nodes", [])) + len(downstream.get("downstream_nodes", [])) + 1
            }
            
        except Exception as e:
            logger.error(f"Failed to get full lineage for {node_name}: {e}")
            return {"error": str(e)}
    
    def find_impact_analysis(self, node_name: str) -> Dict[str, Any]:
        """Find impact analysis - what would be affected if this node changes"""
        try:
            downstream = self.get_downstream_lineage(node_name)
            
            # Get all nodes that depend on this node
            affected_nodes = []
            for node in downstream.get("downstream_nodes", []):
                affected_nodes.append({
                    "name": node.name,
                    "type": node.node_type,
                    "description": node.description,
                    "owner": node.owner
                })
            
            # Get transformations that would be affected
            affected_transformations = []
            for trans_name, transformation in self.transformations.items():
                if node_name in transformation.input_tables:
                    affected_transformations.append({
                        "name": transformation.name,
                        "description": transformation.description,
                        "type": transformation.transformation_type,
                        "owner": transformation.owner
                    })
            
            return {
                "source_node": node_name,
                "affected_nodes": affected_nodes,
                "affected_transformations": affected_transformations,
                "impact_level": "high" if len(affected_nodes) > 5 else "medium" if len(affected_nodes) > 2 else "low"
            }
            
        except Exception as e:
            logger.error(f"Failed to find impact analysis for {node_name}: {e}")
            return {"error": str(e)}
    
    def generate_lineage_report(self) -> Dict[str, Any]:
        """Generate comprehensive lineage report"""
        try:
            report = {
                "timestamp": datetime.now().isoformat(),
                "summary": {
                    "total_nodes": len(self.nodes),
                    "total_edges": len(self.edges),
                    "total_transformations": len(self.transformations),
                    "graph_connected_components": nx.number_weakly_connected_components(self.graph),
                    "graph_density": nx.density(self.graph)
                },
                "nodes_by_type": {},
                "transformations_by_type": {},
                "top_nodes_by_connections": [],
                "orphaned_nodes": [],
                "circular_dependencies": []
            }
            
            # Group nodes by type
            for node in self.nodes.values():
                node_type = node.node_type
                if node_type not in report["nodes_by_type"]:
                    report["nodes_by_type"][node_type] = 0
                report["nodes_by_type"][node_type] += 1
            
            # Group transformations by type
            for transformation in self.transformations.values():
                trans_type = transformation.transformation_type
                if trans_type not in report["transformations_by_type"]:
                    report["transformations_by_type"][trans_type] = 0
                report["transformations_by_type"][trans_type] += 1
            
            # Find top nodes by connections
            node_degrees = dict(self.graph.degree())
            sorted_nodes = sorted(node_degrees.items(), key=lambda x: x[1], reverse=True)
            report["top_nodes_by_connections"] = [
                {"name": name, "connections": degree} 
                for name, degree in sorted_nodes[:10]
            ]
            
            # Find orphaned nodes (no connections)
            orphaned = [name for name, degree in node_degrees.items() if degree == 0]
            report["orphaned_nodes"] = orphaned
            
            # Find circular dependencies
            try:
                cycles = list(nx.simple_cycles(self.graph))
                report["circular_dependencies"] = cycles
            except:
                report["circular_dependencies"] = []
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate lineage report: {e}")
            return {"error": str(e)}
    
    def export_lineage_graph(self, format: str = "json") -> str:
        """Export lineage graph in various formats"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if format == "json":
                # Export as JSON
                graph_data = {
                    "nodes": [asdict(node) for node in self.nodes.values()],
                    "edges": [asdict(edge) for edge in self.edges.values()],
                    "transformations": [asdict(trans) for trans in self.transformations.values()]
                }
                
                # Convert datetime objects to strings
                for node in graph_data["nodes"]:
                    node["created_at"] = node["created_at"].isoformat()
                    node["updated_at"] = node["updated_at"].isoformat()
                
                for edge in graph_data["edges"]:
                    edge["created_at"] = edge["created_at"].isoformat()
                
                for trans in graph_data["transformations"]:
                    trans["created_at"] = trans["created_at"].isoformat()
                    trans["updated_at"] = trans["updated_at"].isoformat()
                
                export_path = os.path.join(self.lineage_dir, f"lineage_export_{timestamp}.json")
                with open(export_path, 'w') as f:
                    json.dump(graph_data, f, indent=2, default=str)
                
            elif format == "graphml":
                # Export as GraphML
                export_path = os.path.join(self.lineage_dir, f"lineage_export_{timestamp}.graphml")
                nx.write_graphml(self.graph, export_path)
                
            elif format == "gexf":
                # Export as GEXF
                export_path = os.path.join(self.lineage_dir, f"lineage_export_{timestamp}.gexf")
                nx.write_gexf(self.graph, export_path)
                
            else:
                raise ValueError(f"Unsupported export format: {format}")
            
            logger.info(f"Lineage graph exported to: {export_path}")
            return export_path
            
        except Exception as e:
            logger.error(f"Failed to export lineage graph: {e}")
            return ""
    
    def scan_dbt_project(self, project_dir: str = ".") -> Dict[str, Any]:
        """Scan dbt project and build lineage automatically"""
        try:
            logger.info("Scanning dbt project for lineage...")
            
            # Find dbt models
            models_dir = os.path.join(project_dir, "models")
            if not os.path.exists(models_dir):
                return {"error": "Models directory not found"}
            
            scanned_models = []
            
            # Walk through models directory
            for root, dirs, files in os.walk(models_dir):
                for file in files:
                    if file.endswith('.sql'):
                        model_path = os.path.join(root, file)
                        model_name = file[:-4]  # Remove .sql extension
                        
                        # Determine model type based on directory
                        relative_path = os.path.relpath(root, models_dir)
                        if relative_path.startswith('staging'):
                            model_type = 'staging'
                        elif relative_path.startswith('marts'):
                            model_type = 'mart'
                        else:
                            model_type = 'intermediate'
                        
                        # Read model content
                        with open(model_path, 'r') as f:
                            model_content = f.read()
                        
                        # Extract dependencies (basic parsing)
                        dependencies = self._extract_dbt_dependencies(model_content)
                        
                        # Add model as transformation
                        transformation = self.add_transformation(
                            name=model_name,
                            description=f"dbt model: {model_name}",
                            input_tables=dependencies,
                            output_tables=[model_name],
                            transformation_type="dbt_model",
                            logic=model_content,
                            owner="dbt",
                            metadata={"file_path": model_path, "model_type": model_type}
                        )
                        
                        scanned_models.append({
                            "name": model_name,
                            "type": model_type,
                            "path": model_path,
                            "dependencies": dependencies
                        })
            
            # Save lineage data
            self._save_lineage_data()
            
            logger.info(f"Scanned {len(scanned_models)} dbt models")
            return {
                "scanned_models": scanned_models,
                "total_models": len(scanned_models),
                "lineage_updated": True
            }
            
        except Exception as e:
            logger.error(f"Failed to scan dbt project: {e}")
            return {"error": str(e)}
    
    def _extract_dbt_dependencies(self, model_content: str) -> List[str]:
        """Extract dependencies from dbt model content"""
        dependencies = []
        
        # Look for ref() functions
        import re
        ref_pattern = r"\{\{\s*ref\(['\"]([^'\"]+)['\"]\)\s*\}\}"
        ref_matches = re.findall(ref_pattern, model_content)
        dependencies.extend(ref_matches)
        
        # Look for source() functions
        source_pattern = r"\{\{\s*source\(['\"]([^'\"]+)['\"],\s*['\"]([^'\"]+)['\"]\)\s*\}\}"
        source_matches = re.findall(source_pattern, model_content)
        for source_name, table_name in source_matches:
            dependencies.append(f"{source_name}.{table_name}")
        
        return list(set(dependencies))  # Remove duplicates

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Lineage Tracker')
    parser.add_argument('action', choices=['scan', 'report', 'lineage', 'impact', 'export'],
                       help='Action to perform')
    parser.add_argument('--node', help='Node name for lineage/impact analysis')
    parser.add_argument('--format', default='json', choices=['json', 'graphml', 'gexf'],
                       help='Export format')
    parser.add_argument('--project-dir', default='.', help='dbt project directory')
    
    args = parser.parse_args()
    
    try:
        tracker = DataLineageTracker()
        
        if args.action == 'scan':
            results = tracker.scan_dbt_project(args.project_dir)
            print(json.dumps(results, indent=2, default=str))
            
        elif args.action == 'report':
            report = tracker.generate_lineage_report()
            print(json.dumps(report, indent=2, default=str))
            
        elif args.action == 'lineage':
            if not args.node:
                print("Error: --node is required for lineage analysis")
                sys.exit(1)
            
            lineage = tracker.get_full_lineage(args.node)
            print(json.dumps(lineage, indent=2, default=str))
            
        elif args.action == 'impact':
            if not args.node:
                print("Error: --node is required for impact analysis")
                sys.exit(1)
            
            impact = tracker.find_impact_analysis(args.node)
            print(json.dumps(impact, indent=2, default=str))
            
        elif args.action == 'export':
            export_path = tracker.export_lineage_graph(args.format)
            if export_path:
                print(f"Lineage exported to: {export_path}")
            else:
                print("Export failed")
                sys.exit(1)
        
    except Exception as e:
        logger.error(f"Data lineage tracker failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
