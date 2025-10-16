#!/usr/bin/env python3
"""
Data Catalog Manager
Manages data catalog, metadata, and governance information
"""

import os
import sys
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set
import pandas as pd
from dataclasses import dataclass, asdict
from pathlib import Path
import yaml
import sqlite3
from enum import Enum

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_catalog.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DataClassification(Enum):
    """Data classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

class DataQualityLevel(Enum):
    """Data quality levels"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"

@dataclass
class DataAsset:
    """Represents a data asset in the catalog"""
    id: str
    name: str
    description: str
    schema: str
    table: str
    classification: DataClassification
    owner: str
    steward: str
    created_at: datetime
    updated_at: datetime
    last_accessed: datetime
    quality_level: DataQualityLevel
    quality_score: float
    columns: List[Dict[str, Any]]
    tags: List[str]
    business_glossary: Dict[str, str]
    usage_statistics: Dict[str, Any]
    lineage: Dict[str, Any]
    compliance_info: Dict[str, Any]
    metadata: Dict[str, Any]

@dataclass
class DataUser:
    """Represents a data user"""
    id: str
    name: str
    email: str
    role: str
    department: str
    access_level: str
    created_at: datetime
    last_login: datetime
    permissions: List[str]
    metadata: Dict[str, Any]

@dataclass
class DataAccess:
    """Represents data access record"""
    id: str
    user_id: str
    asset_id: str
    access_type: str
    granted_at: datetime
    expires_at: Optional[datetime]
    granted_by: str
    purpose: str
    metadata: Dict[str, Any]

class DataCatalogManager:
    """Comprehensive data catalog management system"""
    
    def __init__(self, catalog_db_path: str = "data/catalog.db"):
        self.catalog_db_path = catalog_db_path
        self.catalog_dir = os.path.dirname(catalog_db_path)
        os.makedirs(self.catalog_dir, exist_ok=True)
        
        # Initialize database
        self._init_database()
        
        # Load existing data
        self._load_catalog_data()
    
    def _init_database(self):
        """Initialize SQLite database for catalog"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            # Create tables
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS data_assets (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    schema_name TEXT,
                    table_name TEXT,
                    classification TEXT,
                    owner TEXT,
                    steward TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    last_accessed TEXT,
                    quality_level TEXT,
                    quality_score REAL,
                    columns TEXT,
                    tags TEXT,
                    business_glossary TEXT,
                    usage_statistics TEXT,
                    lineage TEXT,
                    compliance_info TEXT,
                    metadata TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS data_users (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT UNIQUE,
                    role TEXT,
                    department TEXT,
                    access_level TEXT,
                    created_at TEXT,
                    last_login TEXT,
                    permissions TEXT,
                    metadata TEXT
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS data_access (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    asset_id TEXT,
                    access_type TEXT,
                    granted_at TEXT,
                    expires_at TEXT,
                    granted_by TEXT,
                    purpose TEXT,
                    metadata TEXT,
                    FOREIGN KEY (user_id) REFERENCES data_users (id),
                    FOREIGN KEY (asset_id) REFERENCES data_assets (id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS data_quality_metrics (
                    id TEXT PRIMARY KEY,
                    asset_id TEXT,
                    metric_name TEXT,
                    metric_value REAL,
                    threshold REAL,
                    status TEXT,
                    measured_at TEXT,
                    metadata TEXT,
                    FOREIGN KEY (asset_id) REFERENCES data_assets (id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS data_lineage (
                    id TEXT PRIMARY KEY,
                    source_asset_id TEXT,
                    target_asset_id TEXT,
                    transformation_type TEXT,
                    transformation_logic TEXT,
                    created_at TEXT,
                    metadata TEXT,
                    FOREIGN KEY (source_asset_id) REFERENCES data_assets (id),
                    FOREIGN KEY (target_asset_id) REFERENCES data_assets (id)
                )
            ''')
            
            conn.commit()
            conn.close()
            
            logger.info("Data catalog database initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize catalog database: {e}")
            raise
    
    def _load_catalog_data(self):
        """Load existing catalog data"""
        try:
            # This would typically load from the database
            # For now, we'll initialize empty structures
            self.assets: Dict[str, DataAsset] = {}
            self.users: Dict[str, DataUser] = {}
            self.access_records: Dict[str, DataAccess] = {}
            
            logger.info("Catalog data loaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to load catalog data: {e}")
    
    def add_data_asset(self, asset: DataAsset) -> bool:
        """Add a data asset to the catalog"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO data_assets (
                    id, name, description, schema_name, table_name, classification,
                    owner, steward, created_at, updated_at, last_accessed,
                    quality_level, quality_score, columns, tags, business_glossary,
                    usage_statistics, lineage, compliance_info, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                asset.id, asset.name, asset.description, asset.schema, asset.table,
                asset.classification.value, asset.owner, asset.steward,
                asset.created_at.isoformat(), asset.updated_at.isoformat(),
                asset.last_accessed.isoformat(), asset.quality_level.value,
                asset.quality_score, json.dumps(asset.columns),
                json.dumps(asset.tags), json.dumps(asset.business_glossary),
                json.dumps(asset.usage_statistics), json.dumps(asset.lineage),
                json.dumps(asset.compliance_info), json.dumps(asset.metadata)
            ))
            
            conn.commit()
            conn.close()
            
            self.assets[asset.id] = asset
            logger.info(f"Added data asset: {asset.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add data asset {asset.name}: {e}")
            return False
    
    def get_data_asset(self, asset_id: str) -> Optional[DataAsset]:
        """Get a data asset by ID"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('SELECT * FROM data_assets WHERE id = ?', (asset_id,))
            row = cursor.fetchone()
            
            if row:
                asset = self._row_to_asset(row)
                conn.close()
                return asset
            
            conn.close()
            return None
            
        except Exception as e:
            logger.error(f"Failed to get data asset {asset_id}: {e}")
            return None
    
    def _row_to_asset(self, row) -> DataAsset:
        """Convert database row to DataAsset object"""
        return DataAsset(
            id=row[0],
            name=row[1],
            description=row[2],
            schema=row[3],
            table=row[4],
            classification=DataClassification(row[5]),
            owner=row[6],
            steward=row[7],
            created_at=datetime.fromisoformat(row[8]),
            updated_at=datetime.fromisoformat(row[9]),
            last_accessed=datetime.fromisoformat(row[10]),
            quality_level=DataQualityLevel(row[11]),
            quality_score=row[12],
            columns=json.loads(row[13]) if row[13] else [],
            tags=json.loads(row[14]) if row[14] else [],
            business_glossary=json.loads(row[15]) if row[15] else {},
            usage_statistics=json.loads(row[16]) if row[16] else {},
            lineage=json.loads(row[17]) if row[17] else {},
            compliance_info=json.loads(row[18]) if row[18] else {},
            metadata=json.loads(row[19]) if row[19] else {}
        )
    
    def search_assets(self, query: str, filters: Dict[str, Any] = None) -> List[DataAsset]:
        """Search data assets"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            # Build search query
            search_conditions = []
            params = []
            
            if query:
                search_conditions.append("""
                    (name LIKE ? OR description LIKE ? OR 
                     schema_name LIKE ? OR table_name LIKE ? OR
                     tags LIKE ?)
                """)
                query_param = f"%{query}%"
                params.extend([query_param] * 5)
            
            if filters:
                if 'classification' in filters:
                    search_conditions.append("classification = ?")
                    params.append(filters['classification'])
                
                if 'owner' in filters:
                    search_conditions.append("owner = ?")
                    params.append(filters['owner'])
                
                if 'quality_level' in filters:
                    search_conditions.append("quality_level = ?")
                    params.append(filters['quality_level'])
            
            where_clause = " AND ".join(search_conditions) if search_conditions else "1=1"
            
            cursor.execute(f"SELECT * FROM data_assets WHERE {where_clause}", params)
            rows = cursor.fetchall()
            
            assets = [self._row_to_asset(row) for row in rows]
            conn.close()
            
            logger.info(f"Found {len(assets)} assets matching search criteria")
            return assets
            
        except Exception as e:
            logger.error(f"Failed to search assets: {e}")
            return []
    
    def add_data_user(self, user: DataUser) -> bool:
        """Add a data user to the catalog"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO data_users (
                    id, name, email, role, department, access_level,
                    created_at, last_login, permissions, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                user.id, user.name, user.email, user.role, user.department,
                user.access_level, user.created_at.isoformat(),
                user.last_login.isoformat(), json.dumps(user.permissions),
                json.dumps(user.metadata)
            ))
            
            conn.commit()
            conn.close()
            
            self.users[user.id] = user
            logger.info(f"Added data user: {user.name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add data user {user.name}: {e}")
            return False
    
    def grant_data_access(self, access: DataAccess) -> bool:
        """Grant data access to a user"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO data_access (
                    id, user_id, asset_id, access_type, granted_at,
                    expires_at, granted_by, purpose, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                access.id, access.user_id, access.asset_id, access.access_type,
                access.granted_at.isoformat(),
                access.expires_at.isoformat() if access.expires_at else None,
                access.granted_by, access.purpose, json.dumps(access.metadata)
            ))
            
            conn.commit()
            conn.close()
            
            self.access_records[access.id] = access
            logger.info(f"Granted {access.access_type} access to user {access.user_id} for asset {access.asset_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to grant data access: {e}")
            return False
    
    def update_quality_metrics(self, asset_id: str, metrics: Dict[str, Any]) -> bool:
        """Update data quality metrics for an asset"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            # Clear existing metrics
            cursor.execute('DELETE FROM data_quality_metrics WHERE asset_id = ?', (asset_id,))
            
            # Insert new metrics
            for metric_name, metric_data in metrics.items():
                cursor.execute('''
                    INSERT INTO data_quality_metrics (
                        id, asset_id, metric_name, metric_value, threshold,
                        status, measured_at, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    f"{asset_id}_{metric_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    asset_id, metric_name, metric_data.get('value', 0),
                    metric_data.get('threshold', 0), metric_data.get('status', 'unknown'),
                    datetime.now().isoformat(), json.dumps(metric_data.get('metadata', {}))
                ))
            
            # Update asset quality score
            overall_score = sum(metric_data.get('value', 0) for metric_data in metrics.values()) / len(metrics)
            quality_level = self._calculate_quality_level(overall_score)
            
            cursor.execute('''
                UPDATE data_assets 
                SET quality_score = ?, quality_level = ?, updated_at = ?
                WHERE id = ?
            ''', (overall_score, quality_level.value, datetime.now().isoformat(), asset_id))
            
            conn.commit()
            conn.close()
            
            logger.info(f"Updated quality metrics for asset {asset_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to update quality metrics for asset {asset_id}: {e}")
            return False
    
    def _calculate_quality_level(self, score: float) -> DataQualityLevel:
        """Calculate quality level based on score"""
        if score >= 0.95:
            return DataQualityLevel.EXCELLENT
        elif score >= 0.85:
            return DataQualityLevel.GOOD
        elif score >= 0.70:
            return DataQualityLevel.FAIR
        else:
            return DataQualityLevel.POOR
    
    def generate_catalog_report(self) -> Dict[str, Any]:
        """Generate comprehensive catalog report"""
        try:
            conn = sqlite3.connect(self.catalog_db_path)
            cursor = conn.cursor()
            
            # Get asset statistics
            cursor.execute('SELECT COUNT(*) FROM data_assets')
            total_assets = cursor.fetchone()[0]
            
            cursor.execute('SELECT classification, COUNT(*) FROM data_assets GROUP BY classification')
            assets_by_classification = dict(cursor.fetchall())
            
            cursor.execute('SELECT quality_level, COUNT(*) FROM data_assets GROUP BY quality_level')
            assets_by_quality = dict(cursor.fetchall())
            
            # Get user statistics
            cursor.execute('SELECT COUNT(*) FROM data_users')
            total_users = cursor.fetchone()[0]
            
            cursor.execute('SELECT role, COUNT(*) FROM data_users GROUP BY role')
            users_by_role = dict(cursor.fetchall())
            
            # Get access statistics
            cursor.execute('SELECT COUNT(*) FROM data_access')
            total_access_records = cursor.fetchone()[0]
            
            cursor.execute('SELECT access_type, COUNT(*) FROM data_access GROUP BY access_type')
            access_by_type = dict(cursor.fetchall())
            
            # Get quality metrics
            cursor.execute('SELECT AVG(metric_value) FROM data_quality_metrics')
            avg_quality_score = cursor.fetchone()[0] or 0
            
            conn.close()
            
            report = {
                "timestamp": datetime.now().isoformat(),
                "summary": {
                    "total_assets": total_assets,
                    "total_users": total_users,
                    "total_access_records": total_access_records,
                    "average_quality_score": round(avg_quality_score, 3)
                },
                "assets_by_classification": assets_by_classification,
                "assets_by_quality": assets_by_quality,
                "users_by_role": users_by_role,
                "access_by_type": access_by_type,
                "recommendations": self._generate_recommendations(
                    total_assets, total_users, avg_quality_score
                )
            }
            
            return report
            
        except Exception as e:
            logger.error(f"Failed to generate catalog report: {e}")
            return {"error": str(e)}
    
    def _generate_recommendations(self, total_assets: int, total_users: int, avg_quality_score: float) -> List[str]:
        """Generate recommendations based on catalog statistics"""
        recommendations = []
        
        if total_assets < 10:
            recommendations.append("Consider adding more data assets to the catalog")
        
        if total_users < 5:
            recommendations.append("Consider onboarding more data users")
        
        if avg_quality_score < 0.8:
            recommendations.append("Focus on improving data quality across assets")
        
        if avg_quality_score < 0.7:
            recommendations.append("Implement comprehensive data quality monitoring")
        
        return recommendations
    
    def export_catalog(self, format: str = "json") -> str:
        """Export catalog data"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if format == "json":
                export_path = os.path.join(self.catalog_dir, f"catalog_export_{timestamp}.json")
                
                # Get all data
                conn = sqlite3.connect(self.catalog_db_path)
                
                # Export assets
                assets_df = pd.read_sql_query("SELECT * FROM data_assets", conn)
                
                # Export users
                users_df = pd.read_sql_query("SELECT * FROM data_users", conn)
                
                # Export access records
                access_df = pd.read_sql_query("SELECT * FROM data_access", conn)
                
                # Export quality metrics
                quality_df = pd.read_sql_query("SELECT * FROM data_quality_metrics", conn)
                
                conn.close()
                
                export_data = {
                    "timestamp": datetime.now().isoformat(),
                    "assets": assets_df.to_dict('records'),
                    "users": users_df.to_dict('records'),
                    "access_records": access_df.to_dict('records'),
                    "quality_metrics": quality_df.to_dict('records')
                }
                
                with open(export_path, 'w') as f:
                    json.dump(export_data, f, indent=2, default=str)
                
            elif format == "csv":
                export_path = os.path.join(self.catalog_dir, f"catalog_export_{timestamp}.zip")
                
                conn = sqlite3.connect(self.catalog_db_path)
                
                # Export each table as CSV
                tables = ["data_assets", "data_users", "data_access", "data_quality_metrics"]
                csv_files = []
                
                for table in tables:
                    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
                    csv_file = os.path.join(self.catalog_dir, f"{table}_{timestamp}.csv")
                    df.to_csv(csv_file, index=False)
                    csv_files.append(csv_file)
                
                conn.close()
                
                # Create zip file
                import zipfile
                with zipfile.ZipFile(export_path, 'w') as zipf:
                    for csv_file in csv_files:
                        zipf.write(csv_file, os.path.basename(csv_file))
                        os.remove(csv_file)  # Clean up individual CSV files
                
            else:
                raise ValueError(f"Unsupported export format: {format}")
            
            logger.info(f"Catalog exported to: {export_path}")
            return export_path
            
        except Exception as e:
            logger.error(f"Failed to export catalog: {e}")
            return ""

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Data Catalog Manager')
    parser.add_argument('action', choices=['add-asset', 'search', 'report', 'export', 'init'],
                       help='Action to perform')
    parser.add_argument('--query', help='Search query')
    parser.add_argument('--format', default='json', choices=['json', 'csv'],
                       help='Export format')
    parser.add_argument('--filters', help='Search filters as JSON')
    
    args = parser.parse_args()
    
    try:
        catalog = DataCatalogManager()
        
        if args.action == 'init':
            print("Data catalog initialized successfully")
            
        elif args.action == 'add-asset':
            # Example: Add a sample asset
            sample_asset = DataAsset(
                id="sample_asset_001",
                name="Sample Data Asset",
                description="A sample data asset for demonstration",
                schema="public",
                table="sample_table",
                classification=DataClassification.INTERNAL,
                owner="data_team",
                steward="data_steward",
                created_at=datetime.now(),
                updated_at=datetime.now(),
                last_accessed=datetime.now(),
                quality_level=DataQualityLevel.GOOD,
                quality_score=0.85,
                columns=[{"name": "id", "type": "integer", "description": "Primary key"}],
                tags=["sample", "demo"],
                business_glossary={"id": "Unique identifier"},
                usage_statistics={"queries_per_day": 10},
                lineage={"sources": ["source_system"]},
                compliance_info={"retention_period": "7 years"},
                metadata={"version": "1.0"}
            )
            
            if catalog.add_data_asset(sample_asset):
                print("Sample asset added successfully")
            else:
                print("Failed to add sample asset")
                sys.exit(1)
                
        elif args.action == 'search':
            filters = json.loads(args.filters) if args.filters else None
            assets = catalog.search_assets(args.query or "", filters)
            
            print(f"Found {len(assets)} assets:")
            for asset in assets:
                print(f"  - {asset.name} ({asset.classification.value})")
                
        elif args.action == 'report':
            report = catalog.generate_catalog_report()
            print(json.dumps(report, indent=2, default=str))
            
        elif args.action == 'export':
            export_path = catalog.export_catalog(args.format)
            if export_path:
                print(f"Catalog exported to: {export_path}")
            else:
                print("Export failed")
                sys.exit(1)
        
    except Exception as e:
        logger.error(f"Data catalog manager failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
