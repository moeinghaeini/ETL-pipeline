#!/usr/bin/env python3
"""
Environment Management Script
Handles environment-specific configurations and deployments
"""

import os
import sys
import yaml
import logging
from typing import Dict, Any, Optional
from pathlib import Path
import subprocess
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/environment_manager.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class EnvironmentManager:
    """Manages different environments and their configurations"""
    
    def __init__(self, config_file: str = "config/environments.yml"):
        self.config_file = config_file
        self.config = self._load_config()
        self.current_env = os.getenv('ENVIRONMENT', 'development')
        
    def _load_config(self) -> Dict[str, Any]:
        """Load environment configuration from YAML file"""
        try:
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Loaded configuration from {self.config_file}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_file}")
            return {}
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse configuration file: {e}")
            return {}
    
    def get_environment_config(self, env_name: Optional[str] = None) -> Dict[str, Any]:
        """Get configuration for a specific environment"""
        env_name = env_name or self.current_env
        
        if env_name not in self.config.get('environments', {}):
            logger.error(f"Environment '{env_name}' not found in configuration")
            return {}
        
        env_config = self.config['environments'][env_name].copy()
        
        # Merge with global common settings
        if 'common' in self.config.get('global', {}):
            common_config = self.config['global']['common']
            env_config = self._deep_merge(env_config, common_config)
        
        return env_config
    
    def _deep_merge(self, base: Dict[str, Any], update: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries"""
        result = base.copy()
        
        for key, value in update.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result
    
    def set_environment_variables(self, env_name: Optional[str] = None) -> None:
        """Set environment variables for the specified environment"""
        env_name = env_name or self.current_env
        env_config = self.get_environment_config(env_name)
        
        if not env_config:
            logger.error(f"Cannot set environment variables for '{env_name}'")
            return
        
        # Set dbt environment variables
        dbt_config = env_config.get('dbt', {})
        os.environ['DBT_TARGET'] = dbt_config.get('target', env_name)
        os.environ['DBT_THREADS'] = str(dbt_config.get('threads', 2))
        
        # Set Snowflake environment variables
        snowflake_config = env_config.get('snowflake', {})
        os.environ['SNOWFLAKE_WAREHOUSE'] = snowflake_config.get('warehouse', '')
        os.environ['SNOWFLAKE_DATABASE'] = snowflake_config.get('database', '')
        os.environ['SNOWFLAKE_SCHEMA'] = snowflake_config.get('schema', '')
        os.environ['SNOWFLAKE_ROLE'] = snowflake_config.get('role', '')
        
        # Set monitoring environment variables
        monitoring_config = env_config.get('monitoring', {})
        os.environ['MONITORING_ENABLED'] = str(monitoring_config.get('enabled', True))
        os.environ['ALERT_LEVEL'] = monitoring_config.get('alert_level', 'low')
        
        logger.info(f"Environment variables set for '{env_name}'")
    
    def validate_environment(self, env_name: Optional[str] = None) -> bool:
        """Validate that all required environment variables are set"""
        env_name = env_name or self.current_env
        env_config = self.get_environment_config(env_name)
        
        if not env_config:
            return False
        
        required_vars = [
            f'SNOWFLAKE_ACCOUNT_{env_name.upper()}',
            f'SNOWFLAKE_USER_{env_name.upper()}',
            f'SNOWFLAKE_PASSWORD_{env_name.upper()}'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            logger.error(f"Missing required environment variables for '{env_name}': {missing_vars}")
            return False
        
        logger.info(f"Environment '{env_name}' validation passed")
        return True
    
    def run_dbt_command(self, command: str, env_name: Optional[str] = None) -> bool:
        """Run dbt command for specific environment"""
        env_name = env_name or self.current_env
        
        if not self.validate_environment(env_name):
            return False
        
        self.set_environment_variables(env_name)
        
        # Build dbt command
        dbt_cmd = f"dbt {command} --target {env_name}"
        
        try:
            logger.info(f"Running dbt command: {dbt_cmd}")
            result = subprocess.run(
                dbt_cmd.split(),
                capture_output=True,
                text=True,
                check=True
            )
            
            logger.info("dbt command completed successfully")
            logger.info(f"Output: {result.stdout}")
            
            return True
            
        except subprocess.CalledProcessError as e:
            logger.error(f"dbt command failed: {e}")
            logger.error(f"Error output: {e.stderr}")
            return False
    
    def deploy_to_environment(self, env_name: str) -> bool:
        """Deploy the project to a specific environment"""
        logger.info(f"Deploying to environment: {env_name}")
        
        # Validate environment
        if not self.validate_environment(env_name):
            return False
        
        # Set environment variables
        self.set_environment_variables(env_name)
        
        # Run dbt commands in sequence
        commands = [
            "deps",  # Install dependencies
            "compile",  # Compile models
            "run",  # Run models
            "test",  # Run tests
            "docs generate"  # Generate documentation
        ]
        
        for cmd in commands:
            if not self.run_dbt_command(cmd, env_name):
                logger.error(f"Deployment failed at command: {cmd}")
                return False
        
        logger.info(f"Successfully deployed to environment: {env_name}")
        return True
    
    def list_environments(self) -> None:
        """List all available environments"""
        environments = self.config.get('environments', {})
        
        print("\nAvailable Environments:")
        print("=" * 50)
        
        for env_name, env_config in environments.items():
            status = "✓" if self.validate_environment(env_name) else "✗"
            name = env_config.get('name', env_name)
            description = env_config.get('description', 'No description')
            
            print(f"{status} {env_name:<12} - {name}")
            print(f"    {description}")
            print()
    
    def get_environment_status(self, env_name: Optional[str] = None) -> Dict[str, Any]:
        """Get status information for an environment"""
        env_name = env_name or self.current_env
        env_config = self.get_environment_config(env_name)
        
        if not env_config:
            return {'status': 'error', 'message': f'Environment {env_name} not found'}
        
        status = {
            'environment': env_name,
            'name': env_config.get('name', env_name),
            'description': env_config.get('description', ''),
            'validated': self.validate_environment(env_name),
            'config': env_config
        }
        
        return status
    
    def create_environment_script(self, env_name: str) -> str:
        """Create a deployment script for a specific environment"""
        env_config = self.get_environment_config(env_name)
        
        if not env_config:
            logger.error(f"Cannot create script for environment '{env_name}'")
            return ""
        
        script_content = f"""#!/bin/bash
# Deployment script for {env_name} environment
# Generated by Environment Manager

set -e

echo "🚀 Deploying to {env_name} environment..."

# Set environment variables
export ENVIRONMENT={env_name}
export DBT_TARGET={env_name}

# Validate environment
echo "✅ Validating environment configuration..."
python scripts/environment_manager.py validate --env {env_name}

# Install dependencies
echo "📦 Installing dbt dependencies..."
dbt deps --target {env_name}

# Compile models
echo "🔨 Compiling dbt models..."
dbt compile --target {env_name}

# Run models
echo "🏃 Running dbt models..."
dbt run --target {env_name}

# Run tests
echo "🧪 Running dbt tests..."
dbt test --target {env_name}

# Generate documentation
echo "📚 Generating documentation..."
dbt docs generate --target {env_name}

echo "✅ Deployment to {env_name} completed successfully!"
"""
        
        script_path = f"scripts/deploy_{env_name}.sh"
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(script_path, 0o755)
        
        logger.info(f"Created deployment script: {script_path}")
        return script_path

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Environment Manager for ETL Pipeline')
    parser.add_argument('action', choices=['list', 'validate', 'deploy', 'status', 'script'],
                       help='Action to perform')
    parser.add_argument('--env', help='Environment name')
    parser.add_argument('--command', help='dbt command to run (for deploy action)')
    
    args = parser.parse_args()
    
    try:
        manager = EnvironmentManager()
        
        if args.action == 'list':
            manager.list_environments()
            
        elif args.action == 'validate':
            env_name = args.env or manager.current_env
            if manager.validate_environment(env_name):
                print(f"✅ Environment '{env_name}' is valid")
                sys.exit(0)
            else:
                print(f"❌ Environment '{env_name}' validation failed")
                sys.exit(1)
                
        elif args.action == 'deploy':
            env_name = args.env or manager.current_env
            if manager.deploy_to_environment(env_name):
                print(f"✅ Successfully deployed to '{env_name}'")
                sys.exit(0)
            else:
                print(f"❌ Deployment to '{env_name}' failed")
                sys.exit(1)
                
        elif args.action == 'status':
            env_name = args.env or manager.current_env
            status = manager.get_environment_status(env_name)
            print(json.dumps(status, indent=2, default=str))
            
        elif args.action == 'script':
            env_name = args.env or manager.current_env
            script_path = manager.create_environment_script(env_name)
            if script_path:
                print(f"✅ Created deployment script: {script_path}")
            else:
                print(f"❌ Failed to create deployment script for '{env_name}'")
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Environment manager failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
