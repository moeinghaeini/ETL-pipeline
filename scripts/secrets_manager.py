#!/usr/bin/env python3
"""
Secrets Management System
Handles secure storage and retrieval of sensitive configuration data
"""

import os
import sys
import json
import base64
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import keyring
import yaml

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/secrets_manager.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class SecretsManager:
    """Comprehensive secrets management system"""
    
    def __init__(self, master_key: Optional[str] = None):
        self.master_key = master_key or os.getenv('SECRETS_MASTER_KEY')
        self.keyring_service = "etl_pipeline"
        self.encryption_key = self._get_or_create_encryption_key()
        self.cipher_suite = Fernet(self.encryption_key)
        
    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for local file encryption"""
        if self.master_key:
            # Derive key from master key
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=b'etl_pipeline_salt',
                iterations=100000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(self.master_key.encode()))
            return key
        else:
            # Use system keyring for key storage
            try:
                stored_key = keyring.get_password(self.keyring_service, "encryption_key")
                if stored_key:
                    return stored_key.encode()
                else:
                    # Generate new key
                    key = Fernet.generate_key()
                    keyring.set_password(self.keyring_service, "encryption_key", key.decode())
                    return key
            except Exception as e:
                logger.warning(f"Keyring not available, using environment variable: {e}")
                # Fallback to environment variable
                key = os.getenv('ENCRYPTION_KEY')
                if not key:
                    raise ValueError("No encryption key available. Set SECRETS_MASTER_KEY or ENCRYPTION_KEY environment variable.")
                return key.encode()
    
    def encrypt_value(self, value: str) -> str:
        """Encrypt a string value"""
        try:
            encrypted_bytes = self.cipher_suite.encrypt(value.encode())
            return base64.urlsafe_b64encode(encrypted_bytes).decode()
        except Exception as e:
            logger.error(f"Failed to encrypt value: {e}")
            raise
    
    def decrypt_value(self, encrypted_value: str) -> str:
        """Decrypt a string value"""
        try:
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_value.encode())
            decrypted_bytes = self.cipher_suite.decrypt(encrypted_bytes)
            return decrypted_bytes.decode()
        except Exception as e:
            logger.error(f"Failed to decrypt value: {e}")
            raise
    
    def store_secret(self, key: str, value: str, environment: str = "default") -> bool:
        """Store a secret securely"""
        try:
            # Encrypt the value
            encrypted_value = self.encrypt_value(value)
            
            # Store in keyring with environment prefix
            keyring_key = f"{environment}:{key}"
            keyring.set_password(self.keyring_service, keyring_key, encrypted_value)
            
            logger.info(f"Secret stored successfully: {environment}:{key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to store secret {key}: {e}")
            return False
    
    def retrieve_secret(self, key: str, environment: str = "default") -> Optional[str]:
        """Retrieve a secret securely"""
        try:
            keyring_key = f"{environment}:{key}"
            encrypted_value = keyring.get_password(self.keyring_service, keyring_key)
            
            if not encrypted_value:
                logger.warning(f"Secret not found: {environment}:{key}")
                return None
            
            # Decrypt the value
            decrypted_value = self.decrypt_value(encrypted_value)
            
            logger.info(f"Secret retrieved successfully: {environment}:{key}")
            return decrypted_value
            
        except Exception as e:
            logger.error(f"Failed to retrieve secret {key}: {e}")
            return None
    
    def delete_secret(self, key: str, environment: str = "default") -> bool:
        """Delete a secret"""
        try:
            keyring_key = f"{environment}:{key}"
            keyring.delete_password(self.keyring_service, keyring_key)
            
            logger.info(f"Secret deleted successfully: {environment}:{key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete secret {key}: {e}")
            return False
    
    def list_secrets(self, environment: str = "default") -> List[str]:
        """List all secrets for an environment"""
        try:
            # Note: keyring doesn't provide a direct way to list all keys
            # This is a limitation of the keyring library
            logger.warning("Listing secrets is not directly supported by keyring")
            return []
            
        except Exception as e:
            logger.error(f"Failed to list secrets: {e}")
            return []
    
    def store_environment_secrets(self, environment: str, secrets: Dict[str, str]) -> bool:
        """Store multiple secrets for an environment"""
        try:
            success_count = 0
            for key, value in secrets.items():
                if self.store_secret(key, value, environment):
                    success_count += 1
            
            logger.info(f"Stored {success_count}/{len(secrets)} secrets for environment {environment}")
            return success_count == len(secrets)
            
        except Exception as e:
            logger.error(f"Failed to store environment secrets: {e}")
            return False
    
    def load_secrets_from_file(self, file_path: str, environment: str = "default") -> bool:
        """Load secrets from a YAML or JSON file"""
        try:
            with open(file_path, 'r') as f:
                if file_path.endswith('.yaml') or file_path.endswith('.yml'):
                    secrets = yaml.safe_load(f)
                elif file_path.endswith('.json'):
                    secrets = json.load(f)
                else:
                    logger.error(f"Unsupported file format: {file_path}")
                    return False
            
            if not isinstance(secrets, dict):
                logger.error("Secrets file must contain a dictionary")
                return False
            
            return self.store_environment_secrets(environment, secrets)
            
        except Exception as e:
            logger.error(f"Failed to load secrets from file: {e}")
            return False
    
    def export_secrets(self, environment: str = "default", output_file: Optional[str] = None) -> Dict[str, str]:
        """Export secrets for an environment (for backup purposes)"""
        try:
            # Note: This is limited by keyring's inability to list keys
            logger.warning("Export functionality is limited due to keyring constraints")
            return {}
            
        except Exception as e:
            logger.error(f"Failed to export secrets: {e}")
            return {}
    
    def rotate_secret(self, key: str, new_value: str, environment: str = "default") -> bool:
        """Rotate a secret with a new value"""
        try:
            # Store new value
            if self.store_secret(key, new_value, environment):
                logger.info(f"Secret rotated successfully: {environment}:{key}")
                return True
            else:
                logger.error(f"Failed to rotate secret: {environment}:{key}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to rotate secret {key}: {e}")
            return False
    
    def validate_secrets(self, required_secrets: List[str], environment: str = "default") -> Dict[str, bool]:
        """Validate that required secrets exist"""
        results = {}
        
        for secret in required_secrets:
            value = self.retrieve_secret(secret, environment)
            results[secret] = value is not None
        
        return results
    
    def create_env_file(self, environment: str = "default", output_file: str = ".env") -> bool:
        """Create environment file with decrypted secrets"""
        try:
            # Define required secrets for each environment
            required_secrets = {
                'development': [
                    'SNOWFLAKE_ACCOUNT_DEV',
                    'SNOWFLAKE_USER_DEV', 
                    'SNOWFLAKE_PASSWORD_DEV',
                    'SNOWFLAKE_WAREHOUSE_DEV',
                    'SNOWFLAKE_DATABASE_DEV',
                    'SNOWFLAKE_SCHEMA_DEV',
                    'SNOWFLAKE_ROLE_DEV'
                ],
                'staging': [
                    'SNOWFLAKE_ACCOUNT_STAGING',
                    'SNOWFLAKE_USER_STAGING',
                    'SNOWFLAKE_PASSWORD_STAGING', 
                    'SNOWFLAKE_WAREHOUSE_STAGING',
                    'SNOWFLAKE_DATABASE_STAGING',
                    'SNOWFLAKE_SCHEMA_STAGING',
                    'SNOWFLAKE_ROLE_STAGING'
                ],
                'production': [
                    'SNOWFLAKE_ACCOUNT_PROD',
                    'SNOWFLAKE_USER_PROD',
                    'SNOWFLAKE_PASSWORD_PROD',
                    'SNOWFLAKE_WAREHOUSE_PROD', 
                    'SNOWFLAKE_DATABASE_PROD',
                    'SNOWFLAKE_SCHEMA_PROD',
                    'SNOWFLAKE_ROLE_PROD'
                ],
                'testing': [
                    'SNOWFLAKE_ACCOUNT_TEST',
                    'SNOWFLAKE_USER_TEST',
                    'SNOWFLAKE_PASSWORD_TEST',
                    'SNOWFLAKE_WAREHOUSE_TEST',
                    'SNOWFLAKE_DATABASE_TEST', 
                    'SNOWFLAKE_SCHEMA_TEST',
                    'SNOWFLAKE_ROLE_TEST'
                ]
            }
            
            secrets_to_export = required_secrets.get(environment, [])
            
            with open(output_file, 'w') as f:
                f.write(f"# Environment variables for {environment} environment\n")
                f.write(f"# Generated on {datetime.now().isoformat()}\n\n")
                
                for secret in secrets_to_export:
                    value = self.retrieve_secret(secret, environment)
                    if value:
                        f.write(f"{secret}={value}\n")
                    else:
                        f.write(f"# {secret}=<not_found>\n")
            
            logger.info(f"Environment file created: {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create environment file: {e}")
            return False

class SecurityAuditor:
    """Security auditing and compliance checking"""
    
    def __init__(self, secrets_manager: SecretsManager):
        self.secrets_manager = secrets_manager
    
    def audit_secrets_security(self) -> Dict[str, Any]:
        """Audit secrets security configuration"""
        audit_results = {
            'timestamp': datetime.now().isoformat(),
            'checks': {},
            'overall_score': 0,
            'recommendations': []
        }
        
        # Check 1: Master key configuration
        master_key_set = bool(os.getenv('SECRETS_MASTER_KEY'))
        audit_results['checks']['master_key_configured'] = {
            'status': 'PASS' if master_key_set else 'FAIL',
            'description': 'Master key for encryption is configured'
        }
        
        # Check 2: Keyring availability
        try:
            keyring.get_password("test", "test")
            keyring_available = True
        except Exception:
            keyring_available = False
        
        audit_results['checks']['keyring_available'] = {
            'status': 'PASS' if keyring_available else 'WARN',
            'description': 'System keyring is available for secure storage'
        }
        
        # Check 3: Environment variable exposure
        sensitive_vars = ['PASSWORD', 'SECRET', 'KEY', 'TOKEN']
        exposed_vars = []
        for key, value in os.environ.items():
            if any(sensitive in key.upper() for sensitive in sensitive_vars):
                if value and not value.startswith('***'):
                    exposed_vars.append(key)
        
        audit_results['checks']['no_exposed_secrets'] = {
            'status': 'PASS' if not exposed_vars else 'FAIL',
            'description': 'No sensitive environment variables are exposed',
            'details': exposed_vars
        }
        
        # Check 4: File permissions
        sensitive_files = ['.env', 'profiles.yml', 'config/profiles.yml']
        file_permissions_ok = True
        for file_path in sensitive_files:
            if os.path.exists(file_path):
                stat = os.stat(file_path)
                if stat.st_mode & 0o077:  # Check if readable by group/others
                    file_permissions_ok = False
                    break
        
        audit_results['checks']['secure_file_permissions'] = {
            'status': 'PASS' if file_permissions_ok else 'FAIL',
            'description': 'Sensitive files have secure permissions'
        }
        
        # Calculate overall score
        total_checks = len(audit_results['checks'])
        passed_checks = sum(1 for check in audit_results['checks'].values() 
                          if check['status'] == 'PASS')
        audit_results['overall_score'] = (passed_checks / total_checks) * 100
        
        # Generate recommendations
        if not master_key_set:
            audit_results['recommendations'].append(
                "Set SECRETS_MASTER_KEY environment variable for encryption"
            )
        
        if not keyring_available:
            audit_results['recommendations'].append(
                "Install and configure system keyring for secure secret storage"
            )
        
        if exposed_vars:
            audit_results['recommendations'].append(
                "Remove or mask sensitive environment variables"
            )
        
        if not file_permissions_ok:
            audit_results['recommendations'].append(
                "Set secure file permissions (600) for sensitive configuration files"
            )
        
        return audit_results

def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Secrets Manager for ETL Pipeline')
    parser.add_argument('action', choices=['store', 'retrieve', 'delete', 'list', 'load', 'export', 'rotate', 'validate', 'audit', 'create-env'],
                       help='Action to perform')
    parser.add_argument('--key', help='Secret key')
    parser.add_argument('--value', help='Secret value')
    parser.add_argument('--env', default='default', help='Environment name')
    parser.add_argument('--file', help='File path for load/export operations')
    parser.add_argument('--output', help='Output file path')
    parser.add_argument('--required', nargs='+', help='Required secrets for validation')
    
    args = parser.parse_args()
    
    try:
        secrets_manager = SecretsManager()
        
        if args.action == 'store':
            if not args.key or not args.value:
                print("Error: --key and --value are required for store action")
                sys.exit(1)
            
            if secrets_manager.store_secret(args.key, args.value, args.env):
                print(f"✅ Secret stored successfully: {args.env}:{args.key}")
            else:
                print(f"❌ Failed to store secret: {args.env}:{args.key}")
                sys.exit(1)
                
        elif args.action == 'retrieve':
            if not args.key:
                print("Error: --key is required for retrieve action")
                sys.exit(1)
            
            value = secrets_manager.retrieve_secret(args.key, args.env)
            if value:
                print(value)
            else:
                print(f"❌ Secret not found: {args.env}:{args.key}")
                sys.exit(1)
                
        elif args.action == 'delete':
            if not args.key:
                print("Error: --key is required for delete action")
                sys.exit(1)
            
            if secrets_manager.delete_secret(args.key, args.env):
                print(f"✅ Secret deleted successfully: {args.env}:{args.key}")
            else:
                print(f"❌ Failed to delete secret: {args.env}:{args.key}")
                sys.exit(1)
                
        elif args.action == 'list':
            secrets = secrets_manager.list_secrets(args.env)
            if secrets:
                for secret in secrets:
                    print(secret)
            else:
                print("No secrets found or listing not supported")
                
        elif args.action == 'load':
            if not args.file:
                print("Error: --file is required for load action")
                sys.exit(1)
            
            if secrets_manager.load_secrets_from_file(args.file, args.env):
                print(f"✅ Secrets loaded successfully from {args.file}")
            else:
                print(f"❌ Failed to load secrets from {args.file}")
                sys.exit(1)
                
        elif args.action == 'export':
            secrets = secrets_manager.export_secrets(args.env, args.output)
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(secrets, f, indent=2)
                print(f"✅ Secrets exported to {args.output}")
            else:
                print(json.dumps(secrets, indent=2))
                
        elif args.action == 'rotate':
            if not args.key or not args.value:
                print("Error: --key and --value are required for rotate action")
                sys.exit(1)
            
            if secrets_manager.rotate_secret(args.key, args.value, args.env):
                print(f"✅ Secret rotated successfully: {args.env}:{args.key}")
            else:
                print(f"❌ Failed to rotate secret: {args.env}:{args.key}")
                sys.exit(1)
                
        elif args.action == 'validate':
            if not args.required:
                print("Error: --required is required for validate action")
                sys.exit(1)
            
            results = secrets_manager.validate_secrets(args.required, args.env)
            all_valid = all(results.values())
            
            for secret, valid in results.items():
                status = "✅" if valid else "❌"
                print(f"{status} {secret}")
            
            if not all_valid:
                sys.exit(1)
                
        elif args.action == 'audit':
            auditor = SecurityAuditor(secrets_manager)
            audit_results = auditor.audit_secrets_security()
            print(json.dumps(audit_results, indent=2))
            
        elif args.action == 'create-env':
            output_file = args.output or f".env.{args.env}"
            if secrets_manager.create_env_file(args.env, output_file):
                print(f"✅ Environment file created: {output_file}")
            else:
                print(f"❌ Failed to create environment file: {output_file}")
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Secrets manager failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
