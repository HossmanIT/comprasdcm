import os
from pathlib import Path
from dotenv import load_dotenv
from typing import Dict, Any

class ConfigError(Exception):
    """Excepción personalizada para errores de configuración"""
    pass

class DatabaseConfig:
    """Clase base para configuraciones de base de datos"""
    
    def __init__(self, prefix: str):
        self.prefix = prefix
        self._validate_config()
    
    def _validate_config(self):
        """Valida que todas las variables requeridas estén presentes"""
        required_vars = self.get_required_vars()
        missing = [f"{self.prefix}_{var}" for var in required_vars 
                  if not os.getenv(f"{self.prefix}_{var}")]
        
        if missing:
            raise ConfigError(f"Variables faltantes en .env.db: {', '.join(missing)}")
    
    def get_required_vars(self) -> list:
        """Variables requeridas que deben estar en .env.db"""
        raise NotImplementedError
    
    def get_connection_params(self) -> Dict[str, Any]:
        """Parámetros para la conexión a la base de datos"""
        raise NotImplementedError

class FirebirdConfig(DatabaseConfig):
    """Configuración específica para Firebird"""
    
    def get_required_vars(self) -> list:
        return ['HOST', 'DATABASE', 'USER', 'PWD']
    
    def get_connection_params(self) -> Dict[str, Any]:
        return {
            'dsn': f"{os.getenv(f'{self.prefix}_HOST')}:{os.getenv(f'{self.prefix}_DATABASE')}",
            'user': os.getenv(f'{self.prefix}_USER'),
            'password': os.getenv(f'{self.prefix}_PWD'),
            'charset': 'UTF8'
        }

class SQLServerConfig(DatabaseConfig):
    """Configuración específica para SQL Server"""
    
    def get_required_vars(self) -> list:
        return ['DRIVER', 'SERVER', 'DATABASE', 'USER', 'PWD']
    
    def get_connection_params(self) -> Dict[str, Any]:
        # Validación adicional para el driver
        driver = os.getenv(f'{self.prefix}_DRIVER')
        if not driver.startswith('{') or not driver.endswith('}'):
            driver = f'{{{driver}}}'  # Asegurar formato correcto
            
        return {
            'connection_string': (
                f"DRIVER={driver};"
                f"SERVER={os.getenv(f'{self.prefix}_SERVER')};"
                f"DATABASE={os.getenv(f'{self.prefix}_DATABASE')};"
                f"UID={os.getenv(f'{self.prefix}_USER')};"
                f"PWD={os.getenv(f'{self.prefix}_PWD')};"
                "Encrypt=yes;TrustServerCertificate=yes;"  # Para conexiones seguras
            ),
            'timeout': 30  # Timeout de conexión en segundos
        }

def load_configurations():
    """Carga todas las configuraciones desde .env.db"""
    
    # Buscar y cargar el archivo .env.db
    env_path = Path('.env.db')
    if not env_path.exists():
        raise ConfigError(f"No se encontró el archivo .env.db en {env_path.absolute()}")
    
    load_dotenv(env_path)
    
    # Configuraciones
    configs = {
        'firebird': FirebirdConfig('FIREBIRD'),
        'sqlserver': SQLServerConfig('SQL')
    }
    
    #print("Configuraciones cargadas correctamente")
    return configs