import logging
from functools import lru_cache
from config.settings import settings

logger = logging.getLogger(__name__)

@lru_cache()
def get_settings():
    """Obtiene la configuración con cache para mejor performance"""
    logger.info("Cargando configuraciones de la aplicación")
    return settings

def verify_credentials():
    """Verifica que las credenciales esenciales estén configuradas"""
    required_credentials = [
        settings.SQL_SERVER,
        settings.SQL_DATABASE,
        settings.SQL_USER,
        settings.MONDAY_API_KEY,
        settings.MONDAY_BOARD_ID
    ]
    
    if not all(required_credentials):
        missing = [name for name, value in settings.dict().items() if not value]
        raise ValueError(f"Credenciales faltantes: {', '.join(missing)}")