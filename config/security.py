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
    errors = []
    
    if not settings.SQL_SERVER:
        errors.append("SQL_SERVER")
    if not settings.SQL_DATABASE:
        errors.append("SQL_DATABASE") 
    if not settings.SQL_USER:
        errors.append("SQL_USER")
    if not settings.SQL_PASSWORD.get_secret_value():
        errors.append("SQL_PASSWORD")
    if not settings.MONDAY_API_KEY.get_secret_value() or settings.MONDAY_API_KEY.get_secret_value() == "your_api_key_here":
        errors.append("MONDAY_API_KEY")
    if not settings.MONDAY_BOARD_ID:
        errors.append("MONDAY_BOARD_ID")
    
    if errors:
        raise ValueError(f"Credenciales faltantes o inválidas: {', '.join(errors)}")
    
    logger.info("Todas las credenciales verificadas correctamente")