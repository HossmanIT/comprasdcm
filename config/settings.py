from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr
from pathlib import Path

class Settings(BaseSettings):
    # Config SQL
    SQL_SERVER: str = Field(..., min_length=1)
    SQL_DATABASE: str = Field(..., min_length=1)
    SQL_USER: str = Field(..., min_length=1)
    SQL_PASSWORD: SecretStr = Field(...)
    SQL_DRIVER: str = "ODBC Driver 17 for SQL Server"
    
    # Config Monday.com
    MONDAY_API_KEY: SecretStr = Field(...)
    MONDAY_BOARD_ID: str = Field(..., min_length=1)
    MONDAY_API_URL: str = "https://api.monday.com/v2"
    
    # Config FastAPI (agregar estos nuevos campos)
    API_TITLE: str = "Sincronizador SQL a Monday.com"
    API_VERSION: str = "1.0.0"
    API_DESCRIPTION: str = "API para sincronizar datos de SQL Server a Monday.com"

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent / '.env',
        env_file_encoding='utf-8',
        extra='forbid'
    )

# Carga de configuración
settings = Settings()