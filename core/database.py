import pyodbc
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from config.settings import settings
from config.security import verify_credentials
import logging

logger = logging.getLogger(__name__)

# Verificar credenciales al importar
verify_credentials()

# Configuración SQLAlchemy
# Versión corregida
SQLALCHEMY_DATABASE_URL = (
    f"mssql+pyodbc://{settings.SQL_USER}:{settings.SQL_PASSWORD.get_secret_value()}"
    f"@{settings.SQL_SERVER}/{settings.SQL_DATABASE}"
    "?driver=ODBC+Driver+17+for+SQL+Server"
    "&TrustServerCertificate=yes"
)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_pre_ping=True,
    echo=False  # Cambiar a True para debug
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Proveedor de sesión de base de datos para inyección de dependencias"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()