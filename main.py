from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from services.sql_service import SQLService
from services.sync_service import SyncService
from models.schemas import Compra     
from core.database import get_db
from config.settings import settings
import logging

logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION
)

@app.post("/sync-recent-purchasescdcm", tags=["Sync"])
async def sync_recent_purchases(db: Session = Depends(get_db)):
    """Endpoint para sincronizar compras recientes con Monday.com y actualizar SQL"""
    try:
        # Obtener compras recientes
        sql_service = SQLService(db)
        purchases = sql_service.get_recent_purchases()
        
        # Sincronizar con Monday.com y actualizar SQL
        sync_service = SyncService()
        result = sync_service.sync_purchases(purchases, db)  # Pasamos la sesión de DB
        
        return {
            "status": "success",
            **result
        }
    except Exception as e:
        db.rollback()  # Asegurar que no quedan transacciones pendientes
        raise HTTPException(status_code=500, detail=str(e))