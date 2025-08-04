from datetime import datetime, date, timedelta
from typing import List
from sqlalchemy.orm import Session
from models.entities import SQLCOMPC02
import logging

logger = logging.getLogger(__name__)

class SQLService:
    def __init__(self, db: Session):
        self.db = db
    
    def get_recent_purchases(self, days_back: int = 60) -> List[SQLCOMPC02]:
        """Obtiene las facturas de los últimos N días que no han sido sincronizadas"""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        try:
            purchases = self.db.query(SQLCOMPC02).filter(
                SQLCOMPC02.FECHA_DOC >= start_date,
                SQLCOMPC02.FECHA_DOC <= end_date,
                SQLCOMPC02.SINCRONIZADO == False
            ).all()
            
            logger.info(f"Encontradas {len(purchases)} facturas no sincronizadas de los últimos {days_back} días")
            return purchases
        except Exception as e:
            logger.error(f"Error al obtener facturas: {str(e)}")
            raise