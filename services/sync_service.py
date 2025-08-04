from datetime import datetime
from typing import List
from sqlalchemy.orm import Session
from models.schemas import Compra, MondayItem
from models.entities import SQLCOMPC02
from core.monday_client import monday_client
from config.settings import settings
import logging

logger = logging.getLogger(__name__)

class SyncService:
    @staticmethod
    def map_to_monday_format(compra: Compra) -> MondayItem:
        """Mapea los datos de SQL a formato de Monday.com"""
        column_values = {
            "text_mknkr94f":compra.NOMBRE,
            "text_mknk2qt": compra.SU_REFER,
            #"date4":{"date" : "2023-03-30", "time" : "08:49:02"},
            "date4": compra.FECHA_DOC.isoformat(),
            "date_mknkfx2h": compra.FECHA_PAG.isoformat(),
            "text_mkrmc4e5": compra.MONEDA,
            "numeric_mknkwc1": compra.TIPCAMB,
            "numeric_mknkwz9j": compra.TOT_IND,
            "numeric_mknkb26y": compra.IMPORTE,
            "numeric_mknk6qv1": compra.IMPORTEME,
            
        }
        #print(f"Registros transferidos: {compra.FECHA_DOC.isoformat()}")
        return MondayItem(
            name=compra.CVE_DOC,
            column_values=column_values
        )
    
    def sync_purchases(self, purchases: List[Compra], db: Session) -> dict:
        """Sincroniza las compras con Monday.com y actualiza SQL"""
        results = []
        for purchase in purchases:
            try:
                # 1. Obtener o crear el grupo correspondiente al mes de la fecha
                group_id = monday_client.get_or_create_group_by_date(
                    board_id=settings.MONDAY_BOARD_ID,
                    fecha_doc=purchase.FECHA_DOC
                )

                # 2. Mapear datos a formato Monday
                monday_item = self.map_to_monday_format(purchase)

                # 3. Crear item en Monday en el grupo correcto
                result = monday_client.create_item(
                    board_id=settings.MONDAY_BOARD_ID,
                    item_name=monday_item.name,
                    column_values=monday_item.column_values,
                    group_id=group_id  # ← AQUÍ SE ESPECIFICA EL GRUPO
                )

                # 4. Si se sincronizó correctamente, marcarlo en SQL
                if result.get('data', {}).get('create_item', {}).get('id'):
                    db.query(SQLCOMPC02).filter(SQLCOMPC02.CVE_DOC == purchase.CVE_DOC).update({"SINCRONIZADO": True})
                    db.commit()

                    # Calcular nombre del grupo para el log
                    meses = {1: "ENE", 2: "FEB", 3: "MAR", 4: "ABR", 5: "MAY", 6: "JUN",
                             7: "JUL", 8: "AGO", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DIC"}
                    mes_nombre = meses[purchase.FECHA_DOC.month]
                    grupo_nombre = f"{mes_nombre}-{purchase.FECHA_DOC.year}"

                    logger.info(f"Documento {purchase.CVE_DOC} sincronizado en grupo '{grupo_nombre}' (ID: {group_id})")

                results.append({
                    "CVE_DOC": purchase.CVE_DOC,
                    "monday_id": result.get('data', {}).get('create_item', {}).get('id'),
                    "group_id": group_id,
                    "status": "success"
                })

            except Exception as e:
                logger.error(f"Error al sincronizar documento {purchase.CVE_DOC}: {str(e)}")
                results.append({
                    "CVE_DOC": purchase.CVE_DOC,
                    "status": "failed",
                    "error": str(e)
                })
                db.rollback()

        return {
            "synced_items": len([r for r in results if r["status"] == "success"]),
            "failed_items": len([r for r in results if r["status"] == "failed"]),
            "details": results
        }