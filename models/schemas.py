from pydantic import BaseModel
from datetime import datetime

class Compra(BaseModel):
    CVE_DOC: str
    NOMBRE: str
    SU_REFER: str
    FECHA_DOC: datetime
    FECHA_PAG: datetime
    MONEDA: str
    TIPCAMB: float
    TOT_IND: float
    IMPORTE: float
    IMPORTEME: float
    SINCRONIZADO: bool   

class MondayItem(BaseModel):
    name: str
    column_values: dict