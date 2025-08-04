from sqlalchemy import Column, String, String, String, DateTime, DateTime, String, Float, Float, Float, Float, Boolean
from core.database import Base

class SQLCOMPC02(Base):
    __tablename__ = "SQLCOMPC02"
    
    CVE_DOC = Column(String, primary_key=True)
    NOMBRE = Column(String)
    SU_REFER = Column(String)
    FECHA_DOC = Column(DateTime)
    FECHA_PAG = Column(DateTime)
    MONEDA = Column(String)
    TIPCAMB = Column(Float)
    TOT_IND = Column(Float)
    IMPORTE = Column(Float)
    IMPORTEME = Column(Float)
    SINCRONIZADO = Column(Boolean, default=False, nullable=False)