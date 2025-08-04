from datetime import datetime, timedelta
import fdb
import pyodbc
from settingsfb import load_configurations, ConfigError
import os

def exportar_registros():
    try:
        # 1. Cargar configuraciones
        configs = load_configurations()
        
        # Obtener días a transferir desde variable de entorno (por defecto: 30)
        dias_atras = int(os.getenv("DIAS_A_TRANSFERIR", 30))
        fecha_actual = datetime.now().date()
        fecha_inicio = fecha_actual - timedelta(days=dias_atras)
        print(f"\nBuscando registros desde {fecha_inicio} hasta {fecha_actual}")

        # 2. Configuración de conexión a Firebird
        fb_config = configs['firebird'].get_connection_params()

        # 3. Conexión a Firebird
        try:
            firebird_conn = fdb.connect(**fb_config)
            firebird_cursor = firebird_conn.cursor()
            
            # Verificación básica de conexión
            firebird_cursor.execute("SELECT COUNT(*) FROM COMPC02")
            total_registros = firebird_cursor.fetchone()[0]
            
        except fdb.fbcore.DatabaseError as e:
            print(f"❌ Error de conexión a Firebird: {str(e)}")
            return

        # 4. Configuración de conexión a SQL Server
        sql_config = configs['sqlserver'].get_connection_params()

        # 5. Conexión a SQL Server
        try:
            sql_conn = pyodbc.connect(
                sql_config['connection_string'],
                timeout=sql_config.get('timeout', 30)
            )
            sql_cursor = sql_conn.cursor()
            
            # Test simple de conexión
            sql_cursor.execute("SELECT DB_NAME() AS db_name")
            db_name = sql_cursor.fetchone()[0]
            
        except pyodbc.Error as e:
            error_msg = str(e).replace(sql_config['connection_string'], '*****')
            print(f"❌ Error de conexión a SQL Server: {error_msg}")
            return

        # 6. Verificar/crear tabla en SQL Server
        try:
            sql_cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SQLCOMPC02')
            CREATE TABLE SQLCOMPC02 (
                CVE_DOC VARCHAR(50) PRIMARY KEY,
                NOMBRE VARCHAR(100),
                SU_REFER VARCHAR(50),               
                FECHA_DOC DATE NOT NULL,
                FECHA_PAG DATE NOT NULL,               
                MONEDA VARCHAR(100),
                TIPCAMB FLOAT NOT NULL,
                TOT_IND FLOAT NOT NULL,
                IMPORTE FLOAT NOT NULL,
                IMPORTEME FLOAT NOT NULL,                              
                SINCRONIZADO BIT DEFAULT 0                          
            )
            """)
            sql_conn.commit()
        except pyodbc.Error as e:
            print(f"❌ Error al verificar tabla: {str(e)}")
            return

        # 7. Obtener registros ya transferidos
        try:
            sql_cursor.execute("SELECT CVE_DOC FROM SQLCOMPC02")
            docs_transferidos = {row[0] for row in sql_cursor.fetchall()}
        except pyodbc.Error as e:
            print(f"❌ Error al consultar registros existentes: {str(e)}")
            return

        # 8. Consulta Firebird para registros del rango de fechas
        try:
            firebird_cursor.execute("""
            SELECT f.CVE_DOC, c.NOMBRE, f.SU_REFER, CAST(f.FECHA_DOC AS DATE) AS FECHA_DOC, f.FECHA_PAG, m.DESCR AS MONEDA, f.TIPCAMB, f.TOT_IND, f.IMPORTE,
            (CASE WHEN f.TIPCAMB = 0 THEN 0 ELSE f.IMPORTE / f.TIPCAMB END) AS IMPORTEME, 0 AS SINCRONIZADO
            FROM COMPC02 f JOIN PROV02 c ON f.CVE_CLPV = c.CLAVE JOIN MONED02 m ON f.NUM_MONED = m.NUM_MONED
            WHERE CAST(FECHA_DOC AS DATE) BETWEEN ? AND ?
            """, (fecha_inicio, fecha_actual))
            
            registros = [
                row for row in firebird_cursor.fetchall()
                if row[0] not in docs_transferidos
            ]
            
            print(f"Registros nuevos encontrados: {len(registros)}")
            
        except fdb.fbcore.DatabaseError as e:
            print(f"❌ Error al consultar Firebird: {str(e)}")
            return

        # 9. Transferencia de registros
        if registros:
            try:
                print("\nIniciando transferencia...")
                sql_cursor.executemany(
                    "INSERT INTO SQLCOMPC02 (CVE_DOC, NOMBRE, SU_REFER, FECHA_DOC, FECHA_PAG, MONEDA, TIPCAMB, TOT_IND, IMPORTE, IMPORTEME, SINCRONIZADO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                    registros
                )
                sql_conn.commit()
                print(f"✔ Registros transferidos exitosamente: {len(registros)}")
                
            except pyodbc.Error as e:
                print(f"❌ Error durante la transferencia: {str(e)}")
                sql_conn.rollback()
        else:
            print("\nNo hay registros nuevos para transferir en el rango de fechas")

    except ConfigError as e:
        print(f"\n❌ Error de configuración: {str(e)}")
    except Exception as e:
        print(f"\n❌ Error inesperado: {str(e)}")
    finally:
        # 10. Cierre seguro de conexiones
        if 'firebird_cursor' in locals(): 
            firebird_cursor.close()
        if 'firebird_conn' in locals(): 
            firebird_conn.close()
        if 'sql_cursor' in locals(): 
            sql_cursor.close()
        if 'sql_conn' in locals(): 
            sql_conn.close()

if __name__ == "__main__":
    print("=== Inicio del proceso de transferencia ===")
    exportar_registros()
    print("\n=== Proceso completado ===")