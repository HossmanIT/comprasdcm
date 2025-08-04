from datetime import datetime
import fdb
import pyodbc
from settingsfb import load_configurations, ConfigError
import os

def exportar_registros():
    try:
        # 1. Cargar configuraciones
        #print("\nCargando configuraciones...")
        configs = load_configurations()
        
        # 2. Configuración de conexión a Firebird
        fb_config = configs['firebird'].get_connection_params()
        #print("\nConfiguración Firebird:")
        #print(f"Servidor: {os.getenv('FIREBIRD_HOST')}")
        #print(f"Base de datos: {os.getenv('FIREBIRD_DATABASE')}")
        #print(f"Usuario: {os.getenv('FIREBIRD_USER')}")

        # 3. Conexión a Firebird
        try:
            #print("\nConectando a Firebird...")
            firebird_conn = fdb.connect(**fb_config)
            firebird_cursor = firebird_conn.cursor()
            #print("✔ Conexión a Firebird establecida")
            
            # Verificación básica de conexión
            firebird_cursor.execute("SELECT COUNT(*) FROM COMPC01")
            total_registros = firebird_cursor.fetchone()[0]
            #print(f"Total de registros en COMPC01: {total_registros}")
            
        except fdb.fbcore.DatabaseError as e:
            #print(f"❌ Error de conexión a Firebird: {str(e)}")
            #print("\nPosibles soluciones:")
            #print("1. Verifica que el servicio Firebird esté corriendo")
            #print("2. Confirma las credenciales en el archivo .env")
            #print("3. Asegúrate que la ruta de la base de datos sea correcta")
            return

        # 4. Configuración de conexión a SQL Server
        sql_config = configs['sqlserver'].get_connection_params()
        #print("\nConfiguración SQL Server:")
        #print(f"Servidor: {os.getenv('SQL_SERVER')}")
        #print(f"Base de datos: {os.getenv('SQL_DATABASE')}")
        #print(f"Usuario: {os.getenv('SQL_USER')}")

        # 5. Conexión a SQL Server con manejo mejorado
        try:
            #print("\nConectando a SQL Server...")
            sql_conn = pyodbc.connect(
                sql_config['connection_string'],
                timeout=sql_config.get('timeout', 30)
            )
            sql_cursor = sql_conn.cursor()
            #print("✔ Conexión a SQL Server establecida")
            
            # Test simple de conexión
            sql_cursor.execute("SELECT DB_NAME() AS db_name")
            db_name = sql_cursor.fetchone()[0]
            #print(f"Conectado a la base de datos: {db_name}")
            
        except pyodbc.Error as e:
            # Ocultar credenciales en el mensaje de error
            error_msg = str(e).replace(sql_config['connection_string'], '*****')
            #print(f"❌ Error de conexión a SQL Server: {error_msg}")
            #print("\nPosibles soluciones:")
            #print("1. Verifica que el servidor SQL esté accesible")
            #print("2. Confirma usuario y contraseña")
            #print("3. Asegúrate que el usuario tenga permisos")
            #print("4. Verifica que el driver ODBC esté instalado")
            return

        # 6. Verificar/crear tabla en SQL Server
        #print("\nVerificando estructura en SQL Server...")
        try:
            sql_cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'SQLCOMPC01')
            CREATE TABLE SQLCOMPC01 (
                CVE_DOC VARCHAR(50) PRIMARY KEY,
                NOMBRE VARCHAR(100),
                SU_REFER VARCHAR(50),               
                FECHA_DOC DATE NOT NULL,
                MONEDA VARCHAR(100),
                TIPCAMB FLOAT NOT NULL,
                TOT_IND FLOAT NOT NULL,
                IMPORTE FLOAT NOT NULL,
                IMPORTEME FLOAT NOT NULL,                              
                SINCRONIZADO BIT DEFAULT 0                          
            )
            """)
            sql_conn.commit()
            #print("✔ Tabla SQLCOMPC01 verificada/creada")
        except pyodbc.Error as e:
            #print(f"❌ Error al verificar tabla: {str(e)}")
            return

        # 7. Obtener registros ya transferidos
        try:
            sql_cursor.execute("SELECT CVE_DOC FROM SQLCOMPC01")
            docs_transferidos = {row[0] for row in sql_cursor.fetchall()}
            #print(f"Registros ya existentes en destino: {len(docs_transferidos)}")
        except pyodbc.Error as e:
            #print(f"❌ Error al consultar registros existentes: {str(e)}")
            return

        # 8. Consulta Firebird para registros del día
        fecha_actual = datetime.now().date()
        #print(f"\nBuscando registros del día: {fecha_actual}")
        
        try:
            firebird_cursor.execute("""
            SELECT f.CVE_DOC, c.NOMBRE, f.SU_REFER, CAST(f.FECHA_DOC AS DATE) AS FECHA_DOC, m.DESCR AS MONEDA, f.TIPCAMB, f.TOT_IND, f.IMPORTE,
            (CASE WHEN f.TIPCAMB = 0 THEN 0 ELSE f.IMPORTE / f.TIPCAMB END) AS IMPORTEME, 0 AS SINCRONIZADO
            FROM COMPC01 f JOIN PROV01 c ON f.CVE_CLPV = c.CLAVE JOIN MONED01 m ON f.NUM_MONED = m.NUM_MONED
            WHERE CAST(FECHA_DOC AS DATE) = ?
            """, (fecha_actual,))
            
            registros = [
                row for row in firebird_cursor.fetchall()
                if row[0] not in docs_transferidos
            ]
            
            #print(f"Registros nuevos encontrados: {len(registros)}")
            
        except fdb.fbcore.DatabaseError as e:
            #print(f"❌ Error al consultar Firebird: {str(e)}")
            return

        # 9. Transferencia de registros
        if registros:
            try:
                #print("\nIniciando transferencia...")
                sql_cursor.executemany(
                    "INSERT INTO SQLCOMPC01 (CVE_DOC, NOMBRE, SU_REFER, FECHA_DOC, MONEDA, TIPCAMB, TOT_IND, IMPORTE, IMPORTEME, SINCRONIZADO) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
                    registros
                )
                sql_conn.commit()
                #print(f"✔ Registros transferidos exitosamente: {len(registros)}")
                
            except pyodbc.Error as e:
                #print(f"❌ Error durante la transferencia: {str(e)}")
                sql_conn.rollback()
        else:
            print("\nNo hay registros nuevos para transferir hoy")

    except ConfigError as e:
        print(f"\n❌ Error de configuración: {str(e)}")
    except Exception as e:
        print(f"\n❌ Error inesperado: {str(e)}")
    finally:
        # 10. Cierre seguro de conexiones
        #print("\nCerrando conexiones...")
        if 'firebird_cursor' in locals(): 
            firebird_cursor.close()
            #print("Conexión Firebird - Cursor cerrado")
        if 'firebird_conn' in locals(): 
            firebird_conn.close()
            #print("Conexión Firebird - Cerrada")
        if 'sql_cursor' in locals(): 
            sql_cursor.close()
            #print("Conexión SQL Server - Cursor cerrado")
        if 'sql_conn' in locals(): 
            sql_conn.close()
            #print("Conexión SQL Server - Cerrada")

if __name__ == "__main__":
    print("=== Inicio del proceso de transferencia ===")
    exportar_registros()
    print("\n=== Proceso completado ===")