import requests
import json
from typing import Dict, Any
from config.settings import settings
from config.security import verify_credentials
import logging

logger = logging.getLogger(__name__)

class MondayClient:
    def __init__(self):
        verify_credentials()
        self.headers = {
            "Authorization": settings.MONDAY_API_KEY.get_secret_value(),  # Convertir SecretStr a str
            "Content-Type": "application/json"
        }
        self.api_url = settings.MONDAY_API_URL

    def create_item(self, board_id: str, item_name: str, column_values: Dict[str, Any], group_id: str = None):
        """Crea un nuevo ítem en el tablero especificado"""
        group_part = f', group_id: "{group_id}"' if group_id else ''
        escaped_item_name = item_name.replace('"', '\\"')
        column_values_str = json.dumps(column_values)
        column_values_str_escaped = column_values_str.replace('"', '\\"')
        query = f'''
        mutation {{
            create_item (
                board_id: {board_id},
                item_name: "{escaped_item_name}",
                column_values: "{column_values_str_escaped}"{group_part}
            ) {{
                id
            }}
        }}
        '''
        logger.info(f"Query enviado a Monday:\n{query}")

        try:
            response = requests.post(
                self.api_url,
                json={'query': query},
                headers=self.headers
            )
            response.raise_for_status()
            result = response.json()
            logger.info(f"Respuesta de Monday:\n{result}")
            if 'errors' in result:
                logger.error(f"Error en GraphQL: {result['errors']}")
                raise Exception(f"GraphQL errors: {result['errors']}")
            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al crear ítem en Monday: {str(e)}")
            if e.response is not None:
                logger.error(f"Respuesta del servidor: {e.response.text}")
            raise

    def get_or_create_group_by_date(self, board_id: str, fecha_doc):
        """
        Busca un grupo por nombre (ej: 'AGO-2024') en el board.
        Si no existe, lo crea y retorna el ID.
        """
        meses = {1: "ENE", 2: "FEB", 3: "MAR", 4: "ABR", 5: "MAY", 6: "JUN",
                 7: "JUL", 8: "AGO", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DIC"}
        mes_nombre = meses[fecha_doc.month]
        group_name = f"{mes_nombre}-{fecha_doc.year}"

        try:
            # 1. Consultar los grupos existentes
            query = f"""
            query {{
                boards(ids: {board_id}) {{
                    groups {{
                        id
                        title
                    }}
                }}
            }}
            """
            response = requests.post(
                self.api_url,
                json={'query': query},
                headers=self.headers
            )
            response.raise_for_status()
            result = response.json()
            
            # Verificar errores GraphQL
            if 'errors' in result:
                logger.error(f"Error en GraphQL al consultar grupos: {result['errors']}")
                raise Exception(f"GraphQL errors: {result['errors']}")
            
            # Verificar que el board existe
            if not result.get('data', {}).get('boards') or not result['data']['boards']:
                raise Exception(f"Board {board_id} no encontrado")
                
            groups = result['data']['boards'][0]['groups']
            
            # Buscar grupo existente
            for group in groups:
                if group['title'] == group_name:
                    logger.info(f"Grupo '{group_name}' encontrado con ID: {group['id']}")
                    return group['id']

            # 2. Si no existe, crear el grupo
            logger.info(f"Creando nuevo grupo: {group_name}")
            mutation = f"""
            mutation {{
                create_group (board_id: {board_id}, group_name: "{group_name}") {{
                    id
                }}
            }}
            """
            response = requests.post(
                self.api_url,
                json={'query': mutation},
                headers=self.headers
            )
            response.raise_for_status()
            result = response.json()
            
            # Verificar errores GraphQL
            if 'errors' in result:
                logger.error(f"Error en GraphQL al crear grupo: {result['errors']}")
                raise Exception(f"GraphQL errors: {result['errors']}")
                
            group_id = result['data']['create_group']['id']
            logger.info(f"Grupo '{group_name}' creado con ID: {group_id}")
            return group_id
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al gestionar grupo en Monday: {str(e)}")
            if e.response is not None:
                logger.error(f"Respuesta del servidor: {e.response.text}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al gestionar grupo: {str(e)}")
            raise

# Instancia singleton del cliente
monday_client = MondayClient()