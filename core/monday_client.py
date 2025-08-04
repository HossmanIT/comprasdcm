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
    
    def create_item(self, board_id: str, item_name: str, column_values: Dict[str, Any]):
        """Crea un nuevo ítem en el tablero especificado"""
        query = f"""
        mutation {{
            create_item (
                board_id: {board_id},
                item_name: "{item_name}",
                column_values: {json.dumps(json.dumps(column_values))}
            ) {{
                id
            }}
        }}
        """
        
        try:
            response = requests.post(
                self.api_url,
                json={'query': query},
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al crear ítem en Monday: {str(e)}")
            if e.response is not None:
                logger.error(f"Respuesta del servidor: {e.response.text}")
            raise

# Instancia singleton del cliente
monday_client = MondayClient()