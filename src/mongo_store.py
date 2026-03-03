"""
mongo_store.py
----------------
Módulo responsable de la conexión y operaciones básicas con MongoDB.

Este módulo encapsula toda la lógica de persistencia para:
- Guardar resultados parciales (colección: partials)
- Guardar ejecuciones completas (colección: runs)

Separar la capa de persistencia mejora:
- Mantenibilidad
- Testabilidad
- Claridad arquitectónica
"""

from pymongo import MongoClient
from datetime import datetime
from typing import Dict, Any


class MongoStore:
    """
    Clase que gestiona la conexión y operaciones sobre MongoDB.
    """

    def __init__(self, uri: str = "mongodb://localhost:27018",
                 db_name: str = "contar_palabras_db") -> None:
        """
        Inicializa la conexión a MongoDB.

        :param uri: URI de conexión
        :param db_name: Nombre de la base de datos
        """
        self.client = MongoClient(uri)
        self.db = self.client[db_name]
        self.partials = self.db["partials"]
        self.runs = self.db["runs"]

    def ping(self) -> bool:
        """
        Verifica conectividad con el servidor MongoDB.
        """
        try:
            self.client.admin.command("ping")
            return True
        except Exception:
            return False

    def insert_partial(self, run_id: str, worker_id: int,
                       partial_result: Dict[str, int]) -> None:
        """
        Inserta un resultado parcial de un worker.

        :param run_id: Identificador único de la ejecución
        :param worker_id: Identificador del worker
        :param partial_result: Diccionario con conteo parcial
        """
        document = {
            "run_id": run_id,
            "worker_id": worker_id,
            "partial_result": partial_result,
            "timestamp": datetime.utcnow()
        }
        self.partials.insert_one(document)

    def insert_run(self, run_data: Dict[str, Any]) -> None:
        """
        Inserta el resultado final de una ejecución completa.

        :param run_data: Diccionario con métricas y resultados
        """
        run_data["timestamp"] = datetime.utcnow()
        self.runs.insert_one(run_data)

    def clear_collections(self) -> None:
        """
        Limpia las colecciones (útil para pruebas).
        """
        self.partials.delete_many({})
        self.runs.delete_many({})