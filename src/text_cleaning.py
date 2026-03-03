"""
text_cleaning.py
----------------
Módulo responsable de la normalización y tokenización del texto.

Se implementa un pipeline robusto que:
- Normaliza Unicode
- Convierte a minúsculas usando casefold()
- Opcionalmente elimina acentos
- Elimina puntuación
- Permite ignorar stopwords
"""

import re
import unicodedata
from typing import List, Set


def remove_accents(text: str) -> str:
    """
    Elimina los acentos de un texto usando normalización Unicode.

    :param text: Texto de entrada
    :return: Texto sin acentos
    """
    normalized = unicodedata.normalize("NFD", text)
    return "".join(
        char for char in normalized
        if unicodedata.category(char) != "Mn"
    )


def clean_text(text: str,
               remove_acc: bool = False,
               stopwords: Set[str] = None) -> List[str]:
    """
    Limpia y tokeniza el texto.

    :param text: Texto original
    :param remove_acc: Si True elimina acentos
    :param stopwords: Conjunto de palabras a ignorar
    :return: Lista de tokens limpios
    """

    # 1. Normalización básica Unicode
    text = unicodedata.normalize("NFC", text)

    # 2. Minúsculas robustas
    text = text.casefold()

    # 3. Eliminar acentos si se solicita
    if remove_acc:
        text = remove_accents(text)

    # 4. Sustituir cualquier cosa que no sea letra o número por espacio
    text = re.sub(r"[^a-z0-9áéíóúüñ]+", " ", text)

    # 5. Tokenizar
    tokens = text.split()

    # 6. Eliminar stopwords si se proporciona conjunto
    if stopwords:
        tokens = [token for token in tokens if token not in stopwords]

    return tokens