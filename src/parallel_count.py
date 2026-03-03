"""
parallel_count.py
-----------------
Conteo paralelo robusto usando multiprocessing (Windows compatible).

Características:
- Divide líneas en EXACTAMENTE N chunks (uno por worker)
- Cada worker:
    - limpia
    - cuenta
    - inserta parcial en Mongo
- Si Mongo falla en un worker, el programa aborta con error claro
- Compatible con Windows (spawn)
"""

from __future__ import annotations

from collections import Counter
from multiprocessing import Pool
from typing import List, Optional, Set, Tuple, Dict
import math

from text_cleaning import clean_text
from mongo_store import MongoStore


def split_lines_into_chunks(lines: List[str], workers: int) -> List[List[str]]:
    """
    Divide las líneas en EXACTAMENTE 'workers' bloques.
    Algunos pueden quedar vacíos si hay pocas líneas,
    pero así siempre habrá un parcial por worker.
    """
    if workers <= 0:
        raise ValueError("workers debe ser > 0")

    n = len(lines)
    base = n // workers
    remainder = n % workers

    chunks: List[List[str]] = []
    start = 0

    for i in range(workers):
        size = base + (1 if i < remainder else 0)
        chunks.append(lines[start:start + size])
        start += size

    return chunks


def _worker_count_and_store(
    args: Tuple[int, str, List[str], bool, Optional[Set[str]], str, str]
) -> Dict[str, int]:
    """
    Worker ejecutado en proceso hijo.

    args:
        worker_id
        run_id
        chunk_lines
        remove_accents
        stopwords
        mongo_uri
        db_name
    """
    (
        worker_id,
        run_id,
        chunk_lines,
        remove_accents,
        stopwords,
        mongo_uri,
        db_name
    ) = args

    # Unimos el chunk en un único string
    chunk_text = "".join(chunk_lines)

    tokens = clean_text(
        chunk_text,
        remove_acc=remove_accents,
        stopwords=stopwords
    )

    partial_counter = Counter(tokens)
    partial_dict = dict(partial_counter)

    # --- Inserción Mongo con control total de errores ---
    try:
        store = MongoStore(uri=mongo_uri, db_name=db_name)
        store.insert_partial(
            run_id=run_id,
            worker_id=worker_id,
            partial_result=partial_dict
        )
    except Exception as e:
        # Devolvemos un marcador especial que el proceso principal detectará
        return {
            "__mongo_error__": True,
            "__worker_id__": worker_id,
            "__error__": str(e)
        }

    return partial_dict


def parallel_word_count(
    lines: List[str],
    workers: int,
    remove_accents: bool,
    stopwords: Optional[Set[str]],
    run_id: str,
    mongo_uri: str,
    db_name: str
) -> Counter:
    """
    Ejecuta conteo paralelo completo.
    """
    if workers < 2:
        raise ValueError("workers debe ser >= 2 para modo paralelo")

    chunks = split_lines_into_chunks(lines, workers)

    args_list = [
        (
            i,
            run_id,
            chunks[i],
            remove_accents,
            stopwords,
            mongo_uri,
            db_name
        )
        for i in range(workers)
    ]

    with Pool(processes=workers) as pool:
        results = pool.map(_worker_count_and_store, args_list)

    # --- Verificar si hubo errores Mongo ---
    for r in results:
        if isinstance(r, dict) and r.get("__mongo_error__"):
            raise RuntimeError(
                f"Error insertando parcial en Mongo. "
                f"Worker {r['__worker_id__']}: {r['__error__']}"
            )

    # --- Merge final ---
    final_counter = Counter()
    for partial_dict in results:
        final_counter.update(partial_dict)

    return final_counter