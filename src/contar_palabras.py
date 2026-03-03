"""
contar_palabras.py
-------------------
Conteo de palabras: SECUENCIAL y PARALELO (multiprocessing).

- workers=1 -> secuencial
- workers>=2 -> paralelo (multiprocessing) + parciales a Mongo (partials)

Persistencia MongoDB (Docker):
- URI por defecto: mongodb://localhost:27018
- BD: contar_palabras_db
- Colecciones:
    - runs
    - partials

Novedad (para experimentos):
- --tag permite agrupar ejecuciones en MongoDB (por ejemplo: quijote100x_v1)
"""

from __future__ import annotations

import argparse
import os
import time
import uuid
from collections import Counter
from typing import Any, Dict, List, Optional, Set

import multiprocessing as mp

from mongo_store import MongoStore
from text_cleaning import clean_text
from parallel_count import parallel_word_count


def parse_args() -> argparse.Namespace:
    """
    Define y parsea argumentos CLI.
    """
    parser = argparse.ArgumentParser(description="Conteo de palabras (secuencial/paralelo).")
    parser.add_argument(
        "--input",
        type=str,
        required=True,
        help="Ruta del archivo .txt (ej: data\\el_quijote.txt o data\\el_quijote_100x.txt)"
    )
    parser.add_argument(
        "--top",
        type=int,
        default=10,
        help="Número de palabras más frecuentes a mostrar (default: 10)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Número de procesos (1 = secuencial; >=2 = paralelo)"
    )
    parser.add_argument(
        "--remove-accents",
        action="store_true",
        help="Si se activa, elimina acentos (opcional)."
    )
    parser.add_argument(
        "--use-stopwords",
        action="store_true",
        help="Si se activa, ignora stopwords españolas básicas (opcional)."
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        default="mongodb://localhost:27018",
        help="URI MongoDB (default: mongodb://localhost:27018)"
    )
    parser.add_argument(
        "--db-name",
        type=str,
        default="contar_palabras_db",
        help="Nombre de la base de datos MongoDB (default: contar_palabras_db)"
    )
    parser.add_argument(
        "--tag",
        type=str,
        default="",
        help="Etiqueta para agrupar ejecuciones en MongoDB (ej: quijote100x_v1)"
    )
    return parser.parse_args()


def get_basic_stopwords_es() -> Set[str]:
    """
    Stopwords españolas mínimas, autocontenidas.
    Nota: usamos casefold(), así que deben estar en minúsculas.
    """
    return {
        "el", "la", "los", "las", "y", "o", "u", "de", "del", "al",
        "a", "en", "por", "para", "con", "sin", "un", "una", "unos", "unas",
        "que", "se", "su", "sus", "es", "son", "ser", "como", "mas", "menos",
        "no", "si", "ya", "pero", "porque", "cuando", "donde", "mi", "mis",
        "tu", "tus", "lo", "le", "les", "me", "te", "ha", "han"
    }


def read_text_lines(path: str) -> List[str]:
    """
    Leemos por líneas para dividir el trabajo sin partir palabras.
    """
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.readlines()


def format_top(top_items: List[tuple]) -> List[Dict[str, Any]]:
    """
    Convierte top_n (lista de tuplas) a lista de dicts para MongoDB.
    """
    return [{"word": w, "count": c} for (w, c) in top_items]


def main() -> None:
    args = parse_args()

    if not os.path.exists(args.input):
        raise FileNotFoundError(f"No existe el archivo: {args.input}")

    # Stopwords opcionales
    stopwords: Optional[Set[str]] = get_basic_stopwords_es() if args.use_stopwords else None

    # Conexión Mongo
    store = MongoStore(uri=args.mongo_uri, db_name=args.db_name)
    if not store.ping():
        print("No se pudo conectar con MongoDB.")
        print(f"URI usada: {args.mongo_uri}")
        print("Asegúrate de que Docker está levantado y Mongo mapeado al puerto 27018.")
        return

    run_id = str(uuid.uuid4())
    lines = read_text_lines(args.input)

    # --- Ejecución ---
    start = time.perf_counter()

    if args.workers == 1:
        method = "secuencial"

        raw_text = "".join(lines)
        tokens = clean_text(
            raw_text,
            remove_acc=args.remove_accents,
            stopwords=stopwords
        )
        counter = Counter(tokens)

    else:
        method = "paralelo"

        counter = parallel_word_count(
            lines=lines,
            workers=args.workers,
            remove_accents=args.remove_accents,
            stopwords=stopwords,
            run_id=run_id,
            mongo_uri=args.mongo_uri,
            db_name=args.db_name
        )

    top_n = counter.most_common(args.top)
    elapsed = time.perf_counter() - start

    # --- Salida por consola ---
    print("\n==============================")
    print(f"CONTEO {method.upper()}")
    print("==============================")
    print(f"Archivo: {args.input}")
    if args.tag:
        print(f"Tag experimento: {args.tag}")
    print(f"Workers: {args.workers}")
    print(f"Palabras únicas: {len(counter):,}")
    print(f"Tiempo (s): {elapsed:.6f}")
    print(f"Remove accents: {args.remove_accents}")
    print(f"Stopwords: {args.use_stopwords}")
    print("\nTop palabras:\n")

    for i, (w, c) in enumerate(top_n, start=1):
        print(f"{i:>2}. {w:<20} {c}")

    # --- Persistencia B: guardar run final ---
    run_doc: Dict[str, Any] = {
        "run_id": run_id,
        "experiment_tag": args.tag,
        "method": method,
        "input_file": os.path.basename(args.input),
        "workers": args.workers,
        "remove_accents": bool(args.remove_accents),
        "stopwords": bool(args.use_stopwords),
        "execution_time_seconds": elapsed,
        "unique_words": len(counter),
        "top_10": format_top(top_n),
    }
    store.insert_run(run_doc)

    print("\nRun guardado en MongoDB (colección: runs)")
    print(f"run_id: {run_id}")
    if method == "paralelo":
        print("Parciales guardados en MongoDB (colección: partials)")
    print("Revisa en Compass: contar_palabras_db -> runs / partials\n")


if __name__ == "__main__":
    # Necesario en Windows para multiprocessing
    mp.freeze_support()
    main()