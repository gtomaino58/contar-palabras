# Conteo de Palabras – Ejecución Secuencial vs Paralela

## Máster en Big Data
Asignatura: Computación Concurrente y Distribuida  
Autor: Gianfranco Tomaino  
Curso: 2025–2026  

---

## 1. Descripción del proyecto

Este proyecto implementa un sistema de conteo de palabras sobre un corpus textual de gran tamaño utilizando dos enfoques:
- Ejecución secuencial
- Ejecución paralela mediante `multiprocessing` en Python

Además, se incorpora MongoDB como mecanismo de persistencia temporal para:
- Registrar resultados parciales generados por cada proceso
- Registrar métricas completas de cada ejecución experimental

El objetivo es analizar el impacto del paralelismo en función del tamaño del problema y del número de procesos utilizados.

---

## 2. Tecnologías utilizadas

- Python 3.12
- multiprocessing
- MongoDB 7
- Docker
- MongoDB Compass (para inspección)
- collections.Counter
- Unicode normalization

---

## 3. Estructura del proyecto

contar-palabras / docker-compose.yml, requirements.txt, README.md; 
data / el_quijote.txt; 
src / contar_palabras.py, parallel_count.py, text_cleaning.py, mongo_store.py, clear_db.py; 
scripts / make_quijote_100x.ps1; 
reports / 

---

## 4. Requisitos

- Python 3.12 o superior
- Docker instalado y funcionando
- PowerShell (en Windows)

---

## 5. Instalación y configuración

### 5.1 Crear entorno virtual

py -m venv .venv
.venv\Scripts\Activate
pip install -r requirements.txt


### 5.2 Levantar MongoDB con Docker
docker compose up -d

MongoDB quedará accesible en:
mongodb://localhost:27018

---

## 6. Limpieza de base de datos (opcional pero recomendado)

Antes de iniciar experimentos:
python src\clear_db.py

Esto elimina documentos en:
runs
partials

---

## 7. Ejecución del experimento

### 7.1 Dataset original
python src\contar_palabras.py --input data\el_quijote.txt --workers 1 --tag A_quijote_peq
python src\contar_palabras.py --input data\el_quijote.txt --workers 2 --tag A_quijote_peq
python src\contar_palabras.py --input data\el_quijote.txt --workers 4 --tag A_quijote_peq

### 7.2 Generar dataset ampliado (100×)
.\scripts\make_quijote_100x.ps1

Se generará:

data\el_quijote_100x.txt

### 7.3 Dataset ampliado
python src\contar_palabras.py --input data\el_quijote_100x.txt --workers 1 --tag B_quijote_100x
python src\contar_palabras.py --input data\el_quijote_100x.txt --workers 2 --tag B_quijote_100x
python src\contar_palabras.py --input data\el_quijote_100x.txt --workers 4 --tag B_quijote_100x

---

## 8. Verificación en MongoDB

Abrir MongoDB Compass y conectar a:
mongodb://localhost:27018

Base de datos:
contar_palabras_db

Colecciones:
runs
partials

Filtrar por experimento:
{ "experiment_tag": "A_quijote_peq" }
{ "experiment_tag": "B_quijote_100x" }
Ver parciales de un run concreto:
{ "run_id": "<PEGAR_RUN_ID>" }

---

## 9. Métricas evaluadas

Para cada ejecución se registra:

1.- Tiempo de ejecución
2.- Número de palabras únicas
3.- Top 10 palabras más frecuentes
4.- Número de procesos utilizados

Se calculan posteriormente:
Speedup y Eficiencia paralela

---

## 10. Observaciones experimentales

En el dataset pequeño, el paralelismo introduce overhead superior al beneficio.
En el dataset ampliado 100×, el paralelismo muestra mejora significativa.
La eficiencia no es lineal debido al modelo spawn de Windows y al coste de persistencia en MongoDB.

---

## 11. Reproducibilidad

El uso de Docker para MongoDB garantiza que el sistema pueda ejecutarse en cualquier entorno sin depender de instalaciones locales previas.

---

## 12. Autor

Gianfranco Tomaino
Máster en Big Data
2025-2026
Universidad Europea de Madrid

---