# ETL Video Games Sales Database

Este proyecto implementa un proceso ETL (Extract, Transform, Load) para procesar datos de ventas de videojuegos y cargarlos en una base de datos PostgreSQL.

## Estructura del Proyecto

```
steam_db/
├── config/
│   └── configuraciones.py      # Configuración centralizada
├── extract/
│   ├── files/
│   │   └── vgsales_clean.csv   # Datos de entrada
│   └── steam_dbExtract.py      # Módulo de extracción
├── transform/
│   └── steam_dbTransform.py    # Módulo de transformación
├── load/
│   └── steam_dbLoad.py         # Módulo de carga a BD
├── main.py                     # Script principal
├── requirements.txt            # Dependencias
├── setup_database.sql          # Script de configuración de BD
└── .env.example               # Plantilla de variables de entorno
```

## Configuración

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar base de datos

1. Crear la base de datos PostgreSQL:
```bash
psql -U postgres -f setup_database.sql
```

2. Crear archivo `.env` basado en `.env.example`:
```bash
cp .env.example .env
```

3. Editar `.env` con tus credenciales de base de datos:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=steam_db
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
```

## Uso

### Ejecutar el proceso ETL completo

```bash
python main.py
```

### Proceso ETL

El proceso ETL incluye las siguientes fases:

1. **Extracción**: Lee el archivo `vgsales_clean.csv`
2. **Transformación**: 
   - Limpia y valida los datos
   - Convierte tipos de datos
   - Crea columnas adicionales (década, total regional, etc.)
3. **Carga**: 
   - Crea la tabla en PostgreSQL si no existe
   - Inserta los datos transformados
   - Genera estadísticas

## Esquema de la Base de Datos

La tabla `video_games_sales` contiene:

- `id`: Clave primaria autoincremental
- `rank`: Ranking del juego
- `name`: Nombre del juego
- `platform`: Plataforma (Wii, PS4, etc.)
- `year`: Año de lanzamiento
- `genre`: Género del juego
- `publisher`: Editor
- `na_sales`, `eu_sales`, `jp_sales`, `other_sales`: Ventas por región
- `global_sales`: Ventas globales
- `decade`: Década calculada
- `total_regional_sales`: Suma de ventas regionales
- `sales_difference`: Diferencia entre global y regional
- `created_at`: Timestamp de inserción

## Características

- ✅ Procesamiento de datos robusto con validación
- ✅ Manejo de errores y logging detallado
- ✅ Configuración flexible mediante variables de entorno
- ✅ Creación automática de tabla y índices
- ✅ Estadísticas post-carga
- ✅ Backup CSV opcional

## Requisitos

- Python 3.7+
- PostgreSQL 12+
- pandas, numpy, sqlalchemy, psycopg2-binary, python-dotenv
