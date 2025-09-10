from config.configuraciones import configuracion
from extract.steam_dbExtract import SteamDBExtractor
from transform.steam_dbTransform import SteamDBTransform
from load.steam_dbLoad import Load

def main():
    print("=== INICIANDO PROCESO ETL PARA VIDEO GAMES SALES ===")
    
    # Configuración
    config = configuracion()
    print(f"Archivo de entrada: {config.INPUT_PATH}")
    print(f"Base de datos: {config.DB_NAME} en {config.DB_HOST}:{config.DB_PORT}")
    
    # 1. EXTRACCIÓN
    print("\n--- FASE 1: EXTRACCIÓN ---")
    extractor = SteamDBExtractor(config.INPUT_PATH)
    df = extractor.extract()
    if df is None:
        print("❌ No se pudo extraer datos. Terminando proceso ETL.")
        return
    
    print(f"✅ Extracción exitosa: {len(df)} registros extraídos")

    # 2. TRANSFORMACIÓN
    print("\n--- FASE 2: TRANSFORMACIÓN ---")
    transformer = SteamDBTransform(df)
    df_transformed = transformer.clean()
    
    if df_transformed is None or len(df_transformed) == 0:
        print("❌ Error en la transformación. Terminando proceso ETL.")
        return
    
    print(f"✅ Transformación exitosa: {len(df_transformed)} registros transformados")

    # 3. CARGA A BASE DE DATOS
    print("\n--- FASE 3: CARGA A BASE DE DATOS ---")
    loader = Load(df_transformed)
    
    # Cargar datos a la base de datos
    if loader.load_to_database():
        print("✅ Carga a base de datos exitosa")
        
        # Mostrar estadísticas de la base de datos
        print("\n--- ESTADÍSTICAS DE LA BASE DE DATOS ---")
        loader.get_database_stats()
    else:
        print("❌ Error al cargar datos a la base de datos")
        return

    # 4. BACKUP CSV (opcional)
    print("\n--- FASE 4: BACKUP CSV ---")
    loader.clean_csv(config.OUTPUT_PATH)
    
    print("\n=== PROCESO ETL COMPLETADO EXITOSAMENTE ===")
    print("Los datos de video games sales han sido procesados y cargados a la base de datos.")

if __name__ == "__main__":
    main()