from config.configuraciones import configuracion
from extract.steam_dbExtract import SteamDBExtractor
from transform.steam_dbTransform import SteamDBTransform
from load.steam_dbLoad import Load

def main():
    # Extracción
    extractor = SteamDBExtractor(configuracion.INPUT_PATH)
    df = extractor.extract()
    if df is None:
        print("No se pudo extraer datos. Terminando proceso ETL.")
        return

    # Transformación
    transformer = SteamDBTransform(df)
    df_clean = transformer.clean()

    # Carga
    loader = Load(df_clean)
    loader.clean_csv(configuracion.OUTPUT_PATH)

if __name__ == "__main__":
    main()