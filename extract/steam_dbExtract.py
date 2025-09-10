import pandas as pd
import os

class SteamDBExtractor:
    def __init__(self, csv_path):
        self.csv_path = csv_path

    def extract(self):
        try:
            # Verificar si el archivo existe
            if not os.path.exists(self.csv_path):
                print(f"Error: El archivo {self.csv_path} no existe")
                return None
            
            # Leer el archivo CSV
            df = pd.read_csv(self.csv_path)
            print(f"Datos extraídos exitosamente: {len(df)} registros encontrados")
            print(f"Columnas disponibles: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"Error al leer el archivo CSV: {e}")
            return None

    """
    def queries(self):
        self.data = pd.read_csv(self.csv_path)
    
    def response(self):
        return self.data.head(5)
    """
