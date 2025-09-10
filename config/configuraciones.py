import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

class configuracion:
    # Configuración de archivos
    INPUT_PATH = os.getenv('INPUT_PATH', 'extract/files/vgsales.csv')
    OUTPUT_PATH = os.getenv('OUTPUT_PATH', 'extract/files/vgsales_clean.csv')
    
    # Configuración de base de datos
    DB_HOST = os.getenv('DB_HOST')
    DB_PORT = os.getenv('DB_PORT')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    # URL de conexión a la base de datos
    @property
    def DATABASE_URL(self):
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"