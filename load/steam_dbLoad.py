from sqlalchemy import create_engine, text
from config.configuraciones import configuracion

class Load:
    def __init__(self, df):
        self.df = df
        self.config = configuracion()

    def create_table(self):
        """Crear la tabla en la base de datos si no existe"""
        try:
            # Crear conexión usando SQLAlchemy
            engine = create_engine(self.config.DATABASE_URL)
            
            # Definir el esquema de la tabla
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS video_games_sales (
                id SERIAL PRIMARY KEY,
                rank INTEGER,
                name VARCHAR(255) NOT NULL,
                platform VARCHAR(50),
                year INTEGER,
                genre VARCHAR(50),
                publisher VARCHAR(255),
                na_sales DECIMAL(10,2),
                eu_sales DECIMAL(10,2),
                jp_sales DECIMAL(10,2),
                other_sales DECIMAL(10,2),
                global_sales DECIMAL(10,2),
                decade INTEGER,
                total_regional_sales DECIMAL(10,2),
                sales_difference DECIMAL(10,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            with engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
            
            print("Tabla 'video_games_sales' creada exitosamente")
            return True
            
        except Exception as e:
            print(f"Error al crear la tabla: {e}")
            return False

    def load_to_database(self):
        """Cargar datos transformados a la base de datos"""
        try:
            # Crear la tabla primero
            if not self.create_table():
                return False
            
            # Preparar los datos para la inserción
            df_to_load = self.df.copy()
            
            # Mapear nombres de columnas del DataFrame a los nombres de la tabla
            column_mapping = {
                'Rank': 'rank',
                'Name': 'name',
                'Platform': 'platform',
                'Year': 'year',
                'Genre': 'genre',
                'Publisher': 'publisher',
                'NA_Sales': 'na_sales',
                'EU_Sales': 'eu_sales',
                'JP_Sales': 'jp_sales',
                'Other_Sales': 'other_sales',
                'Global_Sales': 'global_sales',
                'Decade': 'decade',
                'Total_Regional_Sales': 'total_regional_sales',
                'Sales_Difference': 'sales_difference'
            }
            
            # Renombrar columnas
            df_to_load = df_to_load.rename(columns=column_mapping)
            
            # Seleccionar solo las columnas que existen en el DataFrame
            available_columns = [col for col in column_mapping.values() if col in df_to_load.columns]
            df_to_load = df_to_load[available_columns]
            
            # Crear conexión usando SQLAlchemy
            engine = create_engine(self.config.DATABASE_URL)
            
            # Cargar datos a la base de datos
            df_to_load.to_sql(
                'video_games_sales',
                engine,
                if_exists='append',  # Agregar datos sin reemplazar
                index=False,
                method='multi'  # Inserción más eficiente
            )
            
            print(f"Datos cargados exitosamente: {len(df_to_load)} registros insertados")
            return True
            
        except Exception as e:
            print(f"Error al cargar datos a la base de datos: {e}")
            return False

    def clean_csv(self, output_path):
        """Método original para guardar CSV (mantenido para compatibilidad)"""
        try:
            self.df.to_csv(output_path, index=False)
            print(f"Datos guardados exitosamente en {output_path}")
        except Exception as e:
            print(f"Error al guardar los datos: {e}")

    def get_database_stats(self):
        """Obtener estadísticas de la base de datos"""
        try:
            engine = create_engine(self.config.DATABASE_URL)
            
            with engine.connect() as conn:
                # Contar registros totales
                result = conn.execute(text("SELECT COUNT(*) FROM video_games_sales"))
                total_records = result.scalar()
                
                # Obtener estadísticas por plataforma
                platform_stats = conn.execute(text("""
                    SELECT platform, COUNT(*) as count 
                    FROM video_games_sales 
                    GROUP BY platform 
                    ORDER BY count DESC 
                    LIMIT 10
                """)).fetchall()
                
                print(f"Total de registros en la base de datos: {total_records}")
                print("Top 10 plataformas por número de juegos:")
                for platform, count in platform_stats:
                    print(f"  {platform}: {count} juegos")
                
                return True
                
        except Exception as e:
            print(f"Error al obtener estadísticas: {e}")
            return False