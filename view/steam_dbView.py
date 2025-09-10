import pandas as pd
import sys
import os
from sqlalchemy import create_engine, text
from datetime import datetime

# Agregar el directorio raíz al path para importaciones
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.configuraciones import configuracion

# Importaciones opcionales para visualización
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False
    print("⚠️  Matplotlib/Seaborn no disponibles. Funcionalidad básica disponible.")

class SteamDBView:
    def __init__(self):
        self.config = configuracion()
        self.engine = None
        self.connect_to_database()

    def connect_to_database(self):
        """Conectar a la base de datos"""
        try:
            if not all([self.config.DB_HOST, self.config.DB_PORT, self.config.DB_NAME, 
                       self.config.DB_USER, self.config.DB_PASSWORD]):
                print("❌ Error: Faltan credenciales de base de datos en el archivo .env")
                print("Asegúrate de tener configurado:")
                print("- DB_HOST")
                print("- DB_PORT") 
                print("- DB_NAME")
                print("- DB_USER")
                print("- DB_PASSWORD")
                return False
            
            self.engine = create_engine(self.config.DATABASE_URL)
            
            # Probar la conexión
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            print("✅ Conexión a la base de datos establecida")
            return True
            
        except Exception as e:
            print(f"❌ Error al conectar a la base de datos: {e}")
            return False

    def get_all_data(self, limit=None):
        """Obtener todos los datos de la tabla"""
        try:
            if not self.engine:
                print("❌ No hay conexión a la base de datos")
                return None
            
            query = "SELECT * FROM video_games_sales"
            if limit:
                query += f" LIMIT {limit}"
            
            df = pd.read_sql(query, self.engine)
            print(f"✅ Datos obtenidos: {len(df)} registros")
            return df
            
        except Exception as e:
            print(f"❌ Error al obtener datos: {e}")
            return None

    def get_basic_stats(self):
        """Obtener estadísticas básicas de la base de datos"""
        try:
            if not self.engine:
                print("❌ No hay conexión a la base de datos")
                return
            
            with self.engine.connect() as conn:
                # Total de registros
                total_result = conn.execute(text("SELECT COUNT(*) FROM video_games_sales"))
                total_records = total_result.scalar()
                
                # Estadísticas por plataforma
                platform_stats = conn.execute(text("""
                    SELECT platform, COUNT(*) as count 
                    FROM video_games_sales 
                    GROUP BY platform 
                    ORDER BY count DESC 
                    LIMIT 10
                """)).fetchall()
                
                # Estadísticas por género
                genre_stats = conn.execute(text("""
                    SELECT genre, COUNT(*) as count 
                    FROM video_games_sales 
                    GROUP BY genre 
                    ORDER BY count DESC 
                    LIMIT 10
                """)).fetchall()
                
                # Top juegos por ventas globales
                top_games = conn.execute(text("""
                    SELECT name, platform, global_sales, year
                    FROM video_games_sales 
                    ORDER BY global_sales DESC 
                    LIMIT 10
                """)).fetchall()
                
                print("=" * 60)
                print("📊 ESTADÍSTICAS DE LA BASE DE DATOS")
                print("=" * 60)
                print(f"📈 Total de registros: {total_records:,}")
                
                print(f"\n🎮 TOP 10 PLATAFORMAS:")
                for i, (platform, count) in enumerate(platform_stats, 1):
                    print(f"  {i:2d}. {platform:<15} {count:>6,} juegos")
                
                print(f"\n🎯 TOP 10 GÉNEROS:")
                for i, (genre, count) in enumerate(genre_stats, 1):
                    print(f"  {i:2d}. {genre:<15} {count:>6,} juegos")
                
                print(f"\n🏆 TOP 10 JUEGOS POR VENTAS GLOBALES:")
                for i, (name, platform, sales, year) in enumerate(top_games, 1):
                    print(f"  {i:2d}. {name:<30} ({platform}) - {sales:>6.2f}M - {year}")
                
        except Exception as e:
            print(f"❌ Error al obtener estadísticas: {e}")

    def get_platform_analysis(self):
        """Análisis detallado por plataforma"""
        try:
            if not self.engine:
                return None
            
            query = """
            SELECT 
                platform,
                COUNT(*) as total_games,
                AVG(global_sales) as avg_sales,
                MAX(global_sales) as max_sales,
                SUM(global_sales) as total_sales,
                AVG(year) as avg_year
            FROM video_games_sales 
            WHERE platform IS NOT NULL
            GROUP BY platform 
            ORDER BY total_sales DESC
            """
            
            df = pd.read_sql(query, self.engine)
            
            print("\n" + "=" * 80)
            print("🎮 ANÁLISIS POR PLATAFORMA")
            print("=" * 80)
            print(f"{'Plataforma':<15} {'Juegos':<8} {'Ventas Avg':<12} {'Ventas Max':<12} {'Ventas Total':<15} {'Año Avg':<8}")
            print("-" * 80)
            
            for _, row in df.iterrows():
                print(f"{row['platform']:<15} {row['total_games']:<8,} {row['avg_sales']:<12.2f} {row['max_sales']:<12.2f} {row['total_sales']:<15.2f} {row['avg_year']:<8.0f}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error en análisis de plataformas: {e}")
            return None

    def get_genre_analysis(self):
        """Análisis detallado por género"""
        try:
            if not self.engine:
                return None
            
            query = """
            SELECT 
                genre,
                COUNT(*) as total_games,
                AVG(global_sales) as avg_sales,
                MAX(global_sales) as max_sales,
                SUM(global_sales) as total_sales
            FROM video_games_sales 
            WHERE genre IS NOT NULL
            GROUP BY genre 
            ORDER BY total_sales DESC
            """
            
            df = pd.read_sql(query, self.engine)
            
            print("\n" + "=" * 70)
            print("🎯 ANÁLISIS POR GÉNERO")
            print("=" * 70)
            print(f"{'Género':<15} {'Juegos':<8} {'Ventas Avg':<12} {'Ventas Max':<12} {'Ventas Total':<15}")
            print("-" * 70)
            
            for _, row in df.iterrows():
                print(f"{row['genre']:<15} {row['total_games']:<8,} {row['avg_sales']:<12.2f} {row['max_sales']:<12.2f} {row['total_sales']:<15.2f}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error en análisis de géneros: {e}")
            return None

    def get_yearly_analysis(self):
        """Análisis por año"""
        try:
            if not self.engine:
                return None
            
            query = """
            SELECT 
                year,
                COUNT(*) as total_games,
                AVG(global_sales) as avg_sales,
                SUM(global_sales) as total_sales
            FROM video_games_sales 
            WHERE year > 0
            GROUP BY year 
            ORDER BY year DESC
            LIMIT 20
            """
            
            df = pd.read_sql(query, self.engine)
            
            print("\n" + "=" * 60)
            print("📅 ANÁLISIS POR AÑO (ÚLTIMOS 20 AÑOS)")
            print("=" * 60)
            print(f"{'Año':<6} {'Juegos':<8} {'Ventas Avg':<12} {'Ventas Total':<15}")
            print("-" * 60)
            
            for _, row in df.iterrows():
                print(f"{row['year']:<6} {row['total_games']:<8,} {row['avg_sales']:<12.2f} {row['total_sales']:<15.2f}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error en análisis por año: {e}")
            return None

    def search_games(self, search_term, limit=10):
        """Buscar juegos por nombre"""
        try:
            if not self.engine:
                return None
            
            query = """
            SELECT name, platform, year, genre, publisher, global_sales
            FROM video_games_sales 
            WHERE LOWER(name) LIKE LOWER(:search_term)
            ORDER BY global_sales DESC
            LIMIT :limit
            """
            
            df = pd.read_sql(query, self.engine, params={
                'search_term': f'%{search_term}%',
                'limit': limit
            })
            
            if len(df) > 0:
                print(f"\n🔍 RESULTADOS DE BÚSQUEDA PARA: '{search_term}'")
                print("=" * 80)
                print(f"{'Nombre':<30} {'Plataforma':<12} {'Año':<6} {'Género':<12} {'Ventas':<8}")
                print("-" * 80)
                
                for _, row in df.iterrows():
                    print(f"{row['name']:<30} {row['platform']:<12} {row['year']:<6} {row['genre']:<12} {row['global_sales']:<8.2f}")
            else:
                print(f"❌ No se encontraron juegos con el término: '{search_term}'")
            
            return df
            
        except Exception as e:
            print(f"❌ Error en búsqueda: {e}")
            return None

    def get_top_publishers(self, limit=15):
        """Obtener top publishers por ventas"""
        try:
            if not self.engine:
                return None
            
            query = """
            SELECT 
                publisher,
                COUNT(*) as total_games,
                SUM(global_sales) as total_sales,
                AVG(global_sales) as avg_sales
            FROM video_games_sales 
            WHERE publisher IS NOT NULL AND publisher != 'Unknown'
            GROUP BY publisher 
            ORDER BY total_sales DESC
            LIMIT :limit
            """
            
            df = pd.read_sql(query, self.engine, params={'limit': limit})
            
            print(f"\n🏢 TOP {limit} PUBLISHERS POR VENTAS")
            print("=" * 70)
            print(f"{'Publisher':<25} {'Juegos':<8} {'Ventas Total':<15} {'Ventas Avg':<12}")
            print("-" * 70)
            
            for _, row in df.iterrows():
                print(f"{row['publisher']:<25} {row['total_games']:<8,} {row['total_sales']:<15.2f} {row['avg_sales']:<12.2f}")
            
            return df
            
        except Exception as e:
            print(f"❌ Error al obtener publishers: {e}")
            return None

    def get_database_info(self):
        """Obtener información general de la base de datos"""
        try:
            if not self.engine:
                return
            
            with self.engine.connect() as conn:
                # Información de la tabla
                table_info = conn.execute(text("""
                    SELECT 
                        COUNT(*) as total_records,
                        MIN(year) as min_year,
                        MAX(year) as max_year,
                        MIN(global_sales) as min_sales,
                        MAX(global_sales) as max_sales,
                        AVG(global_sales) as avg_sales
                    FROM video_games_sales
                """)).fetchone()
                
                print("\n" + "=" * 50)
                print("📊 INFORMACIÓN GENERAL DE LA BASE DE DATOS")
                print("=" * 50)
                print(f"📈 Total de registros: {table_info[0]:,}")
                print(f"📅 Rango de años: {table_info[1]} - {table_info[2]}")
                print(f"💰 Ventas mínimas: {table_info[3]:.2f}M")
                print(f"💰 Ventas máximas: {table_info[4]:.2f}M")
                print(f"💰 Ventas promedio: {table_info[5]:.2f}M")
                
        except Exception as e:
            print(f"❌ Error al obtener información: {e}")

    def export_to_csv(self, filename=None):
        """Exportar datos a CSV"""
        try:
            if not self.engine:
                return False
            
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"video_games_export_{timestamp}.csv"
            
            df = self.get_all_data()
            if df is not None:
                df.to_csv(filename, index=False)
                print(f"✅ Datos exportados exitosamente a: {filename}")
                return True
            return False
            
        except Exception as e:
            print(f"❌ Error al exportar: {e}")
            return False

    def interactive_menu(self):
        """Menú interactivo para explorar los datos"""
        while True:
            print("\n" + "=" * 60)
            print("🎮 MENÚ INTERACTIVO - VIDEO GAMES SALES DATABASE")
            print("=" * 60)
            print("1. Ver estadísticas básicas")
            print("2. Análisis por plataforma")
            print("3. Análisis por género")
            print("4. Análisis por año")
            print("5. Buscar juegos")
            print("6. Top publishers")
            print("7. Ver todos los datos (limitado)")
            print("8. Información de la base de datos")
            print("9. Exportar a CSV")
            print("0. Salir")
            print("-" * 60)
            
            choice = input("Selecciona una opción (0-9): ").strip()
            
            if choice == "0":
                print("👋 ¡Hasta luego!")
                break
            elif choice == "1":
                self.get_basic_stats()
            elif choice == "2":
                self.get_platform_analysis()
            elif choice == "3":
                self.get_genre_analysis()
            elif choice == "4":
                self.get_yearly_analysis()
            elif choice == "5":
                search_term = input("Ingresa el término de búsqueda: ").strip()
                if search_term:
                    self.search_games(search_term)
            elif choice == "6":
                self.get_top_publishers()
            elif choice == "7":
                limit = input("¿Cuántos registros quieres ver? (Enter para 20): ").strip()
                limit = int(limit) if limit.isdigit() else 20
                df = self.get_all_data(limit)
                if df is not None:
                    print(f"\n📋 PRIMEROS {len(df)} REGISTROS:")
                    print(df.to_string(index=False))
            elif choice == "8":
                self.get_database_info()
            elif choice == "9":
                filename = input("Nombre del archivo (Enter para auto): ").strip()
                filename = filename if filename else None
                self.export_to_csv(filename)
            else:
                print("❌ Opción inválida. Intenta de nuevo.")
            
            input("\nPresiona Enter para continuar...")

def main():
    """Función principal para ejecutar la vista"""
    print("🎮 INICIANDO VISTA DE BASE DE DATOS VIDEO GAMES SALES")
    print("=" * 60)
    
    # Crear instancia de la vista
    view = SteamDBView()
    
    # Verificar conexión
    if not view.engine:
        print("❌ No se pudo conectar a la base de datos.")
        print("Asegúrate de que:")
        print("1. PostgreSQL esté ejecutándose")
        print("2. La base de datos 'steam_db' exista")
        print("3. Las credenciales en .env sean correctas")
        return
    
    # Mostrar información básica
    view.get_database_info()
    view.get_basic_stats()
    
    # Iniciar menú interactivo
    view.interactive_menu()

if __name__ == "__main__":
    main()
