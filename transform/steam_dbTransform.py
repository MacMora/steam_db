
import pandas as pd

class SteamDBTransform:
    def __init__(self, df):
        self.df = df

    def clean(self):
        df = self.df.copy()
        print("Iniciando transformación de datos...")
        
        # Asegurar que las columnas de ventas sean numéricas y rellenar nulos con 0
        sales_cols = ['NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']
        for col in sales_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                print(f"Columna {col} convertida a numérico")
        
        # Eliminar filas donde el nombre del juego esté vacío o nulo
        initial_count = len(df)
        df = df.dropna(subset=['Name'])
        df = df[df['Name'].str.strip() != '']  # Eliminar nombres vacíos
        print(f"Eliminadas {initial_count - len(df)} filas con nombres vacíos")
        
        # Rellenar valores nulos en texto con 'Unknown'
        text_cols = ['Platform', 'Genre', 'Publisher']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
                print(f"Columna {col} rellenada con 'Unknown'")
        
        # Asegurar que el año sea numérico y rellenar nulos con 0
        if 'Year' in df.columns:
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce').fillna(0).astype(int)
            print("Columna Year convertida a entero")
        
        # Asegurar que Rank sea numérico
        if 'Rank' in df.columns:
            df['Rank'] = pd.to_numeric(df['Rank'], errors='coerce').fillna(0).astype(int)
            print("Columna Rank convertida a entero")
        
        # Limpiar nombres de juegos (eliminar espacios extra)
        if 'Name' in df.columns:
            df['Name'] = df['Name'].str.strip()
            print("Nombres de juegos limpiados")
        
        # Crear columna de década para análisis
        if 'Year' in df.columns:
            df['Decade'] = (df['Year'] // 10) * 10
            print("Columna Decade creada")
        
        # Crear columna de total de ventas regionales
        regional_sales = ['NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales']
        df['Total_Regional_Sales'] = df[regional_sales].sum(axis=1)
        print("Columna Total_Regional_Sales creada")
        
        # Validar que Global_Sales sea consistente con las ventas regionales
        df['Sales_Difference'] = abs(df['Global_Sales'] - df['Total_Regional_Sales'])
        print(f"Diferencias de ventas calculadas (max: {df['Sales_Difference'].max():.2f})")
        
        self.df = df
        print(f"Transformación completada. Registros finales: {len(df)}")
        return self.df
