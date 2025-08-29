
import pandas as pd
import numpy as np

class SteamDBTransform:
    def __init__(self, df):
        self.df = df

    def clean(self):
        df = self.df.copy()
        # Asegurar que las columnas de ventas sean numéricas y rellenar nulos con 0
        sales_cols = ['NA_Sales', 'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']
        for col in sales_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        # Eliminar filas donde el nombre del juego esté vacío o nulo
        df = df.dropna(subset=['Name'])
        # Rellenar valores nulos en texto con 'Unknown'
        text_cols = ['Platform', 'Genre', 'Publisher']
        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].fillna('Unknown')
        # Asegurar que el año sea numérico y rellenar nulos con 0
        if 'Year' in df.columns:
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce').fillna(0).astype(int)
        self.df = df
        return self.df
