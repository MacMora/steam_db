class Load:

    def __init__(self, df):
        self.df = df

    def clean_csv(self, output_path):
        try:
            self.df.to_csv(output_path, index=False)
            print(f"Datos cargados exitosamente en {output_path}")
        except Exception as e:
            print(f"Error al cargar los datos: {e}")