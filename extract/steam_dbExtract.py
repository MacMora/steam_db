import requests
import pandas as pd
import numpy as np

class SteamDBExtractor:
    def __init__(self, csv_path):
        self.csv_path = csv_path

    def queries(self):
        self.data = pd.read_csv(self.csv_path)
    
    def response(self):
        return self.data.head(5)
