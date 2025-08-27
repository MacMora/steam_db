from extract.steam_dbExtract import SteamDBExtractor

response = SteamDBExtractor("vgsales.csv")
response.queries()
print(response.response())