import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36'
}

BASE_URL = "https://www.transfermarkt.es/laliga/startseite/wettbewerb/ES1"

# Carpetas obligatorias
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
os.makedirs(os.path.join(project_root, "dataset"), exist_ok=True)
dataset_path = os.path.join(project_root, "dataset", "transfermarkt_clubes_laliga.csv")

data = []
print(" Iniciando scraping en Transfermarkt.es (Clubes LaLiga)...")

response = requests.get(BASE_URL, headers=headers)
soup = BeautifulSoup(response.text, 'lxml')

rows = soup.find_all('tr', class_=['odd', 'even'])
print(f"Total filas encontradas: {len(rows)}")

for row in rows:
    try:
        tds = row.find_all('td')
        if len(tds) < 6:
            continue

        # Club
        club_tag = tds[1].find('a', class_='vereinprofil_tooltip')
        club = club_tag.text.strip() if club_tag else tds[1].text.strip()

        # Plantilla (número de jugadores)
        plantilla = tds[2].text.strip() if len(tds) > 2 else 'N/A'

        # Edad media
        edad_media = tds[3].text.strip() if len(tds) > 3 else 'N/A'

        # Extranjeros
        extranjeros = tds[4].text.strip() if len(tds) > 4 else 'N/A'

        # Valor de mercado total
        valor_tag = tds[-1]
        valor_mercado = valor_tag.text.strip() if valor_tag else 'N/A'

        data.append({
            'club': club,
            'plantilla': plantilla,
            'edad_media': edad_media,
            'extranjeros': extranjeros,
            'valor_mercado_total': valor_mercado,
            'liga': 'LaLiga',
            'fecha_scrape': '2026-03'
        })

        print(f"   ✓ {club} | Plantilla: {plantilla} | Edad media: {edad_media} | Valor: {valor_mercado}")

    except:
        continue

df = pd.DataFrame(data)
df.to_csv(dataset_path, index=False)
print(f"\n Fin! Dataset con {len(df)} clubes guardado en:\n{dataset_path}")