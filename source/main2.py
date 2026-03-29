import os
import re
import time
import random
from typing import Optional, Dict, List, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/134.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

COMPETITIONS = {
    "LaLiga": ("laliga", "ES1"),
    "Premier League": ("premier-league", "GB1"),
    "Serie A": ("serie-a", "IT1"),
    "Bundesliga": ("bundesliga", "L1"),
    "Ligue 1": ("ligue-1", "FR1"),
}

SEASONS = [2025, 2024, 2023, 2022, 2021]


def build_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        method_whitelist=["GET"],         # ← Cambiar a method_whitelist
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def fetch_html(session: requests.Session, url: str, sleep_range=(1.0, 2.2)) -> str:
    time.sleep(random.uniform(*sleep_range))
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def clean_text(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def to_int(value) -> Optional[int]:
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", str(value))
    return int(digits) if digits else None


def parse_age(value) -> Optional[float]:
    """
    Convierte textos como:
    '25.7' -> 25.7
    '25,7' -> 25.7
    """
    if value is None:
        return None

    text = str(value).strip().replace(",", ".")
    match = re.search(r"\d{1,2}(?:\.\d)?", text)
    if not match:
        return None

    return round(float(match.group()), 1)


def parse_market_value(value) -> Optional[float]:
    """
    Convierte:
    €1.34bn  -> 1340000000.0
    €587.00m -> 587000000.0
    €500k    -> 500000.0
    """
    if value is None:
        return None

    text = str(value).strip().lower().replace("€", "")
    text = text.replace(",", "").replace("\xa0", "").strip()

    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if not match:
        return None

    num = float(match.group())

    if "bn" in text:
        value_num = num * 1_000_000_000
    elif "m" in text:
        value_num = num * 1_000_000
    elif "k" in text:
        value_num = num * 1_000
    else:
        value_num = num

    return round(value_num, 2)


def is_currency_text(text: Optional[str]) -> bool:
    if not text:
        return False
    return bool(re.search(r"€\s*[\d\.,]+(?:bn|m|k)", text.lower()))


def is_decimal_text(text: Optional[str]) -> bool:
    if not text:
        return False
    return bool(re.fullmatch(r"\d{1,2}[.,]\d", text.strip()))


def is_integer_text(text: Optional[str]) -> bool:
    if not text:
        return False
    return bool(re.fullmatch(r"\d+", text.strip()))


def extract_club_info(row) -> Tuple[Optional[str], Optional[str]]:
    candidates = []

    for a in row.find_all("a", href=True):
        href = a.get("href", "")
        txt = clean_text(a.get_text(" ", strip=True))
        title = clean_text(a.get("title"))

        if "/verein/" in href or "/startseite/verein/" in href:
            name = title or txt
            if name:
                full_url = href if href.startswith("http") else f"https://www.transfermarkt.com{href}"
                candidates.append((name, full_url))

    if candidates:
        candidates = [c for c in candidates if re.search(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]", c[0])]
        if candidates:
            return max(candidates, key=lambda x: len(x[0]))

    for td in row.find_all("td"):
        txt = clean_text(td.get_text(" ", strip=True))
        if txt and re.search(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]", txt) and not is_currency_text(txt):
            return txt, None

    return None, None


def extract_row_number(row) -> Optional[str]:
    """
    Extrae el primer entero de la fila.
    No representa ranking deportivo real; solo posición/orden de la tabla extraída.
    """
    for td in row.find_all("td"):
        txt = clean_text(td.get_text(" ", strip=True))
        if txt and re.fullmatch(r"\d+", txt):
            return txt
    return None


def parse_row_cells(row) -> Optional[Dict]:
    tds = row.find_all("td")
    if not tds:
        return None

    cell_texts = [clean_text(td.get_text(" ", strip=True)) for td in tds]

    club, club_url = extract_club_info(row)
    fila_tabla = extract_row_number(row)

    currency_positions = []
    decimal_positions = []
    integer_positions = []

    for i, txt in enumerate(cell_texts):
        if is_currency_text(txt):
            currency_positions.append(i)
        elif is_decimal_text(txt):
            decimal_positions.append(i)
        elif is_integer_text(txt):
            integer_positions.append(i)

    if not club:
        return None

    valor_mercado_promedio = cell_texts[currency_positions[0]] if len(currency_positions) >= 1 else None
    valor_mercado_total = cell_texts[currency_positions[1]] if len(currency_positions) >= 2 else None

    edad_media = cell_texts[decimal_positions[0]] if decimal_positions else None
    age_idx = decimal_positions[0] if decimal_positions else None

    plantilla = None
    if age_idx is not None:
        ints_before_age = [i for i in integer_positions if i < age_idx]
        if ints_before_age:
            plantilla = cell_texts[ints_before_age[-1]]

    extranjeros = None
    if age_idx is not None:
        first_currency_idx = currency_positions[0] if currency_positions else 10**9
        ints_after_age = [i for i in integer_positions if age_idx < i < first_currency_idx]
        if ints_after_age:
            extranjeros = cell_texts[ints_after_age[0]]

    return {
        "fila_tabla": fila_tabla,
        "club": club,
        "club_url": club_url,
        "plantilla": plantilla,
        "edad_media": edad_media,
        "extranjeros": extranjeros,
        "valor_mercado_promedio": valor_mercado_promedio,
        "valor_mercado_total": valor_mercado_total,
    }


def extract_clubs_from_html(html: str, competition: str, season: int) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")

    table = soup.find("table", class_="items")
    if not table:
        print("   ⚠ No se encontró la tabla principal con class='items'")
        return pd.DataFrame()

    rows = table.find_all("tr", class_=["odd", "even"])
    print(f"   Filas HTML detectadas: {len(rows)}")

    data: List[Dict] = []

    for idx, row in enumerate(rows, start=1):
        try:
            parsed = parse_row_cells(row)
            if not parsed:
                continue

            parsed["fila_tabla_num"] = to_int(parsed["fila_tabla"])
            parsed["plantilla_num"] = to_int(parsed["plantilla"])
            parsed["edad_media_num"] = parse_age(parsed["edad_media"])
            parsed["extranjeros_num"] = to_int(parsed["extranjeros"])
            parsed["valor_mercado_promedio_eur"] = parse_market_value(parsed["valor_mercado_promedio"])
            parsed["valor_mercado_total_eur"] = parse_market_value(parsed["valor_mercado_total"])
            parsed["competicion"] = competition
            parsed["temporada"] = season
            parsed["fecha_scrape"] = pd.Timestamp.now().strftime("%Y-%m-%d")

            data.append(parsed)

        except Exception as e:
            print(f"   ⚠ Error procesando fila {idx}: {e}")
            continue

    df = pd.DataFrame(data)

    if df.empty:
        print("   ⚠ No se extrajo ninguna fila útil")
        return df

    print("\n   🔍 Muestra antes del filtro:")
    sample_cols = [
        "fila_tabla",
        "club",
        "plantilla",
        "edad_media",
        "extranjeros",
        "valor_mercado_promedio",
        "valor_mercado_total",
        "fila_tabla_num",
        "plantilla_num",
        "edad_media_num",
        "valor_mercado_total_eur",
    ]
    print(df[sample_cols].head(5).to_string(index=False))

    valid_mask = (
        df["club"].notna()
        & df["plantilla_num"].notna()
        & df["edad_media_num"].notna()
        & df["valor_mercado_promedio_eur"].notna()
        & df["valor_mercado_total_eur"].notna()
    )

    df = df[valid_mask].copy()
    df = df[df["club"].str.lower() != "club"].copy()
    df = df[~df["club"].str.fullmatch(r"[\d\.]+", na=False)].copy()

    df = df.drop_duplicates(
        subset=["club", "competicion", "temporada"]
    ).reset_index(drop=True)

    return df


def scrape_competition_season(
    session: requests.Session,
    competition: str,
    slug: str,
    code: str,
    season: int
) -> pd.DataFrame:
    url = f"https://www.transfermarkt.com/{slug}/startseite/wettbewerb/{code}?saison_id={season}"
    print(f"\n {competition} {season}: {url}")

    html = fetch_html(session, url)

    debug_dir = os.path.join(os.path.dirname(__file__), "debug_html")
    os.makedirs(debug_dir, exist_ok=True)
    debug_path = os.path.join(debug_dir, f"{competition}_{season}.html".replace(" ", "_"))
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(html)

    df = extract_clubs_from_html(html, competition, season)
    print(f"   ✓ Registros válidos: {len(df)}")

    return df


def quality_checks(df: pd.DataFrame) -> None:
    if df.empty:
        print("⚠ Dataset vacío.")
        return

    print("\n===== CHEQUEOS DE CALIDAD =====")
    print(f"Filas totales: {len(df)}")
    print(f"Clubes únicos: {df['club'].nunique()}")
    print(f"Competiciones: {sorted(df['competicion'].dropna().unique().tolist())}")
    print(f"Temporadas: {sorted(df['temporada'].dropna().unique().tolist())}")

    print("\nValores faltantes por columna:")
    print(df.isna().sum().to_string())

    print("\nResumen numérico:")
    print(
        df[
            [
                "fila_tabla_num",
                "plantilla_num",
                "edad_media_num",
                "extranjeros_num",
                "valor_mercado_promedio_eur",
                "valor_mercado_total_eur",
            ]
        ].describe().round(2).to_string()
    )


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    dataset_dir = os.path.join(project_root, "dataset")
    os.makedirs(dataset_dir, exist_ok=True)

    full_dataset_path = os.path.join(dataset_dir, "transfermarkt_clubes_multiliga_completo.csv")
    clean_dataset_path = os.path.join(dataset_dir, "transfermarkt_clubes_multiliga_limpio.csv")

    print(" Iniciando scraping de competiciones en Transfermarkt...")

    session = build_session()
    all_frames = []

    for competition, (slug, code) in COMPETITIONS.items():
        for season in SEASONS:
            try:
                df = scrape_competition_season(session, competition, slug, code, season)
                if not df.empty:
                    all_frames.append(df)
            except Exception as e:
                print(f"   ⚠ Error en {competition} {season}: {e}")

    if all_frames:
        final_df = pd.concat(all_frames, ignore_index=True)
        final_df = final_df.drop_duplicates(
            subset=["club", "competicion", "temporada"]
        ).reset_index(drop=True)

        # redondeo final para evitar ruido flotante
        float_cols = [
            "edad_media_num",
            "valor_mercado_promedio_eur",
            "valor_mercado_total_eur",
        ]
        for col in float_cols:
            if col in final_df.columns:
                final_df[col] = final_df[col].round(2)

        final_df = final_df.sort_values(
            ["temporada", "competicion", "valor_mercado_total_eur"],
            ascending=[False, True, False]
        )

        # dataset completo
        full_columns = [
            "fila_tabla",
            "fila_tabla_num",
            "club",
            "club_url",
            "competicion",
            "temporada",
            "plantilla",
            "plantilla_num",
            "edad_media",
            "edad_media_num",
            "extranjeros",
            "extranjeros_num",
            "valor_mercado_promedio",
            "valor_mercado_promedio_eur",
            "valor_mercado_total",
            "valor_mercado_total_eur",
            "fecha_scrape",
        ]
        final_df = final_df[full_columns].copy()

        # dataset limpio para análisis
        clean_columns = [
            "club",
            "club_url",
            "competicion",
            "temporada",
            "fila_tabla_num",
            "plantilla_num",
            "edad_media_num",
            "extranjeros_num",
            "valor_mercado_promedio_eur",
            "valor_mercado_total_eur",
            "fecha_scrape",
        ]
        clean_df = final_df[clean_columns].copy()

    else:
        final_df = pd.DataFrame()
        clean_df = pd.DataFrame()

    final_df.to_csv(full_dataset_path, index=False, encoding="utf-8-sig")
    clean_df.to_csv(clean_dataset_path, index=False, encoding="utf-8-sig")

    print(f"\n Dataset completo guardado en:\n{full_dataset_path}")
    print(f"Dataset limpio guardado en:\n{clean_dataset_path}")
    print(f"Total de registros: {len(final_df)}")
    print(f"Columnas dataset completo: {list(final_df.columns)}")
    print(f"Columnas dataset limpio: {list(clean_df.columns)}")

    quality_checks(clean_df)


if __name__ == "__main__":
    main()