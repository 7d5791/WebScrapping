import re
import time
import pandas as pd
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager


URLS = [
    # Pichincha
    "https://www.plusvalia.com/alquiler/departamentos/pichincha/quito",

    # Guayas
    "https://www.plusvalia.com/alquiler/departamentos/guayas/guayaquil",

    # Azuay
    "https://www.plusvalia.com/alquiler/departamentos/azuay/cuenca",

    # Manabí
    "https://www.plusvalia.com/alquiler/departamentos/manabi/manta",

    # Tungurahua
    "https://www.plusvalia.com/alquiler/departamentos/tungurahua/ambato",

    # Loja
    "https://www.plusvalia.com/alquiler/departamentos/loja/loja",

    # Imbabura
    "https://www.plusvalia.com/alquiler/departamentos/imbabura/ibarra",

    # El Oro
    "https://www.plusvalia.com/alquiler/departamentos/el-oro/machala",
]

OUTPUT_PATH = "../dataset/plusvalia_ecuador_alquiler_departamentos.csv"


def configurar_driver():
    options = Options()

    # Déjalo comentado mientras pruebas visualmente
    # options.add_argument("--headless=new")

    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    options.add_argument(
        "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    driver.implicitly_wait(8)
    return driver


def limpiar_texto(texto):
    if texto is None:
        return None
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto if texto else None


def extraer_numero(texto):
    if not texto:
        return None

    texto = texto.replace(".", "").replace(",", ".")
    match = re.search(r"(\d+(?:\.\d+)?)", texto)

    if not match:
        return None

    try:
        return float(match.group(1))
    except ValueError:
        return None


def extraer_entero_patron(texto, patron):
    if not texto:
        return None

    match = re.search(patron, texto, re.IGNORECASE)
    if not match:
        return None

    try:
        return int(match.group(1))
    except ValueError:
        return None


def extraer_precio(texto):
    if not texto:
        return None

    match = re.search(r"USD\s*([\d\.\,]+)", texto, re.IGNORECASE)
    if match:
        return extraer_numero(match.group(1))

    match = re.search(r"\$\s*([\d\.\,]+)", texto)
    if match:
        return extraer_numero(match.group(1))

    return None


def extraer_alicuota(texto):
    if not texto:
        return None

    match = re.search(r"USD\s*([\d\.\,]+)\s*Al[ií]cuota", texto, re.IGNORECASE)
    if match:
        return extraer_numero(match.group(1))

    match = re.search(r"Al[ií]cuota[:\s]*([\d\.\,]+)", texto, re.IGNORECASE)
    if match:
        return extraer_numero(match.group(1))

    return None


def extraer_titulo_desde_href(href):
    if not href:
        return None

    match = re.search(r"/clasificado/([^.]+)\.html", href)
    if not match:
        return None

    slug = match.group(1)

    slug = re.sub(
        r"^(alclapin|veclapin|cvclapin|vcclapin)-",
        "",
        slug,
        flags=re.IGNORECASE
    )
    slug = re.sub(r"-\d+$", "", slug)
    slug = slug.replace("-", " ")
    slug = limpiar_texto(slug)

    return slug.capitalize() if slug else None


def extraer_estacionamientos(texto):
    if not texto:
        return None

    patrones = [
        r"(\d+)\s*estac",
        r"(\d+)\s*parqueaderos?",
        r"(\d+)\s*parqueadero",
        r"(\d+)\s*garage"
    ]

    for patron in patrones:
        match = re.search(patron, texto, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None

    return None


def extraer_ubicacion_textual(texto):
    if not texto:
        return None

    patrones = [
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Quito)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Guayaquil)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Cuenca)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Manta)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Ambato)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Loja)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Ibarra)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Machala)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Pichincha)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Guayas)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Azuay)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Manab[ií])",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Tungurahua)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Loja)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*Imbabura)",
        r"([A-ZÁÉÍÓÚÑa-záéíóúñ0-9\s]+,\s*El Oro)",
    ]

    for patron in patrones:
        match = re.search(patron, texto)
        if match:
            return limpiar_texto(match.group(1))

    return None


def extraer_ciudad_provincia(url):
    match = re.search(r"departamentos/([^/]+)/([^/?]+)", url)

    if not match:
        return None, None

    provincia = match.group(1).replace("-", " ").title()
    ciudad = match.group(2).replace("-", " ").title()

    return ciudad, provincia


def es_anuncio_alquiler_valido(texto):
    if not texto:
        return False

    t = texto.lower()

    if "venta o renta" in t or "renta o venta" in t:
        return False

    if "venta" in t and "alquiler" not in t and "arriendo" not in t and "renta" not in t:
        return False

    return True


def obtener_cards_resultados(driver):
    anchors = driver.find_elements(By.XPATH, "//a[contains(@href, '/propiedades/')]")

    cards = []
    vistos = set()

    for anchor in anchors:
        href = anchor.get_attribute("href")

        if not href or href in vistos:
            continue

        vistos.add(href)

        try:
            contenedor = anchor.find_element(
                By.XPATH,
                "./ancestor::*[self::article or self::div][1]"
            )
            texto = limpiar_texto(contenedor.text)
        except Exception:
            texto = limpiar_texto(anchor.text)

        cards.append({
            "href": href,
            "texto": texto
        })

    return cards


def extraer_datos_card(card, url_resultado):
    href = card["href"]
    texto = card["texto"]

    if not texto:
        return None

    texto_bajo = texto.lower()

    if texto_bajo == "plusvalia" or "publicidad" in texto_bajo:
        return None

    if not es_anuncio_alquiler_valido(texto):
        return None

    ciudad, provincia = extraer_ciudad_provincia(url_resultado)

    fila = {
        "fecha_extraccion": datetime.now().isoformat(),
        "operacion": "alquiler",
        "tipo_propiedad": "departamento",
        "titulo": extraer_titulo_desde_href(href),
        "precio_usd": extraer_precio(texto),
        "alicuota_usd": extraer_alicuota(texto),
        "superficie_m2": extraer_entero_patron(texto, r"(\d+)\s*m²"),
        "habitaciones": extraer_entero_patron(texto, r"(\d+)\s*hab"),
        "banos": extraer_entero_patron(texto, r"(\d+)\s*bañ"),
        "estacionamientos": extraer_estacionamientos(texto),
        "ubicacion_textual": extraer_ubicacion_textual(texto),
        "ciudad": ciudad,
        "provincia": provincia,
        "texto_card": texto,
        "url_aviso": href,
        "pagina_resultado": url_resultado
    }

    if fila["titulo"] is None and texto:
        palabras = texto.split()
        fila["titulo"] = " ".join(palabras[:8]) if palabras else None

    return fila


def procesar_pagina_resultados(driver, url):
    print(f"Extrayendo desde: {url}")
    driver.get(url)

    wait = WebDriverWait(driver, 20)

    try:
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    except TimeoutException:
        print("No cargó la página:", url)
        return []

    time.sleep(6)

    cards = obtener_cards_resultados(driver)
    print("Tarjetas detectadas:", len(cards))

    resultados = []

    for card in cards:
        fila = extraer_datos_card(card, url)
        if fila:
            resultados.append(fila)

    return resultados


def main():
    driver = configurar_driver()
    resultados = []

    try:
        for url in URLS:
            filas = procesar_pagina_resultados(driver, url)
            resultados.extend(filas)
            time.sleep(3)

    finally:
        driver.quit()

    df = pd.DataFrame(resultados)

    if not df.empty:
        df.drop_duplicates(subset=["url_aviso"], inplace=True)

    if not df.empty:
        df = df[
            df["precio_usd"].notna() |
            df["superficie_m2"].notna() |
            df["habitaciones"].notna() |
            df["banos"].notna() |
            df["estacionamientos"].notna()
        ]

    df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print("\nDataset generado")
    print("Registros:", len(df))

    if not df.empty:
        print(df.head().to_string())
        print("\nNulos:")
        print(df.isnull().sum())
    else:
        print("No se extrajeron registros útiles.")


if __name__ == "__main__":
    main()