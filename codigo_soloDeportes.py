# -*- coding: utf-8 -*-
"""
SOLODEPORTES vs NIKE - Adaptado de StockCenter v4
SOLO CON LOS CAMBIOS SOLICITADOS:
- StyleColor va OpenAI (screenshot + descripcin) - NO de la URL
- 3 workers en paralelo
- TODO el assortment de SoloDeportes (est o no en StatusBooks)
- Mismos campos de output que el script original de SoloDeportes
"""

import os
import re
import time
import gc
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math
import json
import random
import datetime as dt
import unicodedata
from urllib.parse import urljoin

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from openai import OpenAI

# =========================
# CONFIGURACIN ESPECFICA SOLODEPORTES
# =========================

# OpenAI
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = "gpt-4o-mini"  # Cambiar a "gpt-4o" si necesits ms precisin
OPENAI_TIMEOUT_S = 45

# Proxy (opcional)
PROXY_SERVER = "http://dc.decodo.com:10001"
PROXY_USERNAME = "sp6g2od2ak"
PROXY_PASSWORD = "9hbclm71oFtP_8BgAq"

# =========================
# CONFIGURACIN GENERAL
# =========================

# Archivos de entrada
LINKS_XLSX = "Links Retail.xlsx"
LINKS_SHEET = "SoloDeportes"
LINKS_COL_CATEGORY = "Categoria"
LINKS_COL_LINK = "LINK"

# Cdigo postal (para clculos de envo)
CP_AMBA = "1425"

# Parmetros de envo
NIKE_STD_SHIPPING_ARS = 8899.0
NIKE_FREE_SHIP_FROM_ARS = 99000.0
RETAILER_STD_SHIPPING_ARS = 6999.0
RETAILER_FREE_SHIP_FROM_ARS = 149999.0

# Cuotas Nike
NIKE_CUOTAS_SIMPLE_MODE = True
NIKE_CUOTAS_ALL = 3
NIKE_CUOTAS_HIGH = 6
NIKE_CUOTAS_HIGH_FROM = 79000.0

# Season
SEASON = "SP26"

# StatusBooks (Nike)
STATUSBOOKS_FILE = "StatusBooks NDDC ARG SP26.xlsb"
STATUSBOOKS_SHEET = "Books NDDC"

# Archivo auxiliar para productos no-NDDC
AUX_ASSORT_FILE = "01 Lista actualizada a nivel GRUPO DE ARTÍCULOS total.xlsx"
AUX_ASSORT_SHEET = "Primera Calidad"
AUX_COL_STYLE = "Material Nike"
AUX_COL_NAME = "Nombre material"
AUX_COL_GRUPO = "Grupo de carga"
AUX_COL_ANIO = "Año"

# Performance
Headless = True
AGENTS = int(os.getenv("AGENTS", "3"))  # 3 workers en paralelo (override por ENV)

# Debug/Testing
DEBUG_LIMIT = int(os.getenv("DEBUG_LIMIT", "0"))  # 0 = sin lmite, N = limitar a N productos
DEBUG_OFFSET = int(os.getenv("DEBUG_OFFSET", "0"))  # saltear primeros N productos para pruebas de otros lotes

# Scroll y navegacin
MAX_PLP_SCROLL_ROUNDS = 30  # Reducido de 40 para ser más rápido
PLP_STAGNATION_ROUNDS = 4  # Reducido de 6 para terminar antes si se estanca
PLP_SCROLL_PIXELS = 1400

# Timeouts
PDP_WAIT_MS = 25_000
PLP_WAIT_MS = 35_000  # Reducido de 50s a 35s

# Cache
CACHE_PATH = "solodeportes_cache.json"

# Refresh
REFRESH_CACHED = os.getenv("REFRESH_CACHED", "1").strip().lower() in {"1", "true", "yes", "y"}
VALID_PRICE_MIN = 0.0

# Reset de browser cada N productos
BROWSER_RESET_EVERY = 90
RESET_SLEEP_SECONDS = 8

# Timestamps
TS = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DATE = dt.datetime.now().strftime("%Y-%m-%d")
RUN_DATETIME = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# Archivos de salida
OUT_RAW_CSV = f"solodeportes_vs_nike_raw_{TS}.csv"
OUT_VISUAL_XLSX = f"solodeportes_vs_nike_visual_{TS}.xlsx"

# =========================
# FUNCIONES DE OPENAI
# =========================

def _b64_png(png_bytes: bytes) -> str:
    """Convierte bytes de PNG a base64"""
    return base64.b64encode(png_bytes).decode("ascii")

def _normalize_stylecolor_candidate(txt: str) -> str | None:
    if not txt:
        return None
    cleaned = re.sub(r"[^A-Z0-9\-]", "", str(txt).upper().strip())
    if not cleaned or cleaned == "NO_ENCONTRADO":
        return None
    m = re.search(r"([A-Z0-9]{4,8})-([0-9]{3})", cleaned)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m2 = re.search(r"([A-Z0-9]{4,8})([0-9]{3})", cleaned)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}"
    return None

def extract_stylecolor_from_solo_sku(raw_sku: str) -> str:
    """Intenta extraer StyleColor Nike embebido en SKU de retailer (ej: 510010FD6454001 -> FD6454-001)."""
    if not raw_sku:
        return ""
    sku = re.sub(r"[^A-Z0-9\-]", "", str(raw_sku).upper())
    if not sku:
        return ""
    m = re.search(r"([A-Z]{2,3}[0-9]{4,6})[-_]?([0-9]{3})", sku)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m2 = re.search(r"([A-Z0-9]{4,8})-([0-9]{3})", sku)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}"
    return ""

def extract_candidate_sku_from_page(page) -> str:
    """Busca un SKU/código candidato en HTML + texto visible."""
    blobs = []
    try:
        blobs.append(page.content() or "")
    except Exception:
        pass
    try:
        body_txt = page.locator("body").first.inner_text(timeout=1500)
        blobs.append(body_txt or "")
    except Exception:
        pass
    joined = "\n".join(blobs)
    if not joined:
        return ""
    patterns = [
        r'"sku"\s*:\s*"([A-Za-z0-9\-]{8,40})"',
        r"(?i)SKU\s*[:#\-]?\s*([A-Za-z0-9\-]{8,40})",
        r"\b\d{4,8}[A-Z]{2,3}\d{6,10}\b",
        r"\b[A-Z]{2,3}\d{4,6}[-_]?\d{3}\b",
    ]
    for pat in patterns:
        m = re.search(pat, joined)
        if m:
            return (m.group(1) if m.groups() else m.group(0)).strip()
    return ""

def ask_stylecolor_from_text_safe(client: OpenAI, sku: str, name: str = "", description: str = "") -> tuple[str | None, str | None]:
    """Consulta OpenAI por texto (SKU + descripción), sin imagen."""
    if not sku:
        return None, "Sin SKU para consulta texto-only"

    parsed = extract_stylecolor_from_solo_sku(sku)
    if parsed:
        return parsed, None

    prompt = f"""Necesito extraer el StyleColor de Nike SOLO con texto.

SKU retailer: {sku}
Nombre producto: {name}
Descripción: {description}

Regla: el StyleColor tiene formato como FJ2587-400, CW4554-101, DV1312-001.
Si el SKU parece tener el style embebido, reconstruyelo con guion antes de los últimos 3 dígitos.

Respondé SOLO el StyleColor en MAYÚSCULAS.
Si no hay suficiente información, respondé NO_ENCONTRADO."""
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=20,
            temperature=0.0,
            timeout=OPENAI_TIMEOUT_S,
        )
        txt = (response.choices[0].message.content or "").strip().upper()
        print(f"       OpenAI text-only raw response: '{txt}'")
        normalized = _normalize_stylecolor_candidate(txt)
        if normalized:
            return normalized, None
        return None, f"Texto-only sin formato válido: '{txt}'"
    except Exception as e:
        return None, f"Texto-only error: {str(e)[:100]}"

def ask_stylecolor_from_image_safe(client: OpenAI, png_bytes: bytes, page_description: str = "") -> tuple[str | None, str | None]:
    """
    Consulta OpenAI Vision para obtener StyleColor desde screenshot + descripcin
    Esta es la NICA forma de obtener StyleColor - NO se extrae de la URL
    """
    img_size_kb = len(png_bytes) / 1024
    print(f"       Tamao imagen: {img_size_kb:.1f} KB")
    
    if img_size_kb > 20 * 1024:  # >20MB
        return None, f"Imagen demasiado grande ({img_size_kb:.1f} KB > 20MB)"
    
    b64 = _b64_png(png_bytes)
    
    prompt = f"""En esta imagen de una pgina de producto de Nike en SoloDeportes, necesito encontrar el cdigo StyleColor de Nike.

{page_description}

El StyleColor de Nike tiene formato como "FJ2587-400" o "CW4554-101" o "DV1312-001" (letras y nmeros, generalmente con un guin).

BUSCA EN:
1. El nombre del producto (ej: "Zapatillas Nike Revolution 7 FJ2587-400")
2. El cdigo de producto o referencia que aparece en la pgina
3. Debajo del ttulo del producto
4. En cualquier lugar donde aparezca un cdigo alfanumrico con guin

Respond SOLAMENTE con el cdigo StyleColor en MAYSCULAS, sin texto adicional.
Si no encontrs ningn cdigo con alta confianza, respond "NO_ENCONTRADO"."""
    
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:image/png;base64,{b64}",
                                "detail": "high"
                            }
                        }
                    ]
                }
            ],
            max_tokens=30,
            temperature=0.0,
            timeout=OPENAI_TIMEOUT_S
        )
        
        txt = (response.choices[0].message.content or "").strip().upper()
        print(f"       OpenAI raw response: '{txt}'")
        
        # Limpiar respuesta
        txt = re.sub(r'[^\w\-]', '', txt)
        
        if txt == "NO_ENCONTRADO":
            return None, "OpenAI no detect StyleColor"
        
        # Validar formatos de StyleColor
        if re.match(r'^[A-Z0-9]{4,8}-[0-9]{3}$', txt):
            return txt, None
        if re.match(r'^[A-Z0-9]{9,11}$', txt):
            # Si no tiene guin, agregarlo
            if len(txt) >= 9:
                return txt[:-3] + "-" + txt[-3:], None
            return txt, None
        
        if txt and len(txt) > 4:
            # Intentar extraer patrn con guin
            match = re.search(r'([A-Z0-9]{4,8})-?([0-9]{3})', txt)
            if match:
                return f"{match.group(1)}-{match.group(2)}", None
        
        return None, f"No formato vlido: '{txt}'"
        
    except Exception as e:
        error_msg = str(e)
        print(f"       Error OpenAI: {error_msg[:100]}")
        
        if "401" in error_msg:
            return None, "API key invlida"
        elif "429" in error_msg:
            return None, "Rate limit excedido"
        elif "quota" in error_msg.lower():
            return None, "Quota excedido"
        elif "timeout" in error_msg.lower():
            return None, "Timeout"
        else:
            return None, f"Error: {error_msg[:100]}"

# =========================
# FUNCIONES AUXILIARES
# =========================

def human_pause(a=0.25, b=0.85):
    time.sleep(random.uniform(a, b))

def log(msg: str):
    import sys
    import io
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        # Usar UTF-8 buffer directamente para evitar limitaciones de cp1252
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        print(msg, flush=True)

def _is_fatal_nav_error(e: Exception) -> bool:
    s = f"{type(e).__name__}: {e}"
    s_low = s.lower()
    fatal_tokens = [
        "net::err_insufficient_resources",
        "net::err_aborted",
        "connection closed while reading from the driver",
        "target closed",
        "browser has been closed",
        "page.goto: target closed",
        "page.goto: browser has been closed",
        "page.goto: navigation failed",
        "page.goto: net::",
    ]
    return any(t in s_low for t in fatal_tokens)

def _reset_triplet(worker_id: int, pw, browser, context, page, reason: str = ""):
    try:
        log(f" (W{worker_id}) RESET POR ERROR: {reason}".strip())
    except Exception:
        pass
    try:
        page.close()
    except Exception:
        pass
    try:
        context.close()
    except Exception:
        pass
    try:
        browser.close()
    except Exception:
        pass
    try:
        gc.collect()
    except Exception:
        pass
    time.sleep(RESET_SLEEP_SECONDS + random.uniform(1.0, 3.0))
    return new_browser_context(pw)

def normalize_stylecolor(style: str) -> str:
    """Normaliza StyleColor (solo quita NI del inicio si existe)"""
    if not style:
        return ""
    style = str(style).strip().upper()
    if style.startswith("NI") and len(style) >= 3:
        return style[2:]
    return style

def extract_stylecolor_from_url(url: str) -> str:
    """
     NOTA: Esta funcin NO se usa para obtener StyleColor.
    Solo se mantiene por compatibilidad pero NO se llama en el flujo principal.
    El StyleColor viene EXCLUSIVAMENTE de OpenAI.
    """
    return ""

def parse_money_ar_to_float(s) -> float:
    """Parsea montos ARS en cualquier formato a float.

    Soporta:
    - Floats/ints de Python directos (ej: 159999.0 de pyxlsb)
    - Strings con formato AR: '$ 159.999,00' / '159999' / '159.999'
    - Strings con .0 / .00 al final (pyxlsb serializa floats así)
    """
    if s is None:
        return 0.0
    # Si ya es numérico (pyxlsb devuelve floats directamente)
    if isinstance(s, (int, float)):
        return float(s) if s == s else 0.0  # NaN check
    s = str(s).strip()
    if not s or s.lower() in {"nan", "none", "null", ""}:
        return 0.0
    cleaned = re.sub(r"[^\d\.,\-]", "", s)
    if not cleaned:
        return 0.0
    # Formato AR con coma decimal: "159.999,00" → 159999.00
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
        try:
            return float(cleaned)
        except Exception:
            return 0.0
    # Tiene punto: puede ser separador decimal o de miles
    if "." in cleaned:
        parts = cleaned.split(".")
        # Exactamente 2 dígitos después del último punto → decimal (ej: "159999.00")
        if len(parts[-1]) == 2:
            try:
                # Reconstruct: remove internal dots (miles) keep last as decimal
                integer_part = "".join(parts[:-1])
                return float(integer_part + "." + parts[-1])
            except Exception:
                return 0.0
        # 1 dígito after dot (ej: "159999.0" de pyxlsb) → es .0, tratar como entero
        if len(parts[-1]) == 1:
            try:
                return float("".join(parts[:-1]) + parts[-1][0] if parts[-1] == "0" else "".join(parts))
            except Exception:
                pass
            # Safe fallback: remove the trailing .0
            try:
                return float(parts[0].replace(".", ""))
            except Exception:
                return 0.0
        # 3+ dígitos after dot → el punto es separador de miles (ej: "159.999")
        cleaned = cleaned.replace(".", "")
        try:
            return float(cleaned)
        except Exception:
            return 0.0
    # Sin punto ni coma: entero puro
    try:
        return float(cleaned)
    except Exception:
        return 0.0

def parse_sale_percent_to_decimal(s: str) -> float:
    if s is None:
        return 0.0
    s = str(s).strip()
    if not s or s.lower() == "nan":
        return 0.0
    m = re.search(r"(\d+(?:[.,]\d+)?)\s*%", s)
    if m:
        val = m.group(1).replace(",", ".")
        try:
            pct = float(val)
            return max(0.0, min(1.0, pct / 100.0))
        except Exception:
            return 0.0
    s2 = re.sub(r"[^\d\.,\-]", "", s).replace(",", ".")
    try:
        x = float(s2)
        if x > 1.0:
            return max(0.0, min(1.0, x / 100.0))
        return max(0.0, min(1.0, x))
    except Exception:
        return 0.0

def parse_stock_bl_to_float(v) -> float:
    if v is None:
        return 0.0
    s = str(v).strip()
    if s == "" or s == "-" or s.lower() in {"nan", "none", "null"}:
        return 0.0
    s2 = s.replace(",", ".")
    s2 = re.sub(r"[^0-9\.\-]", "", s2)
    if s2 in {"", "-", "."}:
        return 0.0
    try:
        x = float(s2)
        return x if x > 0 else 0.0
    except Exception:
        return 0.0

def atomic_write_json(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def load_cache(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        tmp = path + ".tmp"
        if os.path.exists(tmp):
            try:
                with open(tmp, "r", encoding="utf-8") as f:
                    return json.load(f) or {}
            except Exception:
                pass
        raise

def load_plps_from_links_excel(path: str, sheet=None) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"No encuentro el Excel de links: {path}")
    
    raw = pd.read_excel(path, sheet_name=sheet)
    
    if isinstance(raw, dict):
        sheet_name, df = next(iter(raw.items()))
        log(f" Excel con mltiples sheets. Usando la primera: '{sheet_name}'")
    else:
        df = raw
    
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    
    if LINKS_COL_CATEGORY not in df.columns or LINKS_COL_LINK not in df.columns:
        raise ValueError(
            f"El Excel debe tener columnas '{LINKS_COL_CATEGORY}' y '{LINKS_COL_LINK}'. "
            f"Columnas detectadas: {list(df.columns)}"
        )
    
    out = df[[LINKS_COL_CATEGORY, LINKS_COL_LINK]].copy()
    out[LINKS_COL_CATEGORY] = out[LINKS_COL_CATEGORY].astype(str).str.strip()
    out[LINKS_COL_LINK] = out[LINKS_COL_LINK].astype(str).str.strip()
    
    out = out[out[LINKS_COL_LINK].str.startswith("http", na=False)]
    out = out[out[LINKS_COL_CATEGORY].ne("")]
    
    if out.empty:
        raise ValueError("El Excel de links qued vaco luego de limpiar. Revis contenido.")
    
    return out

def _read_excel_any_noheader(path: str, sheet: str):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsb":
        return pd.read_excel(path, sheet_name=sheet, header=None, dtype=str, engine="pyxlsb")
    return pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)

def load_statusbooks_map(path: str, sheet: str):
    log("\n Cargando Status Books...")
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"No encuentro el archivo de StatusBooks: {path} (SEASON={SEASON})")
    
    raw = _read_excel_any_noheader(path, sheet)
    
    header_row = None
    for r in range(min(500, len(raw))):
        row = raw.iloc[r, :].astype(str).fillna("")
        if (row == "Product Code").any():
            header_row = r
            break
    
    if header_row is None:
        for r in range(min(800, len(raw))):
            row = raw.iloc[r, :].astype(str).fillna("").str.strip()
            if (row.str.lower() == "product code").any():
                header_row = r
                break
    
    if header_row is None:
        raise ValueError(
            "No pude encontrar la fila header (no aparece 'Product Code'). "
            "Revis el StatusBooks: quizs cambi el nombre del campo."
        )
    
    headers = raw.iloc[header_row, :].fillna("").astype(str).str.strip().tolist()
    sb = raw.iloc[header_row + 1 :, :].copy()
    sb.columns = headers
    
    sb.columns = [c if c else f"__EMPTY__{i}" for i, c in enumerate(sb.columns)]
    
    log(f"    Archivo: {path} | Sheet: {sheet} | Filas: {len(sb)} | HeaderRow: {header_row+1}")
    
    COL_CODE = "Product Code"
    COL_NAME = "Marketing Name"
    COL_DIV = "BU"
    COL_CAT = "Category"
    COL_FR = "Franchise"
    
    full_price_col_idx = None
    # Buscar la columna cuyo HEADER es exactamente el valor de SEASON (ej: "SP26")
    # El header está en header_row (la fila con "Product Code").
    # Según el requerimiento, la columna SP26 tiene su nombre en la fila 7 (índice 6, base-0).
    # Estrategia: buscar en los headers directamente una columna cuyo nombre sea exactamente SEASON.
    season_col_name = str(SEASON).strip()
    if season_col_name in sb.columns:
        full_price_col_idx = list(sb.columns).index(season_col_name)
        log(f"    Full Price: columna '{season_col_name}' encontrada directamente en headers (col #{full_price_col_idx+1})")
    else:
        # Fallback: buscar en la fila anterior al header (celdas mergeadas con "(SP26)" o "SP26")
        group_row = header_row - 1
        if group_row >= 0:
            top = raw.iloc[group_row, :].fillna("").astype(str)
            season_patterns = [f"({SEASON})", str(SEASON)]
            match_idxs = []
            for i, val in enumerate(top.tolist()):
                v = str(val).strip()
                if any(p in v for p in season_patterns):
                    match_idxs.append(i)
            if match_idxs:
                full_price_col_idx = match_idxs[0]
                col_name_fb = str(sb.columns[full_price_col_idx]) if full_price_col_idx < len(sb.columns) else ""
                log(f"    Full Price: columna '{season_col_name}' hallada por fila de grupo (col #{full_price_col_idx+1}, header='{col_name_fb}')")
    
    COL_SALE = "SALE"
    COL_STOCK = "STOCK BL (Inventario Brandlive)"
    COL_SSN = "SSN VTA"
    
    missing = [c for c in [COL_CODE, COL_NAME, COL_DIV, COL_CAT, COL_FR, COL_SALE, COL_STOCK] if c not in sb.columns]
    if missing:
        raise KeyError(
            f"Faltan columnas en StatusBooks (header detectado en fila {header_row+1}): {missing}. "
            f"Ejemplo de columnas detectadas (primeras 40): {list(sb.columns)[:40]}"
        )
    
    if full_price_col_idx is None:
        raise KeyError(
            f"No pude detectar la columna de Full Price para SEASON={SEASON}. "
            f"Busqué header exacto '{season_col_name}' y la fila de grupo anterior al header. "
            f"HeaderRow detectado: {header_row+1}. "
            f"Ejemplo de headers (primeras 40): {list(sb.columns)[:40]}"
        )
    
    work = pd.DataFrame()
    work["ProductCode"] = sb[COL_CODE].astype(str).str.strip().str.upper()
    work["MarketingName"] = sb[COL_NAME].astype(str).str.strip()
    work["SB_Division"] = sb[COL_DIV].astype(str).str.strip()
    work["SB_Category"] = sb[COL_CAT].astype(str).str.strip()
    work["SB_Franchise"] = sb[COL_FR].astype(str).str.strip()
    
    if COL_SSN in sb.columns:
        work["SB_SSN_VTA"] = sb[COL_SSN].astype(str).str.strip()
    else:
        work["SB_SSN_VTA"] = ""
        log(" StatusBooks: no encontr columna 'SSN VTA'. Season por fila quedar vaco/no registrado para NDDC.")
    
    sb = sb.reset_index(drop=True)
    work = work.reset_index(drop=True)
    # Debug: mostrar primeros valores crudos de la columna SP26 para verificar parsing
    _raw_sp26 = sb.iloc[:, full_price_col_idx]
    _sample = _raw_sp26.dropna().head(5)
    log(f"   [DEBUG] Primeros valores CRUDOS columna {season_col_name} (col #{full_price_col_idx}): {[repr(v) for v in _sample.tolist()]}")
    nike_full_series = _raw_sp26.apply(parse_money_ar_to_float)
    _sample_parsed = nike_full_series.dropna()
    _sample_nonzero = _sample_parsed[_sample_parsed > 0].head(5)
    log(f"   [DEBUG] Primeros valores PARSEADOS (>0): {_sample_nonzero.tolist()}")

    # Detección de precios en $k (miles): pyxlsb a veces serializa 299.999 como 299.999
    # en lugar de 299999. Si la mediana es < 2000 → los valores están en miles → x1000.
    _vals = pd.to_numeric(nike_full_series, errors="coerce")
    _med  = _vals[_vals > 0].median()
    if pd.notna(_med) and _med < 2000:
        work["NikeFullPrice"] = (_vals * 1000.0).to_numpy()
        log(f"   [DEBUG] NikeFullPrice en $k detectado (mediana={_med:.2f}) → multiplico x1000")
    else:
        work["NikeFullPrice"] = _vals.to_numpy()
        log(f"   [DEBUG] NikeFullPrice en ARS directo (mediana={_med:.2f})")

    work["NikeSaleDecimal"] = sb[COL_SALE].apply(parse_sale_percent_to_decimal)
    work["NikeFinalPrice"] = work["NikeFullPrice"] * (1.0 - work["NikeSaleDecimal"])
    
    work["StockBL"] = sb[COL_STOCK].apply(parse_stock_bl_to_float)
    
    work = work[work["ProductCode"].ne("")].copy()
    work = work[work["NikeFinalPrice"] > 0].copy()
    work = work[work["StockBL"] > 0].copy()
    
    sb_map = {}
    for _, r in work.iterrows():
        code = r["ProductCode"]
        data = {
            "SB_ProductCode": code,
            "SB_MarketingName": r["MarketingName"],
            "SB_Division": r["SB_Division"],
            "SB_Category": r["SB_Category"],
            "SB_Franchise": r["SB_Franchise"],
            "SB_SSN_VTA": r.get("SB_SSN_VTA", ""),
            "NikeFullPrice": float(r["NikeFullPrice"]),
            "NikeSaleDecimal": float(r["NikeSaleDecimal"]),
            "NikeFinalPrice": float(r["NikeFinalPrice"]),
        }
        sb_map[code] = data
        if code.startswith("NI"):
            sb_map[code[2:]] = data
        else:
            sb_map["NI" + code] = data
    
    log(f"    Product Codes vlidos (Final>0 y StockBL>0): {len(work)}")
    log(f"    Keys en mapa (incluye variantes con/sin NI): {len(sb_map)}")
    return sb_map

def _map_grupo_to_bu(grupo: str) -> str:
    try:
        g = str(grupo or "").strip().lower()
    except Exception:
        return ""
    if g == "accesorios":
        return "EQ"
    if g == "calzado":
        return "FW"
    if g == "indumentaria":
        return "APP"
    return ""

def load_aux_assort_map(path: str, sheet: str) -> dict:
    """Mapa auxiliar para completar campos cuando el producto NO est en StatusBooks"""
    log(f"\n Buscando AUX_ASSORT_FILE: {path}")
    resolved_path = path
    
    # 1. Intentar ruta directa
    if os.path.exists(resolved_path):
        log(f"    ✓ Encontrado directamente: {resolved_path}")
    else:
        log(f"    ✗ No encontrado en ruta directa")
        try:
            wanted = os.path.basename(path)
            wanted_norm = unicodedata.normalize("NFKD", wanted).encode("ascii", "ignore").decode("ascii").lower()
            log(f"    Buscando variantes de: '{wanted}' (norm: '{wanted_norm}')")

            # Directorios a buscar (incluyendo el script dir)
            script_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else None
            search_dirs = [os.getcwd()]
            if script_dir and script_dir != os.getcwd():
                search_dirs.append(script_dir)
            
            log(f"    Directorios de búsqueda: {search_dirs}")
            
            checked = set()
            candidates = []

            for d in search_dirs:
                d_abs = os.path.abspath(d)
                if d_abs in checked or not os.path.isdir(d_abs):
                    continue
                checked.add(d_abs)
                log(f"    Escaneando: {d_abs}")
                
                try:
                    files_in_dir = os.listdir(d_abs)
                except Exception as e:
                    log(f"      ✗ Error listando directorio: {e}")
                    continue
                    
                for fname in files_in_dir:
                    if not fname.lower().endswith(".xlsx"):
                        continue
                    if fname.startswith("~$"):
                        continue

                    fname_norm = unicodedata.normalize("NFKD", fname).encode("ascii", "ignore").decode("ascii").lower()
                    
                    # Match exacto
                    if fname_norm == wanted_norm:
                        resolved_path = os.path.join(d_abs, fname)
                        log(f"      ✓ Match exacto: {fname}")
                        break

                    # Match por palabras clave
                    if (
                        "lista actualizada" in fname_norm
                        and "grupo de art" in fname_norm
                        and "total" in fname_norm
                    ):
                        candidate = os.path.join(d_abs, fname)
                        candidates.append(candidate)
                        log(f"      ~ Candidato: {fname}")
                        
                if os.path.exists(resolved_path) and resolved_path != path:
                    break

            if not os.path.exists(resolved_path) and candidates:
                resolved_path = candidates[0]
                log(f"    → Usando primer candidato: {resolved_path}")
                
        except Exception as e:
            log(f"    ✗ Error en búsqueda avanzada: {e}")

    if not os.path.exists(resolved_path):
        log(f" No encuentro AUX_ASSORT_FILE: {path}. Continuo sin completado auxiliar.")
        return {}

    if os.path.abspath(resolved_path) != os.path.abspath(path):
        log(f" AUX_ASSORT_FILE resuelto automticamente: {resolved_path}")

    log(f"    Leyendo Excel (puede tardar 15-30s)...")
    try:
        # Leer con motor openpyxl read_only para acelerar
        df = pd.read_excel(
            resolved_path, 
            sheet_name=sheet, 
            dtype=str,
            usecols=[AUX_COL_STYLE, AUX_COL_NAME, AUX_COL_GRUPO, AUX_COL_ANIO],
            engine='openpyxl'
        )
    except Exception as e:
        error_msg = str(e).lower()
        log(f"    Error con usecols: {type(e).__name__}")
        
        # Si el error es por columnas faltantes, intentar leer todo
        if 'usecols' in error_msg or 'column' in error_msg:
            try:
                log(f"    Reintentando lectura completa...")
                df = pd.read_excel(resolved_path, sheet_name=sheet, dtype=str, engine='openpyxl')
            except Exception as e2:
                log(f"    Error en lectura completa: {type(e2).__name__}: {str(e2)[:200]}. Continuo sin completado auxiliar.")
                return {}
        else:
            log(f"    Error: {type(e).__name__}: {str(e)[:200]}. Continuo sin completado auxiliar.")
            return {}
    
    log(f"    ✓ Excel leído: {len(df)} filas")
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    
    missing = [c for c in [AUX_COL_STYLE, AUX_COL_NAME, AUX_COL_GRUPO] if c not in df.columns]
    if missing:
        log(f" AUX_ASSORT_FILE: faltan columnas {missing}. Detectadas: {list(df.columns)[:40]}. Continuo sin completado auxiliar.")
        return {}
    
    log(f"    Procesando filas...")
    out = {}
    for _, r in df.iterrows():
        sc = normalize_stylecolor(str(r.get(AUX_COL_STYLE, "") or "").strip().upper())
        if not sc:
            continue
        name = str(r.get(AUX_COL_NAME, "") or "").strip()
        grupo = str(r.get(AUX_COL_GRUPO, "") or "").strip()
        bu = _map_grupo_to_bu(grupo)
        anio = str(r.get(AUX_COL_ANIO, "") or "").strip()
        out[sc] = {"SB_MarketingName": name, "SB_Division": bu, "Aux_AO": anio}
    log(f" ✓ AUX map cargado: {len(out)} items (sheet='{sheet}')")
    return out

def try_close_overlays(page):
    candidates = [
        "button:has-text('Aceptar')",
        "button:has-text('ACEPTAR')",
        "button:has-text('Entendido')",
        "button:has-text('OK')",
        "button:has-text('Cerrar')",
        "button[aria-label='Cerrar']",
        "[aria-label='close']",
        ".modal-close",
        ".close",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible():
                loc.click(timeout=1200)
                human_pause(0.2, 0.5)
        except Exception:
            pass

def try_set_postal_code(page, cp: str):
    try:
        human_pause(0.8, 1.4)
        try_close_overlays(page)
        
        possible_inputs = page.locator("input").all()
        for inp in possible_inputs[:30]:
            try:
                ph = (inp.get_attribute("placeholder") or "").lower()
            except Exception:
                ph = ""
            if any(k in ph for k in ["cdigo postal", "codigo postal", "cp"]):
                inp.click(timeout=1500)
                human_pause(0.2, 0.5)
                inp.fill(cp)
                human_pause(0.2, 0.5)
                inp.press("Enter")
                human_pause(0.8, 1.2)
                return
    except Exception:
        pass

def collect_plp_links_nike(page) -> list[str]:
    base = "https://www.solodeportes.com.ar"
    items = page.evaluate(
        """
        () => {
            const out = [];
            const anchors = Array.from(document.querySelectorAll("a[href$='.html']"));
            for (const a of anchors) {
                const href = (a.getAttribute("href") || "").trim();
                if (!href) continue;
                if (href.includes("/marcas/") || href.includes("/search")) continue;
                
                const img = a.querySelector("img");
                const alt = img ? (img.getAttribute("alt") || "") : "";
                
                out.push({ href, alt });
            }
            return out;
        }
        """
    )
    
    links = []
    for it in items:
        href = (it.get("href") or "").strip()
        alt_text = (it.get("alt") or "")
        if not href:
            continue
        
        is_nike = "nike" in alt_text.lower()
        if is_nike:
            links.append(urljoin(base, href))
    
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out

def _click_quiero_ver_mas(page) -> bool:
    try:
        # Múltiples selectores para encontrar el botón "Quiero ver más"
        selectors = [
            ("button.more", re.compile(r"quiero\s+ver\s+m[a]s", re.I)),
            ("button.btn.more", re.compile(r"quiero\s+ver\s*m[a]s", re.I)),
            ("button", re.compile(r"quiero\s+ver\s+m[a]s", re.I)),
            ("div[role='button']", re.compile(r"quiero\s+ver\s+m[a]s", re.I)),
        ]
        
        btn = None
        for selector, pattern in selectors:
            try:
                btns = page.locator(selector, has_text=pattern)
                if btns.count() > 0:
                    btn = btns.first
                    break
            except Exception:
                pass
        
        if btn is None:
            # Último intento: buscar cualquier elemento que contenga el texto
            try:
                elements = page.locator("text=/quiero\\s+ver\\s+m[a]s/i")
                if elements.count() > 0:
                    btn = elements.first
            except Exception:
                pass
        
        if btn is None:
            return False
        
        try_close_overlays(page)
        
        # Scroll del botón a la vista
        try:
            btn.scroll_into_view_if_needed(timeout=2000)
            human_pause(0.4, 0.8)
        except Exception:
            pass
        
        # Si no está visible, hacer scroll manual
        if not btn.is_visible(timeout=1000):
            try:
                page.mouse.wheel(0, 1800)
                human_pause(0.4, 0.9)
                btn.scroll_into_view_if_needed(timeout=2000)
                human_pause(0.3, 0.6)
            except Exception:
                pass
        
        if not btn.is_visible(timeout=1000):
            return False
        
        try:
            if btn.is_disabled(timeout=1000):
                return False
        except Exception:
            pass
        
        # Click con retry
        try:
            btn.click(timeout=4000, force=True)
            human_pause(0.3, 0.6)
            return True
        except Exception as e:
            # Intentar click con JavaScript como fallback
            try:
                page.evaluate("arguments[0].click();", btn.element_handle())
                human_pause(0.3, 0.6)
                return True
            except Exception:
                return False
    except Exception as e:
        return False

def collect_pdp_links_from_plp_no_loadmore(page, plp_url: str):
    """Recolecta todos los PDPs scrolleando automáticamente (infinite scroll)"""
    log(f"\n Abriendo PLP: {plp_url}")
    log(f"    Esperando carga de página...")
    # Usar domcontentloaded (más rápido) en lugar de networkidle (muy lento)
    page.goto(plp_url, wait_until="domcontentloaded", timeout=PLP_WAIT_MS)
    log(f"    ✓ Página cargada")
    human_pause(0.8, 1.4)
    try_close_overlays(page)
    try_set_postal_code(page, CP_AMBA)
    
    all_links = []
    seen = set()
    stagnation = 0
    prev_total_unique = 0
    MAX_PRODUCTS_PER_PLP = 500  # Corte máximo por PLP
    
    log(f"    Iniciando recolección con infinite scroll (máx {MAX_PRODUCTS_PER_PLP} productos)...")
    
    for r in range(1, MAX_PLP_SCROLL_ROUNDS + 1):
        # Scroll agresivo
        try:
            for _ in range(2):
                page.mouse.wheel(0, 2500)  # 2 scrolls de 2500
                human_pause(0.15, 0.3)
        except Exception as e:
            log(f"    Advertencia: error en scroll {e}")
        
        try_close_overlays(page)
        
        # Recolectar links actuales
        links_now = collect_plp_links_nike(page)
        new_added = 0
        for u in links_now:
            if u not in seen:
                seen.add(u)
                all_links.append(u)
                new_added += 1
        
        total_unique = len(seen)
        
        # Detectar estancamiento
        if total_unique <= prev_total_unique and new_added == 0:
            stagnation += 1
        else:
            stagnation = 0
        
        prev_total_unique = total_unique
        
        log(f"    [Scroll {r}] Total: {total_unique} | Nuevos: {new_added} | Sin cambios: {stagnation}/{PLP_STAGNATION_ROUNDS}")
        
        # Corte por alcanzar máximo
        if total_unique >= MAX_PRODUCTS_PER_PLP:
            log(f"    ✓ Límite de {MAX_PRODUCTS_PER_PLP} productos alcanzado en esta PLP")
            break
        
        if stagnation >= PLP_STAGNATION_ROUNDS:
            log(f"    ✓ Fin: estancado después de {stagnation} scrolls sin nuevos productos")
            break
        
        # Pequeña pausa entre scrolls para que carguen asincronamente
        human_pause(0.4, 0.8)
    
    log(f"    ✓ FIN PLP: {len(all_links)} productos totales recolectados")
    return all_links

def extract_price_final_by_label(page) -> float:
    """Extrae precio final de SoloDeportes"""
    try:
        # Selectores para SoloDeportes
        selectors = [
            "span.value[data-js-marketing-price]",
            "span.sales span.value",
            ".vtex-product-price-1-x-sellingPrice",
            ".vtex-store-components-3-x-price"
        ]
        
        for sel in selectors:
            loc = page.locator(sel).first
            if loc.count() > 0:
                content = (loc.get_attribute("content") or "").strip()
                if content:
                    v = parse_money_ar_to_float(content)
                    if v > 0:
                        return float(v)
                txt = (loc.inner_text() or "").strip()
                v = parse_money_ar_to_float(txt)
                if v > 0:
                    return float(v)
    except Exception:
        pass
    return 0.0

def new_browser_context(pw):
    last_err = None
    for attempt in range(1, 4):
        try:
            browser = pw.chromium.launch(
                headless=HEADLESS,
                timeout=300_000,
                args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
            )
            context = browser.new_context(
                viewport={"width": 1400, "height": 900},
                locale="es-AR",
                timezone_id="America/Argentina/Buenos_Aires",
            )
            page = context.new_page()
            return browser, context, page
        except Exception as e:
            last_err = e
            time.sleep(4 * attempt)
    raise last_err

def extract_price_from_jsonld(page) -> float:
    try:
        scripts = page.locator("script[type='application/ld+json']").all()
        for s in scripts[:12]:
            raw = s.inner_text().strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            
            def walk(obj):
                if isinstance(obj, dict):
                    if "offers" in obj:
                        off = obj["offers"]
                        if isinstance(off, dict) and off.get("price") is not None:
                            return parse_money_ar_to_float(str(off.get("price")))
                        if isinstance(off, list):
                            for o in off:
                                if isinstance(o, dict) and o.get("price") is not None:
                                    return parse_money_ar_to_float(str(o.get("price")))
                    if obj.get("price") is not None:
                        return parse_money_ar_to_float(str(obj.get("price")))
                    for v in obj.values():
                        got = walk(v)
                        if got:
                            return got
                elif isinstance(obj, list):
                    for it in obj:
                        got = walk(it)
                        if got:
                            return got
                return 0.0
            
            price = walk(data)
            if price and price > 0:
                return float(price)
    except Exception:
        pass
    return 0.0

def extract_price_from_meta(page) -> float:
    candidates = [
        "meta[property='product:price:amount']",
        "meta[property='og:price:amount']",
        "meta[name='price']",
        "meta[itemprop='price']",
    ]
    for sel in candidates:
        try:
            m = page.locator(sel).first
            if m.count() > 0:
                content = m.get_attribute("content") or ""
                p = parse_money_ar_to_float(content)
                if p > 0:
                    return float(p)
        except Exception:
            continue
    return 0.0

def extract_full_price_from_strike(page) -> float:
    """Extrae precio original (tachado) de SoloDeportes"""
    try:
        selectors = [
            "del span.value",
            ".old-price .price",
            ".vtex-product-price-1-x-listPrice"
        ]
        
        for sel in selectors:
            loc = page.locator(sel).first
            if loc.count() > 0:
                content = (loc.get_attribute("content") or "").strip()
                if content:
                    v = parse_money_ar_to_float(content)
                    if v > 0:
                        return float(v)
                txt = (loc.inner_text() or "").strip()
                v = parse_money_ar_to_float(txt)
                if v > 0:
                    return float(v)
    except Exception:
        pass
    return 0.0

def extract_max_cuotas_habituales(page) -> int:
    """Extrae el máximo de cuotas sin interés de la PDP de SoloDeportes.

    Estrategia multi-capa:
    1) Lee via JS el contenido completo de div.installments (incluyendo ::before
       pseudo-elements que inner_text() no captura — ej: "6 cuotas")
    2) Busca en div.installments p con inner_text() (fallback DOM)
    3) Busca "Cuotas habituales" heading y sube al contenedor padre
    4) Escanea todo el texto visible de la página (último recurso)
    """
    CUOTAS_RE = re.compile(r"(\d+)\s*cuotas?\s*sin\s*inter[eé]s", re.I)

    nums = []

    # ── Estrategia 1: JS evaluate sobre div.installments ──────────────────────
    # Reads textContent (includes ::before rendered text in Chromium) and also
    # walks child nodes to catch split "6 cuotas" + "sin interés" patterns.
    try:
        page.wait_for_selector("div.installments", timeout=3000)
        js_result = page.evaluate("""
            () => {
                const containers = document.querySelectorAll(
                    'div.installments, .installments-pdp, .installments'
                );
                const texts = [];
                containers.forEach(el => {
                    // textContent includes ::before pseudo-elements rendered text in Chromium
                    texts.push(el.textContent || "");
                    // Also walk direct children to catch split text nodes
                    el.childNodes.forEach(n => {
                        if (n.textContent) texts.push(n.textContent);
                    });
                });
                return texts.join(" ");
            }
        """)
        if js_result:
            found = CUOTAS_RE.findall(js_result)
            nums += [int(x) for x in found]
    except Exception:
        pass

    # ── Estrategia 2: inner_text() de párrafos dentro de div.installments ─────
    try:
        ps = page.locator("div.installments p, .installments-pdp p")
        cnt = ps.count()
        for i in range(min(cnt, 20)):
            t = (ps.nth(i).inner_text() or "").strip()
            if t:
                nums += [int(x) for x in CUOTAS_RE.findall(t)]
    except Exception:
        pass

    if nums:
        return max(nums)

    # ── Estrategia 3: buscar "Cuotas habituales" heading ──────────────────────
    try:
        heading = page.locator("text=/Cuotas habituales/i").first
        if heading.count() > 0:
            # Subir hasta el contenedor padre que contenga el texto completo
            for xpath in [
                "xpath=ancestor::*[self::div or self::section][1]",
                "xpath=ancestor::*[1]",
            ]:
                try:
                    container = heading.locator(xpath).first
                    if container.count() > 0:
                        txt = container.inner_text() or ""
                        found = CUOTAS_RE.findall(txt)
                        if found:
                            return max(int(x) for x in found)
                except Exception:
                    continue
    except Exception:
        pass

    # ── Estrategia 4: escaneo de toda la página ───────────────────────────────
    try:
        full_text = page.evaluate("() => document.body.textContent || ''")
        found = CUOTAS_RE.findall(full_text)
        if found:
            # Filtrar valores absurdos (> 24 cuotas no es real)
            valid = [int(x) for x in found if 1 <= int(x) <= 24]
            if valid:
                return max(valid)
    except Exception:
        pass

    return 0

def scrape_pdp(page, url: str, client: OpenAI = None, cache: dict = None) -> dict:
    """
    Scrapea PDP de SoloDeportes
    El StyleColor se obtiene por fallback: SKU/Cache-SKU-Lookup/Texto/OpenAI Vision
    """
    page.goto(url, wait_until="domcontentloaded", timeout=PDP_WAIT_MS)
    human_pause(0.5, 1.1)
    try_close_overlays(page)
    try_set_postal_code(page, CP_AMBA)
    
    # Obtener nombre del producto (para la descripcin)
    name = ""
    try:
        h1 = page.locator("h1").first
        if h1.count() > 0:
            name = h1.inner_text().strip()
    except Exception:
        name = ""
    if not name:
        try:
            name = page.title()
        except Exception:
            name = ""
    
    # Obtener descripcin del producto
    description = ""
    try:
        desc_selectors = [
            ".product-description",
            ".vtex-store-components-3-x-productDescription",
            "[itemprop='description']"
        ]
        for sel in desc_selectors:
            desc = page.locator(sel).first
            if desc.count() > 0:
                description = desc.inner_text().strip()[:200]
                break
    except Exception:
        pass
    
    # Intento 1: SKU -> StyleColor (sin imagen)
    stylecolor = None
    style_method = "NONE"
    openai_error = None
    candidate_sku = ""

    try:
        candidate_sku = extract_candidate_sku_from_page(page)
        if candidate_sku:
            log(f"       SKU candidato: {candidate_sku}")
            stylecolor = extract_stylecolor_from_solo_sku(candidate_sku)
            if stylecolor:
                style_method = "SKU"
                log(f"       StyleColor desde SKU: {stylecolor}")
    except Exception:
        candidate_sku = ""

    # Intento 1.5: Buscar SKU en cache (cross-check para reutilizar StyleColor ya procesado)
    if not stylecolor and candidate_sku and cache:
        try:
            for cached_url, cached_data in cache.items():
                if not isinstance(cached_data, dict):
                    continue
                cached_sku = cached_data.get("Retailer_SKU", "")
                if cached_sku == candidate_sku:
                    cached_style = cached_data.get("Retailer_StyleColor")
                    if cached_style:
                        stylecolor = cached_style
                        style_method = "CACHE_SKU_MATCH"
                        log(f"       StyleColor desde cache (SKU match): {stylecolor}")
                        break
        except Exception as e:
            log(f"       Error buscando SKU en cache: {e}")

    # Intento 2: OpenAI texto-only (SKU + descripción)
    if client and not stylecolor and candidate_sku:
        stylecolor, openai_error = ask_stylecolor_from_text_safe(
            client,
            sku=candidate_sku,
            name=name,
            description=description,
        )
        if stylecolor:
            style_method = "TEXT_ONLY"
        time.sleep(0.5)
    
    # Intento 3: OpenAI Vision (fallback)
    
    if client and not stylecolor:
        try:
            # Buscar rea del producto para screenshot
            product_area = page.locator(".product-media, .product-image, .product-info-main, .product.media").first
            if product_area.count() > 0:
                png = product_area.screenshot()
                print(f"       Screenshot de rea producto")
            else:
                png = page.screenshot(full_page=False)
                print(f"       Screenshot completo")
            
            # Descripcin para el prompt
            page_description = f"Nombre del producto: {name}\n"
            if description:
                page_description += f"Descripcin: {description}\n"
            if candidate_sku:
                page_description += f"SKU candidato: {candidate_sku}\n"
            
            # Consultar OpenAI
            stylecolor, openai_error = ask_stylecolor_from_image_safe(client, png, page_description)
            if stylecolor:
                style_method = "VISION"
            time.sleep(1)  # Pausa para evitar rate limiting
            
        except Exception as e:
            openai_error = str(e)
            print(f"       Error en screenshot/OpenAI: {e}")
    
    # Extraer precios
    price_final = 0.0
    full_price = 0.0
    
    for _try in range(2):
        try:
            price_final = extract_price_final_by_label(page)
            full_price = extract_full_price_from_strike(page)
            if price_final > 0:
                break
        except Exception:
            pass
        try:
            page.wait_for_timeout(350)
        except Exception:
            pass
    
    if price_final <= 0:
        price_final = extract_price_from_jsonld(page)
        if price_final <= 0:
            price_final = extract_price_from_meta(page)
    
    max_cuotas = extract_max_cuotas_habituales(page)
    
    if full_price <= 0 or (price_final > 0 and full_price < price_final):
        full_price = float(price_final)
    
    # Sanity check
    RATIO_MAX_FULL_VS_FINAL = 3.0
    if price_final and full_price and full_price > (price_final * RATIO_MAX_FULL_VS_FINAL):
        full_price = float(price_final)
    
    sale_dec = 0.0
    if full_price > 0 and price_final > 0 and full_price >= price_final:
        sale_dec = (float(full_price) - float(price_final)) / float(full_price)
        sale_dec = max(0.0, min(1.0, float(sale_dec)))
    
    # Si no se detectó StyleColor, usar valor explícito en lugar de None/vacío
    # para que el campo quede visible en el output y no se confunda con "no scrapeado"
    if not stylecolor:
        stylecolor = "STYLECOLOR NO ENCONTRADO"

    return {
        "Retailer": "solodeportes",
        "Retailer_URL": url,
        "Retailer_Name": name,
        "Retailer_Description": description,
        "Retailer_SKU": candidate_sku,  # Para cache lookup
        "Retailer_StyleColor": stylecolor,  # Viene de SKU/Cache/OpenAI o "STYLECOLOR NO ENCONTRADO"
        "Retailer_StyleColor_Method": style_method,
        "Retailer_StyleColor_Error": openai_error,
        "Retailer_FullPrice": float(full_price),
        "RetailerSaleDecimal": float(sale_dec),
        "Retailer_FinalPrice": float(price_final),
        "Retailer_MaxCuotasSinInteres": int(max_cuotas),
        "Retailer_LastUpdated": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

def _bml_label_from_prices(comp_price: float, nike_price: float) -> str:
    """BML (Beat/Meet/Lose)"""
    try:
        comp = float(comp_price)
        nike = float(nike_price)
    except Exception:
        return ""
    if comp <= 0 or nike <= 0:
        return ""
    if comp < nike * 0.98:
        return "Lose"
    if nike < comp * 0.98:
        return "Beat"
    return "Meet"

def _ship_for_price_nike(price: float) -> float:
    try:
        p = float(price)
    except Exception:
        return 0.0
    if p <= 0:
        return 0.0
    try:
        # Envío Nike: gratis si precio >= umbral (99000), se cobra si precio < umbral
        return 0.0 if p >= float(NIKE_FREE_SHIP_FROM_ARS) else float(NIKE_STD_SHIPPING_ARS)
    except Exception:
        return 0.0

def _ship_for_price_retailer(price: float) -> float:
    try:
        p = float(price)
    except Exception:
        return 0.0
    if p <= 0:
        return 0.0
    try:
        return 0.0 if p >= float(RETAILER_FREE_SHIP_FROM_ARS) else float(RETAILER_STD_SHIPPING_ARS)
    except Exception:
        return 0.0

def _nike_cuotas_for_price(nike_final_price: float) -> int:
    try:
        if not NIKE_CUOTAS_SIMPLE_MODE:
            return int(NIKE_CUOTAS_ALL)
        return int(NIKE_CUOTAS_HIGH) if float(nike_final_price) >= float(NIKE_CUOTAS_HIGH_FROM) else int(NIKE_CUOTAS_ALL)
    except Exception:
        return int(NIKE_CUOTAS_ALL)

def _gender_from_category(cat: str) -> str:
    try:
        s = str(cat or '').strip().lower()
    except Exception:
        return ''
    if any(k in s for k in ['mujer','women','dama','damas','femenino','female']):
        return 'Mujer'
    if any(k in s for k in ['hombre','men','caballero','masculino','male']):
        return 'Hombre'
    if any(k in s for k in ['nio','nino','kids','infantil','junior','nia','nina','youth']):
        return 'Kids'
    if 'unisex' in s:
        return 'Unisex'
    return ''

def build_template_df(df: pd.DataFrame) -> pd.DataFrame:
    """Construye el DataFrame de salida con el mismo formato que el script original de SoloDeportes"""
    df = df.copy()
    
    base_cols = [
        'Retailer_StyleColor','SB_ProductCode', 'SB_MarketingName', 'SB_Category','SB_MarketingName','SB_Division','SB_Franchise',
        'PLP_PrimaryCategoria','Retailer_URL',
        'Retailer_FullPrice','RetailerSaleDecimal','Retailer_FinalPrice',
        'NikeFullPrice','NikeSaleDecimal','NikeFinalPrice', 'FechaCorrida',
        'Retailer_MaxCuotasSinInteres','Retailer_LastUpdated','SB_SSN_VTA','Aux_AO','InStatusBooks'
    ]
    for c in base_cols:
        if c not in df.columns:
            df[c] = pd.NA
    
    df['StyleColor'] = df['Retailer_StyleColor'].astype(str).fillna('').str.strip()
    df['ProductCode'] = df['Retailer_SKU'].astype(str).fillna('').str.strip()
    df['Marketing Name'] = df['SB_MarketingName'].astype(str).fillna('').str.strip()
    df['Category'] = df['SB_Category'].astype(str).fillna('').str.strip()
    df['Division'] = df['SB_Division'].astype(str).fillna('').str.strip()
    df['Franchise'] = df['SB_Franchise'].astype(str).fillna('').str.strip()
    df['Gender'] = df['PLP_PrimaryCategoria'].apply(_gender_from_category)
    df['Link PDP Competitor'] = df['Retailer_URL'].astype(str).fillna('').str.strip()
    
    df['Competitor Full Price'] = pd.to_numeric(df['Retailer_FullPrice'], errors='coerce')
    df['Competitor Markdown'] = pd.to_numeric(df['RetailerSaleDecimal'], errors='coerce')
    df['Competitor Final Price'] = pd.to_numeric(df['Retailer_FinalPrice'], errors='coerce')
    
    _m_full_missing = df['Competitor Full Price'].isna() | (df['Competitor Full Price'] <= 0)
    if _m_full_missing.any():
        df.loc[_m_full_missing, 'Competitor Full Price'] = df.loc[_m_full_missing, 'Competitor Final Price']
        df.loc[_m_full_missing, 'Competitor Markdown'] = 0.0
    
    df['Nike Full Price'] = pd.to_numeric(df['NikeFullPrice'], errors='coerce')
    df['Nike Markdown'] = pd.to_numeric(df['NikeSaleDecimal'], errors='coerce')
    df['Nike Final Price'] = pd.to_numeric(df['NikeFinalPrice'], errors='coerce')
    
    # BML para Full Price
    df['BML Full Price'] = df.apply(lambda r: _bml_label_from_prices(r.get('Competitor Full Price'), r.get('Nike Full Price')), axis=1)
    
    # BML para Final Price
    df['BML Final Price'] = df.apply(lambda r: _bml_label_from_prices(r.get('Competitor Final Price'), r.get('Nike Final Price')), axis=1)
    
    # Shipping
    df['Competitor Shipping'] = df['Competitor Final Price'].apply(lambda x: _ship_for_price_retailer(float(x)) if pd.notna(x) else 0.0)
    df['Nike Shipping'] = df['Nike Final Price'].apply(lambda x: _ship_for_price_nike(float(x)) if (pd.notna(x) and float(x) > 0) else 0.0)
    df['Nike Price + Shipping'] = df['Nike Final Price'] + df['Nike Shipping']
    df['Competitor Price + Shipping'] = df['Competitor Final Price'] + df['Competitor Shipping']
    
    df['BML with Shipping'] = df.apply(lambda r: _bml_label_from_prices(r.get('Competitor Price + Shipping'), r.get('Nike Price + Shipping')), axis=1)
    
    # Cuotas
    df['Cuotas Competitor'] = pd.to_numeric(df['Retailer_MaxCuotasSinInteres'], errors='coerce').fillna(0).astype(int)
    
    df['Cuotas Nike'] = df.apply(lambda r: _nike_cuotas_for_price(float(r.get('Nike Final Price'))) if (r.get('InStatusBooks') is True and pd.notna(r.get('Nike Final Price'))) else pd.NA, axis=1)
    
    def _bml_cuotas_row(row):
        v = row.get('Cuotas Competitor')
        nk = row.get('Cuotas Nike')
        if row.get('InStatusBooks') is not True:
            return ''
        if pd.isna(nk):
            return ''
        try:
            v = int(float(v)) if pd.notna(v) else 0
        except Exception:
            v = 0
        try:
            nk = int(float(nk)) if pd.notna(nk) else 0
        except Exception:
            nk = 0
        if v == nk:
            return 'Meet'
        return 'Beat' if nk > v else 'Lose'
    df['BML Cuotas'] = df.apply(_bml_cuotas_row, axis=1)
    
    # Season
    def _season_row(row):
        def _safe_cell(x):
            if x is None or pd.isna(x):
                return ""
            return str(x).strip()

        if row.get('InStatusBooks') is True:
            v = _safe_cell(row.get('SB_SSN_VTA'))
            return v if v else 'no registrado'
        v2 = _safe_cell(row.get('Aux_AO'))
        return v2 if v2 else 'no registrado'
    df['Season'] = df.apply(_season_row, axis=1)
    df['Fecha Corrida'] = RUN_DATE
    
    df['NDDC'] = df.get('InStatusBooks').apply(lambda x: 'NDDC' if x is True else '')
    
    df['Competitor'] = 'solodeportes'
    df['Last Update Competitor'] = df['Retailer_LastUpdated'].astype(str).fillna('').str.strip()
    
    # Columnas finales (mismo orden que el script original)
    cols = [
        'StyleColor','ProductCode','Marketing Name','Category','Division','Franchise','Gender',
        'Link PDP Competitor',
        'Competitor Full Price','Competitor Markdown','Competitor Final Price',
        'Nike Full Price','Nike Markdown','Nike Final Price',
        'BML Final Price','BML Full Price',
        'Competitor Shipping','Nike Shipping','Nike Price + Shipping','Competitor Price + Shipping',
        'BML with Shipping',
        'Cuotas Competitor','Cuotas Nike','BML Cuotas',
        'Competitor','Season','Fecha Corrida','Last Update Competitor','NDDC'
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ''
    out = df[cols].copy()
    
    # Convertir columnas numricas
    num_cols = [
        'Competitor Full Price','Competitor Markdown','Competitor Final Price',
        'Nike Full Price','Nike Markdown','Nike Final Price',
        'Competitor Shipping','Nike Shipping','Nike Price + Shipping','Competitor Price + Shipping',
        'Cuotas Competitor','Cuotas Nike'
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors='coerce')
    return out

def write_visual_xlsx(df: pd.DataFrame, path: str):
    """Escribe XLSX con formato visual"""
    df = df.copy()
    with pd.ExcelWriter(path, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Output', index=False)
        wb = writer.book
        ws = writer.sheets['Output']
        
        header_fmt = wb.add_format({'bold': True, 'font_color': 'white', 'bg_color': '#111827', 'border': 1})
        money_fmt = wb.add_format({'num_format': '$#,##0', 'border': 1})
        pct_fmt = wb.add_format({'num_format': '0.0%', 'border': 1})
        text_fmt = wb.add_format({'border': 1})
        link_fmt = wb.add_format({'font_color': 'blue', 'underline': 1, 'border': 1})
        beat_fmt = wb.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100', 'border': 1})
        meet_fmt = wb.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500', 'border': 1})
        lose_fmt = wb.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006', 'border': 1})
        
        for col_i, name in enumerate(df.columns):
            ws.write(0, col_i, name, header_fmt)
        
        col_idx = {c: i for i, c in enumerate(df.columns)}
        nrows = len(df)
        
        if 'Marketing Name' in col_idx:
            ws.set_column(col_idx['Marketing Name'], col_idx['Marketing Name'], 35, text_fmt)
        if 'Fecha Corrida' in col_idx:
            ws.set_column(col_idx['Fecha Corrida'], col_idx['Fecha Corrida'], 14, text_fmt)
        
        def set_col_format(colname, fmt):
            if colname not in col_idx:
                return
            i = col_idx[colname]
            ws.set_column(i, i, 18, fmt)
        
        for c in ['Competitor Full Price','Competitor Final Price','Nike Full Price','Nike Final Price',
                  'Competitor Shipping','Nike Shipping','Nike Price + Shipping','Competitor Price + Shipping']:
            set_col_format(c, money_fmt)
        
        for c in ['Competitor Markdown','Nike Markdown']:
            set_col_format(c, pct_fmt)
        
        if 'Link PDP Competitor' in col_idx:
            i = col_idx['Link PDP Competitor']
            ws.set_column(i, i, 55, link_fmt)
        
        for c in ['BML Final Price','BML Full Price','BML with Shipping','BML Cuotas']:
            if c not in col_idx:
                continue
            i = col_idx[c]
            ws.set_column(i, i, 18, text_fmt)
            ws.conditional_format(1, i, nrows, i, {'type': 'text', 'criteria': 'containing', 'value': 'Beat', 'format': beat_fmt})
            ws.conditional_format(1, i, nrows, i, {'type': 'text', 'criteria': 'containing', 'value': 'Meet', 'format': meet_fmt})
            ws.conditional_format(1, i, nrows, i, {'type': 'text', 'criteria': 'containing', 'value': 'Lose', 'format': lose_fmt})
        
        ws.freeze_panes(1, 0)

def write_fast_xlsx(df: pd.DataFrame, path: str):
    """Escritura XLSX rpida (sin formato pesado)"""
    df = df.copy()
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="DATA")

def _visual_xlsx_worker(df: pd.DataFrame, tmp_path: str):
    try:
        write_visual_xlsx(df, tmp_path)
    except Exception:
        pass

def safe_write_excel_with_timeout(df: pd.DataFrame, final_path: str, timeout_sec: int = 360):
    """Siempre produce XLSX. Intenta visual con timeout; si falla, deja FAST."""
    write_fast_xlsx(df, final_path)
    log(f" XLSX (FAST) written -> {final_path}")
    
    tmp_visual = final_path.replace(".xlsx", "__VISUAL_TMP__.xlsx")
    log(f" Intentando XLSX VISUAL (timeout={timeout_sec}s) -> {tmp_visual}")
    
    try:
        import multiprocessing as mp
        ctx = mp.get_context("spawn")
        p = ctx.Process(target=_visual_xlsx_worker, args=(df, tmp_visual), daemon=True)
        p.start()
        p.join(timeout_sec)
        
        if p.is_alive():
            try:
                p.terminate()
            except Exception:
                pass
            log(f" XLSX VISUAL timeout. Se mantiene FAST.")
            return
        
        if os.path.exists(tmp_visual) and os.path.getsize(tmp_visual) > 0:
            try:
                os.replace(tmp_visual, final_path)
                log(" XLSX VISUAL OK (reemplaz FAST).")
            except Exception as e:
                log(f" No pude reemplazar: {e}")
        else:
            log(" XLSX VISUAL no se gener correctamente.")
    finally:
        try:
            if os.path.exists(tmp_visual):
                os.remove(tmp_visual)
        except Exception:
            pass

# =========================
# MAIN
# =========================

def main():
    log("=" * 70)
    log(" SOLODEPORTES vs NIKE - Adaptado de StockCenter v4")
    log(f" Season: {SEASON}")
    log(f" Workers paralelos: {AGENTS}")
    log("=" * 70)
    log(f" Headless={HEADLESS} | REFRESH_CACHED={REFRESH_CACHED}")
    if DEBUG_LIMIT > 0:
        log(f" DEBUG_LIMIT activo: limitando a {DEBUG_LIMIT} productos")
    if DEBUG_OFFSET > 0:
        log(f" DEBUG_OFFSET activo: salteando {DEBUG_OFFSET} productos")
    log(f" StatusBooks: {STATUSBOOKS_FILE}")
    log(f" CACHE: {CACHE_PATH}")
    log(f" Export RAW: {OUT_RAW_CSV}")
    log(f" Export XLSX: {OUT_VISUAL_XLSX}")
    
    # Inicializar OpenAI
    client = None
    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            log(" OpenAI inicializado correctamente")
            # Test rpido
            test_response = client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": "Test"}],
                max_tokens=5
            )
            log(" Conexin OpenAI verificada")
        except Exception as e:
            log(f" Error inicializando OpenAI: {e}")
            client = None
    else:
        log(" OpenAI API key no configurada")
    
    # Cargar cache
    cache = load_cache(CACHE_PATH)
    if not isinstance(cache, dict):
        cache = {}
    log(f" Cache cargado: {len(cache)} stylecolors")
    
    # Cargar PLPs
    plps_df = load_plps_from_links_excel(LINKS_XLSX, LINKS_SHEET)
    log(f" PLPs cargados: {len(plps_df)}")
    
    # Cargar StatusBooks y aux
    sb_map = load_statusbooks_map(STATUSBOOKS_FILE, STATUSBOOKS_SHEET)
    aux_map = load_aux_assort_map(AUX_ASSORT_FILE, AUX_ASSORT_SHEET)
    
    log("\n Stage 1: recolectando PDPs desde PLPs (Nike only)...")
    style_meta = {}
    
    with sync_playwright() as pw:
        browser, context, _ = new_browser_context(pw)
        target_collect = (DEBUG_LIMIT + DEBUG_OFFSET) if DEBUG_LIMIT > 0 else 0
        
        for _, r in plps_df.iterrows():
            cat = str(r[LINKS_COL_CATEGORY]).strip()
            plp = str(r[LINKS_COL_LINK]).strip()
            
            # Crear una página NUEVA para cada PLP para evitar conflictos
            page = context.new_page()
            try:
                links = collect_pdp_links_from_plp_no_loadmore(page, plp)
            except PWTimeoutError:
                log(f" Timeout en PLP: {plp} | Salteo.")
                links = []
            except Exception as e:
                log(f" Error en PLP: {plp} | {e} | Salteo.")
                links = []
            finally:
                # Cerrar página para liberar recursos
                try:
                    page.close()
                except Exception:
                    pass
            
            log(f" PDPs Nike en '{cat}': {len(links)}")
            for u in links:
                # NO extraemos StyleColor de la URL
                # Solo guardamos la URL y categora
                if u not in style_meta:
                    style_meta[u] = {
                        "url": u,
                        "categorias": set([cat]),
                        "primary_cat": cat,
                        "source_plp": plp,
                    }
                else:
                    style_meta[u]["categorias"].add(cat)
                
                # DEBUG_LIMIT: romper si alcanzamos el lmite
                if target_collect > 0 and len(style_meta) >= target_collect:
                    log(f" DEBUG_LIMIT+OFFSET alcanzado ({target_collect}). Corto recoleccin de PLPs.")
                    break
            
            # Si alcanzamos el lmite, salir del loop de PLPs
            if target_collect > 0 and len(style_meta) >= target_collect:
                break
        
        try:
            page.close()
        except Exception:
            pass
        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass
    
    all_urls = list(style_meta.keys())
    log(f"\n Total PDPs nicas detectadas: {len(all_urls)}")
    
    log("\n Stage 2: scraping PDPs + OpenAI para StyleColor...")
    
    urls_ordered = list(all_urls)
    if DEBUG_OFFSET > 0:
        urls_ordered = urls_ordered[DEBUG_OFFSET:]
    if DEBUG_LIMIT > 0:
        urls_ordered = urls_ordered[:DEBUG_LIMIT]
    total_target = len(urls_ordered)
    url_to_idx = {u: i for i, u in enumerate(urls_ordered, start=1)}
    log(f" URLs a procesar en Stage 2: {total_target}")
    
    cache_lock = threading.Lock()
    
    def _process_urls_worker(worker_id: int, urls_chunk: list) -> dict:
        local_processed = 0
        local_scraped_new = 0
        local_skipped_safe = 0
        local_skipped_not_in_sb = 0
        local_errors = 0
        
        with sync_playwright() as pw:
            browser, context, page = new_browser_context(pw)
            
            for url in urls_chunk:
                idx = url_to_idx.get(url, 0)
                meta = style_meta[url]
                
                with cache_lock:
                    # Usar la URL como clave en cache
                    if url not in cache:
                        cache[url] = {}
                    
                    cache[url]["PLP_Categorias"] = " | ".join(sorted(list(meta["categorias"])))
                    cache[url]["PLP_PrimaryCategoria"] = meta["primary_cat"]
                    cache[url]["PLP_SourcePLP"] = meta["source_plp"]
                    cache[url]["Retailer_URL"] = url
                    
                    # Verificar si ya tenemos StyleColor en cache
                    # Si StyleColor es None/null/vacío, siempre re-scrapar para re-intentar OpenAI
                    cached_style = cache[url].get("Retailer_StyleColor")
                    already_has_style = bool(cached_style)  # False si es None, "", o ausente
                    already_has_valid_price = float(cache[url].get("Retailer_FinalPrice", 0.0)) > VALID_PRICE_MIN
                
                # Si StyleColor está en cache como null/vacío, forzamos re-scrape aunque REFRESH_CACHED=0
                style_is_null_in_cache = not already_has_style
                
                if (not REFRESH_CACHED) and already_has_style and already_has_valid_price and not style_is_null_in_cache:
                    with cache_lock:
                        local_skipped_safe += 1
                        atomic_write_json(CACHE_PATH, cache)
                    log(f"[{idx}/{total_target}]  (W{worker_id}) SAFE-SKIP {url} | ya cacheado con StyleColor")
                    local_processed += 1
                else:
                    log(f"[{idx}/{total_target}]  (W{worker_id}) SCRAPE {url}")
                    try:
                        row = None
                        for _attempt in range(2):
                            try:
                                row = scrape_pdp(page, url, client, cache)
                                break
                            except PWTimeoutError as _te:
                                if _attempt == 0:
                                    browser, context, page = _reset_triplet(worker_id, pw, browser, context, page, reason=f"Timeout -> reset")
                                    continue
                                raise
                            except Exception as _e:
                                if _attempt == 0 and _is_fatal_nav_error(_e):
                                    browser, context, page = _reset_triplet(worker_id, pw, browser, context, page, reason=f"{_e}")
                                    continue
                                raise
                        
                        with cache_lock:
                            for k, v in row.items():
                                cache[url][k] = v
                            
                            # Obtener StyleColor de OpenAI (ya viene en row)
                            stylecolor = row.get("Retailer_StyleColor")
                            
                            # "STYLECOLOR NO ENCONTRADO" es un valor centinela, no un código real
                            stylecolor_real = stylecolor and stylecolor != "STYLECOLOR NO ENCONTRADO"

                            # Buscar en StatusBooks (solo si tenemos StyleColor real)
                            if stylecolor_real:
                                sb = sb_map.get(stylecolor) or sb_map.get("NI" + stylecolor)
                                if sb:
                                    cache[url]["InStatusBooks"] = True
                                    for k, v in sb.items():
                                        cache[url][k] = v
                                else:
                                    cache[url]["InStatusBooks"] = False
                                    # Completar con aux
                                    aux = aux_map.get(stylecolor) if isinstance(aux_map, dict) else None
                                    cache[url]["SB_ProductCode"] = stylecolor
                                    cache[url]["SB_MarketingName"] = (aux.get("SB_MarketingName") if aux else "") or ""
                                    cache[url]["SB_Division"] = (aux.get("SB_Division") if aux else "") or ""
                                    cache[url]["Aux_AO"] = (aux.get("Aux_AO") if aux else "") or ""
                                    cache[url]["SB_Category"] = ""
                                    cache[url]["SB_Franchise"] = ""
                                    cache[url]["NikeFullPrice"] = None
                                    cache[url]["NikeSaleDecimal"] = None
                                    cache[url]["NikeFinalPrice"] = None
                            else:
                                # Sin StyleColor real: no hay match posible
                                cache[url]["InStatusBooks"] = False
                                cache[url]["SB_ProductCode"] = ""
                                cache[url]["SB_MarketingName"] = ""
                                cache[url]["SB_Division"] = ""
                                cache[url]["SB_Category"] = ""
                                cache[url]["SB_Franchise"] = ""
                                cache[url]["NikeFullPrice"] = None
                                cache[url]["NikeSaleDecimal"] = None
                                cache[url]["NikeFinalPrice"] = None
                            
                            atomic_write_json(CACHE_PATH, cache)
                        
                        local_scraped_new += 1
                        local_processed += 1
                        log(f"    StyleColor: {stylecolor or 'NO DETECTADO'} | FinalPrice={row.get('Retailer_FinalPrice', 0):,.2f}")
                        
                    except PWTimeoutError:
                        with cache_lock:
                            local_errors += 1
                            cache[url]["Retailer_Error"] = "Timeout"
                            cache[url]["Retailer_LastErrorAt"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            atomic_write_json(CACHE_PATH, cache)
                        log("    Timeout en PDP. Guardado error en JSON.")
                        local_processed += 1
                        
                    except Exception as e:
                        with cache_lock:
                            local_errors += 1
                            cache[url]["Retailer_Error"] = f"{type(e).__name__}: {e}"
                            cache[url]["Retailer_LastErrorAt"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            atomic_write_json(CACHE_PATH, cache)
                        log(f"    Error en PDP: {e}")
                        local_processed += 1
                
                if local_processed > 0 and (local_processed % BROWSER_RESET_EVERY == 0):
                    log(f"\n (W{worker_id}) RESET: {local_processed} items procesados\n")
                    with cache_lock:
                        try:
                            atomic_write_json(CACHE_PATH, cache)
                        except Exception as e:
                            log(f" (W{worker_id}) No pude guardar cache: {e}")
                    
                    try:
                        page.close()
                    except Exception:
                        pass
                    try:
                        context.close()
                    except Exception:
                        pass
                    try:
                        browser.close()
                    except Exception:
                        pass
                    
                    time.sleep(RESET_SLEEP_SECONDS)
                    browser, context, page = new_browser_context(pw)
            
            try:
                context.close()
            except Exception:
                pass
            try:
                browser.close()
            except Exception:
                pass
        
        return {
            "processed": local_processed,
            "scraped_new": local_scraped_new,
            "skipped_safe": local_skipped_safe,
            "skipped_not_in_sb": local_skipped_not_in_sb,
            "errors": local_errors,
        }
    
    # Procesar con AGENTS workers
    proc_total = 0
    scraped_total = 0
    skipped_safe_total = 0
    skipped_not_in_sb_total = 0
    errors_total = 0
    
    if AGENTS <= 1:
        log(" Modo secuencial")
        stats = _process_urls_worker(worker_id=1, urls_chunk=urls_ordered)
        proc_total = stats["processed"]
        scraped_total = stats["scraped_new"]
        skipped_safe_total = stats["skipped_safe"]
        skipped_not_in_sb_total = stats["skipped_not_in_sb"]
        errors_total = stats["errors"]
    else:
        log(f" Modo paralelo: AGENTS={AGENTS}")
        chunks = [urls_ordered[i::AGENTS] for i in range(AGENTS)]
        
        with ThreadPoolExecutor(max_workers=AGENTS) as ex:
            futs = [ex.submit(_process_urls_worker, wi + 1, ch) for wi, ch in enumerate(chunks) if ch]
            for fut in as_completed(futs):
                st = fut.result()
                proc_total += st["processed"]
                scraped_total += st["scraped_new"]
                skipped_safe_total += st["skipped_safe"]
                skipped_not_in_sb_total += st["skipped_not_in_sb"]
                errors_total += st["errors"]
    
    log("\n Stage 3: exportando reporte...")
    cache = load_cache(CACHE_PATH)
    
    rows = []
    for url, rec in cache.items():
        if not isinstance(rec, dict):
            continue
        if not rec.get("Retailer_URL"):
            continue
        rows.append(rec)
    
    df = pd.DataFrame(rows)
    
    if not df.empty:
        df_out = build_template_df(df)
        df_out = df_out.sort_values(by=["StyleColor"], ascending=True).reset_index(drop=True)
    else:
        df_out = build_template_df(pd.DataFrame())
    
    # Filtrar solo actualizados hoy
    if not df_out.empty and "Last Update Competitor" in df_out.columns:
        _lu = df_out["Last Update Competitor"].astype(str).fillna("").str.strip()
        _lu_date = _lu.str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
        _lu_date = _lu_date.fillna(_lu.str.slice(0, 10))
        _before = len(df_out)
        df_out = df_out[_lu_date == RUN_DATE].copy()
        log(f" Filtro 'solo actualizados hoy': {_before} -> {len(df_out)} filas")
    
    df_out.to_csv(OUT_RAW_CSV, index=False, encoding="utf-8-sig")
    log(f" RAW CSV: {OUT_RAW_CSV}")
    
    safe_write_excel_with_timeout(df_out, OUT_VISUAL_XLSX, timeout_sec=360)
    log(f" Visual XLSX: {OUT_VISUAL_XLSX}")
    
    log("\n Resumen:")
    log(f"    URLs detectadas (PLPs): {len(all_urls)}")
    log(f"    Procesadas esta corrida: {proc_total}")
    log(f"    Scrapes nuevos: {scraped_total}")
    log(f"    SAFE skips: {skipped_safe_total}")
    log(f"    Errores: {errors_total}")
    log("\n DONE")


if __name__ == "__main__":
    main()
