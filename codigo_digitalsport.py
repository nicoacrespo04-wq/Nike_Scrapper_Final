# -*- coding: utf-8 -*-
"""
DIGITALSPORT vs NIKE - v8
Basado en digitalsport_v7 + mejoras del soloDeportes_v4:
  - StyleColor null en cache → re-intentar OpenAI en cada corrida
  - Full Price Nike desde columna exacta con nombre SEASON (ej: "SP26") en headers
  - Nike Shipping solo aplica si Final Price > umbral (NIKE_FREE_SHIP_FROM_ARS)
  - load_aux_assort_map con búsqueda robusta de archivo (igual que soloDeportes v4)
  - SB_SSN_VTA y Aux_AO en output (Season dinámico por producto)
  - DEBUG_LIMIT / DEBUG_OFFSET para pruebas por lotes
  - log() con fallback UTF-8 (evita cp1252 en Windows)
  - Proxy configurable
"""

import os
import re
import time
import gc
import base64
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import json
import random
import datetime as dt
from urllib.parse import urljoin

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from openai import OpenAI

# =========================
# CONFIGURACIÓN OPENAI
# =========================
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL   = "gpt-4o-mini"
OPENAI_TIMEOUT_S = 45

# =========================
# PROXY (opcional)
# =========================
PROXY_SERVER   = "http://dc.decodo.com:10001"
PROXY_USERNAME = "sp6g2od2ak"
PROXY_PASSWORD = "9hbclm71oFtP_8BgAq"

# =========================
# ARCHIVOS DE ENTRADA
# =========================
LINKS_XLSX         = "Links Retail.xlsx"
LINKS_SHEET        = "DigitalSport"
LINKS_COL_CATEGORY = "Categoria"
LINKS_COL_LINK     = "LINK"

# =========================
# PARÁMETROS DE ENVÍO
# =========================
CP_AMBA = "1425"

NIKE_STD_SHIPPING_ARS         = 8899.0
NIKE_FREE_SHIP_FROM_ARS       = 99000.0   # umbral: envío Nike solo si precio > este valor
RETAILER_STD_SHIPPING_ARS     = 12599.0
RETAILER_FREE_SHIP_FROM_ARS   = 169999.0

# =========================
# CUOTAS NIKE
# =========================
NIKE_CUOTAS_SIMPLE_MODE = True
NIKE_CUOTAS_ALL         = 3
NIKE_CUOTAS_HIGH        = 6
NIKE_CUOTAS_HIGH_FROM   = 79000.0

# =========================
# SEASON / STATUSBOOKS
# =========================
SEASON              = "SP26"
STATUSBOOKS_FILE    = "StatusBooks NDDC ARG SP26.xlsb"
STATUSBOOKS_SHEET   = "Books NDDC"

# =========================
# ARCHIVO AUXILIAR (non-NDDC)
# =========================
AUX_ASSORT_FILE  = "01 Lista actualizada a nivel GRUPO DE ARTÍCULOS total.xlsx"
AUX_ASSORT_SHEET = "Primera Calidad"
AUX_COL_STYLE    = "Material Nike"
AUX_COL_NAME     = "Nombre material"
AUX_COL_GRUPO    = "Grupo de carga"
AUX_COL_ANIO     = "Año"

# =========================
# PERFORMANCE
# =========================
HEADLESS = True
AGENTS   = int(os.getenv("AGENTS", "3"))

DEBUG_LIMIT  = 0
DEBUG_OFFSET = int(os.getenv("DEBUG_OFFSET", "0"))   # saltear N productos

# =========================
# SCROLL Y NAVEGACIÓN
# =========================
MAX_PLP_SCROLL_ROUNDS = 18
PLP_STAGNATION_ROUNDS = 4
PLP_SCROLL_PIXELS     = 1400

PDP_WAIT_MS = 25_000
PLP_WAIT_MS = 35_000

# =========================
# CACHE
# =========================
CACHE_PATH      = "digitalsport_cache.json"
REFRESH_CACHED  = os.getenv("REFRESH_CACHED", "1").strip().lower() in {"1", "true", "yes", "y"}
VALID_PRICE_MIN = 0.0

# =========================
# RESET DE BROWSER
# =========================
BROWSER_RESET_EVERY  = 90
RESET_SLEEP_SECONDS  = 8

# =========================
# TIMESTAMPS / SALIDA
# =========================
TS           = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DATE     = dt.datetime.now().strftime("%Y-%m-%d")
RUN_DATETIME = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

OUT_RAW_CSV    = f"digitalsport_vs_nike_raw_{TS}.csv"
OUT_VISUAL_XLSX = f"digitalsport_vs_nike_visual_{TS}.xlsx"


# =========================
# UTILIDADES GENERALES
# =========================

def human_pause(a=0.25, b=0.85):
    time.sleep(random.uniform(a, b))


def log(msg: str):
    import sys, io
    try:
        print(msg, flush=True)
    except UnicodeEncodeError:
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        print(msg, flush=True)


def _is_fatal_nav_error(e: Exception) -> bool:
    s = f"{type(e).__name__}: {e}".lower()
    tokens = [
        "net::err_insufficient_resources", "net::err_aborted",
        "connection closed while reading from the driver", "target closed",
        "browser has been closed", "page.goto: target closed",
        "page.goto: browser has been closed", "page.goto: navigation failed",
        "page.goto: net::",
    ]
    return any(t in s for t in tokens)


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


def _reset_triplet(worker_id, pw, browser, context, page, reason=""):
    log(f" (W{worker_id}) RESET: {reason}".strip())
    for obj in (page, context, browser):
        try:
            obj.close()
        except Exception:
            pass
    try:
        gc.collect()
    except Exception:
        pass
    time.sleep(RESET_SLEEP_SECONDS + random.uniform(1.0, 3.0))
    return new_browser_context(pw)


# =========================
# PARSERS DE DATOS
# =========================

def parse_money_ar_to_float(s) -> float:
    if s is None:
        return 0.0
    if isinstance(s, (int, float)):
        return float(s) if s == s else 0.0
    s = str(s).strip()
    if not s or s.lower() in {"nan", "none", "null", ""}:
        return 0.0
    cleaned = re.sub(r"[^\d\.,\-]", "", s)
    if not cleaned:
        return 0.0
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
        try:
            return float(cleaned)
        except Exception:
            return 0.0
    if "." in cleaned:
        parts = cleaned.split(".")
        if len(parts[-1]) == 2:
            try:
                return float("".join(parts[:-1]) + "." + parts[-1])
            except Exception:
                return 0.0
        if len(parts[-1]) == 1:
            try:
                return float("".join(parts[:-1]) + parts[-1][0] if parts[-1] == "0" else "".join(parts))
            except Exception:
                pass
            try:
                return float(parts[0].replace(".", ""))
            except Exception:
                return 0.0
        cleaned = cleaned.replace(".", "")
        try:
            return float(cleaned)
        except Exception:
            return 0.0
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
            return max(0.0, min(1.0, float(val) / 100.0))
        except Exception:
            return 0.0
    s2 = re.sub(r"[^\d\.,\-]", "", s).replace(",", ".")
    try:
        x = float(s2)
        return max(0.0, min(1.0, x / 100.0 if x > 1.0 else x))
    except Exception:
        return 0.0


def parse_stock_bl_to_float(v) -> float:
    if v is None:
        return 0.0
    s = str(v).strip()
    if s in {"", "-"} or s.lower() in {"nan", "none", "null"}:
        return 0.0
    s2 = re.sub(r"[^0-9\.\-]", "", s.replace(",", "."))
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


# =========================
# NORMALIZACIÓN STYLECOLOR
# =========================

def normalize_stylecolor(style: str) -> str:
    if not style:
        return ""
    style = str(style).strip().upper()
    if style.startswith("NI") and len(style) >= 3:
        return style[2:]
    return style


def extract_stylecolor_from_ds_sku(ds_sku: str) -> str:
    """
    DigitalSport expone data-sku como 'DM3493206' (sin guión).
    Convierte a 'DM3493-206' si matchea patrón Nike típico.
    """
    if not ds_sku:
        return ""
    s = str(ds_sku).strip().upper().replace(" ", "")
    # Patrón estándar Nike: 6 alfanum + 3 dígitos (ej DM3493 + 206)
    if len(s) == 9 and re.match(r"^[A-Z0-9]{6}[0-9]{3}$", s):
        return f"{s[:6]}-{s[6:]}"
    # Ya viene con guión
    if re.match(r"^[A-Z0-9]{5,20}-[A-Z0-9]{2,10}$", s):
        return s
    return ""


def extract_stylecolor_from_url(url: str) -> str:
    """
    Fallback: extrae StyleColor de URLs tipo .../DV0740-004.html
    DigitalSport NO siempre trae stylecolor en URL, preferimos data-sku.
    """
    m = re.search(r"/([A-Za-z0-9]{5,20}-[A-Za-z0-9]{2,10})\.html", url)
    return m.group(1).upper() if m else ""


# =========================
# OPENAI — SKU → StyleColor
# =========================

def _b64_png(png_bytes: bytes) -> str:
    return base64.b64encode(png_bytes).decode("ascii")


def _normalize_stylecolor_candidate(txt: str):
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
    """Intenta extraer StyleColor Nike embebido en SKU (ej: 510010FD6454001 → FD6454-001)."""
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
    """Busca un SKU/código candidato en HTML + texto visible de la PDP."""
    blobs = []
    try:
        blobs.append(page.content() or "")
    except Exception:
        pass
    try:
        blobs.append(page.locator("body").first.inner_text(timeout=1500) or "")
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
        mm = re.search(pat, joined)
        if mm:
            return (mm.group(1) if mm.groups() else mm.group(0)).strip()
    return ""


def ask_stylecolor_from_text_safe(client, sku: str, name: str = "", description: str = ""):
    """Consulta OpenAI por texto (SKU + descripción), sin imagen."""
    if not sku:
        return None, "Sin SKU"
    parsed = extract_stylecolor_from_solo_sku(sku)
    if parsed:
        return parsed, None
    prompt = (
        f"Necesito extraer el StyleColor de Nike SOLO con texto.\n\n"
        f"SKU retailer: {sku}\nNombre producto: {name}\nDescripción: {description}\n\n"
        "Regla: el StyleColor tiene formato como FJ2587-400, CW4554-101, DV1312-001.\n"
        "Si el SKU parece tener el style embebido, reconstruyelo con guion antes de los últimos 3 dígitos.\n\n"
        "Respondé SOLO el StyleColor en MAYÚSCULAS.\n"
        "Si no hay suficiente información, respondé NO_ENCONTRADO."
    )
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=20,
            temperature=0.0,
            timeout=OPENAI_TIMEOUT_S,
        )
        txt = (response.choices[0].message.content or "").strip().upper()
        log(f"       OpenAI text-only raw: '{txt}'")
        normalized = _normalize_stylecolor_candidate(txt)
        if normalized:
            return normalized, None
        return None, f"Sin formato válido: '{txt}'"
    except Exception as e:
        return None, f"Texto-only error: {str(e)[:100]}"


def ask_stylecolor_from_image_safe(client, png_bytes: bytes, page_description: str = ""):
    """Consulta OpenAI Vision para obtener StyleColor desde screenshot."""
    img_size_kb = len(png_bytes) / 1024
    if img_size_kb > 20 * 1024:
        return None, f"Imagen demasiado grande ({img_size_kb:.1f} KB)"
    b64 = _b64_png(png_bytes)
    prompt = (
        "En esta imagen de una página de producto de Nike en DigitalSport, "
        "necesito encontrar el código StyleColor de Nike.\n\n"
        f"{page_description}\n\n"
        "El StyleColor tiene formato como \"FJ2587-400\" o \"DV1312-001\" (letras+números con guion).\n\n"
        "BUSCA EN: nombre del producto, código de referencia, debajo del título.\n\n"
        "Respondé SOLAMENTE con el código StyleColor en MAYÚSCULAS.\n"
        "Si no encontrás ningún código con alta confianza, respondé \"NO_ENCONTRADO\"."
    )
    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": [
                {"type": "text", "text": prompt},
                {"type": "image_url", "image_url": {
                    "url": f"data:image/png;base64,{b64}", "detail": "high"
                }},
            ]}],
            max_tokens=30,
            temperature=0.0,
            timeout=OPENAI_TIMEOUT_S,
        )
        txt = (response.choices[0].message.content or "").strip().upper()
        log(f"       OpenAI vision raw: '{txt}'")
        txt = re.sub(r"[^\w\-]", "", txt)
        if txt == "NO_ENCONTRADO":
            return None, "OpenAI no detectó StyleColor"
        if re.match(r"^[A-Z0-9]{4,8}-[0-9]{3}$", txt):
            return txt, None
        if re.match(r"^[A-Z0-9]{9,11}$", txt):
            return txt[:-3] + "-" + txt[-3:], None
        mm = re.search(r"([A-Z0-9]{4,8})-?([0-9]{3})", txt)
        if mm:
            return f"{mm.group(1)}-{mm.group(2)}", None
        return None, f"Sin formato válido: '{txt}'"
    except Exception as e:
        err = str(e)
        if "401" in err:
            return None, "API key inválida"
        elif "429" in err:
            return None, "Rate limit"
        elif "quota" in err.lower():
            return None, "Quota excedido"
        elif "timeout" in err.lower():
            return None, "Timeout"
        return None, f"Error: {err[:100]}"


# =========================
# CARGA DE ARCHIVOS MAESTROS
# =========================

def load_plps_from_links_excel(path: str, sheet=None) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"No encuentro el Excel de links: {path}")
    raw = pd.read_excel(path, sheet_name=sheet)
    if isinstance(raw, dict):
        sheet_name, df = next(iter(raw.items()))
        log(f" Excel con múltiples sheets. Usando: '{sheet_name}'")
    else:
        df = raw
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    if LINKS_COL_CATEGORY not in df.columns or LINKS_COL_LINK not in df.columns:
        raise ValueError(
            f"El Excel debe tener columnas '{LINKS_COL_CATEGORY}' y '{LINKS_COL_LINK}'. "
            f"Detectadas: {list(df.columns)}"
        )
    out = df[[LINKS_COL_CATEGORY, LINKS_COL_LINK]].copy()
    out[LINKS_COL_CATEGORY] = out[LINKS_COL_CATEGORY].astype(str).str.strip()
    out[LINKS_COL_LINK]     = out[LINKS_COL_LINK].astype(str).str.strip()
    out = out[out[LINKS_COL_LINK].str.startswith("http", na=False)]
    out = out[out[LINKS_COL_CATEGORY].ne("")]
    if out.empty:
        raise ValueError("El Excel de links quedó vacío luego de limpiar.")
    return out


def _read_excel_any_noheader(path: str, sheet: str):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsb":
        return pd.read_excel(path, sheet_name=sheet, header=None, dtype=str, engine="pyxlsb")
    return pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)


def load_statusbooks_map(path: str, sheet: str):
    log("\n Cargando Status Books...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No encuentro StatusBooks: {path} (SEASON={SEASON})")

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
        raise ValueError("No pude encontrar la fila header ('Product Code') en StatusBooks.")

    headers = raw.iloc[header_row, :].fillna("").astype(str).str.strip().tolist()
    sb = raw.iloc[header_row + 1:, :].copy()
    sb.columns = headers
    sb.columns = [c if c else f"__EMPTY__{i}" for i, c in enumerate(sb.columns)]

    log(f"    Archivo: {path} | Sheet: {sheet} | Filas: {len(sb)} | HeaderRow: {header_row+1}")

    COL_CODE  = "Product Code"
    COL_NAME  = "Marketing Name"
    COL_DIV   = "BU"
    COL_CAT   = "Category"
    COL_FR    = "Franchise"
    COL_SALE  = "SALE"
    COL_STOCK = "STOCK BL (Inventario Brandlive)"
    COL_SSN   = "SSN VTA"

    # ── Detectar columna Full Price por nombre exacto SEASON (ej: "SP26") ──
    season_col_name = str(SEASON).strip()
    full_price_col_idx = None

    if season_col_name in sb.columns:
        full_price_col_idx = list(sb.columns).index(season_col_name)
        log(f"    Full Price: columna '{season_col_name}' encontrada directamente en headers (col #{full_price_col_idx+1})")
    else:
        # Fallback: buscar en fila de grupos mergeados anterior al header
        group_row = header_row - 1
        if group_row >= 0:
            top = raw.iloc[group_row, :].fillna("").astype(str)
            season_patterns = [f"({SEASON})", str(SEASON)]
            for i, val in enumerate(top.tolist()):
                v = str(val).strip()
                if any(p in v for p in season_patterns):
                    full_price_col_idx = i
                    col_name_fb = str(sb.columns[i]) if i < len(sb.columns) else ""
                    log(f"    Full Price: '{season_col_name}' hallada por fila de grupo (col #{i+1}, header='{col_name_fb}')")
                    break

    if full_price_col_idx is None:
        raise KeyError(
            f"No pude detectar columna Full Price para SEASON={SEASON}. "
            f"Busqué header exacto '{season_col_name}' y la fila de grupo anterior. "
            f"Headers (primeros 40): {list(sb.columns)[:40]}"
        )

    missing = [c for c in [COL_CODE, COL_NAME, COL_DIV, COL_CAT, COL_FR, COL_SALE, COL_STOCK] if c not in sb.columns]
    if missing:
        raise KeyError(
            f"Faltan columnas en StatusBooks (header en fila {header_row+1}): {missing}. "
            f"Headers detectados (primeros 40): {list(sb.columns)[:40]}"
        )

    sb = sb.reset_index(drop=True)

    # Debug primeros valores crudos
    _raw_col = sb.iloc[:, full_price_col_idx]
    _sample  = _raw_col.dropna().head(5)
    log(f"   [DEBUG] Primeros valores CRUDOS columna {season_col_name}: {[repr(v) for v in _sample.tolist()]}")
    nike_full_series = _raw_col.apply(parse_money_ar_to_float)
    _nonzero = nike_full_series[nike_full_series > 0].head(5)
    log(f"   [DEBUG] Primeros valores PARSEADOS (>0): {_nonzero.tolist()}")

    work = pd.DataFrame()
    work["ProductCode"]  = sb[COL_CODE].astype(str).str.strip().str.upper()
    work["MarketingName"] = sb[COL_NAME].astype(str).str.strip()
    work["SB_Division"]  = sb[COL_DIV].astype(str).str.strip()
    work["SB_Category"]  = sb[COL_CAT].astype(str).str.strip()
    work["SB_Franchise"] = sb[COL_FR].astype(str).str.strip()

    if COL_SSN in sb.columns:
        work["SB_SSN_VTA"] = sb[COL_SSN].astype(str).str.strip()
    else:
        work["SB_SSN_VTA"] = ""
        log(" StatusBooks: no encontré columna 'SSN VTA'.")

    # Detección de precios en $k (miles): pyxlsb a veces lee 299.999 en lugar de 299999.
    # Si la mediana de los valores > 0 es < 2000 → están en miles → multiplicar x1000.
    _vals = pd.to_numeric(nike_full_series, errors="coerce")
    _med  = _vals[_vals > 0].median()
    if pd.notna(_med) and _med < 2000:
        work["NikeFullPrice"] = (_vals * 1000.0).to_numpy()
        log(f"   [DEBUG] NikeFullPrice en $k detectado (mediana={_med:.2f}) → x1000")
    else:
        work["NikeFullPrice"] = _vals.to_numpy()
        log(f"   [DEBUG] NikeFullPrice en ARS directo (mediana={_med:.2f})")

    work["NikeSaleDecimal"]  = sb[COL_SALE].apply(parse_sale_percent_to_decimal)
    work["NikeFinalPrice"]   = work["NikeFullPrice"] * (1.0 - work["NikeSaleDecimal"])
    work["StockBL"]          = sb[COL_STOCK].apply(parse_stock_bl_to_float)

    work = work[work["ProductCode"].ne("")].copy()
    work = work[work["NikeFinalPrice"] > 0].copy()
    work = work[work["StockBL"] > 0].copy()

    sb_map = {}
    for _, r in work.iterrows():
        code = r["ProductCode"]
        data = {
            "SB_ProductCode":    code,
            "SB_MarketingName":  r["MarketingName"],
            "SB_Division":       r["SB_Division"],
            "SB_Category":       r["SB_Category"],
            "SB_Franchise":      r["SB_Franchise"],
            "SB_SSN_VTA":        r.get("SB_SSN_VTA", ""),
            "NikeFullPrice":     float(r["NikeFullPrice"]),
            "NikeSaleDecimal":   float(r["NikeSaleDecimal"]),
            "NikeFinalPrice":    float(r["NikeFinalPrice"]),
        }
        sb_map[code] = data
        if code.startswith("NI"):
            sb_map[code[2:]] = data
        else:
            sb_map["NI" + code] = data

    log(f"    Product Codes válidos (Final>0 y StockBL>0): {len(work)}")
    log(f"    Keys en mapa (con/sin NI): {len(sb_map)}")
    return sb_map


def _map_grupo_to_bu(grupo: str) -> str:
    g = str(grupo or "").strip().lower()
    if g == "accesorios":   return "EQ"
    if g == "calzado":      return "FW"
    if g == "indumentaria": return "APP"
    return ""


def load_aux_assort_map(path: str, sheet: str) -> dict:
    """Mapa auxiliar para productos no-NDDC. Búsqueda robusta del archivo (igual que soloDeportes v4)."""
    log(f"\n Buscando AUX_ASSORT_FILE: {path}")
    resolved_path = path

    if os.path.exists(resolved_path):
        log(f"    ✓ Encontrado directamente: {resolved_path}")
    else:
        log(f"    ✗ No encontrado en ruta directa, buscando variantes...")
        try:
            wanted = os.path.basename(path)
            wanted_norm = unicodedata.normalize("NFKD", wanted).encode("ascii", "ignore").decode("ascii").lower()
            script_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else None
            search_dirs = [os.getcwd()]
            if script_dir and script_dir != os.getcwd():
                search_dirs.append(script_dir)

            checked    = set()
            candidates = []

            for d in search_dirs:
                d_abs = os.path.abspath(d)
                if d_abs in checked or not os.path.isdir(d_abs):
                    continue
                checked.add(d_abs)
                try:
                    files_in_dir = os.listdir(d_abs)
                except Exception as e:
                    log(f"      ✗ Error listando directorio: {e}")
                    continue

                for fname in files_in_dir:
                    if not fname.lower().endswith(".xlsx") or fname.startswith("~$"):
                        continue
                    fname_norm = unicodedata.normalize("NFKD", fname).encode("ascii", "ignore").decode("ascii").lower()
                    if fname_norm == wanted_norm:
                        resolved_path = os.path.join(d_abs, fname)
                        log(f"      ✓ Match exacto: {fname}")
                        break
                    if ("lista actualizada" in fname_norm and "grupo de art" in fname_norm and "total" in fname_norm):
                        candidates.append(os.path.join(d_abs, fname))
                        log(f"      ~ Candidato: {fname}")
                if os.path.exists(resolved_path) and resolved_path != path:
                    break

            if not os.path.exists(resolved_path) and candidates:
                resolved_path = candidates[0]
                log(f"    → Usando primer candidato: {resolved_path}")
        except Exception as e:
            log(f"    ✗ Error en búsqueda avanzada: {e}")

    if not os.path.exists(resolved_path):
        log(f" No encuentro AUX_ASSORT_FILE: {path}. Continúo sin completado auxiliar.")
        return {}

    log(f"    Leyendo Excel (puede tardar 15-30s)...")
    try:
        df = pd.read_excel(
            resolved_path, sheet_name=sheet, dtype=str,
            usecols=[AUX_COL_STYLE, AUX_COL_NAME, AUX_COL_GRUPO, AUX_COL_ANIO],
            engine="openpyxl"
        )
    except Exception as e:
        log(f"    Error con usecols: {type(e).__name__}, reintentando lectura completa...")
        try:
            df = pd.read_excel(resolved_path, sheet_name=sheet, dtype=str, engine="openpyxl")
        except Exception as e2:
            log(f"    Error en lectura completa: {e2}. Continúo sin completado auxiliar.")
            return {}

    log(f"    ✓ Excel leído: {len(df)} filas")
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in [AUX_COL_STYLE, AUX_COL_NAME, AUX_COL_GRUPO] if c not in df.columns]
    if missing:
        log(f" AUX_ASSORT_FILE: faltan columnas {missing}. Continúo sin completado auxiliar.")
        return {}

    out = {}
    for _, r in df.iterrows():
        sc = normalize_stylecolor(str(r.get(AUX_COL_STYLE, "") or "").strip().upper())
        if not sc:
            continue
        name  = str(r.get(AUX_COL_NAME,  "") or "").strip()
        grupo = str(r.get(AUX_COL_GRUPO, "") or "").strip()
        bu    = _map_grupo_to_bu(grupo)
        anio  = str(r.get(AUX_COL_ANIO,  "") or "").strip()
        out[sc] = {"SB_MarketingName": name, "SB_Division": bu, "Aux_AO": anio}

    log(f" ✓ AUX map cargado: {len(out)} items (sheet='{sheet}')")
    return out


# =========================
# OVERLAYS Y NAVEGACIÓN
# =========================

def try_close_overlays(page):
    candidates = [
        "button:has-text('Aceptar')", "button:has-text('ACEPTAR')",
        "button:has-text('Entendido')", "button:has-text('OK')",
        "button:has-text('Cerrar')", "button[aria-label='Cerrar']",
        "[aria-label='close']", ".modal-close", ".close",
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
        for inp in page.locator("input").all()[:30]:
            try:
                ph = (inp.get_attribute("placeholder") or "").lower()
            except Exception:
                ph = ""
            if any(k in ph for k in ["código postal", "codigo postal", "cp"]):
                inp.click(timeout=1500)
                human_pause(0.2, 0.5)
                inp.fill(cp)
                human_pause(0.2, 0.5)
                inp.press("Enter")
                human_pause(0.8, 1.2)
                return
    except Exception:
        pass


# =========================
# RECOLECCIÓN PLP — DIGITALSPORT
# =========================

def collect_plp_links_digitalsport(page) -> list:
    """
    DigitalSport PLP:
      - Wrapper: div.products_wraper
      - Cada producto: <a class="product" productid="..." href="/digitalsport/prod/...">
      - data-sku en el <a> (si existe) es el SKU Nike sin guión
      - Carga por infinite scroll; #loadmore puede existir pero no es obligatorio
    Devuelve lista de dicts: { url, ds_sku, productid }
    """
    base = "https://www.digitalsport.com.ar"

    container_sels = ["div.products_wraper", "div.products_wrapper", "div.products"]
    candidate_sels = [
        "div.products_wraper a.product[productid][href^='/digitalsport/prod/']",
        "div.products_wraper a.product[productid]",
        "div.products_wraper a.product[href*='/digitalsport/prod/']",
        "a.product[productid][href^='/digitalsport/prod/']",
        "a.product[productid]",
        "div.products_wraper a.product",
        "a.product",
    ]

    def pick_selector():
        for sel in candidate_sels:
            try:
                if page.locator(sel).count() > 0:
                    return sel
            except Exception:
                continue
        return None

    # Esperar wrapper
    for cs in container_sels:
        try:
            page.wait_for_selector(cs, timeout=8_000)
            break
        except Exception:
            pass

    chosen = None
    t0 = time.time()
    while time.time() - t0 < 20:
        chosen = pick_selector()
        if chosen:
            break
        page.wait_for_timeout(500)

    if not chosen:
        log(" PLP DigitalSport: no pude detectar cards (0 matches).")
        try:
            log(f"   DEBUG count a.product = {page.locator('a.product').count()}")
        except Exception:
            pass
        return []

    log(f" PLP selector elegido: {chosen}")

    def pull_items(sel: str) -> list:
        try:
            items = page.eval_on_selector_all(
                sel,
                """els => els.map(a => ({
                    href: a.getAttribute('href'),
                    productid: a.getAttribute('productid'),
                    ds_sku: a.getAttribute('data-sku')
                })).filter(x => x && x.href)"""
            ) or []
        except Exception:
            items = []

        out = []
        for it in items:
            href = str((it.get("href") or "")).strip()
            if not href:
                continue
            if "/digitalsport/prod/" not in href:
                continue
            url = urljoin(base, href)
            pid = str((it.get("productid") or "")).strip()
            sku = str((it.get("ds_sku") or "")).strip()
            out.append({"url": url, "productid": pid, "ds_sku": sku})
        return out

    # Scroll infinito con deduplicación por URL
    by_url = {}
    for it in pull_items(chosen):
        by_url[it["url"]] = it

    last_count = len(by_url)
    stagnation = 0

    for r in range(1, MAX_PLP_SCROLL_ROUNDS + 1):
        try:
            page.mouse.wheel(0, 1800)
        except Exception:
            pass
        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        except Exception:
            pass

        human_pause(0.6, 1.2)
        try_close_overlays(page)

        # Clickear #loadmore si existe y es visible
        try:
            lm = page.locator("#loadmore").first
            if lm.count() > 0 and lm.is_visible():
                try:
                    lm.scroll_into_view_if_needed(timeout=1200)
                except Exception:
                    pass
                try:
                    lm.click(timeout=1200)
                except Exception:
                    pass
                human_pause(0.4, 0.8)
        except Exception:
            pass

        added = 0
        for it in pull_items(chosen):
            u = it["url"]
            if u not in by_url:
                by_url[u] = it
                added += 1
            else:
                # Completar meta si llegó vacío antes
                if not by_url[u].get("ds_sku") and it.get("ds_sku"):
                    by_url[u]["ds_sku"] = it["ds_sku"]
                if not by_url[u].get("productid") and it.get("productid"):
                    by_url[u]["productid"] = it["productid"]

        if len(by_url) == last_count:
            stagnation += 1
        else:
            stagnation = 0
            last_count = len(by_url)

        log(f"   [Scroll {r}] total={len(by_url)} (+{added}) | stagn={stagnation}/{PLP_STAGNATION_ROUNDS}")
        if stagnation >= PLP_STAGNATION_ROUNDS:
            break

    items = list(by_url.values())
    log(f" PLP DigitalSport: {len(items)} PDP links únicos.")
    if items:
        ex_sku = next((x.get("ds_sku") for x in items if x.get("ds_sku")), "")
        if ex_sku:
            log(f"   Ejemplo data-sku: {ex_sku}")
    return items


def collect_pdp_links_from_plp(page, plp_url: str) -> list:
    log(f"\n Abriendo PLP: {plp_url}")
    page.goto(plp_url, wait_until="domcontentloaded", timeout=PLP_WAIT_MS)
    page.wait_for_timeout(1200)
    try_close_overlays(page)
    try_set_postal_code(page, CP_AMBA)
    return collect_plp_links_digitalsport(page)


# =========================
# EXTRACCIÓN DE PRECIOS — DIGITALSPORT
# =========================

def extract_price_from_jsonld(page) -> float:
    try:
        for s in page.locator("script[type='application/ld+json']").all()[:12]:
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
                            return parse_money_ar_to_float(str(off["price"]))
                        if isinstance(off, list):
                            for o in off:
                                if isinstance(o, dict) and o.get("price") is not None:
                                    return parse_money_ar_to_float(str(o["price"]))
                    if obj.get("price") is not None:
                        return parse_money_ar_to_float(str(obj["price"]))
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
            if price > 0:
                return float(price)
    except Exception:
        pass
    return 0.0


def extract_price_from_meta(page) -> float:
    for sel in [
        "meta[property='product:price:amount']",
        "meta[property='og:price:amount']",
        "meta[name='price']",
        "meta[itemprop='price']",
    ]:
        try:
            m = page.locator(sel).first
            if m.count() > 0:
                p = parse_money_ar_to_float(m.get_attribute("content") or "")
                if p > 0:
                    return float(p)
        except Exception:
            continue
    return 0.0


def extract_price_from_visible_avoid_strike(page) -> float:
    """Último recurso: regex sobre texto visible, ignorando elementos tachados."""
    try:
        vals = page.evaluate("""
            () => {
              const priceRegex = /\\$\\s*\\d+(?:\\.\\d{3})*(?:,\\d{2})?/;
              const nodes = Array.from(document.querySelectorAll("span,div,p,strong,b"));
              const out = [];
              for (const el of nodes) {
                const txt = (el.innerText || "").trim();
                if (!txt || txt.length > 40) continue;
                if (!priceRegex.test(txt)) continue;
                const tag = (el.tagName || "").toLowerCase();
                if (tag === "del") continue;
                const style = window.getComputedStyle(el);
                if ((style.textDecorationLine || "").includes("line-through")) continue;
                let cur = el, bad = false;
                for (let i=0; i<6 && cur; i++) {
                  const t = (cur.tagName || "").toLowerCase();
                  if (t === "del") { bad = true; break; }
                  const st = window.getComputedStyle(cur);
                  if ((st.textDecorationLine || "").includes("line-through")) { bad = true; break; }
                  cur = cur.parentElement;
                }
                if (bad) continue;
                out.push(txt);
              }
              return out.slice(0, 20);
            }
        """)
        if vals:
            bad_tokens = ("cuota", "cuotas", "x ", " x", "sin interés", "interés", "cft", "tea")
            filtered = []
            for sv in vals:
                if any(t in str(sv).lower() for t in bad_tokens):
                    continue
                v = parse_money_ar_to_float(sv)
                if v > 0:
                    filtered.append(v)
            return float(max(filtered)) if filtered else 0.0
    except Exception:
        pass
    return 0.0


def extract_price_final_by_label(page) -> float:
    """Extractor robusto de precio final para DigitalSport."""
    # 1) JSON-LD
    v = extract_price_from_jsonld(page)
    if v > 0:
        return float(v)
    # 2) Meta tags
    v = extract_price_from_meta(page)
    if v > 0:
        return float(v)
    # 3) DOM — selectores específicos de DigitalSport / Dabra
    dom_sels = [
        "[itemprop='price']", "meta[itemprop='price']",
        ".price .value", ".price .amount", ".product-price", ".product__price",
        ".precio", ".price", ".sales .value", "#price-reload span.value",
        "div.prices span.value", ".product-data .price",
        ".product_info .price", ".product-detail .price",
    ]
    for sel in dom_sels:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue
            try:
                tag = (loc.evaluate("e => e.tagName") or "").upper()
            except Exception:
                tag = ""
            if tag == "META":
                v = parse_money_ar_to_float(loc.get_attribute("content") or "")
                if v > 0:
                    return float(v)
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
            continue
    # 4) Último recurso
    return extract_price_from_visible_avoid_strike(page)


def extract_full_price_from_strike(page) -> float:
    """Extrae precio tachado (precio original antes de descuento) de DigitalSport."""
    for sel in ["del", ".old-price", ".price-old", ".price__old", ".was-price",
                ".list-price", ".strike", ".tachado", "s"]:
        try:
            loc = page.locator(sel).first
            if loc.count() == 0:
                continue
            txt = (loc.inner_text() or "").strip()
            v = parse_money_ar_to_float(txt)
            if v > 0:
                return float(v)
            v = parse_money_ar_to_float(loc.get_attribute("content") or "")
            if v > 0:
                return float(v)
        except Exception:
            continue
    return 0.0


def extract_max_cuotas_habituales(page) -> int:
    """
    Extrae el máximo de cuotas sin interés de la PDP.
    Multi-estrategia: JS evaluate → DOM p → heading → texto completo.
    """
    CUOTAS_RE = re.compile(r"(\d+)\s*cuotas?\s*sin\s*inter[eé]s", re.I)
    nums = []

    # Estrategia 1: JS sobre div.installments
    try:
        page.wait_for_selector("div.installments", timeout=3000)
        js_result = page.evaluate("""
            () => {
                const containers = document.querySelectorAll(
                    'div.installments, .installments-pdp, .installments'
                );
                const texts = [];
                containers.forEach(el => {
                    texts.push(el.textContent || "");
                    el.childNodes.forEach(n => { if (n.textContent) texts.push(n.textContent); });
                });
                return texts.join(" ");
            }
        """)
        if js_result:
            nums += [int(x) for x in CUOTAS_RE.findall(js_result)]
    except Exception:
        pass

    # Estrategia 2: párrafos dentro de div.installments
    try:
        ps = page.locator("div.installments p, .installments-pdp p")
        for i in range(min(ps.count(), 20)):
            t = (ps.nth(i).inner_text() or "").strip()
            if t:
                nums += [int(x) for x in CUOTAS_RE.findall(t)]
    except Exception:
        pass

    if nums:
        return max(nums)

    # Estrategia 3: heading "Cuotas habituales"
    try:
        heading = page.locator("text=/Cuotas habituales/i").first
        if heading.count() > 0:
            for xpath in ["xpath=ancestor::*[self::div or self::section][1]", "xpath=ancestor::*[1]"]:
                try:
                    container = heading.locator(xpath).first
                    if container.count() > 0:
                        found = CUOTAS_RE.findall(container.inner_text() or "")
                        if found:
                            return max(int(x) for x in found)
                except Exception:
                    continue
    except Exception:
        pass

    # Estrategia 4: texto completo de la página
    try:
        full_text = page.evaluate("() => document.body.textContent || ''")
        found = [int(x) for x in CUOTAS_RE.findall(full_text) if 1 <= int(x) <= 24]
        if found:
            return max(found)
    except Exception:
        pass

    return 0


# =========================
# SCRAPE PDP — DIGITALSPORT
# =========================

def scrape_pdp(page, url: str, style_norm_hint: str = "", ds_sku_raw: str = "",
               client=None, cache: dict = None) -> dict:
    """
    Scrapea una PDP de DigitalSport.
    StyleColor por fallbacks en orden:
      0) hint del PLP (data-sku convertido)
      1) SKU candidato en PDP + regex
      1.5) Cross-lookup en cache por SKU
      2) OpenAI texto-only
      3) OpenAI Vision
    """
    page.goto(url, wait_until="domcontentloaded", timeout=PDP_WAIT_MS)
    human_pause(0.5, 1.1)
    try_close_overlays(page)
    try_set_postal_code(page, CP_AMBA)

    # Nombre
    name = ""
    try:
        h1 = page.locator("h1").first
        if h1.count() > 0:
            name = h1.inner_text().strip()
    except Exception:
        pass
    if not name:
        try:
            name = page.title()
        except Exception:
            pass

    # Descripción
    description = ""
    try:
        for sel in [".product-description", "[itemprop='description']", ".product-detail-description"]:
            d = page.locator(sel).first
            if d.count() > 0:
                description = d.inner_text().strip()[:200]
                break
    except Exception:
        pass

    # ── Resolución de StyleColor ──────────────────────────────────────────────
    style_norm    = ""
    style_method  = "NONE"
    openai_error  = None
    candidate_sku = ""

    # Intento 0: hint del PLP (data-sku ya convertido)
    if style_norm_hint:
        style_norm   = normalize_stylecolor(style_norm_hint)
        style_method = "PLP_SKU"
        log(f"       StyleColor desde PLP SKU hint: {style_norm}")

    # Intento 1: SKU candidato en PDP + regex
    if not style_norm:
        try:
            candidate_sku = extract_candidate_sku_from_page(page)
            if candidate_sku:
                log(f"       SKU candidato en PDP: {candidate_sku}")
                extracted = extract_stylecolor_from_solo_sku(candidate_sku)
                if extracted:
                    style_norm   = normalize_stylecolor(extracted)
                    style_method = "PDP_SKU_REGEX"
                    log(f"       StyleColor desde SKU PDP: {style_norm}")
        except Exception:
            candidate_sku = ""

    # Intento 1.5: cross-lookup en cache por SKU
    if not style_norm and candidate_sku and cache:
        try:
            for _key, cached_data in cache.items():
                if not isinstance(cached_data, dict):
                    continue
                if cached_data.get("digitalsport_DS_SKU_Raw") == candidate_sku:
                    cached_style = cached_data.get("digitalsport_StyleColor_Norm")
                    if cached_style:
                        style_norm   = cached_style
                        style_method = "CACHE_SKU_MATCH"
                        log(f"       StyleColor desde cache (SKU match): {style_norm}")
                        break
        except Exception as e:
            log(f"       Error buscando SKU en cache: {e}")

    # Intento 2: OpenAI texto-only
    if client and not style_norm and (candidate_sku or ds_sku_raw):
        sku_for_ai = candidate_sku or ds_sku_raw
        style_norm, openai_error = ask_stylecolor_from_text_safe(
            client, sku=sku_for_ai, name=name, description=description
        )
        if style_norm:
            style_norm   = normalize_stylecolor(style_norm)
            style_method = "TEXT_ONLY"
            log(f"       StyleColor desde OpenAI texto: {style_norm}")
        time.sleep(0.5)

    # Intento 3: OpenAI Vision (fallback final)
    if client and not style_norm:
        try:
            product_area = page.locator(
                ".product-media, .product-image, .product-info-main, .product.media"
            ).first
            png = product_area.screenshot() if product_area.count() > 0 else page.screenshot(full_page=False)
            page_desc = f"Nombre del producto: {name}\n"
            if description:
                page_desc += f"Descripción: {description}\n"
            if candidate_sku or ds_sku_raw:
                page_desc += f"SKU candidato: {candidate_sku or ds_sku_raw}\n"
            style_norm, openai_error = ask_stylecolor_from_image_safe(client, png, page_desc)
            if style_norm:
                style_norm   = normalize_stylecolor(style_norm)
                style_method = "VISION"
                log(f"       StyleColor desde OpenAI Vision: {style_norm}")
            time.sleep(1)
        except Exception as e:
            openai_error = str(e)
            log(f"       Error en screenshot/OpenAI: {e}")

    if not style_norm:
        log(f"       StyleColor NO detectado para {url}")
        style_norm = "STYLECOLOR NO ENCONTRADO"
    # ─────────────────────────────────────────────────────────────────────────

    # Precios
    price_final = 0.0
    full_price  = 0.0
    for _try in range(3):
        try:
            price_final = extract_price_final_by_label(page)
            full_price  = extract_full_price_from_strike(page)
            if price_final > 0:
                break
        except Exception:
            pass
        try:
            page.wait_for_timeout(400)
        except Exception:
            pass

    if price_final <= 0:
        log("    (NO_PRICE_DOM) precio no encontrado en DOM/JSON-LD/meta")

    max_cuotas = extract_max_cuotas_habituales(page)

    if full_price <= 0 or (price_final > 0 and full_price < price_final):
        full_price = float(price_final)

    if price_final and full_price and full_price > (price_final * 3.0):
        full_price = float(price_final)

    sale_dec = 0.0
    if full_price > 0 and price_final > 0 and full_price >= price_final:
        sale_dec = max(0.0, min(1.0, (full_price - price_final) / full_price))

    return {
        "Retailer":                         "digitalsport",
        "digitalsport_URL":                 url,
        "digitalsport_Name":                name,
        "digitalsport_StyleColor_Norm":     style_norm,
        "digitalsport_StyleColor_Raw":      style_norm,
        "digitalsport_StyleColor_Method":   style_method,
        "digitalsport_StyleColor_Error":    openai_error,
        "digitalsport_DS_SKU_Raw":          ds_sku_raw or "",
        "digitalsport_DS_SKU_Page":         candidate_sku or "",
        "digitalsport_FullPrice":           float(full_price),
        "digitalsportSaleDecimal":          float(sale_dec),
        "digitalsport_FinalPrice":          float(price_final),
        "digitalsport_MaxCuotasSinInteres": int(max_cuotas),
        "digitalsport_LastUpdated":         dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


# =========================
# BML / SHIPPING / CUOTAS
# =========================

def _bml_label_from_prices(comp_price, nike_price) -> str:
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
    """
    Envío Nike solo aplica si el Final Price está POR ENCIMA del umbral.
    Si el precio es menor o igual al umbral → envío 0.
    """
    try:
        p = float(price)
    except Exception:
        return 0.0
    if p <= 0:
        return 0.0
    return 0.0 if p >= float(NIKE_FREE_SHIP_FROM_ARS) else float(NIKE_STD_SHIPPING_ARS)


def _ship_for_price_retailer(price: float) -> float:
    try:
        p = float(price)
    except Exception:
        return 0.0
    if p <= 0:
        return 0.0
    return 0.0 if p >= float(RETAILER_FREE_SHIP_FROM_ARS) else float(RETAILER_STD_SHIPPING_ARS)


def _nike_cuotas_for_price(nike_final_price: float) -> int:
    try:
        if not NIKE_CUOTAS_SIMPLE_MODE:
            return int(NIKE_CUOTAS_ALL)
        return int(NIKE_CUOTAS_HIGH) if float(nike_final_price) >= float(NIKE_CUOTAS_HIGH_FROM) else int(NIKE_CUOTAS_ALL)
    except Exception:
        return int(NIKE_CUOTAS_ALL)


def _gender_from_category(cat: str) -> str:
    try:
        s = str(cat or "").strip().lower()
    except Exception:
        return ""
    if any(k in s for k in ["mujer", "women", "dama", "damas", "femenino", "female"]):
        return "Mujer"
    if any(k in s for k in ["hombre", "men", "caballero", "masculino", "male"]):
        return "Hombre"
    if any(k in s for k in ["niño", "nino", "kids", "infantil", "junior", "niña", "nina", "youth"]):
        return "Kids"
    if "unisex" in s:
        return "Unisex"
    return ""


# =========================
# BUILD OUTPUT
# =========================

def build_template_df(df: pd.DataFrame) -> pd.DataFrame:
    """Construye el DataFrame de salida con el mismo formato estándar."""
    df = df.copy()

    base_cols = [
        "digitalsport_StyleColor_Norm", "SB_ProductCode", "SB_MarketingName",
        "SB_Category", "SB_Division", "SB_Franchise",
        "PLP_PrimaryCategoria", "digitalsport_URL",
        "digitalsport_FullPrice", "digitalsportSaleDecimal", "digitalsport_FinalPrice",
        "NikeFullPrice", "NikeSaleDecimal", "NikeFinalPrice",
        "digitalsport_MaxCuotasSinInteres", "digitalsport_LastUpdated",
        "SB_SSN_VTA", "Aux_AO", "InStatusBooks",
        "digitalsport_DS_SKU_Raw", "digitalsport_DS_SKU_Page",
    ]
    for c in base_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df["StyleColor"]      = df["digitalsport_StyleColor_Norm"].astype(str).fillna("").str.strip()
    df["ProductCode"]     = (
        df["digitalsport_DS_SKU_Raw"].astype(str).fillna("").str.strip()
    )
    _m_empty_pc = df["ProductCode"].eq("")
    if _m_empty_pc.any():
        df.loc[_m_empty_pc, "ProductCode"] = (
            df.loc[_m_empty_pc, "digitalsport_DS_SKU_Page"].astype(str).fillna("").str.strip()
        )
    _m_empty_pc = df["ProductCode"].eq("")
    if _m_empty_pc.any():
        df.loc[_m_empty_pc, "ProductCode"] = (
            df.loc[_m_empty_pc, "SB_ProductCode"].astype(str).fillna("").str.strip()
        )
    df["Marketing Name"]  = df["SB_MarketingName"].astype(str).fillna("").str.strip()
    df["Category"]        = df["SB_Category"].astype(str).fillna("").str.strip()
    df["Division"]        = df["SB_Division"].astype(str).fillna("").str.strip()
    df["Franchise"]       = df["SB_Franchise"].astype(str).fillna("").str.strip()
    df["Gender"]          = df["PLP_PrimaryCategoria"].apply(_gender_from_category)
    df["Link PDP Competitor"] = df["digitalsport_URL"].astype(str).fillna("").str.strip()

    df["Competitor Full Price"]  = pd.to_numeric(df["digitalsport_FullPrice"], errors="coerce")
    df["Competitor Markdown"]    = pd.to_numeric(df["digitalsportSaleDecimal"], errors="coerce")
    df["Competitor Final Price"] = pd.to_numeric(df["digitalsport_FinalPrice"], errors="coerce")

    _missing_full = df["Competitor Full Price"].isna() | (df["Competitor Full Price"] <= 0)
    if _missing_full.any():
        df.loc[_missing_full, "Competitor Full Price"] = df.loc[_missing_full, "Competitor Final Price"]
        df.loc[_missing_full, "Competitor Markdown"]   = 0.0

    df["Nike Full Price"]  = pd.to_numeric(df["NikeFullPrice"],  errors="coerce")
    df["Nike Markdown"]    = pd.to_numeric(df["NikeSaleDecimal"], errors="coerce")
    df["Nike Final Price"] = pd.to_numeric(df["NikeFinalPrice"],  errors="coerce")

    df["BML Full Price"]  = df.apply(lambda r: _bml_label_from_prices(r.get("Competitor Full Price"),  r.get("Nike Full Price")),  axis=1)
    df["BML Final Price"] = df.apply(lambda r: _bml_label_from_prices(r.get("Competitor Final Price"), r.get("Nike Final Price")), axis=1)

    df["Competitor Shipping"] = df["Competitor Final Price"].apply(
        lambda x: _ship_for_price_retailer(float(x)) if pd.notna(x) else 0.0
    )
    df["Nike Shipping"] = df["Nike Final Price"].apply(
        lambda x: _ship_for_price_nike(float(x)) if (pd.notna(x) and float(x) > 0) else 0.0
    )
    df["Nike Price + Shipping"]       = df["Nike Final Price"]       + df["Nike Shipping"]
    df["Competitor Price + Shipping"] = df["Competitor Final Price"] + df["Competitor Shipping"]

    df["BML with Shipping"] = df.apply(
        lambda r: _bml_label_from_prices(r.get("Competitor Price + Shipping"), r.get("Nike Price + Shipping")), axis=1
    )

    df["Cuotas Competitor"] = pd.to_numeric(df["digitalsport_MaxCuotasSinInteres"], errors="coerce").fillna(0).astype(int)

    df["Cuotas Nike"] = df.apply(
        lambda r: _nike_cuotas_for_price(float(r.get("Nike Final Price")))
        if (r.get("InStatusBooks") is True and pd.notna(r.get("Nike Final Price")))
        else pd.NA,
        axis=1
    )

    def _bml_cuotas_row(row):
        if row.get("InStatusBooks") is not True:
            return ""
        nk = row.get("Cuotas Nike")
        if pd.isna(nk):
            return ""
        v  = int(float(row.get("Cuotas Competitor") or 0))
        nk = int(float(nk))
        if v == nk:
            return "Meet"
        return "Beat" if nk > v else "Lose"

    df["BML Cuotas"] = df.apply(_bml_cuotas_row, axis=1)

    # Season dinámico por producto (igual que soloDeportes v4)
    def _season_row(row):
        def _safe(x):
            return "" if (x is None or pd.isna(x)) else str(x).strip()
        if row.get("InStatusBooks") is True:
            v = _safe(row.get("SB_SSN_VTA"))
            return v if v else "no registrado"
        v2 = _safe(row.get("Aux_AO"))
        return v2 if v2 else "no registrado"

    df["Season"]           = df.apply(_season_row, axis=1)
    df["Fecha Corrida"]    = RUN_DATE
    df["NDDC"]             = df.get("InStatusBooks").apply(lambda x: "NDDC" if x is True else "")
    df["Competitor"]       = "digitalsport"
    df["Last Update Competitor"] = df["digitalsport_LastUpdated"].astype(str).fillna("").str.strip()

    cols = [
        "StyleColor", "ProductCode", "Marketing Name", "Category", "Division", "Franchise", "Gender",
        "Link PDP Competitor",
        "Competitor Full Price", "Competitor Markdown", "Competitor Final Price",
        "Nike Full Price", "Nike Markdown", "Nike Final Price",
        "BML Final Price", "BML Full Price",
        "Competitor Shipping", "Nike Shipping", "Nike Price + Shipping", "Competitor Price + Shipping",
        "BML with Shipping",
        "Cuotas Competitor", "Cuotas Nike", "BML Cuotas",
        "Competitor", "Season", "Fecha Corrida", "Last Update Competitor", "NDDC",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    out = df[cols].copy()

    num_cols = [
        "Competitor Full Price", "Competitor Markdown", "Competitor Final Price",
        "Nike Full Price", "Nike Markdown", "Nike Final Price",
        "Competitor Shipping", "Nike Shipping", "Nike Price + Shipping", "Competitor Price + Shipping",
        "Cuotas Competitor", "Cuotas Nike",
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


# =========================
# ESCRITURA XLSX
# =========================

def write_visual_xlsx(df: pd.DataFrame, path: str):
    df = df.copy()
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Output", index=False)
        wb = writer.book
        ws = writer.sheets["Output"]

        header_fmt = wb.add_format({"bold": True, "font_color": "white", "bg_color": "#111827", "border": 1})
        money_fmt  = wb.add_format({"num_format": "$#,##0", "border": 1})
        pct_fmt    = wb.add_format({"num_format": "0.0%", "border": 1})
        text_fmt   = wb.add_format({"border": 1})
        link_fmt   = wb.add_format({"font_color": "blue", "underline": 1, "border": 1})
        beat_fmt   = wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100", "border": 1})
        meet_fmt   = wb.add_format({"bg_color": "#FFEB9C", "font_color": "#9C6500", "border": 1})
        lose_fmt   = wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006", "border": 1})

        for col_i, name in enumerate(df.columns):
            ws.write(0, col_i, name, header_fmt)

        col_idx = {c: i for i, c in enumerate(df.columns)}
        nrows   = len(df)

        if "Marketing Name" in col_idx:
            ws.set_column(col_idx["Marketing Name"], col_idx["Marketing Name"], 35, text_fmt)
        if "Fecha Corrida" in col_idx:
            ws.set_column(col_idx["Fecha Corrida"], col_idx["Fecha Corrida"], 14, text_fmt)

        def set_col_fmt(colname, fmt):
            if colname in col_idx:
                i = col_idx[colname]
                ws.set_column(i, i, 18, fmt)

        for c in ["Competitor Full Price", "Competitor Final Price", "Nike Full Price", "Nike Final Price",
                  "Competitor Shipping", "Nike Shipping", "Nike Price + Shipping", "Competitor Price + Shipping"]:
            set_col_fmt(c, money_fmt)

        for c in ["Competitor Markdown", "Nike Markdown"]:
            set_col_fmt(c, pct_fmt)

        if "Link PDP Competitor" in col_idx:
            ws.set_column(col_idx["Link PDP Competitor"], col_idx["Link PDP Competitor"], 55, link_fmt)

        for c in ["BML Final Price", "BML Full Price", "BML with Shipping", "BML Cuotas"]:
            if c not in col_idx:
                continue
            i = col_idx[c]
            ws.set_column(i, i, 18, text_fmt)
            ws.conditional_format(1, i, nrows, i, {"type": "text", "criteria": "containing", "value": "Beat", "format": beat_fmt})
            ws.conditional_format(1, i, nrows, i, {"type": "text", "criteria": "containing", "value": "Meet", "format": meet_fmt})
            ws.conditional_format(1, i, nrows, i, {"type": "text", "criteria": "containing", "value": "Lose", "format": lose_fmt})

        ws.freeze_panes(1, 0)


def write_fast_xlsx(df: pd.DataFrame, path: str):
    df = df.copy()
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="DATA")


def _visual_xlsx_worker(df: pd.DataFrame, tmp_path: str):
    try:
        write_visual_xlsx(df, tmp_path)
    except Exception:
        pass


def safe_write_excel_with_timeout(df: pd.DataFrame, final_path: str, timeout_sec: int = 360):
    write_fast_xlsx(df, final_path)
    log(f" XLSX (FAST) written -> {final_path}")
    tmp_visual = final_path.replace(".xlsx", "__VISUAL_TMP__.xlsx")
    log(f" Intentando XLSX VISUAL (timeout={timeout_sec}s) -> {tmp_visual}")
    try:
        import multiprocessing as mp
        ctx = mp.get_context("spawn")
        p   = ctx.Process(target=_visual_xlsx_worker, args=(df, tmp_visual), daemon=True)
        p.start()
        p.join(timeout_sec)
        if p.is_alive():
            try:
                p.terminate()
            except Exception:
                pass
            log(" XLSX VISUAL timeout. Se mantiene FAST.")
            return
        if os.path.exists(tmp_visual) and os.path.getsize(tmp_visual) > 0:
            try:
                os.replace(tmp_visual, final_path)
                log(" XLSX VISUAL OK (reemplazó FAST).")
            except Exception as e:
                log(f" No pude reemplazar XLSX VISUAL: {e}")
        else:
            log(" XLSX VISUAL no se generó correctamente.")
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
    log(" DIGITALSPORT vs NIKE - v8")
    log(f" Season: {SEASON}")
    log(f" Workers paralelos: {AGENTS}")
    log("=" * 70)
    log(f" Headless={HEADLESS} | REFRESH_CACHED={REFRESH_CACHED}")
    if DEBUG_OFFSET > 0: log(f" DEBUG_OFFSET activo: salteando {DEBUG_OFFSET} productos")
    log(f" StatusBooks: {STATUSBOOKS_FILE}")
    log(f" CACHE: {CACHE_PATH}")
    log(f" Export RAW: {OUT_RAW_CSV}")
    log(f" Export XLSX: {OUT_VISUAL_XLSX}")

    # OpenAI
    client = None
    if OPENAI_API_KEY:
        try:
            client = OpenAI(api_key=OPENAI_API_KEY)
            log(" OpenAI inicializado correctamente")
            client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": "Test"}],
                max_tokens=5
            )
            log(" Conexión OpenAI verificada")
        except Exception as e:
            log(f" Error inicializando OpenAI: {e}")
            client = None
    else:
        log(" OpenAI API key no configurada")

    # Cache
    cache = load_cache(CACHE_PATH)
    if not isinstance(cache, dict):
        cache = {}
    log(f" Cache cargado: {len(cache)} entradas")

    # PLPs
    plps_df = load_plps_from_links_excel(LINKS_XLSX, LINKS_SHEET)
    log(f" PLPs cargados: {len(plps_df)}")

    # StatusBooks + Aux
    sb_map  = load_statusbooks_map(STATUSBOOKS_FILE, STATUSBOOKS_SHEET)
    aux_map = load_aux_assort_map(AUX_ASSORT_FILE, AUX_ASSORT_SHEET)

    # ── Stage 1: recolectar URLs de PDPs desde PLPs ──────────────────────────
    log("\n Stage 1: recolectando PDPs desde PLPs (DigitalSport Nike)...")
    # Usamos la URL como clave (igual que soloDeportes v4), NO el stylecolor
    # porque el stylecolor puede ser null/incompleto desde data-sku
    url_meta = {}   # url → { url, categorias, primary_cat, source_plp, ds_sku, productid }

    target_collect = 0

    with sync_playwright() as pw:
        browser, context, _ = new_browser_context(pw)

        for _, r in plps_df.iterrows():
            cat = str(r[LINKS_COL_CATEGORY]).strip()
            plp = str(r[LINKS_COL_LINK]).strip()

            page = context.new_page()
            try:
                links = collect_pdp_links_from_plp(page, plp)
            except PWTimeoutError:
                log(f" Timeout en PLP: {plp} | Salteo.")
                links = []
            except Exception as e:
                log(f" Error en PLP: {plp} | {e} | Salteo.")
                links = []
            finally:
                try:
                    page.close()
                except Exception:
                    pass

            log(f" PDPs Nike en '{cat}': {len(links)}")
            for it in links:
                u      = it.get("url", "")
                ds_sku = it.get("ds_sku", "")
                pid    = it.get("productid", "")
                if not u:
                    continue

                if u not in url_meta:
                    url_meta[u] = {
                        "url":         u,
                        "categorias":  set([cat]),
                        "primary_cat": cat,
                        "source_plp":  plp,
                        "ds_sku":      ds_sku,
                        "productid":   pid,
                    }
                else:
                    url_meta[u]["categorias"].add(cat)
                    if ds_sku and not url_meta[u].get("ds_sku"):
                        url_meta[u]["ds_sku"] = ds_sku
                    if pid and not url_meta[u].get("productid"):
                        url_meta[u]["productid"] = pid

                if target_collect > 0 and len(url_meta) >= target_collect:
                    log(f" DEBUG_LIMIT+OFFSET alcanzado ({target_collect}). Corto recolección.")
                    break

            if target_collect > 0 and len(url_meta) >= target_collect:
                break

        try:
            context.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass

    all_urls = list(url_meta.keys())
    log(f"\n Total PDPs únicas detectadas: {len(all_urls)}")

    # ── Stage 2: scraping PDPs ───────────────────────────────────────────────
    log("\n Stage 2: scraping PDPs + OpenAI para StyleColor...")

    urls_ordered = list(all_urls)
    if DEBUG_OFFSET > 0:
        urls_ordered = urls_ordered[DEBUG_OFFSET:]
    total_target = len(urls_ordered)
    url_to_idx   = {u: i for i, u in enumerate(urls_ordered, start=1)}
    log(f" URLs a procesar en Stage 2: {total_target}")

    cache_lock = threading.Lock()

    def _process_urls_worker(worker_id: int, urls_chunk: list) -> dict:
        local_processed      = 0
        local_scraped_new    = 0
        local_skipped_safe   = 0
        local_errors         = 0

        with sync_playwright() as pw:
            browser, context, page = new_browser_context(pw)

            for url in urls_chunk:
                idx  = url_to_idx.get(url, 0)
                meta = url_meta[url]

                # Pre-calcular stylecolor hint desde data-sku del PLP
                ds_sku_raw   = meta.get("ds_sku", "")
                style_hint   = normalize_stylecolor(extract_stylecolor_from_ds_sku(ds_sku_raw) or "")

                with cache_lock:
                    if url not in cache:
                        cache[url] = {}

                    cache[url]["PLP_Categorias"]      = " | ".join(sorted(list(meta["categorias"])))
                    cache[url]["PLP_PrimaryCategoria"] = meta["primary_cat"]
                    cache[url]["PLP_SourcePLP"]        = meta["source_plp"]
                    cache[url]["digitalsport_URL"]     = url

                    # Verificar estado del cache para esta URL
                    cached_style        = cache[url].get("digitalsport_StyleColor_Norm")
                    already_has_style   = bool(cached_style)   # False si null/vacío
                    already_has_price   = float(cache[url].get("digitalsport_FinalPrice", 0.0)) > VALID_PRICE_MIN

                # Si StyleColor es null en cache → forzar re-scrape aunque REFRESH_CACHED=0
                style_is_null_in_cache = not already_has_style

                if (not REFRESH_CACHED) and already_has_style and already_has_price and not style_is_null_in_cache:
                    with cache_lock:
                        local_skipped_safe += 1
                        atomic_write_json(CACHE_PATH, cache)
                    dex_price = float(cache[url].get("digitalsport_FinalPrice", 0.0))
                    log(f"[{idx}/{total_target}]  (W{worker_id}) SAFE-SKIP {url} | StyleColor={cached_style} | FinalPrice={dex_price:,.2f}")
                    local_processed += 1

                else:
                    log(f"[{idx}/{total_target}]  (W{worker_id}) SCRAPE {url}")
                    try:
                        row = None
                        for _attempt in range(2):
                            try:
                                row = scrape_pdp(
                                    page, url,
                                    style_norm_hint=style_hint,
                                    ds_sku_raw=ds_sku_raw,
                                    client=client,
                                    cache=cache,
                                )
                                break
                            except PWTimeoutError as _te:
                                if _attempt == 0:
                                    browser, context, page = _reset_triplet(
                                        worker_id, pw, browser, context, page, reason="Timeout → reset"
                                    )
                                    continue
                                raise
                            except Exception as _e:
                                if _attempt == 0 and _is_fatal_nav_error(_e):
                                    browser, context, page = _reset_triplet(
                                        worker_id, pw, browser, context, page, reason=str(_e)
                                    )
                                    continue
                                raise

                        with cache_lock:
                            for k, v in row.items():
                                cache[url][k] = v

                            stylecolor = row.get("digitalsport_StyleColor_Norm")

                            # "STYLECOLOR NO ENCONTRADO" es centinela, no un código real
                            stylecolor_real = stylecolor and stylecolor != "STYLECOLOR NO ENCONTRADO"

                            # Matching con StatusBooks (solo con StyleColor real)
                            if stylecolor_real:
                                sb = sb_map.get(stylecolor) or sb_map.get("NI" + stylecolor)
                                if sb:
                                    cache[url]["InStatusBooks"] = True
                                    for k, v in sb.items():
                                        cache[url][k] = v
                                else:
                                    cache[url]["InStatusBooks"] = False
                                    aux = aux_map.get(stylecolor) if isinstance(aux_map, dict) else None
                                    cache[url]["SB_ProductCode"]  = stylecolor
                                    cache[url]["SB_MarketingName"] = (aux.get("SB_MarketingName") if aux else "") or ""
                                    cache[url]["SB_Division"]     = (aux.get("SB_Division") if aux else "") or ""
                                    cache[url]["Aux_AO"]          = (aux.get("Aux_AO") if aux else "") or ""
                                    cache[url]["SB_Category"]     = ""
                                    cache[url]["SB_Franchise"]    = ""
                                    cache[url]["NikeFullPrice"]   = None
                                    cache[url]["NikeSaleDecimal"] = None
                                    cache[url]["NikeFinalPrice"]  = None
                            else:
                                # Sin StyleColor real: no hay match posible
                                cache[url]["InStatusBooks"]  = False
                                cache[url]["SB_ProductCode"] = ""
                                cache[url]["SB_MarketingName"] = ""
                                cache[url]["SB_Division"]    = ""
                                cache[url]["SB_Category"]    = ""
                                cache[url]["SB_Franchise"]   = ""
                                cache[url]["NikeFullPrice"]  = None
                                cache[url]["NikeSaleDecimal"] = None
                                cache[url]["NikeFinalPrice"] = None

                            atomic_write_json(CACHE_PATH, cache)

                        local_scraped_new += 1
                        local_processed   += 1
                        dex_price = float(row.get("digitalsport_FinalPrice", 0))
                        log(f"    StyleColor: {stylecolor or 'NO DETECTADO'} | FinalPrice={dex_price:,.2f}")

                    except PWTimeoutError:
                        with cache_lock:
                            local_errors += 1
                            cache[url]["digitalsport_Error"]       = "Timeout"
                            cache[url]["digitalsport_LastErrorAt"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            atomic_write_json(CACHE_PATH, cache)
                        log("    Timeout en PDP. Guardado error en JSON.")
                        local_processed += 1

                    except Exception as e:
                        with cache_lock:
                            local_errors += 1
                            cache[url]["digitalsport_Error"]       = f"{type(e).__name__}: {e}"
                            cache[url]["digitalsport_LastErrorAt"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            atomic_write_json(CACHE_PATH, cache)
                        log(f"    Error en PDP: {e}")
                        local_processed += 1

                # Reset periódico del browser
                if local_processed > 0 and (local_processed % BROWSER_RESET_EVERY == 0):
                    log(f"\n (W{worker_id}) RESET: {local_processed} items procesados\n")
                    with cache_lock:
                        try:
                            atomic_write_json(CACHE_PATH, cache)
                        except Exception as e:
                            log(f" (W{worker_id}) No pude guardar cache: {e}")
                    for obj in (page, context, browser):
                        try:
                            obj.close()
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
            "processed":    local_processed,
            "scraped_new":  local_scraped_new,
            "skipped_safe": local_skipped_safe,
            "errors":       local_errors,
        }

    # Ejecutar workers
    proc_total   = 0
    scraped_total = 0
    skipped_total = 0
    errors_total  = 0

    if AGENTS <= 1:
        log(" Modo secuencial")
        stats         = _process_urls_worker(worker_id=1, urls_chunk=urls_ordered)
        proc_total    = stats["processed"]
        scraped_total = stats["scraped_new"]
        skipped_total = stats["skipped_safe"]
        errors_total  = stats["errors"]
    else:
        log(f" Modo paralelo: AGENTS={AGENTS}")
        chunks = [urls_ordered[i::AGENTS] for i in range(AGENTS)]
        with ThreadPoolExecutor(max_workers=AGENTS) as ex:
            futs = [ex.submit(_process_urls_worker, wi + 1, ch) for wi, ch in enumerate(chunks) if ch]
            for fut in as_completed(futs):
                st = fut.result()
                proc_total    += st["processed"]
                scraped_total += st["scraped_new"]
                skipped_total += st["skipped_safe"]
                errors_total  += st["errors"]

    # ── Stage 3: exportar reporte ────────────────────────────────────────────
    log("\n Stage 3: exportando reporte...")
    cache = load_cache(CACHE_PATH)

    rows = []
    for url, rec in cache.items():
        if not isinstance(rec, dict):
            continue
        if not rec.get("digitalsport_URL"):
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
        _lu      = df_out["Last Update Competitor"].astype(str).fillna("").str.strip()
        _lu_date = _lu.str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False).fillna(_lu.str.slice(0, 10))
        _before  = len(df_out)
        df_out   = df_out[_lu_date == RUN_DATE].copy()
        log(f" Filtro 'solo actualizados hoy': {_before} -> {len(df_out)} filas")

    df_out.to_csv(OUT_RAW_CSV, index=False, encoding="utf-8-sig")
    log(f" RAW CSV: {OUT_RAW_CSV}")

    safe_write_excel_with_timeout(df_out, OUT_VISUAL_XLSX, timeout_sec=360)
    log(f" Visual XLSX: {OUT_VISUAL_XLSX}")

    log("\n Resumen:")
    log(f"    URLs detectadas (PLPs): {len(all_urls)}")
    log(f"    Procesadas esta corrida: {proc_total}")
    log(f"    Scrapes nuevos: {scraped_total}")
    log(f"    SAFE skips: {skipped_total}")
    log(f"    Errores: {errors_total}")
    log("\n DONE")


if __name__ == "__main__":
    main()
