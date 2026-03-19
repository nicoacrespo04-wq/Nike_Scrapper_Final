import re
import time
import math
import random
import datetime as dt
from dataclasses import dataclass
from typing import Dict, Any, List, Optional, Set, Tuple

import requests
import pandas as pd
from bs4 import BeautifulSoup
import xlsxwriter

# -------------------------------------------------------------------
# CONFIGURACIÓN
# -------------------------------------------------------------------
SEASON = "SP26"
STATUSBOOKS_PATH = "StatusBooks NDDC ARG SP26.xlsb"
LINKS_PLP_PATH = "Links PLP Scrapping.xlsx"


# UI SEARCH FALLBACK (Playwright) – usa el buscador del home cuando /w?q=... no devuelve resultados
UI_SEARCH_FALLBACK = True
UI_SEARCH_HEADLESS = True
UI_SEARCH_SLOWMO_MS = 0
UI_SEARCH_MAX_STYLES_PER_FRANCHISE = 30  # safety
SCRAPING_APPAREL = True
SCRAPING_EQUIPMENT = True

FX_MODE = "oficial"
TASA_CAMBIO_FALLBACK = 1480.0

IVA_AR = 0.21
BANK_FEES = 0.08  # 8%

ARG_FREE_SHIP_THRESHOLD_ARS = 99_000
US_FREE_SHIP_THRESHOLD_USD = 50.0
ARG_SHIPPING_ARS = 8_890
US_SHIPPING_USD = 5.0

MAX_AR_STYLES_PER_SEARCH = 50
MAX_PLP_PRODUCTS_SCAN = 220

FOOTBALL_MAX_PAGES = 6
NONFOOTBALL_MAX_PAGES = 4
APPAREL_MAX_PAGES = 2
EQUIPMENT_MAX_PAGES = 2

APPAREL_TARGET_STYLES = 24
EQUIPMENT_TARGET_STYLES = 16
FOOTBALL_MAX_STYLE_TRIES_PER_KEY = 20

SLEEP_RANGE = (0.1, 0.3)
VERBOSE = True

BML_TOL = 0.02
NIKE_BASE = "https://www.nike.com/w"

KIDS_RE = re.compile(r"(kid|boy|girl|youth|junior)", re.I)
GTX_RE = re.compile(r"\b(gtx|gore\s*tex|gore-tex)\b", re.I)

WANTED_NONFOOTBALL_LINK_SHEETS = {"running", "sportswear", "training", "jordan", "basketball"}

ALLOWED_SINGLE_TOKEN_SLASH_SUFFIX = {
    "mid", "low", "high", "premium", "retro", "next", "platform",
    "vintage", "lx", "se", "sp", "lt",
    "max", "air", "zoom", "reactx", "react", "flyknit", "shield",
}

TEMPLATE_COLS = [
    "fecha", "Division", "Franchise", "Gender", "GAMA BOTINES", "PLATO",
    "Category", "Marketing Name (AR)", "Style", "Nike US Product Name",
    "PDP USA", "Retail ARG (ARS)", "FX ARS/USD", "ARG (USD)", "USA Full (USD)",
    "Dif FP vs USA", "USA + 21% IVA", "Dif FP + IVA", "BML c IVA",
    "USA + 21% + BF 8% (USD)", "Dif FP + 21% + BF", "BML c IVA + BF",
    "AR + Shp", "US + Shp", "Dif", "BML + Shp"
]


# -------------------------------------------------------------------
# UTILIDADES
# -------------------------------------------------------------------
def log(msg: str):
    timestamp = dt.datetime.now().strftime("%H:%M:%S")
    if VERBOSE:
        print(f"[{timestamp}] {msg}", flush=True)


def human_pause(a: float, b: float):
    time.sleep(random.uniform(a, b))


def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str):
            s = x.strip()
            if s == "" or s.upper() == "#N/A":
                return None
            s = s.replace("$", "").replace(",", "").strip()
            if s == "" or s == "-":
                return None
            v = float(s)
        else:
            v = float(x)
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def get_usd_ars_venta(mode: str) -> float:
    mode = (mode or "").strip().lower()
    endpoint_map = {
        "oficial": "https://dolarapi.com/v1/dolares/oficial",
        "mep": "https://dolarapi.com/v1/dolares/bolsa",
        "bolsa": "https://dolarapi.com/v1/dolares/bolsa",
        "blue": "https://dolarapi.com/v1/dolares/blue",
    }
    url = endpoint_map.get(mode, endpoint_map["oficial"])
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
        for k in ("venta", "promedio", "compra"):
            if k in data and isinstance(data[k], (int, float)) and data[k] > 0:
                return float(data[k])
    except Exception:
        pass
    return float(TASA_CAMBIO_FALLBACK)


def bml_label(diff: Optional[float], tol: float = BML_TOL) -> str:
    if diff is None:
        return "NO_US_DATA"
    if abs(diff) <= tol:
        return "MEET"
    if diff < -tol:
        return "BEAT"
    return "LOSE"


def normalize_text_basic(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("’", "'")
    s = re.sub(r"[^a-z0-9\s\-\/]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def tokenize(s: str) -> List[str]:
    s = normalize_text_basic(s)
    s = s.replace("/", " ")
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return []
    return [t for t in s.split(" ") if t]


def canonicalize_goretex_in_tokens(tokens: List[str]) -> List[str]:
    if not tokens:
        return tokens
    out = []
    i = 0
    while i < len(tokens):
        if i + 1 < len(tokens) and tokens[i] == "gore" and tokens[i + 1] == "tex":
            out.append("gtx")
            i += 2
            continue
        if tokens[i] in ("goretex", "gore-tex"):
            out.append("gtx")
            i += 1
            continue
        out.append(tokens[i])
        i += 1
    return out


def expand_token_alternatives(tok: str) -> Set[str]:
    if tok == "gtx":
        return {"gtx", "goretex", "gore", "tex"}
    if tok == "tiempo":
        return {"tiempo", "legend"}
    return {tok}


def tokens_match_all(text: str, query_tokens: List[str]) -> bool:
    """
    Verifica que TODOS los tokens del query aparezcan en el texto.
    No importa el orden, solo que estén todos presentes.
    """
    tt = set(tokenize(text))
    if not tt:
        return False
    for qt in query_tokens:
        alts = expand_token_alternatives(qt)
        if not any(a in tt for a in alts):
            return False
    return True


def slash_rule_ok_text(text: str, base_tokens: List[str]) -> bool:
    """
    Aplica la regla de la /:
    - Si es un solo token, después solo pueden venir números o sufijos permitidos
    - Si son múltiples tokens, después solo pueden venir números
    """
    toks = tokenize(text)
    if not toks or not base_tokens:
        return False
    n = len(base_tokens)
    for i in range(0, len(toks) - n + 1):
        if toks[i: i + n] == base_tokens:
            tail = toks[i + n:]
            if not tail:
                return True
            if len(base_tokens) == 1:
                for t in tail:
                    if re.fullmatch(r"\d+", t):
                        continue
                    if t in ALLOWED_SINGLE_TOKEN_SLASH_SUFFIX:
                        continue
                    return False
                return True
            for t in tail:
                if not re.fullmatch(r"\d+", t):
                    return False
            return True
    return False


def slash_rule_ok_us_title(text: str, base_tokens: List[str]) -> bool:
    """
    Versión para títulos de productos US de la regla '/'.
    La regla sigue siendo: después de los base_tokens solo puede venir un número.
    Pero en US, después de ese número puede haber descriptores (Men's, Road, Shoes, etc.)
    que ignoramos — lo que importa es que lo PRIMERO después del bloque sea un número.

    Ej OK:    "Nike Vomero 18 Men's Road Running Shoes"  → tail=["18","men","s","road",...] → primer tok es número ✓
    Ej OK:    "Nike Air Max 90 Women's Shoes"             → tail=["women","s","shoes"] → base_tokens=["air","max","90"], tail vacío ✓
    Ej FAIL:  "Nike Vomero Plus Road Running Shoes"       → tail=["plus","road",...] → primer tok no es número ✗
    Ej FAIL:  "Nike Vomero Next% 3"                       → tail=["next","3"] → primer tok no es número ✗
    """
    toks = tokenize(text)
    if not toks or not base_tokens:
        return False
    n = len(base_tokens)
    for i in range(0, len(toks) - n + 1):
        if toks[i: i + n] == base_tokens:
            tail = toks[i + n:]
            if not tail:
                return True
            # Si el último token de base_tokens ya es un número (ej: "Air Max 90/"),
            # el tail puede contener descriptores libremente.
            if re.fullmatch(r"\d+", base_tokens[-1]):
                return True
            # Sino, lo primero del tail debe ser el número de modelo.
            first = tail[0]
            if re.fullmatch(r"\d+", first):
                return True
            if first in ALLOWED_SINGLE_TOKEN_SLASH_SUFFIX:
                return True
            return False
    return False


def contains_gtx(text: str) -> bool:
    return bool(GTX_RE.search(text or ""))


def normalize_upper(s: Any) -> str:
    if s is None:
        return ""
    ss = str(s).strip()
    if ss.lower() == "nan":
        return ""
    return ss.upper().strip()


def normalize_plato(plato: str) -> str:
    p = normalize_upper(plato)
    if p in ("TURF", "TF"):
        return "TF"
    if p == "FG":
        return "FG"
    return p


# -------------------------------------------------------------------
# FUNCIONES NUEVAS PARA MANEJO DE STYLES, URLS Y FALLBACK
# -------------------------------------------------------------------
def extract_base_style(style_with_color: str) -> str:
    """
    Extrae el style base de un stylecolor tipo "FJ1287-001"
    Devuelve "FJ1287"
    """
    if not style_with_color:
        return ""
    base = style_with_color.split('-')[0].strip().upper()
    return base


def build_nike_search_url(franchise: str, market: str = "us") -> str:
    """
    Construye URL de búsqueda de Nike a partir del nombre de la franquicia.
    - Elimina la '/' final si existe (solo para la URL)
    - Reemplaza espacios por %20
    """
    clean = franchise.strip().rstrip('/')
    tokens = clean.lower().split()
    query = '%20'.join(tokens)
    return f"https://www.nike.com/w?q={query}"


def extract_model_number(text: str) -> Optional[str]:
    """
    Extrae el primer número que parece ser el modelo (ej: 18 de 'Vomero 18')
    """
    if not text:
        return None
    # Busca números de 1-3 dígitos
    nums = re.findall(r'\b(\d{1,3})\b', text)
    return nums[0] if nums else None


def calculate_text_similarity(text1: str, text2: str) -> float:
    """
    Calcula qué tan similares son dos textos basado en tokens comunes
    Devuelve un score entre 0 y 1
    """
    tokens1 = set(tokenize(text1))
    tokens2 = set(tokenize(text2))
    
    if not tokens1 or not tokens2:
        return 0.0
    
    intersection = tokens1 & tokens2
    union = tokens1 | tokens2
    
    return len(intersection) / len(union)


def fallback_match_by_similarity(
    product_title: str,
    franchise_tokens: List[str],
    ar_marketing_names: Dict[str, str],
    has_slash: bool = False
) -> Optional[Tuple[str, float]]:
    """
    Fallback avanzado: busca el style AR cuyo Marketing Name sea más similar al título US
    Considera:
    1. Debe contener todos los tokens de la franquicia
    2. Si tiene /, aplica regla de sufijos
    3. Compara similitud general del texto
    4. Da bonus si los números coinciden
    """
    us_number = extract_model_number(product_title)
    log(f"            🔍 Fallback por similitud - Número US: {us_number}")
    
    candidates = []
    
    for style, ar_name in ar_marketing_names.items():
        # PASO 1: Verificar que contenga todos los tokens de franquicia
        if not tokens_match_all(ar_name, franchise_tokens):
            continue
        
        # PASO 2: Si tiene slash, aplicar regla
        if has_slash:
            if not slash_rule_ok_text(ar_name, franchise_tokens):
                continue
        
        # PASO 3: Calcular similitud base
        similarity = calculate_text_similarity(product_title, ar_name)
        
        # PASO 4: Bonus por números
        ar_number = extract_model_number(ar_name)
        number_bonus = 0.2 if (us_number and ar_number and us_number == ar_number) else 0
        
        final_score = similarity + number_bonus
        candidates.append((final_score, style, ar_name))
    
    if candidates:
        # Ordenar por score (mayor primero)
        candidates.sort(reverse=True)
        best_score, best_style, best_name = candidates[0]
        log(f"            ✅ Mejor candidato: {best_style} - '{best_name}' (score: {best_score:.2f})")
        return (best_style, best_score)
    
    log(f"            ❌ No se encontraron candidatos por similitud")
    return None


# -------------------------------------------------------------------
# CARGA DE DATOS
# -------------------------------------------------------------------
def load_statusbooks_filtered(path: str, season: str) -> pd.DataFrame:
    log("📚 Cargando StatusBooks...")
    df = pd.read_excel(path, engine="pyxlsb", sheet_name="Books NDDC", header=6)
    df.columns = [str(c).strip() for c in df.columns]
    
    required = [
        "Style", "Product Code", "Marketing Name", "BU", "Category",
        "Gender", "Franchise", "SILO BOTINES", "PLATO",
        "STOCK BL (Inventario Brandlive)", season,
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"StatusBooks missing columns: {missing}")

    df = df.copy()
    df["_price"] = df[season].apply(safe_float)
    df = df[df["_price"].apply(lambda v: v is not None and v > 0)]
    df["_stock"] = df["STOCK BL (Inventario Brandlive)"].apply(lambda x: safe_float(x) or 0.0)

    # ✅ STOCK a nivel STYLE (ANY color): si un style tiene al menos un style-color con stock BL>0, entra.
    # Esto evita falsos negativos cuando el primer Product Code del style tiene stock=0 pero otros colores sí tienen.
    df["Style"] = df["Style"].astype(str).fillna("").map(lambda s: s.strip())
    df["_stock_style_sum"] = df.groupby("Style")["_stock"].transform("sum")
    df = df[df["_stock_style_sum"] > 0].copy()

    for c in ["Style", "Product Code", "Marketing Name", "BU", "Category", "Gender", "Franchise", "SILO BOTINES", "PLATO"]:
        df[c] = df[c].astype(str).fillna("").map(lambda s: s.strip())

    log(f"✅ StatusBooks final: {len(df):,} filas")
    return df


def build_style_price_map(df_sb: pd.DataFrame) -> Dict[str, float]:
    log("💰 Construyendo mapa de precios por Style...")
    first = df_sb.sort_index().groupby("Style")["_price"].first()
    out = {}
    for k, v in first.to_dict().items():
        st = str(k).strip().upper()
        if st and v is not None:
            out[st] = float(v)
    log(f"   ✅ {len(out)} styles con precio")
    return out


def build_style_meta_map(df_sb: pd.DataFrame) -> Dict[str, Dict[str, str]]:
    log("📋 Construyendo mapa de metadatos por Style...")
    g = df_sb.sort_index().groupby("Style").first(numeric_only=False)
    out: Dict[str, Dict[str, str]] = {}
    for style, row in g.iterrows():
        st = str(style).strip().upper()
        if not st:
            continue
        out[st] = {
            "Marketing Name": str(row.get("Marketing Name", "")).strip(),
            "Franchise": str(row.get("Franchise", "")).strip(),
            "Gender": str(row.get("Gender", "")).strip(),
            "BU": str(row.get("BU", "")).strip(),
            "Category": str(row.get("Category", "")).strip(),
            "SILO BOTINES": str(row.get("SILO BOTINES", "")).strip(),
            "PLATO": str(row.get("PLATO", "")).strip(),
        }
    log(f"   ✅ {len(out)} styles con metadatos")
    return out


def load_links_sheets(path: str) -> Dict[str, pd.DataFrame]:
    log("🔗 Cargando sheets de Links PLP...")
    xls = pd.ExcelFile(path)
    sheets: Dict[str, pd.DataFrame] = {}
    for sh in xls.sheet_names:
        df = pd.read_excel(path, sheet_name=sh)
        df.columns = [str(c).strip() for c in df.columns]
        sheets[sh] = df
        log(f"   Sheet '{sh}': {len(df)} filas, columnas: {list(df.columns)}")
    return sheets


def selected_nonfootball_link_sheets(links_sheets: Dict[str, pd.DataFrame]) -> List[str]:
    return [
        sh for sh in links_sheets.keys()
        if str(sh).strip().lower() in WANTED_NONFOOTBALL_LINK_SHEETS
    ]


# -------------------------------------------------------------------
# FILTROS SOBRE STATUSBOOKS
# -------------------------------------------------------------------
def filter_nonfootball_sb(df_sb: pd.DataFrame) -> pd.DataFrame:
    df = df_sb.copy()
    df["_is_kids"] = df["Gender"].apply(lambda x: bool(KIDS_RE.search(str(x or ""))))
    df = df[~df["_is_kids"]]
    df["_is_football"] = df["BU"].astype(str).str.contains("FW", case=False, na=False) & df["Category"].astype(str).str.contains(
        "FOOTBALL|SOCCER", case=False, na=False
    )
    df = df[~df["_is_football"]]
    df = df[~df["BU"].astype(str).str.contains("AP", case=False, na=False)]
    df = df[~df["BU"].astype(str).str.contains("EQ", case=False, na=False)]
    return df


def filter_football_sb(df_sb: pd.DataFrame) -> pd.DataFrame:
    df = df_sb.copy()
    df["_is_football"] = df["BU"].astype(str).str.contains("FW", case=False, na=False) & df["Category"].astype(str).str.contains(
        "FOOTBALL|SOCCER", case=False, na=False
    )
    return df[df["_is_football"]]


def filter_apparel_sb(df_sb: pd.DataFrame) -> pd.DataFrame:
    df = df_sb.copy()
    df["_is_kids"] = df["Gender"].apply(lambda x: bool(KIDS_RE.search(str(x or ""))))
    df = df[~df["_is_kids"]]
    df["_is_football"] = df["BU"].astype(str).str.contains("FW", case=False, na=False) & df["Category"].astype(str).str.contains(
        "FOOTBALL|SOCCER", case=False, na=False
    )
    df = df[~df["_is_football"]]
    df = df[df["BU"].astype(str).str.contains("AP", case=False, na=False)]
    return df


def filter_equipment_sb(df_sb: pd.DataFrame) -> pd.DataFrame:
    df = df_sb.copy()
    df["_is_kids"] = df["Gender"].apply(lambda x: bool(KIDS_RE.search(str(x or ""))))
    df = df[~df["_is_kids"]]
    df["_is_football"] = df["BU"].astype(str).str.contains("FW", case=False, na=False) & df["Category"].astype(str).str.contains(
        "FOOTBALL|SOCCER", case=False, na=False
    )
    df = df[~df["_is_football"]]
    df = df[df["BU"].astype(str).str.contains("EQ", case=False, na=False)]
    return df


def filter_kids_sb(df_sb: pd.DataFrame) -> pd.DataFrame:
    """Devuelve solo filas donde Gender indica kids (excluye football)."""
    df = df_sb.copy()
    df["_is_kids"] = df["Gender"].apply(lambda x: bool(KIDS_RE.search(str(x or ""))))
    df["_is_football"] = df["BU"].astype(str).str.contains("FW", case=False, na=False) & df["Category"].astype(str).str.contains(
        "FOOTBALL|SOCCER", case=False, na=False
    )
    return df[df["_is_kids"] & ~df["_is_football"]].copy()


def build_ar_styles_by_marketing_name(df_nf: pd.DataFrame, franquicia_excel: str, cap_styles: int = 50) -> List[str]:
    """
    Filtra StatusBooks por Marketing Name
    - Toma la franquicia del Excel (ej: "Pegasus Plus/")
    - Saca la / para buscar (ej: "Pegasus Plus")
    - Busca Marketing Names que contengan TODAS las palabras (ej: "pegasus" Y "plus")
    - Si tiene /, aplica la regla de números/sufijos
    - Devuelve lista de styles que cumplen y tienen precio
    """
    fr_raw = (franquicia_excel or "").strip()
    has_slash = fr_raw.endswith("/")
    fr_no_slash = fr_raw[:-1].strip() if has_slash else fr_raw.strip()
    
    q_tokens = canonicalize_goretex_in_tokens(tokenize(fr_no_slash))
    base_tokens = q_tokens.copy()
    
    log(f"      Buscando styles para '{fr_raw}' (has_slash={has_slash})")
    log(f"         Tokens requeridos: {q_tokens}")
    
    if not q_tokens:
        log(f"         ⚠️ Sin tokens válidos")
        return []
    
    styles: List[str] = []
    seen = set()
    
    for _, r in df_nf.iterrows():
        st = str(r.get("Style", "")).strip().upper()
        if not st or st in seen:
            continue
            
        mn = str(r.get("Marketing Name", "")).strip()
        if not mn:
            continue
        
        if not tokens_match_all(mn, q_tokens):
            continue
        
        if has_slash:
            if not slash_rule_ok_text(mn, base_tokens):
                continue

        # Si la franquicia no contiene "plus" o "premium", el Marketing Name tampoco puede
        mn_toks = tokenize(mn)
        skip = False
        for qualifier in ("plus", "premium"):
            if qualifier not in q_tokens and qualifier in mn_toks:
                skip = True
                break
        if skip:
            continue

        styles.append(st)
        seen.add(st)

        if len(styles) >= cap_styles:
            break
    
    log(f"         ✅ Encontrados {len(styles)} styles")
    if styles:
        log(f"         Ejemplos: {styles[:5]}")
    
    return styles


def build_ar_kids_styles(df_kids: pd.DataFrame, franquicia_excel: str, cap_styles: int = 50) -> List[str]:
    """
    Como build_ar_styles_by_marketing_name pero para kids:
    - El StatusBook no incluye 'Kids' en Marketing Name, solo en Gender.
    - Stripea 'kids' del nombre de franquicia antes de tokenizar para que
      el match funcione contra Marketing Names tipo 'Nike Air Force 1'.
    - Usa un DataFrame ya filtrado solo a kids (filter_kids_sb).
    """
    fr_raw = (franquicia_excel or "").strip()
    has_slash = fr_raw.endswith("/")
    fr_no_slash = fr_raw[:-1].strip() if has_slash else fr_raw.strip()

    # Remover 'kids' (no está en Marketing Name del StatusBook)
    fr_no_kids = re.sub(r"\s*\bkids?\b\s*", " ", fr_no_slash, flags=re.I).strip()

    q_tokens = canonicalize_goretex_in_tokens(tokenize(fr_no_kids))
    base_tokens = q_tokens.copy()

    log(f"      Buscando styles KIDS para '{fr_raw}' → sin kids: '{fr_no_kids}'")
    log(f"         Tokens requeridos: {q_tokens}")

    if not q_tokens:
        log(f"         ⚠️ Sin tokens válidos")
        return []

    styles: List[str] = []
    seen: Set[str] = set()

    for _, r in df_kids.iterrows():
        st = str(r.get("Style", "")).strip().upper()
        if not st or st in seen:
            continue

        mn = str(r.get("Marketing Name", "")).strip()
        if not mn:
            continue

        if not tokens_match_all(mn, q_tokens):
            continue

        if has_slash:
            if not slash_rule_ok_text(mn, base_tokens):
                continue

        mn_toks = tokenize(mn)
        skip = False
        for qualifier in ("plus", "premium"):
            if qualifier not in q_tokens and qualifier in mn_toks:
                skip = True
                break
        if skip:
            continue

        styles.append(st)
        seen.add(st)

        if len(styles) >= cap_styles:
            break

    log(f"         ✅ Encontrados {len(styles)} styles kids")
    if styles:
        log(f"         Ejemplos: {styles[:5]}")

    return styles


# -------------------------------------------------------------------
# SCRAPING
# -------------------------------------------------------------------
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
        ]),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Connection": "keep-alive",
    })
    return s


def fetch_html(session: requests.Session, url: str, max_retries: int = 2, timeout: int = 15) -> str:
    for i in range(max_retries):
        try:
            log(f"         🌍 Fetching: {url} (intento {i+1}/{max_retries})")
            r = session.get(url, timeout=timeout)
            
            if r.status_code == 404:
                log(f"         ⚠️ 404 Not Found - el producto no existe")
                return ""
                
            if r.status_code in (403, 429, 503):
                wait = (2**i) + random.uniform(1, 2)
                log(f"         ⚠️ Status {r.status_code}, esperando {wait:.2f}s")
                time.sleep(wait)
                continue
                
            r.raise_for_status()
            log(f"         ✅ {len(r.text)} bytes recibidos")
            return r.text
            
        except requests.exceptions.Timeout:
            wait = (2**i) + random.uniform(1, 2)
            log(f"         ⚠️ Timeout, esperando {wait:.2f}s")
            time.sleep(wait)
        except Exception as e:
            if "404" in str(e):
                log(f"         ⚠️ 404 Not Found")
                return ""
            wait = (2**i) + random.uniform(0.5, 1.5)
            log(f"         ⚠️ Error: {e}, esperando {wait:.2f}s")
            time.sleep(wait)
    
    raise RuntimeError(f"Failed fetch {url} after {max_retries} attempts")


def normalize_url(url: str) -> str:
    if not url:
        return ""
    url = url.strip()
    if url.startswith("//"):
        url = "https:" + url
    if url.startswith("/"):
        url = NIKE_BASE + url
    return url


def extract_style_from_url(url: str) -> Optional[str]:
    """
    Extrae el stylecolor completo de la URL (ej: "FJ1287-001")
    """
    if not url:
        return None
    m = re.search(r"/([A-Z0-9]{3,10})-([0-9A-Z]{2,6})(?:$|\?|#)", url, re.I)
    if m:
        return f"{m.group(1)}-{m.group(2)}".upper()
    return None


def parse_first_price(text: str) -> Optional[float]:
    if not text:
        return None
    m = re.findall(r"\$[\s]*([0-9]+(?:\.[0-9]{1,2})?)", text.replace(",", ""))
    if not m:
        return None
    try:
        return float(m[0])
    except Exception:
        return None


def is_full_price(price_text: str) -> bool:
    if not price_text:
        return False
    nums = re.findall(r"\$[\s]*[0-9]+(?:\.[0-9]{1,2})?", price_text)
    return len(nums) == 1


@dataclass
class PLPProduct:
    title: str
    pdp_url: str
    style_color: str
    style_base: str
    price_usd: Optional[float]
    full_price_flag: bool
    sold_out_flag: bool


def parse_plp_products_from_html(html: str, max_cards: int = 220) -> List[PLPProduct]:
    if not html or len(html) < 1000:
        log(f"         ⚠️ HTML demasiado pequeño, posiblemente sin resultados")
        return []
        
    soup = BeautifulSoup(html, "html.parser")
    products: List[PLPProduct] = []
    
    cards = soup.select('div[data-testid="product-card"]')
    if not cards:
        cards = soup.select("div.product-card") or soup.select('div[class*="product-card"]')
    
    log(f"         📦 Encontradas {len(cards)} tarjetas de producto")
    
    for idx, card in enumerate(cards[:max_cards]):
        try:
            a = card.select_one('a[href*="/t/"]')
            if not a:
                continue
            
            href = normalize_url(a.get("href", ""))
            if "/t/" not in href:
                continue
            
            title_el = (
                card.select_one('div[data-testid="product-card__title"]')
                or card.select_one("div.product-card__title")
                or card.select_one("a")
            )
            title = (title_el.get_text(" ", strip=True) if title_el else "").strip()
            
            style_color = extract_style_from_url(href) or ""
            style_base = extract_base_style(style_color)
            
            price_block = card.select_one('div[data-testid="product-price"]') or card.select_one('div[class*="product-price"]')
            price_text = (price_block.get_text(" ", strip=True) if price_block else "").strip()
            
            fp = is_full_price(price_text)
            price = parse_first_price(price_text)
            
            card_text = card.get_text(" ", strip=True).lower()
            sold_out = ("sold out" in card_text) or ("coming soon" in card_text)
            
            if style_base:
                products.append(
                    PLPProduct(
                        title=title,
                        pdp_url=href,
                        style_color=style_color,
                        style_base=style_base,
                        price_usd=price,
                        full_price_flag=fp,
                        sold_out_flag=sold_out,
                    )
                )
                
                if idx < 3:
                    log(f"            Producto {idx+1}: {title[:30]}... | style_base={style_base} | price=${price}")
                    
        except Exception as e:
            log(f"            ⚠️ Error parseando producto: {e}")
            continue
    
    log(f"         ✅ Parseados {len(products)} productos válidos")
    return products


# -------------------------------------------------------------------
# UI SEARCH FALLBACK (Playwright)
# - Abre nike.com, usa el buscador del header y parsea los product-cards renderizados
# - Devuelve lista de PLPProduct usando el MISMO parser HTML (consistencia de flags/precio)
# -------------------------------------------------------------------
class NikeUISearcher:
    def __init__(self, headless: bool = True, slowmo_ms: int = 0):
        self.headless = headless
        self.slowmo_ms = slowmo_ms
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def start(self):
        if self._page is not None:
            return
        try:
            from playwright.sync_api import sync_playwright
        except Exception as e:
            raise RuntimeError(
                "Playwright no está instalado o no está disponible. "
                "Instalá con: pip install playwright && playwright install"
            ) from e

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=self.headless, slow_mo=self.slowmo_ms)
        self._context = self._browser.new_context(
            viewport={"width": 1400, "height": 900},
            locale="en-US",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(30000)

        self._page.goto("https://www.nike.com/w", wait_until="domcontentloaded")
        human_pause(0.8, 1.4)
        self._try_accept_cookies()

    def close(self):
        try:
            if self._context:
                self._context.close()
        except Exception:
            pass
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass
        try:
            if self._pw:
                self._pw.stop()
        except Exception:
            pass
        self._pw = self._browser = self._context = self._page = None

    def _try_accept_cookies(self):
        if not self._page:
            return
        candidates = [
            "button:has-text('Accept')",
            "button:has-text('I Agree')",
            "button:has-text('Agree')",
            "button:has-text('Aceptar')",
            "button:has-text('Acepto')",
            "button:has-text('OK')",
        ]
        for sel in candidates:
            try:
                btn = self._page.locator(sel).first
                if btn.count() > 0 and btn.is_visible():
                    btn.click(timeout=2000)
                    human_pause(0.4, 0.8)
                    return
            except Exception:
                pass

    def search_products(self, query: str, max_cards: int = 220) -> List[PLPProduct]:
        """Busca usando la UI y devuelve productos parseados del HTML renderizado."""
        q = (query or "").strip()
        if not q:
            return []
        self.start()
        page = self._page

        # abrir buscador
        try:
            btn = page.locator("button[aria-label='Search']").first
            if btn.count() > 0 and btn.is_visible():
                btn.click(timeout=8000)
        except Exception:
            pass

        human_pause(0.3, 0.7)

        # input
        try:
            inp = page.locator("#gn-search-input").first
            inp.wait_for(state="visible", timeout=12000)
            inp.click()
            inp.fill("")
            inp.type(q, delay=35)
            inp.press("Enter")
        except Exception as e:
            log(f"         ❌ UI search: no pude tipear query='{q}': {e}")
            return []

        # esperar resultados renderizados
        try:
            page.wait_for_load_state("domcontentloaded", timeout=20000)
        except Exception:
            pass
        human_pause(1.0, 1.8)

        html = page.content()
        prods = parse_plp_products_from_html(html, max_cards=max_cards)
        return prods


def ui_search_products_fallback(ui: Optional[NikeUISearcher], query: str, max_cards: int = 220) -> Tuple[Optional[NikeUISearcher], List[PLPProduct]]:
    """Helper seguro: crea el searcher cuando se necesita."""
    if not UI_SEARCH_FALLBACK:
        return ui, []
    try:
        if ui is None:
            ui = NikeUISearcher(headless=UI_SEARCH_HEADLESS, slowmo_ms=UI_SEARCH_SLOWMO_MS)
        prods = ui.search_products(query=query, max_cards=max_cards)
        return ui, prods
    except Exception as e:
        log(f"         ⚠️ UI SEARCH fallback falló para query='{query}': {e}")
        return ui, []

def paginate_url(url: str, page: int) -> str:
    if page <= 1:
        return url
    if "page=" in url:
        return re.sub(r"([?&])page=\d+", r"\1page=" + str(page), url)
    sep = "&" if "?" in url else "?"
    return url + f"{sep}page={page}"


def scrape_plp_products_paged(
    session: requests.Session, 
    url: str, 
    pages: int, 
    max_cards: int = 220,
    target_style: str = None
) -> List[PLPProduct]:
    """
    Scrapea páginas de PLP, con opción de detenerse al encontrar un style específico
    """
    all_products: List[PLPProduct] = []
    seen = set()
    
    log(f"      📑 Scrapeando {pages} páginas...")
    
    for p in range(1, pages + 1):
        u = paginate_url(url, p)
        log(f"         Página {p}: {u}")
        
        try:
            html = fetch_html(session, u)
            products = parse_plp_products_from_html(html, max_cards=max_cards)
        except Exception as e:
            log(f"         ❌ Error en página {p}: {e}")
            continue
        
        new_count = 0
        for pr in products:
            key = pr.pdp_url
            if key in seen:
                continue
            seen.add(key)
            all_products.append(pr)
            new_count += 1
            
            if target_style and pr.style_base == target_style:
                log(f"         🎯 Style {target_style} encontrado! Deteniendo búsqueda.")
                return all_products
        
        log(f"         ➕ {new_count} productos nuevos (total: {len(all_products)})")
        
        if new_count == 0 and p > 1:
            log(f"         ⏹️ Sin productos nuevos, deteniendo.")
            break
            
        human_pause(0.05, 0.15)
    
    log(f"      ✅ Total productos únicos: {len(all_products)}")
    return all_products


def extract_max_model_number(title: str) -> int:
    if not title:
        return -1
    nums = re.findall(r"\b(\d{1,3})\b", title)
    if not nums:
        return -1
    try:
        return max(int(n) for n in nums)
    except Exception:
        return -1


def choose_product_from_plp(
    products: List[PLPProduct], 
    franquicia_raw: str, 
    valid_styles_set: Set[str],
    style_meta_map: Dict[str, Dict[str, str]]
) -> Optional[PLPProduct]:
    """
    Elige el mejor producto de USA con lógica mejorada:
    - PASO 1: Match por STYLE BASE
    - PASO 2: Fallback por MARKETING NAME
    - PASO 3: Fallback por SIMILITUD (para casos como Vomero, Alphafly)
    """
    fr_raw = (franquicia_raw or "").strip()
    has_slash = fr_raw.endswith('/')
    fr_no_slash = fr_raw[:-1].strip() if has_slash else fr_raw.strip()

    q_tokens = canonicalize_goretex_in_tokens(tokenize(fr_no_slash))
    base_tokens = q_tokens.copy()

    log(f"          🔍 Buscando para '{fr_raw}'")
    log(f"             Tokens: {q_tokens}, has_slash={has_slash}")
    log(f"             Styles válidos AR: {len(valid_styles_set)}")

    # --------------------------------------------------------------
    # PASO 1: Match por STYLE BASE
    # --------------------------------------------------------------
    candidates_by_style: List[PLPProduct] = []
    
    for p in products:
        if p.sold_out_flag:
            continue
        if not p.full_price_flag:
            continue
        if not p.price_usd or p.price_usd <= 0:
            continue
        
        if p.style_base not in valid_styles_set:
            continue
        
        if not tokens_match_all(p.title or "", q_tokens):
            continue

        if has_slash:
            if contains_gtx(p.title or ""):
                continue
            if not slash_rule_ok_us_title(p.title or "", base_tokens):
                continue

        # Si la franquicia no contiene "plus" o "premium", el título US tampoco puede
        title_toks = tokenize(p.title or "")
        skip = False
        for qualifier in ("plus", "premium"):
            if qualifier not in q_tokens and qualifier in title_toks:
                skip = True
                break
        if skip:
            continue

        candidates_by_style.append(p)
    
    if candidates_by_style:
        log(f"             ✅ {len(candidates_by_style)} candidatos por STYLE BASE")
        candidates_by_style.sort(key=lambda x: (extract_max_model_number(x.title), x.price_usd or 0.0), reverse=True)
        best = candidates_by_style[0]
        log(f"             🎯 Mejor: {best.title[:50]} | style_base={best.style_base} | price=${best.price_usd}")
        return best

    # --------------------------------------------------------------
    # PASO 2: Fallback por MARKETING NAME
    # --------------------------------------------------------------
    log(f"             ⚠️ Sin matches por style, intentando por Marketing Name...")
    
    candidates_by_name: List[PLPProduct] = []
    
    for p in products:
        if p.sold_out_flag:
            continue
        if not p.full_price_flag:
            continue
        if not p.price_usd or p.price_usd <= 0:
            continue
        
        if not tokens_match_all(p.title or "", q_tokens):
            continue

        if has_slash:
            if contains_gtx(p.title or ""):
                continue
            if not slash_rule_ok_us_title(p.title or "", base_tokens):
                continue

        # Si la franquicia no contiene "plus" o "premium", el título US tampoco puede
        title_toks = tokenize(p.title or "")
        skip = False
        for qualifier in ("plus", "premium"):
            if qualifier not in q_tokens and qualifier in title_toks:
                skip = True
                break
        if skip:
            continue

        candidates_by_name.append(p)
    
    if candidates_by_name:
        log(f"             ✅ {len(candidates_by_name)} candidatos por MARKETING NAME")
        candidates_by_name.sort(key=lambda x: (extract_max_model_number(x.title), x.price_usd or 0.0), reverse=True)
        best = candidates_by_name[0]
        log(f"             🎯 Mejor por nombre: {best.title[:50]} | style_base={best.style_base} | price=${best.price_usd}")
        return best

    # --------------------------------------------------------------
    # PASO 3: Fallback por SIMILITUD (para Vomero, Alphafly, etc.)
    # --------------------------------------------------------------
    log(f"             ⚠️ Sin matches por nombre, intentando Fallback por Similitud...")
    
    # Preparar diccionario de Marketing Names AR
    ar_marketing_names = {
    style: meta.get("Marketing Name", "") 
    for style, meta in style_meta_map.items()
    if meta.get("Marketing Name", "")  # Solo los que tienen nombre
    }
    
    best_candidate = None
    best_score = 0
    
    for p in products:
        if p.sold_out_flag:
            continue
        if not p.full_price_flag:
            continue
        if not p.price_usd or p.price_usd <= 0:
            continue
        
        # Si la franquicia no contiene "plus" o "premium", el título US tampoco puede
        title_toks_fb = tokenize(p.title or "")
        skip_fb = False
        for qualifier in ("plus", "premium"):
            if qualifier not in q_tokens and qualifier in title_toks_fb:
                skip_fb = True
                break
        if skip_fb:
            continue

        result = fallback_match_by_similarity(
            p.title,
            q_tokens,
            ar_marketing_names,
            has_slash
        )
        
        if result:
            matched_style, score = result
            if score > best_score:
                best_score = score
                # Crear copia del producto con el style AR asignado
                p.style_base = matched_style
                best_candidate = p
    
    if best_candidate:
        log(f"             🎯 Mejor por similitud (score: {best_score:.2f}): {best_candidate.title[:50]}")
        log(f"                Style AR asignado: {best_candidate.style_base}")
        return best_candidate

    log(f"             ❌ No se encontraron candidatos")
    return None


# -------------------------------------------------------------------
# CONSTRUCCIÓN DE ROWS
# -------------------------------------------------------------------
def make_row(
    *,
    run_date: str,
    division: str,
    franchise: str,
    category: str,
    marketing_ar: str,
    style: str,
    us_name: str,
    pdp: str,
    retail_ars: Optional[float],
    fx_ars_per_usd: float,
    gender: str = "",
    silo: str = "",
    plato: str = "",
    us_full: Optional[float] = None,
) -> Dict[str, Any]:
    arg_usd = (float(retail_ars) / fx_ars_per_usd) if (retail_ars is not None and fx_ars_per_usd) else None
    dif_fp_vs_us = (arg_usd / us_full - 1.0) if (arg_usd is not None and us_full) else None

    us_iva = (us_full * (1.0 + IVA_AR)) if us_full else None
    dif_fp_iva = (arg_usd / us_iva - 1.0) if (arg_usd is not None and us_iva) else None
    bml_iva = bml_label(dif_fp_iva, tol=BML_TOL)

    us_tax_bf = (us_full * (1.0 + IVA_AR) * (1.0 + BANK_FEES)) if us_full else None
    dif_fp_tax_bf = (arg_usd / us_tax_bf - 1.0) if (arg_usd is not None and us_tax_bf) else None
    bml_tax_bf = bml_label(dif_fp_tax_bf, tol=BML_TOL)

    ship_ar_usd = None
    if retail_ars is not None and fx_ars_per_usd:
        ship_ar_ars = 0.0 if float(retail_ars) >= ARG_FREE_SHIP_THRESHOLD_ARS else float(ARG_SHIPPING_ARS)
        ship_ar_usd = ship_ar_ars / float(fx_ars_per_usd)

    ship_us_total = None
    if us_full is not None:
        ship_us_base = 0.0 if float(us_full) >= US_FREE_SHIP_THRESHOLD_USD else float(US_SHIPPING_USD)
        ship_us_total = ship_us_base * (1.0 + IVA_AR) * (1.0 + BANK_FEES)

    ar_plus_shp = (arg_usd + ship_ar_usd) if (arg_usd is not None and ship_ar_usd is not None) else None
    us_plus_shp = (us_tax_bf + ship_us_total) if (us_tax_bf is not None and ship_us_total is not None) else None
    dif_shp = (ar_plus_shp / us_plus_shp - 1.0) if (ar_plus_shp is not None and us_plus_shp) else None
    bml_shp = bml_label(dif_shp, tol=BML_TOL)

    row = {
        "fecha": run_date,
        "Division": division,
        "Franchise": franchise,
        "Gender": gender,
        "GAMA BOTINES": silo,
        "PLATO": plato,
        "Category": category,
        "Marketing Name (AR)": marketing_ar,
        "Style": style,
        "Nike US Product Name": us_name,
        "PDP USA": pdp,
        "Retail ARG (ARS)": retail_ars,
        "FX ARS/USD": fx_ars_per_usd,
        "ARG (USD)": arg_usd,
        "USA Full (USD)": us_full,
        "Dif FP vs USA": dif_fp_vs_us,
        "USA + 21% IVA": us_iva,
        "Dif FP + IVA": dif_fp_iva,
        "BML c IVA": bml_iva,
        "USA + 21% + BF 8% (USD)": us_tax_bf,
        "Dif FP + 21% + BF": dif_fp_tax_bf,
        "BML c IVA + BF": bml_tax_bf,
        "AR + Shp": ar_plus_shp,
        "US + Shp": us_plus_shp,
        "Dif": dif_shp,
        "BML + Shp": bml_shp,
    }
    for c in TEMPLATE_COLS:
        row.setdefault(c, None)
    return row


# -------------------------------------------------------------------
# CONSTRUCCIÓN DE OUTPUTS - NON FOOTBALL
# -------------------------------------------------------------------
def build_nonfootball_output(
    session: requests.Session,
    df_sb: pd.DataFrame,
    style_price_map: Dict[str, float],
    style_meta_map: Dict[str, Dict[str, str]],
    links_sheets: Dict[str, pd.DataFrame],
    fx_ars_per_usd: float,
    run_date: str,
) -> List[Dict[str, Any]]:
    out_rows: List[Dict[str, Any]] = []
    df_nf = filter_nonfootball_sb(df_sb)

    for sheet in selected_nonfootball_link_sheets(links_sheets):
        
        df_links = links_sheets[sheet].copy()
        df_links.columns = [str(c).strip() for c in df_links.columns]
        
        has_url = "URL_US" in df_links.columns
        
        log(f"\n🌎 [NON-FOOTBALL] sheet={sheet} | {len(df_links)} franquicias")
        
        for idx, r in df_links.iterrows():
            franquicia = str(r.get("Franquicia", "")).strip()
            if not franquicia:
                continue

            # Kids se procesan en build_kids_output()
            if KIDS_RE.search(franquicia):
                continue

            log(f"\n   🔍 Procesando: '{franquicia}' ({idx+1}/{len(df_links)})")
            
            if has_url:
                url_us = str(r.get("URL_US", "")).strip()
                if not url_us:
                    url_us = build_nike_search_url(franquicia)
            else:
                url_us = build_nike_search_url(franquicia)

            styles_list = build_ar_styles_by_marketing_name(df_nf, franquicia, cap_styles=MAX_AR_STYLES_PER_SEARCH)
            valid_styles = set([s.upper() for s in styles_list])
            
            if not valid_styles:
                log(f"      ⚠️ No se encontraron styles argentinos para '{franquicia}'")
                out_rows.append(
                    make_row(
                        run_date=run_date,
                        division="FW",
                        franchise=franquicia,
                        category=sheet,
                        marketing_ar="",
                        style="",
                        us_name="",
                        pdp="",
                        retail_ars=None,
                        fx_ars_per_usd=fx_ars_per_usd,
                        us_full=None,
                    )
                )
                human_pause(*SLEEP_RANGE)
                continue

            pick = None
            try:
                log(f"      Scrapeando URL: {url_us}")
                products = scrape_plp_products_paged(
                    session, url_us, 
                    pages=NONFOOTBALL_MAX_PAGES, 
                    max_cards=MAX_PLP_PRODUCTS_SCAN
                )
                log(f"      Total productos encontrados: {len(products)}")
                
                if products:
                    pick = choose_product_from_plp(products, franquicia, valid_styles, style_meta_map)
                else:
                    log(f"      ⚠️ No se encontraron productos en la PLP")
                    
            except Exception as e:
                log(f"      ❌ Error scraping PLP: {e}")
                pick = None


            # ----------------------------------------------------------
            # FALLBACK EXTRA (UI SEARCH): si no hubo pick desde PLP, buscar
            # cada STYLE argentino (que pasó filtros) directamente en el
            # buscador del home de Nike US. Esto evita issues de paginado
            # /w?q=... y markup server-side incompleto.
            # ----------------------------------------------------------
            if pick is None and valid_styles:
                log(f"      🧩 Fallback UI Search: buscando {min(len(valid_styles), UI_SEARCH_MAX_STYLES_PER_FRANCHISE)} styles en el buscador del home...")
                ui = None
                combined: List[PLPProduct] = []
                for j, st in enumerate(list(valid_styles)[:UI_SEARCH_MAX_STYLES_PER_FRANCHISE], start=1):
                    log(f"         🔎 UI search ({j}) style={st}")
                    ui, ui_products = ui_search_products_fallback(ui, query=st, max_cards=MAX_PLP_PRODUCTS_SCAN)
                    if not ui_products:
                        continue
                    # quedarnos con productos cuyo style_base matchee
                    for p in ui_products:
                        if p.style_base and p.style_base.upper() == st.upper():
                            combined.append(p)
                try:
                    if ui:
                        ui.close()
                except Exception:
                    pass

                # de-dup por URL
                dedup = {}
                for p in combined:
                    dedup[p.pdp_url] = p
                combined = list(dedup.values())
                log(f"      🧩 UI Search products (post-filter): {len(combined)}")

                if combined:
                    pick = choose_product_from_plp(combined, franquicia, valid_styles, style_meta_map)

            if pick is None:
                log(f"      ❌ No se encontró producto válido para '{franquicia}'")
                out_rows.append(
                    make_row(
                        run_date=run_date,
                        division="FW",
                        franchise=franquicia,
                        category=sheet,
                        marketing_ar="",
                        style="",
                        us_name="",
                        pdp="",
                        retail_ars=None,
                        fx_ars_per_usd=fx_ars_per_usd,
                        us_full=None,
                    )
                )
            else:
                style = pick.style_base
                ar_ars = style_price_map.get(style)
                meta = style_meta_map.get(style, {})
                marketing = meta.get("Marketing Name", "")
                
                log(f"      ✅ Producto seleccionado: {pick.title}")
                log(f"         Style: {style}, Precio US: ${pick.price_usd}, Precio AR: ${ar_ars}")
                
                out_rows.append(
                    make_row(
                        run_date=run_date,
                        division="FW",
                        franchise=franquicia,
                        category=sheet,
                        marketing_ar=marketing,
                        style=style,
                        us_name=pick.title,
                        pdp=pick.pdp_url,
                        retail_ars=float(ar_ars) if ar_ars is not None else None,
                        fx_ars_per_usd=fx_ars_per_usd,
                        us_full=float(pick.price_usd) if pick.price_usd is not None else None,
                    )
                )
            
            human_pause(*SLEEP_RANGE)
    
    return out_rows


def build_top_styles_by_stock(df: pd.DataFrame) -> List[str]:
    stock_by_style = df.groupby("Style")["_stock"].sum().sort_values(ascending=False)
    return [str(s).strip().upper() for s in stock_by_style.index.tolist() if str(s).strip()]


# -------------------------------------------------------------------
# CONSTRUCCIÓN DE OUTPUTS - KIDS
# -------------------------------------------------------------------
def build_kids_output(
    session: requests.Session,
    df_sb: pd.DataFrame,
    style_price_map: Dict[str, float],
    style_meta_map: Dict[str, Dict[str, str]],
    links_sheets: Dict[str, pd.DataFrame],
    fx_ars_per_usd: float,
    run_date: str,
) -> List[Dict[str, Any]]:
    out_rows: List[Dict[str, Any]] = []
    df_kids = filter_kids_sb(df_sb)

    log(f"\n👦 [KIDS] StatusBooks kids: {len(df_kids)} filas")

    for sheet in selected_nonfootball_link_sheets(links_sheets):

        df_links = links_sheets[sheet].copy()
        df_links.columns = [str(c).strip() for c in df_links.columns]
        has_url = "URL_US" in df_links.columns

        # Solo filas cuya franquicia sea kids
        kids_mask = df_links["Franquicia"].apply(lambda x: bool(KIDS_RE.search(str(x or ""))))
        df_kids_links = df_links[kids_mask]

        if df_kids_links.empty:
            continue

        log(f"\n👦 [KIDS] sheet={sheet} | {len(df_kids_links)} franquicias kids")

        for idx, r in df_kids_links.iterrows():
            franquicia = str(r.get("Franquicia", "")).strip()
            if not franquicia:
                continue

            franquicia_match = re.sub(r"\s*\bkids?\b\s*", " ", franquicia, flags=re.I).strip()
            if not franquicia_match:
                franquicia_match = franquicia

            log(f"\n   🔍 [KIDS] Procesando: '{franquicia}'")

            if has_url:
                url_us = str(r.get("URL_US", "")).strip()
                if not url_us:
                    url_us = build_nike_search_url(franquicia)
            else:
                url_us = build_nike_search_url(franquicia)

            styles_list = build_ar_kids_styles(df_kids, franquicia, cap_styles=MAX_AR_STYLES_PER_SEARCH)
            valid_styles = set(s.upper() for s in styles_list)

            if not valid_styles:
                log(f"      ⚠️ No se encontraron styles kids AR para '{franquicia}'")
                out_rows.append(
                    make_row(
                        run_date=run_date,
                        division="FW",
                        franchise=franquicia,
                        category=sheet,
                        marketing_ar="",
                        style="",
                        us_name="",
                        pdp="",
                        retail_ars=None,
                        fx_ars_per_usd=fx_ars_per_usd,
                        us_full=None,
                    )
                )
                human_pause(*SLEEP_RANGE)
                continue

            pick = None
            try:
                log(f"      Scrapeando URL: {url_us}")
                products = scrape_plp_products_paged(
                    session, url_us,
                    pages=NONFOOTBALL_MAX_PAGES,
                    max_cards=MAX_PLP_PRODUCTS_SCAN,
                )
                log(f"      Total productos encontrados: {len(products)}")

                if products:
                    pick = choose_product_from_plp(products, franquicia_match, valid_styles, style_meta_map)
                else:
                    log(f"      ⚠️ No se encontraron productos en la PLP")

            except Exception as e:
                log(f"      ❌ Error scraping PLP: {e}")

            if pick is None and valid_styles:
                log(f"      🧩 Fallback UI Search kids: {min(len(valid_styles), UI_SEARCH_MAX_STYLES_PER_FRANCHISE)} styles...")
                ui = None
                combined: List[PLPProduct] = []
                for j, st in enumerate(list(valid_styles)[:UI_SEARCH_MAX_STYLES_PER_FRANCHISE], start=1):
                    log(f"         🔎 UI search ({j}) style={st}")
                    ui, ui_products = ui_search_products_fallback(ui, query=st, max_cards=MAX_PLP_PRODUCTS_SCAN)
                    for p in ui_products:
                        if p.style_base and p.style_base.upper() == st.upper():
                            combined.append(p)
                try:
                    if ui:
                        ui.close()
                except Exception:
                    pass
                dedup = {p.pdp_url: p for p in combined}
                combined = list(dedup.values())
                log(f"      🧩 UI Search products (post-filter): {len(combined)}")
                if combined:
                    pick = choose_product_from_plp(combined, franquicia_match, valid_styles, style_meta_map)

            if pick is None:
                log(f"      ❌ No se encontró producto válido para '{franquicia}'")
                out_rows.append(
                    make_row(
                        run_date=run_date,
                        division="FW",
                        franchise=franquicia,
                        category=sheet,
                        marketing_ar="",
                        style="",
                        us_name="",
                        pdp="",
                        retail_ars=None,
                        fx_ars_per_usd=fx_ars_per_usd,
                        us_full=None,
                    )
                )
            else:
                style = pick.style_base
                ar_ars = style_price_map.get(style)
                meta = style_meta_map.get(style, {})
                marketing = meta.get("Marketing Name", "")

                log(f"      ✅ Producto seleccionado: {pick.title}")
                log(f"         Style: {style}, Precio US: ${pick.price_usd}, Precio AR: ${ar_ars}")

                out_rows.append(
                    make_row(
                        run_date=run_date,
                        division="FW",
                        franchise=franquicia,
                        category=sheet,
                        marketing_ar=marketing,
                        style=style,
                        us_name=pick.title,
                        pdp=pick.pdp_url,
                        retail_ars=float(ar_ars) if ar_ars is not None else None,
                        fx_ars_per_usd=fx_ars_per_usd,
                        us_full=float(pick.price_usd) if pick.price_usd is not None else None,
                    )
                )

            human_pause(*SLEEP_RANGE)

    return out_rows


# -------------------------------------------------------------------
# CONSTRUCCIÓN DE OUTPUTS - APPAREL
# -------------------------------------------------------------------
def build_apparel_output(
    session: requests.Session,
    df_sb: pd.DataFrame,
    style_price_map: Dict[str, float],
    style_meta_map: Dict[str, Dict[str, str]],
    fx_ars_per_usd: float,
    run_date: str,
) -> List[Dict[str, Any]]:
    out_rows: List[Dict[str, Any]] = []
    df_ap = filter_apparel_sb(df_sb)

    styles_ordered = build_top_styles_by_stock(df_ap)
    log(f"\n👕 [APPAREL] {len(styles_ordered)} styles candidatos por stock | target={APPAREL_TARGET_STYLES}")

    ok_count = 0
    for idx, st in enumerate(styles_ordered):
        if ok_count >= APPAREL_TARGET_STYLES:
            break
        if st not in style_price_map:
            continue

        log(f"\n   🔍 {idx+1}/{len(styles_ordered)}: Style {st}")
        
        meta = style_meta_map.get(st, {})
        franchise = meta.get("Franchise", "").strip()
        
        pick = None
        
        if franchise:
            url_us = build_nike_search_url(franchise)
            log(f"      Buscando por franquicia: '{franchise}' → {url_us}")
            try:
                products = scrape_plp_products_paged(
                    session, url_us, 
                    pages=APPAREL_MAX_PAGES, 
                    max_cards=MAX_PLP_PRODUCTS_SCAN,
                    target_style=st
                )
                for p in products:
                    if p.sold_out_flag:
                        continue
                    if not p.full_price_flag:
                        continue
                    if not p.price_usd or p.price_usd <= 0:
                        continue
                    if p.style_base == st:
                        pick = p
                        log(f"         ✅ Encontrado por franquicia: {p.title}")
                        break
                if not pick:
                    log(f"         ⚠️ Style {st} no encontrado en resultados de franquicia")
            except Exception as e:
                log(f"      ⚠️ Error buscando por franquicia: {e}")
        
        if not pick:
            log(f"      ⚠️ Buscando por style directamente...")
            url_us = f"https://www.nike.com/w?q={st}"
            try:
                products = scrape_plp_products_paged(
                    session, url_us, 
                    pages=APPAREL_MAX_PAGES, 
                    max_cards=MAX_PLP_PRODUCTS_SCAN,
                    target_style=st
                )
                for p in products:
                    if p.sold_out_flag:
                        continue
                    if not p.full_price_flag:
                        continue
                    if not p.price_usd or p.price_usd <= 0:
                        continue
                    if p.style_base == st:
                        pick = p
                        log(f"         ✅ Encontrado por style: {p.title}")
                        break
            except Exception as e:
                log(f"      ⚠️ Error buscando por style: {e}")

            # UI SEARCH fallback (más confiable que /w?q=... cuando no pagina)
            if pick is None:
                log(f"      🧩 UI Search fallback por style: {st}")
                ui = None
                ui, ui_products = ui_search_products_fallback(ui, query=st, max_cards=MAX_PLP_PRODUCTS_SCAN)
                try:
                    if ui:
                        ui.close()
                except Exception:
                    pass
                for p in ui_products:
                    if p.sold_out_flag:
                        continue
                    if not p.full_price_flag:
                        continue
                    if not p.price_usd or p.price_usd <= 0:
                        continue
                    if p.style_base == st:
                        pick = p
                        log(f"         ✅ Encontrado por UI search: {p.title}")
                        break
        
        if pick is None:
            log(f"      ❌ No encontrado en US")
            continue

        ar_ars = style_price_map.get(st)
        marketing = meta.get("Marketing Name", "")
        category_sb = meta.get("Category", "") or "Apparel"

        out_rows.append(
            make_row(
                run_date=run_date,
                division="APP",
                franchise="-",
                category=category_sb,
                marketing_ar=marketing,
                style=st,
                us_name=pick.title,
                pdp=pick.pdp_url,
                retail_ars=float(ar_ars) if ar_ars is not None else None,
                fx_ars_per_usd=fx_ars_per_usd,
                us_full=float(pick.price_usd) if pick.price_usd is not None else None,
            )
        )
        ok_count += 1
        log(f"      ✅ #{ok_count}/{APPAREL_TARGET_STYLES} | {st} -> {pick.title[:50]}")
        human_pause(*SLEEP_RANGE)

    if ok_count < APPAREL_TARGET_STYLES:
        log(f"   ⚠️ Apparel completado con {ok_count}/{APPAREL_TARGET_STYLES} matches")
    
    return out_rows


# -------------------------------------------------------------------
# CONSTRUCCIÓN DE OUTPUTS - EQUIPMENT
# -------------------------------------------------------------------
def build_equipment_output(
    session: requests.Session,
    df_sb: pd.DataFrame,
    style_price_map: Dict[str, float],
    style_meta_map: Dict[str, Dict[str, str]],
    fx_ars_per_usd: float,
    run_date: str,
) -> List[Dict[str, Any]]:
    out_rows: List[Dict[str, Any]] = []
    df_eq = filter_equipment_sb(df_sb)

    styles_ordered = build_top_styles_by_stock(df_eq)
    log(f"\n🎒 [EQUIPMENT] {len(styles_ordered)} styles candidatos por stock | target={EQUIPMENT_TARGET_STYLES}")

    ok_count = 0
    for idx, st in enumerate(styles_ordered):
        if ok_count >= EQUIPMENT_TARGET_STYLES:
            break
        if st not in style_price_map:
            continue

        log(f"\n   🔍 {idx+1}/{len(styles_ordered)}: Style {st}")
        
        meta = style_meta_map.get(st, {})
        franchise = meta.get("Franchise", "").strip()
        
        pick = None
        
        if franchise:
            url_us = build_nike_search_url(franchise)
            log(f"      Buscando por franquicia: '{franchise}' → {url_us}")
            try:
                products = scrape_plp_products_paged(
                    session, url_us, 
                    pages=EQUIPMENT_MAX_PAGES, 
                    max_cards=MAX_PLP_PRODUCTS_SCAN,
                    target_style=st
                )
                for p in products:
                    if p.sold_out_flag:
                        continue
                    if not p.full_price_flag:
                        continue
                    if not p.price_usd or p.price_usd <= 0:
                        continue
                    if p.style_base == st:
                        pick = p
                        log(f"         ✅ Encontrado por franquicia: {p.title}")
                        break
                if not pick:
                    log(f"         ⚠️ Style {st} no encontrado en resultados de franquicia")
            except Exception as e:
                log(f"      ⚠️ Error buscando por franquicia: {e}")
        
        if not pick:
            log(f"      ⚠️ Buscando por style directamente...")
            url_us = f"https://www.nike.com/w?q={st}"
            try:
                products = scrape_plp_products_paged(
                    session, url_us, 
                    pages=EQUIPMENT_MAX_PAGES, 
                    max_cards=MAX_PLP_PRODUCTS_SCAN,
                    target_style=st
                )
                for p in products:
                    if p.sold_out_flag:
                        continue
                    if not p.full_price_flag:
                        continue
                    if not p.price_usd or p.price_usd <= 0:
                        continue
                    if p.style_base == st:
                        pick = p
                        log(f"         ✅ Encontrado por style: {p.title}")
                        break
            except Exception as e:
                log(f"      ⚠️ Error buscando por style: {e}")

            # UI SEARCH fallback (más confiable que /w?q=... cuando no pagina)
            if pick is None:
                log(f"      🧩 UI Search fallback por style: {st}")
                ui = None
                ui, ui_products = ui_search_products_fallback(ui, query=st, max_cards=MAX_PLP_PRODUCTS_SCAN)
                try:
                    if ui:
                        ui.close()
                except Exception:
                    pass
                for p in ui_products:
                    if p.sold_out_flag:
                        continue
                    if not p.full_price_flag:
                        continue
                    if not p.price_usd or p.price_usd <= 0:
                        continue
                    if p.style_base == st:
                        pick = p
                        log(f"         ✅ Encontrado por UI search: {p.title}")
                        break
        
        if pick is None:
            log(f"      ❌ No encontrado en US")
            continue

        ar_ars = style_price_map.get(st)
        marketing = meta.get("Marketing Name", "")
        category_sb = meta.get("Category", "") or "Equipment"

        out_rows.append(
            make_row(
                run_date=run_date,
                division="EQ",
                franchise="-",
                category=category_sb,
                marketing_ar=marketing,
                style=st,
                us_name=pick.title,
                pdp=pick.pdp_url,
                retail_ars=float(ar_ars) if ar_ars is not None else None,
                fx_ars_per_usd=fx_ars_per_usd,
                us_full=float(pick.price_usd) if pick.price_usd is not None else None,
            )
        )
        ok_count += 1
        log(f"      ✅ #{ok_count}/{EQUIPMENT_TARGET_STYLES} | {st} -> {pick.title[:50]}")
        human_pause(*SLEEP_RANGE)

    if ok_count < EQUIPMENT_TARGET_STYLES:
        log(f"   ⚠️ Equipment completado con {ok_count}/{EQUIPMENT_TARGET_STYLES} matches")
    
    return out_rows


# -------------------------------------------------------------------
# CONSTRUCCIÓN DE OUTPUTS - FOOTBALL
# -------------------------------------------------------------------
def build_football_output(
    session: requests.Session,
    df_sb: pd.DataFrame,
    style_price_map: Dict[str, float],
    style_meta_map: Dict[str, Dict[str, str]],
    links_sheets: Dict[str, pd.DataFrame],
    fx_ars_per_usd: float,
    run_date: str,
) -> List[Dict[str, Any]]:
    if "Football" not in links_sheets:
        return []

    df_links = links_sheets["Football"].copy()
    df_links.columns = pd.Index([str(c).strip() for c in df_links.columns])
    
    expected = ["Franchise", "Gender", "SILO BOTINES", "PLATO"]
    missing = [c for c in expected if c not in df_links.columns]
    if missing:
        log(f"❌ Football sheet missing columns: {missing}")
        return []

    df_fb = filter_football_sb(df_sb)

    df_fb["_F_FRANCHISE"] = df_fb["Franchise"].apply(normalize_upper)
    df_fb["_F_GENDER"] = df_fb["Gender"].apply(normalize_upper)
    df_fb["_F_SILO"] = df_fb["SILO BOTINES"].apply(normalize_upper)
    df_fb["_F_PLATO"] = df_fb["PLATO"].apply(lambda x: normalize_plato(x))

    out_rows: List[Dict[str, Any]] = []
    log(f"\n⚽ [FOOTBALL] {len(df_links)} combinaciones")

    for idx, r in df_links.iterrows():
        franchise = str(r.get("Franchise", "")).strip()
        gender = str(r.get("Gender", "")).strip()
        silo = str(r.get("SILO BOTINES", "")).strip()
        plato = str(r.get("PLATO", "")).strip()

        if not (franchise and gender and plato):
            continue

        base_query = f"{franchise} {silo} {plato}".strip() if silo else f"{franchise} {plato}".strip()
        url_us = build_nike_search_url(base_query, market="us")

        fr_u = normalize_upper(franchise)
        ge_u = normalize_upper(gender)
        si_u = normalize_upper(silo)
        pl_u = normalize_plato(plato)

        log(f"\n   ⚽ [{idx+1}/{len(df_links)}] {franchise} | {gender} | {silo} | {plato}")
        log(f"      URL: {url_us}")

        m = (df_fb["_F_FRANCHISE"] == fr_u) & (df_fb["_F_GENDER"] == ge_u) & (df_fb["_F_PLATO"] == pl_u)
        if si_u != "":
            m = m & (df_fb["_F_SILO"] == si_u)

        styles = [str(s).strip().upper() for s in df_fb.loc[m, "Style"].dropna().unique().tolist() if str(s).strip()]
        log(f"      Styles SB encontrados: {len(styles)}")

        if not styles:
            out_rows.append(
                make_row(
                    run_date=run_date,
                    division="FW",
                    franchise=franchise,
                    category="Football",
                    marketing_ar="",
                    style="",
                    us_name="",
                    pdp="",
                    retail_ars=None,
                    fx_ars_per_usd=fx_ars_per_usd,
                    gender=gender,
                    silo=silo,
                    plato=plato,
                    us_full=None,
                )
            )
            human_pause(*SLEEP_RANGE)
            continue

        pick = None
        try:
            log(f"      Scrapeando PLP...")
            products = scrape_plp_products_paged(
                session, url_us, 
                pages=FOOTBALL_MAX_PAGES, 
                max_cards=MAX_PLP_PRODUCTS_SCAN
            )
            
            by_style: Dict[str, List[PLPProduct]] = {}
            for p in products:
                if not p.style_base:
                    continue
                by_style.setdefault(p.style_base.upper(), []).append(p)

            tried = 0
            for st in styles:
                if tried >= FOOTBALL_MAX_STYLE_TRIES_PER_KEY:
                    break
                tried += 1
                
                plist = by_style.get(st, [])
                plist = [p for p in plist if (not p.sold_out_flag) and p.full_price_flag and p.price_usd and p.price_usd > 0]
                
                if not plist:
                    continue
                    
                plist.sort(key=lambda x: (extract_max_model_number(x.title), x.price_usd or 0.0), reverse=True)
                pick = plist[0]
                log(f"         ✅ Match encontrado: {st} -> {pick.title}")
                break
                
        except Exception as e:
            log(f"      ❌ Error scraping: {e}")
            pick = None

        if pick is None:
            log(f"      ❌ No se encontró producto en US")
            out_rows.append(
                make_row(
                    run_date=run_date,
                    division="FW",
                    franchise=franchise,
                    category="Football",
                    marketing_ar="",
                    style="",
                    us_name="",
                    pdp="",
                    retail_ars=None,
                    fx_ars_per_usd=fx_ars_per_usd,
                    gender=gender,
                    silo=silo,
                    plato=plato,
                    us_full=None,
                )
            )
        else:
            style = pick.style_base
            ar_ars = style_price_map.get(style)
            meta = style_meta_map.get(style, {})
            marketing = meta.get("Marketing Name", "")

            out_rows.append(
                make_row(
                    run_date=run_date,
                    division="FW",
                    franchise=franchise,
                    category="Football",
                    marketing_ar=marketing,
                    style=style,
                    us_name=pick.title,
                    pdp=pick.pdp_url,
                    retail_ars=float(ar_ars) if ar_ars is not None else None,
                    fx_ars_per_usd=fx_ars_per_usd,
                    gender=gender,
                    silo=silo,
                    plato=plato,
                    us_full=float(pick.price_usd) if pick.price_usd is not None else None,
                )
            )
        
        human_pause(*SLEEP_RANGE)

    return out_rows


# -------------------------------------------------------------------
# ESCRITURA DE ARCHIVOS
# -------------------------------------------------------------------
def write_xlsx_template(output_path: str, rows: List[Dict[str, Any]]):
    log(f"\n📝 Escribiendo Excel: {output_path}")
    wb = xlsxwriter.Workbook(output_path)

    header_fmt = wb.add_format({"bold": True, "font_color": "white", "bg_color": "#1F4E79", "align": "center", "valign": "vcenter", "border": 1})
    text_fmt = wb.add_format({"align": "left", "valign": "vcenter"})
    money_ars = wb.add_format({"num_format": '"$" #,##0', "align": "left", "valign": "vcenter"})
    money_usd = wb.add_format({"num_format": "$#,##0.00", "align": "left", "valign": "vcenter"})
    fx_fmt = wb.add_format({"num_format": "#,##0.00", "align": "left", "valign": "vcenter"})
    pct_fmt = wb.add_format({"num_format": "0.00%", "align": "left", "valign": "vcenter"})
    link_fmt = wb.add_format({"font_color": "blue", "underline": 1, "align": "left", "valign": "vcenter"})
    zebra_fmt = wb.add_format({"bg_color": "#F2F2F2"})

    cell_beat = wb.add_format({"bg_color": "#C6EFCE"})
    cell_MEET = wb.add_format({"bg_color": "#FFEB9C"})
    cell_lose = wb.add_format({"bg_color": "#FFC7CE"})
    cell_nou = wb.add_format({"bg_color": "#D9D9D9"})

    ws = wb.add_worksheet("Correccion")
    ws.freeze_panes(1, 0)
    ws.autofilter(0, 0, 0, len(TEMPLATE_COLS) - 1)
    ws.set_row(0, 22)

    for j, c in enumerate(TEMPLATE_COLS):
        ws.write(0, j, c, header_fmt)

    ars_cols = {"Retail ARG (ARS)"}
    usd_cols = {"ARG (USD)", "USA Full (USD)", "USA + 21% IVA", "USA + 21% + BF 8% (USD)", "AR + Shp", "US + Shp"}
    pct_cols = {"Dif FP vs USA", "Dif FP + IVA", "Dif FP + 21% + BF", "Dif"}
    fx_cols = {"FX ARS/USD"}
    bml_cols = {"BML c IVA", "BML c IVA + BF", "BML + Shp"}

    for i, row in enumerate(rows, start=1):
        ws.set_row(i, 18)
        for j, c in enumerate(TEMPLATE_COLS):
            v = row.get(c)
            if c == "PDP USA":
                if isinstance(v, str) and v.startswith("http"):
                    ws.write_url(i, j, v, link_fmt, string="Open")
                else:
                    ws.write(i, j, v if v is not None else "", text_fmt)
            elif c in ars_cols:
                if v is not None:
                    ws.write_number(i, j, float(v), money_ars)
                else:
                    ws.write(i, j, "", text_fmt)
            elif c in usd_cols:
                if v is not None:
                    ws.write_number(i, j, float(v), money_usd)
                else:
                    ws.write(i, j, "", text_fmt)
            elif c in pct_cols:
                if v is not None:
                    ws.write_number(i, j, float(v), pct_fmt)
                else:
                    ws.write(i, j, "", text_fmt)
            elif c in fx_cols:
                if v is not None:
                    ws.write_number(i, j, float(v), fx_fmt)
                else:
                    ws.write(i, j, "", text_fmt)
            else:
                ws.write(i, j, v if v is not None else "", text_fmt)

    ws.conditional_format(1, 0, len(rows), len(TEMPLATE_COLS) - 1, {"type": "formula", "criteria": "=MOD(ROW(),2)=0", "format": zebra_fmt})

    for bcol in bml_cols:
        j = TEMPLATE_COLS.index(bcol)
        col_letter = xlsxwriter.utility.xl_col_to_name(j)
        ws.conditional_format(1, j, len(rows), j, {"type": "formula", "criteria": f'=LOWER(${col_letter}2)="beat"', "format": cell_beat})
        ws.conditional_format(1, j, len(rows), j, {"type": "formula", "criteria": f'=LOWER(${col_letter}2)="MEET"', "format": cell_MEET})
        ws.conditional_format(1, j, len(rows), j, {"type": "formula", "criteria": f'=LOWER(${col_letter}2)="lose"', "format": cell_lose})
        ws.conditional_format(1, j, len(rows), j, {"type": "formula", "criteria": f'=LOWER(${col_letter}2)="no_us_data"', "format": cell_nou})

    widths = {c: max(10, min(60, len(c) + 2)) for c in TEMPLATE_COLS}
    for r in rows[:200]:
        for c in TEMPLATE_COLS:
            v = r.get(c)
            if v is None:
                continue
            widths[c] = max(widths[c], min(60, len(str(v)) + 2))
    
    for j, c in enumerate(TEMPLATE_COLS):
        ws.set_column(j, j, widths[c])

    wb.close()


def export_csv(output_path: str, rows: List[Dict[str, Any]]):
    log(f"📝 Escribiendo CSV: {output_path}")
    pd.DataFrame(rows)[TEMPLATE_COLS].to_csv(output_path, index=False, encoding="utf-8-sig")
    log(f"   ✅ CSV guardado")


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
def main():
    start_time = time.time()
    log("="*60)
    log("🚀 Nike AR vs Nike US – Price Monitoring (VERSIÓN DEFINITIVA CON FALLBACK)")
    log("="*60)
    log(f"🏷️  SEASON={SEASON} | APPAREL={SCRAPING_APPAREL} | EQUIPMENT={SCRAPING_EQUIPMENT}")
    log("")

    run_date = dt.datetime.now().strftime("%Y-%m-%d")
    
    try:
        df_sb = load_statusbooks_filtered(STATUSBOOKS_PATH, SEASON)
        style_price_map = build_style_price_map(df_sb)
        style_meta_map = build_style_meta_map(df_sb)
        links_sheets = load_links_sheets(LINKS_PLP_PATH)
        
        log("")
        fx = get_usd_ars_venta(FX_MODE)
        log(f"💱 USD/ARS venta ({FX_MODE}) = {fx:.2f}")
        log("")

        session = build_session()
        rows: List[Dict[str, Any]] = []

        log("\n" + "="*60)
        rows.extend(build_nonfootball_output(session, df_sb, style_price_map, style_meta_map, links_sheets, fx, run_date))

        log("\n" + "="*60)
        rows.extend(build_kids_output(session, df_sb, style_price_map, style_meta_map, links_sheets, fx, run_date))

        if SCRAPING_APPAREL:
            log("\n" + "="*60)
            rows.extend(build_apparel_output(session, df_sb, style_price_map, style_meta_map, fx, run_date))

        if SCRAPING_EQUIPMENT:
            log("\n" + "="*60)
            rows.extend(build_equipment_output(session, df_sb, style_price_map, style_meta_map, fx, run_date))

        log("\n" + "="*60)
        rows.extend(build_football_output(session, df_sb, style_price_map, style_meta_map, links_sheets, fx, run_date))

        log("\n" + "="*60)
        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_xlsx = f"Nike_US US_{SEASON}_{ts}.xlsx"
        out_csv = f"Nike_US US_{SEASON}_{ts}.csv"

        write_xlsx_template(out_xlsx, rows)
        export_csv(out_csv, rows)

        elapsed = time.time() - start_time
        log("\n" + "="*60)
        log(f"🎉 Proceso completado en {elapsed:.2f} segundos")
        log(f"✅ Excel: {out_xlsx}")
        log(f"✅ CSV: {out_csv}")
        log("="*60)

    except Exception as e:
        log(f"\n❌ ERROR FATAL: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()