# ============================================================
# ADIDAS AR + US — Price Monitoring
# Outputs:
#   1. Excel visual con 2 solapas:
#      - "Nike AR vs Adidas AR"
#      - "Adidas AR vs Adidas US"
#   2. CSV espejo de solapa 1
#   3. CSV espejo de solapa 2
#
# Entradas:
#   - StatusBooks NDDC ARG SP26.xlsb
#   - Comparativa_Nike_Adidas.xlsx  (cols: Categoria | NIKE | ADIDAS)
#
# Proxies:
#   - AR : gate.decodo.com:10001-10010  (Playwright + proxy residencial)
#   - US : unblock.decodo.com:60000     (requests + Site Unblocker)
# ============================================================

import asyncio
import datetime as dt
import json
import math
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import nest_asyncio
import pandas as pd
import requests
import urllib3
import xlsxwriter
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

nest_asyncio.apply()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────
SEASON           = "SP26"
STATUSBOOKS_PATH = "StatusBooks NDDC ARG SP26.xlsb"
COMPARATIVA_PATH = "Comparativa_Nike_Adidas.xlsx"

# Proxy AR — residencial sticky
AR_HOST  = "gate.decodo.com"
AR_PORTS = list(range(10001, 10011))   # 10 puertos = 10 IPs distintas
AR_USER  = "spyrndvq0x"
AR_PASS  = "8eOzLZZj3i3b=mcoc8"

# Proxy US — Site Unblocker
UB_HOST  = "unblock.decodo.com"
UB_PORT  = 60000
UB_USER  = "U0000358219"
UB_PASS  = "PW_13c62a6853fe2bb0a6b377b55a4e8d8ec"

BASE_AR  = "https://www.adidas.com.ar"
BASE_US  = "https://www.adidas.com/us"

HEADLESS    = False
FX_MODE     = "oficial"
FX_FALLBACK = 1480.0

IVA     = 0.21
BF      = 0.08
BML_TOL = 0.02

# Shipping
NIKE_FREE_ARS   = 99_000
NIKE_SHIP_ARS   = 8_890
ADIDAS_FREE_ARS = 199_999
ADIDAS_SHIP_ARS = 8_999
ADIDAS_FREE_USD = 50.0
ADIDAS_SHIP_USD = 5.0

# Nombres de producto inválidos (adidas los muestra cuando hay restricción de cuenta)
BLOCKED_NAMES = {"account-portal-disable", "account portal disable"}

# Timeouts
TO_NAV_AR       = 40_000   # ms — goto con wait_until="commit"
TO_WARMUP_AR    = 45_000   # ms
TO_NEXT_DATA_AR = 30       # segundos de polling __NEXT_DATA__
TO_REQ_US       = 60       # segundos requests


# ─────────────────────────────────────────────────────────────────
# UTILIDADES
# ─────────────────────────────────────────────────────────────────
def log(msg: str):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str):
            s = x.strip().replace("$", "").replace(",", "").strip()
            try:
                v = float(s)
                return None if math.isnan(v) else v
            except Exception:
                pass
            s2 = x.strip().replace("$", "").replace(".", "").replace(",", ".").strip()
            try:
                v = float(s2)
                return None if math.isnan(v) else v
            except Exception:
                return None
        v = float(x)
        return None if math.isnan(v) else v
    except Exception:
        return None


def bml(ar_val: Optional[float], ref: Optional[float]) -> str:
    if ar_val is None or ref is None or ref == 0:
        return "N/D"
    diff = ar_val / ref - 1
    if abs(diff) <= BML_TOL:
        return "MEET"
    return "BEAT" if diff < -BML_TOL else "LOSE"


def shipping_adidas_ar(price: Optional[float]) -> float:
    if price is None: return ADIDAS_SHIP_ARS
    return 0.0 if price > ADIDAS_FREE_ARS else ADIDAS_SHIP_ARS


def shipping_nike_ar(price: Optional[float]) -> float:
    if price is None: return NIKE_SHIP_ARS
    return 0.0 if price > NIKE_FREE_ARS else NIKE_SHIP_ARS


def shipping_us(price: Optional[float]) -> float:
    if price is None: return ADIDAS_SHIP_USD
    return 0.0 if price >= ADIDAS_FREE_USD else ADIDAS_SHIP_USD


def get_fx() -> float:
    urls = {
        "oficial": "https://dolarapi.com/v1/dolares/oficial",
        "mep":     "https://dolarapi.com/v1/dolares/bolsa",
        "blue":    "https://dolarapi.com/v1/dolares/blue",
    }
    try:
        r = requests.get(urls.get(FX_MODE, urls["oficial"]), timeout=15)
        r.raise_for_status()
        data = r.json()
        for k in ("venta", "promedio", "compra"):
            if k in data and isinstance(data[k], (int, float)) and data[k] > 0:
                log(f"   💱 FX {FX_MODE} = {data[k]:.2f} ARS/USD")
                return float(data[k])
    except Exception as e:
        log(f"   ⚠️  FX API error: {e} — fallback {FX_FALLBACK}")
    return FX_FALLBACK


def is_blocked_name(name: str) -> bool:
    return name.lower().strip() in BLOCKED_NAMES


# ─────────────────────────────────────────────────────────────────
# CARGA DE DATOS
# ─────────────────────────────────────────────────────────────────
def load_statusbooks(path: str, season: str) -> pd.DataFrame:
    log(f"📚 Cargando StatusBooks...")
    df = pd.read_excel(path, engine="pyxlsb", sheet_name="Books NDDC", header=6)
    df.columns = [str(c).strip() for c in df.columns]
    required = ["Style", "Product Code", "Marketing Name", "BU",
                "Category", "Gender", "Franchise", season]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"StatusBooks missing cols: {missing}")
    df["_price"] = df[season].apply(safe_float)
    df = df[df["_price"].notna() & (df["_price"] > 0)].copy()
    for c in ["Style", "Product Code", "Marketing Name", "Franchise", "Gender", "BU", "Category"]:
        df[c] = df[c].astype(str).str.strip()
    log(f"   ✅ {len(df):,} filas con precio > 0")
    return df


def load_comparativa(path: str) -> pd.DataFrame:
    log(f"📋 Cargando Comparativa...")
    df = pd.read_excel(path)
    df.columns = [str(c).strip() for c in df.columns]
    col_map = {}
    for c in df.columns:
        cu = c.upper().strip()
        if cu in ("CATEGORIA", "CATEGORÍA", "CATEGORY"): col_map[c] = "Categoria"
        elif cu == "NIKE":   col_map[c] = "NIKE"
        elif cu == "ADIDAS": col_map[c] = "ADIDAS"
    df.rename(columns=col_map, inplace=True)
    missing = [c for c in ["Categoria", "NIKE", "ADIDAS"] if c not in df.columns]
    if missing:
        raise RuntimeError(f"Comparativa missing cols: {missing}. Encontradas: {list(df.columns)}")
    df = df.dropna(subset=["NIKE", "ADIDAS"]).copy()
    df["NIKE"]      = df["NIKE"].astype(str).str.strip()
    df["ADIDAS"]    = df["ADIDAS"].astype(str).str.strip()
    df["Categoria"] = df["Categoria"].astype(str).str.strip()
    log(f"   ✅ {len(df)} pares Nike↔Adidas")
    return df


# ─────────────────────────────────────────────────────────────────
# FRANQUICIAS Y MATCHING
# ─────────────────────────────────────────────────────────────────

# Gamas de botines: tokens cortos y genéricos que necesitan tokens específicos
# para no matchear productos equivocados en páginas de categoría amplia
BOTINES_FILTER_TOKENS: Dict[str, List[str]] = {
    "CLUB":        ["f50", "club"],
    "LEAGUE":      ["f50", "league"],
    "PRO":         ["predator", "pro"],
    "ELITE":       ["predator", "elite"],
    "SPEEDPORTAL": ["f50"],   # SPEEDPORTAL fue reemplazado por F50 — buscar cualquier F50
}

# Override de tokens para franquicias cuyo nombre no coincide exactamente
# con los nombres de producto en el catálogo de adidas
FRANCHISE_TOKEN_OVERRIDE: Dict[str, List[str]] = {
    "EQ AGRAVIC":         ["agravic"],          # Se llama "Terrex Agravic", no "EQ Agravic"
    "EQUIPMENT AGRAVIC":  ["agravic"],          # Nombre alternativo del catálogo
    "COURT BASE":         ["grand", "court", "base"],  # Requiere "base" para no matchear Grand Court sin Base
    'SUPERNOVA "STRIDE"': ["supernova", "stride"],  # Preferir Stride; si no hay, aceptar Supernova solo
}

# Override de tokens para buscar en StatusBooks Nike (Franchise o Marketing Name).
# Para franchises cuyo nombre en Comparativa no coincide con el campo Franchise del SB.
NIKE_SB_TOKEN_OVERRIDE: Dict[str, List[str]] = {
    'AIRMAX "DN"':    ["air", "max", "dn"],   # SB dice "AIR MAX DN", no "AIRMAX DN"
    "NIKE AIR MAX SC": ["air", "max", "sc"],  # SB dice "AIR MAX SC" — quitar prefijo "NIKE"
    'PEGASUS "PLUS"': ["pegasus", "plus"],    # SB tiene "PEGASUS PLUS" dentro de franchise PEGASUS
}


def parse_franchise_tokens(franchise: str) -> List[str]:
    """
    Extrae tokens de una franquicia respetando la lógica de comillas.
    'ADIZERO "BOSTON"' → ['adizero', 'boston']  (ambos requeridos, cualquier orden)
    'FORUM LOW'        → ['forum', 'low']
    'SAMBA'            → ['samba']
    """
    quoted   = re.findall(r'"([^"]+)"', franchise)
    unquoted = re.sub(r'"[^"]+"', '', franchise).strip()
    tokens: List[str] = []
    for w in unquoted.split():
        if w.strip(): tokens.append(w.strip().lower())
    for q in quoted:
        for w in q.split():
            if w.strip(): tokens.append(w.strip().lower())
    return tokens


def franchise_matches_text(text: str, tokens: List[str]) -> bool:
    t = text.lower()
    return all(tok in t for tok in tokens)


# Franquicias Nike que son botines — buscar modelo con número más alto
NIKE_BOTINES_FRANCHISES = {"PREMIER", "ACADEMY", "CLUB", "PRO", "ELITE"}

# Gamas de botines Nike que NO son franchises en StatusBooks — aparecen en el
# nombre del producto (Marketing Name). Para estas, buscar por nombre en lugar
# de por franchise.
NIKE_BOTINES_GAMAS = {"ACADEMY", "CLUB", "PRO", "ELITE"}

# Tokens Nike que identifican botines válidos (excluir calzado no botín)
# "promina" es zapatilla, no botín
NIKE_BOTINES_BLOCKED = {"promina"}

def _extract_model_number(marketing_name: str) -> int:
    """Extrae el número de modelo más alto de un Marketing Name. '11' > '9' > sin número."""
    nums = re.findall(r'\b(\d+)\b', marketing_name)
    return max(int(n) for n in nums) if nums else 0


def _sb_rows_by_franchise(df_sb: pd.DataFrame, tokens: List[str], is_botin: bool) -> List[Tuple]:
    """Busca candidatos en el campo Franchise del StatusBooks."""
    candidates = []
    for _, row in df_sb.iterrows():
        sc = str(row.get("Product Code", "")).strip().upper()
        mn = str(row.get("Marketing Name", "")).strip()
        if not sc or row.get("_price", 0) <= 0:
            continue
        sb_fr = str(row.get("Franchise", "")).strip()
        if not franchise_matches_text(sb_fr, tokens):
            continue
        if is_botin and any(blk in mn.lower() for blk in NIKE_BOTINES_BLOCKED):
            log(f"   ⚠️  Nike botin '{mn}' excluido (nombre no botin)")
            continue
        candidates.append((sc, mn, row.get("_price", 0)))
    return candidates


def _sb_rows_by_marketing_name(df_sb: pd.DataFrame, tokens: List[str], is_botin: bool) -> List[Tuple]:
    """Busca candidatos en el campo Marketing Name del StatusBooks."""
    candidates = []
    for _, row in df_sb.iterrows():
        sc = str(row.get("Product Code", "")).strip().upper()
        mn = str(row.get("Marketing Name", "")).strip()
        if not sc or row.get("_price", 0) <= 0:
            continue
        if not franchise_matches_text(mn, tokens):
            continue
        if is_botin and any(blk in mn.lower() for blk in NIKE_BOTINES_BLOCKED):
            log(f"   ⚠️  Nike botin '{mn}' excluido (nombre no botin)")
            continue
        candidates.append((sc, mn, row.get("_price", 0)))
    return candidates


def find_nike_stylecolor(nike_franchise: str, df_sb: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    fu = nike_franchise.upper()
    is_botin = fu in NIKE_BOTINES_FRANCHISES
    is_gama  = fu in NIKE_BOTINES_GAMAS  # gama: buscar en Marketing Name, no en Franchise

    # Tokens: respetar override si existe, sino parsear normalmente
    tokens = NIKE_SB_TOKEN_OVERRIDE.get(nike_franchise) \
          or NIKE_SB_TOKEN_OVERRIDE.get(fu) \
          or parse_franchise_tokens(nike_franchise)
    if not tokens:
        return None, None

    if is_gama:
        # Gamas de botines (ACADEMY, CLUB, PRO, ELITE): solo buscar en Marketing Name
        candidates = _sb_rows_by_marketing_name(df_sb, tokens, is_botin)
    else:
        # 1er intento: buscar por campo Franchise
        candidates = _sb_rows_by_franchise(df_sb, tokens, is_botin)
        if not candidates:
            # Fallback: buscar tokens en Marketing Name (ej. PEGASUS "PLUS", AIRMAX "DN")
            log(f"   🔄 Sin match por Franchise para '{nike_franchise}' — fallback Marketing Name")
            candidates = _sb_rows_by_marketing_name(df_sb, tokens, is_botin)

    if not candidates:
        return None, None

    # Siempre elegir el modelo con número más alto (running y botines)
    best = max(candidates, key=lambda x: _extract_model_number(x[1]))
    log(f"   ✅ Nike SB match '{nike_franchise}' → '{best[1]}' ({best[0]})")
    return best[0], best[1]


# Palabras que indican producto infantil/bebé — penalizar en scoring
KIDS_TOKENS = {"inf", "kids", "toddler", "child", "junior", "baby", "niños", "niñas", "niño", "niña", " jr"}


def _get_tokens(franchise: str) -> List[str]:
    """Retorna los tokens de matching para una franquicia, respetando overrides."""
    fu = franchise.upper()
    if fu in BOTINES_FILTER_TOKENS:
        return BOTINES_FILTER_TOKENS[fu]
    if franchise in FRANCHISE_TOKEN_OVERRIDE:
        return FRANCHISE_TOKEN_OVERRIDE[franchise]
    if fu in FRANCHISE_TOKEN_OVERRIDE:
        return FRANCHISE_TOKEN_OVERRIDE[fu]
    return parse_franchise_tokens(franchise)


# Tokens de variantes/collab que penalizan cuando buscamos el modelo base
COLLAB_TOKENS = {"jane", "wales bonner", "humanrace", "pharrell", "bad bunny",
                 "disney", "marvel", "star wars", "marvel", "pixar", "x ", " x "}

def score_product(name: str, franchise: str) -> float:
    if is_blocked_name(name):
        return -999.0
    nl = name.lower()
    tokens = _get_tokens(franchise)
    if not tokens:
        return 0.0
    s = sum(2.0 for tok in tokens if tok in nl)
    # Penalizar productos infantiles
    if any(kt in nl for kt in KIDS_TOKENS):
        s -= 5.0
    # Penalizar variantes collab/especiales (para preferir el modelo base)
    if any(ct in nl for ct in COLLAB_TOKENS):
        s -= 3.0
    s += 0.5
    return s


def pick_best_product(products: List[dict], franchise: str) -> Optional[dict]:
    if not products:
        return None
    valid = [p for p in products if not is_blocked_name(p.get("name", ""))]
    if not valid:
        log(f"   ⚠️  Todos bloqueados (ACCOUNT-PORTAL-DISABLE)")
        return None

    tokens = _get_tokens(franchise)

    # Filtrar estrictamente: requiere TODOS los tokens del franchise
    if tokens:
        strict = [p for p in valid if franchise_matches_text(p.get("name", ""), tokens)]
        if not strict and len(tokens) > 1:
            # Fallback: relajar el último token (ej. "stride" en US → solo "supernova";
            # "base" de COURT BASE en US → solo "grand court")
            relaxed = tokens[:-1]
            strict = [p for p in valid if franchise_matches_text(p.get("name", ""), relaxed)]
            if strict:
                log(f"   ⚠️  Token '{tokens[-1]}' no matchea — usando fallback {relaxed}")
        if strict:
            valid = strict  # usar solo los que matchean
        else:
            # Ningún producto tiene todos los tokens → retornar None (N/D)
            log(f"   ⚠️  Ningún producto contiene todos los tokens de '{franchise}': {tokens}")
            return None

    best = max(valid, key=lambda p: score_product(p.get("name", ""), franchise))
    s   = score_product(best.get("name", ""), franchise)
    log(f"   🎯 Best: '{best.get('name','')[:50]}' | score={s:.1f} | {best.get('currency','?')} {best.get('full_price',0):,.0f}")
    return best


# ─────────────────────────────────────────────────────────────────
# URL MAPS
# ─────────────────────────────────────────────────────────────────
FRANCHISE_URLS_AR: Dict[str, List[str]] = {
    # Nota: adidas.com.ar usa guiones (-) en slugs, NO guiones bajos (_)
    # Los slugs con _ redirigen a /zapatillas genérico → siempre poner el slug real primero
    "ADIZERO ADIOS PRO":   ["zapatillas-adizero", "zapatillas-running"],
    "ULTRABOOST":          ["zapatillas-ultraboost", "zapatillas-running"],
    'ADIZERO "BOSTON"':    ["zapatillas-adizero", "zapatillas-running"],
    "ADISTAR":             ["zapatillas-adistar", "zapatillas-running"],
    "PUREBOOST":           ["zapatillas-pureboost", "zapatillas-running"],
    'SUPERNOVA "STRIDE"':  ["zapatillas-supernova", "zapatillas-running"],
    "QUESTAR":             ["zapatillas-questar", "zapatillas-running"],
    "RESPONSE":            ["zapatillas-response", "zapatillas-running"],
    "DURAMO SL":           ["zapatillas-duramo", "zapatillas-running"],
    "RUNFALCON":           ["zapatillas-runfalcon", "zapatillas-running"],
    "EQ AGRAVIC":          ["zapatillas-terrex-agravic", "zapatillas-trail", "zapatillas-running"],
    "EQUIPMENT AGRAVIC":   ["zapatillas-terrex-agravic", "zapatillas-trail", "zapatillas-running"],
    'SAMBA "DECON"':       ["samba", "zapatillas"],
    "FORUM LOW":           ["zapatillas-forum", "zapatillas"],
    "SAMBA":               ["samba", "zapatillas"],
    "CAMPUS":              ["zapatillas-campus", "zapatillas"],
    'RIVALRY "LOW"':       ["zapatillas-rivalry", "zapatillas"],
    "VL COURT 3.0":        ["zapatillas-court", "zapatillas"],
    "GRAND COURT":         ["zapatillas-grand-court", "zapatillas-court", "zapatillas"],
    "COURT BASE":          ["zapatillas-court", "zapatillas"],
    # Botines: URLs reales con guiones, luego fallback a categoría amplia
    "SPEEDPORTAL":         ["botines-f50", "botines"],
    "CLUB":                ["botines-f50", "botines"],
    "LEAGUE":              ["botines-f50", "botines"],
    "PRO":                 ["botines-predator", "botines"],
    "ELITE":               ["botines-predator", "botines"],
}

FRANCHISE_URLS_US: Dict[str, List[str]] = {
    "ADIZERO ADIOS PRO":   [f"{BASE_US}/adizero_adios_pro-running-shoes", f"{BASE_US}/adizero-running-shoes"],
    "ULTRABOOST":          [f"{BASE_US}/ultraboost-shoes", f"{BASE_US}/ultraboost-running-shoes"],
    'ADIZERO "BOSTON"':    [f"{BASE_US}/adizero_boston-shoes", f"{BASE_US}/adizero_boston-running-shoes", f"{BASE_US}/adizero-running-shoes"],
    "ADISTAR":             [f"{BASE_US}/adistar-shoes", f"{BASE_US}/adistar-running-shoes"],
    "PUREBOOST":           [f"{BASE_US}/pureboost-shoes", f"{BASE_US}/pureboost-running-shoes"],
    'SUPERNOVA "STRIDE"':  [f"{BASE_US}/supernova-running-shoes", f"{BASE_US}/running-shoes"],
    "QUESTAR":             [f"{BASE_US}/questar-shoes", f"{BASE_US}/running-shoes"],
    "RESPONSE":            [f"{BASE_US}/response-shoes", f"{BASE_US}/response-running-shoes"],
    "DURAMO SL":           [f"{BASE_US}/duramo_sl-shoes", f"{BASE_US}/duramo-shoes"],
    "RUNFALCON":           [f"{BASE_US}/runfalcon-shoes", f"{BASE_US}/running-shoes"],
    "EQ AGRAVIC":          [f"{BASE_US}/terrex_agravic-trail-running-shoes", f"{BASE_US}/terrex-trail-running-shoes"],
    "EQUIPMENT AGRAVIC":   [f"{BASE_US}/terrex_agravic-trail-running-shoes", f"{BASE_US}/terrex-trail-running-shoes"],
    'SAMBA "DECON"':       [f"{BASE_US}/samba_decon-shoes", f"{BASE_US}/samba-shoes"],
    "FORUM LOW":           [f"{BASE_US}/forum_low-shoes", f"{BASE_US}/forum-shoes"],
    "SAMBA":               [f"{BASE_US}/samba-shoes"],
    "CAMPUS":              [f"{BASE_US}/campus-shoes"],
    'RIVALRY "LOW"':       [f"{BASE_US}/rivalry-originals-shoes", f"{BASE_US}/originals-shoes"],
    "VL COURT 3.0":        [f"{BASE_US}/vl_court-shoes"],
    "GRAND COURT":         [f"{BASE_US}/grand_court-shoes"],
    "COURT BASE":          [f"{BASE_US}/grand_court_base-shoes", f"{BASE_US}/grand_court-shoes"],
    # Botines: URLs específicas por gama
    "SPEEDPORTAL":         [f"{BASE_US}/x_speedportal-soccer-shoes", f"{BASE_US}/f50-soccer-shoes"],
    "CLUB":                [f"{BASE_US}/f50_club-soccer-shoes", f"{BASE_US}/f50-soccer-shoes"],
    "LEAGUE":              [f"{BASE_US}/f50_league-soccer-shoes", f"{BASE_US}/f50-soccer-shoes"],
    "PRO":                 [f"{BASE_US}/predator_pro-soccer-shoes", f"{BASE_US}/predator-soccer-shoes"],
    "ELITE":               [f"{BASE_US}/predator_elite-soccer-shoes", f"{BASE_US}/predator-soccer-shoes"],
}


def get_urls_ar(franchise: str) -> List[str]:
    if franchise in FRANCHISE_URLS_AR:
        return FRANCHISE_URLS_AR[franchise]
    clean = re.sub(r'["\']', '', franchise).strip()
    for key, urls in FRANCHISE_URLS_AR.items():
        if re.sub(r'["\']', '', key).strip().upper() == clean.upper():
            return urls
    slug = re.sub(r'_+', '_', re.sub(r'["\' ]+', '_', clean.lower()).strip('_'))
    return [f"zapatillas-{slug}", slug, "zapatillas"]


def get_urls_us(franchise: str) -> List[str]:
    if franchise in FRANCHISE_URLS_US:
        return FRANCHISE_URLS_US[franchise]
    clean = re.sub(r'["\']', '', franchise).strip()
    for key, urls in FRANCHISE_URLS_US.items():
        if re.sub(r'["\']', '', key).strip().upper() == clean.upper():
            return urls
    slug = re.sub(r'_+', '_', re.sub(r'["\' ]+', '_', clean.lower()).strip('_'))
    return [f"{BASE_US}/{slug}-shoes", f"{BASE_US}/{slug}-running-shoes"]


# ─────────────────────────────────────────────────────────────────
# STEALTH + NEXT_DATA JS
# ─────────────────────────────────────────────────────────────────
STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver',  { get: () => undefined,         configurable: true });
    Object.defineProperty(navigator, 'languages',  { get: () => ['es-AR','es','en'], configurable: true });
    Object.defineProperty(navigator, 'plugins',    { get: () => [1,2,3,4,5],        configurable: true });
    if (!window.chrome) { window.chrome = { app:{}, runtime:{} }; }
    ['__playwright','__pwInitScripts','__pw_manual'].forEach(k=>{ try{ delete window[k]; }catch(e){} });
}
"""

NEXT_DATA_JS = """
() => {
    // ── Intento 1: __NEXT_DATA__ tag ──
    function extractFromObj(d) {
        // rutas conocidas (priorizadas de más específica a más general)
        const candidates = [
            d?.props?.pageProps?.products,
            d?.props?.pageProps?.searchResult?.products,
            d?.props?.pageProps?.plpData?.products,
            d?.props?.pageProps?.initialData?.searchResult?.products,
            d?.props?.pageProps?.plp?.products,
            d?.props?.pageProps?.categoryData?.products,
            d?.props?.pageProps?.data?.products,
            d?.props?.pageProps?.serverData?.products,
            d?.props?.pageProps?.initialProps?.products,
            d?.props?.pageProps?.dehydratedState?.queries?.[0]?.state?.data?.pages?.[0]?.products,
            d?.props?.pageProps?.dehydratedState?.queries?.[0]?.state?.data?.products,
        ];
        for (const c of candidates) {
            if (Array.isArray(c) && c.length > 0) return c;
        }
        // búsqueda profunda genérica
        function deepFind(o, depth) {
            if (depth > 10 || !o || typeof o !== 'object') return null;
            if (Array.isArray(o) && o.length >= 1 && typeof o[0] === 'object') {
                const first = o[0];
                const hasName = first?.name || first?.title || first?.displayName;
                const hasId   = first?.url  || first?.productId || first?.id || first?.modelId;
                if (hasName && hasId) return o;
            }
            const vals = Array.isArray(o) ? o : Object.values(o);
            for (const v of vals) {
                const r = deepFind(v, depth + 1);
                if (r) return r;
            }
            return null;
        }
        return deepFind(d, 0) || [];
    }

    function parsePrice(p) {
        let full = 0, final = 0;
        if (p.priceData?.prices) {
            const orig = p.priceData.prices.find(x => x.type === 'original' || x.type === 'standard');
            const sale = p.priceData.prices.find(x => x.type === 'sale'     || x.type === 'current');
            full  = orig ? parseFloat(orig.value || orig.amount || 0) : 0;
            final = sale ? parseFloat(sale.value || sale.amount || 0) : full;
        } else if (p.pricingInformation) {
            full  = parseFloat(p.pricingInformation.standardPrice || p.pricingInformation.originalPrice || 0);
            final = parseFloat(p.pricingInformation.currentPrice  || p.pricingInformation.salePrice     || full);
        } else if (p.price) {
            if (typeof p.price === 'object') {
                full  = parseFloat(p.price.regular || p.price.original || p.price.current || p.price.value || p.price.standard || 0);
                final = parseFloat(p.price.current || p.price.sale     || p.price.value   || full);
            } else { full = final = parseFloat(p.price) || 0; }
        } else if (p.pricing) {
            full  = parseFloat(p.pricing.standard || p.pricing.regular || p.pricing.original || 0);
            final = parseFloat(p.pricing.sale     || p.pricing.current || full);
        } else if (p.prices) {
            if (Array.isArray(p.prices)) {
                const orig = p.prices.find(x => x.type === 'original' || x.type === 'standard');
                const sale = p.prices.find(x => x.type === 'sale');
                full  = orig ? parseFloat(orig.value || 0) : 0;
                final = sale ? parseFloat(sale.value || 0) : full;
            }
        }
        return { full, final };
    }

    function mapProducts(items, baseUrl) {
        return items.map(p => {
            const { full, final } = parsePrice(p);
            let url = p.url || p.link || p.productLink || p.pdpUrl || p.href || '';
            if (url && !url.startsWith('http')) url = baseUrl + url;
            return {
                name:        p.title || p.name || p.displayName || p.productName || '',
                url,
                full_price:  full,
                final_price: final,
                currency:    'ARS'
            };
        }).filter(p => p.name && (p.full_price > 0 || p.final_price > 0));
    }

    const BASE = 'https://www.adidas.com.ar';

    // ── Intento 1: __NEXT_DATA__ ──
    const ndEl = document.getElementById('__NEXT_DATA__');
    if (ndEl && ndEl.textContent) {
        try {
            const d     = JSON.parse(ndEl.textContent);
            const items = extractFromObj(d);
            if (items.length > 0) return mapProducts(items, BASE);
        } catch(e) {}
    }

    // ── Intento 2: window.__PRELOADED_STATE__ / window.__STATE__ ──
    for (const key of ['__PRELOADED_STATE__', '__STATE__', '__INITIAL_STATE__', '__REDUX_STATE__']) {
        try {
            const st = window[key];
            if (st && typeof st === 'object') {
                const items = extractFromObj(st);
                if (items.length > 0) return mapProducts(items, BASE);
            }
        } catch(e) {}
    }

    // ── Intento 3: JSON-LD product listings ──
    try {
        const scripts = document.querySelectorAll('script[type="application/ld+json"]');
        for (const sc of scripts) {
            const d = JSON.parse(sc.textContent);
            const arr = Array.isArray(d) ? d : (d['@graph'] || [d]);
            const products = arr.filter(x => x['@type'] === 'Product' || x['@type'] === 'ItemList');
            if (products.length > 0) {
                return products.flatMap(x => {
                    if (x['@type'] === 'ItemList') {
                        return (x.itemListElement || []).map(el => ({
                            name:        el.name || el.item?.name || '',
                            url:         el.url  || el.item?.url  || '',
                            full_price:  parseFloat(el.item?.offers?.price || el.offers?.price || 0),
                            final_price: parseFloat(el.item?.offers?.price || el.offers?.price || 0),
                            currency:    'ARS'
                        }));
                    }
                    return [{
                        name:        x.name || '',
                        url:         x.url  || '',
                        full_price:  parseFloat(x.offers?.price || 0),
                        final_price: parseFloat(x.offers?.price || 0),
                        currency:    'ARS'
                    }];
                }).filter(p => p.name && p.full_price > 0);
            }
        }
    } catch(e) {}

    // ── Intento 4: buscar JSON inline en cualquier script ──
    try {
        const allScripts = document.querySelectorAll('script:not([src])');
        for (const sc of allScripts) {
            const txt = sc.textContent || '';
            if (!txt.includes('"products"') && !txt.includes('"items"')) continue;
            const match = txt.match(/\\{[\\s\\S]*?"products"\\s*:\\s*\\[[\\s\\S]*?\\]/);
            if (match) {
                try {
                    const obj = JSON.parse('{' + match[0].replace(/^\\{/, ''));
                    const items = extractFromObj(obj);
                    if (items.length > 0) return mapProducts(items, BASE);
                } catch(e2) {}
            }
        }
    } catch(e) {}

    return [];
}
"""

# Señales inequívocas de challenge — NO incluir "adidas" solo porque
# muchas PLPs válidas tienen ese título mientras cargan
CHALLENGE_SIGNALS = [
    "unable to give you access",
    "security issue was automatically",
    "we are unable to give you",
    "pardon our interruption",
    "robot or human",
    "are you a robot",
    "access denied",
    "blocked",
]

def is_challenge_page(title: str) -> bool:
    t = title.lower().strip()
    return any(s in t for s in CHALLENGE_SIGNALS)


# ─────────────────────────────────────────────────────────────────
# PARSING HTML FALLBACK — para cuando JS no hidrata (AR y US)
# ─────────────────────────────────────────────────────────────────
def extract_next_data_html(html: str, currency: str = "ARS", base_url: str = "https://www.adidas.com.ar",
                           requested_slug: str = "") -> List[dict]:
    """
    Extrae productos de HTML parseando __NEXT_DATA__ y otros JSON embebidos.
    Se usa como fallback cuando Playwright no logra evaluar JS.
    Si requested_slug se provee, detecta redirects de adidas.com.ar y descarta la página.
    """
    results: List[dict] = []

    # ── Intento 1: __NEXT_DATA__ script tag ──
    m = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>', html, re.DOTALL)
    if m:
        try:
            # Detectar redirect AR (slug con _ redirige a /zapatillas o /botines genérico)
            if requested_slug and "adidas.com.ar" in base_url:
                if _detect_ar_redirect(m.group(1), requested_slug):
                    return []  # Página redirigida — descartar
            results = _parse_next_data_json(m.group(1), currency, base_url)
            if results:
                return results
        except Exception:
            pass

    # ── Intento 2: JSON-LD ──
    for m in re.finditer(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>', html, re.DOTALL):
        try:
            d = json.loads(m.group(1))
            arr = d if isinstance(d, list) else d.get("@graph", [d])
            for item in arr:
                if item.get("@type") in ("Product",):
                    price = safe_float(item.get("offers", {}).get("price", 0))
                    name  = item.get("name", "")
                    url   = item.get("url", "")
                    if name and price and price > 0:
                        results.append({"name": name, "url": url,
                                         "full_price": price, "final_price": price, "currency": currency})
            if results:
                return results
        except Exception:
            pass

    # ── Intento 3: window.__PRELOADED_STATE__ o similar ──
    for pat in [
        r'window\.__PRELOADED_STATE__\s*=\s*(\{.*?\});\s*(?:window|</script>)',
        r'window\.__STATE__\s*=\s*(\{.*?\});\s*(?:window|</script>)',
        r'__INITIAL_STATE__\s*=\s*(\{.*?\});',
    ]:
        m = re.search(pat, html, re.DOTALL)
        if m:
            try:
                results = _parse_next_data_json(m.group(1), currency, base_url)
                if results:
                    return results
            except Exception:
                pass

    return results


def _detect_ar_redirect(raw: str, requested_slug: str) -> bool:
    """
    Devuelve True si la página redirigió a un slug diferente (ej. slug con _ → /zapatillas).
    Compara requested_slug con fullUrl en pageProps.
    """
    try:
        d = json.loads(raw)
        full_url = d.get("props", {}).get("pageProps", {}).get("fullUrl", "")
        if not full_url:
            return False
        actual = full_url.rstrip("/").split("adidas.com.ar")[-1].lstrip("/")
        # Si el slug pedido termina dentro de la URL real → no redirigió
        if requested_slug and actual == requested_slug:
            return False
        # Si la URL real es /zapatillas, /botines, u otro slug completamente diferente
        if requested_slug and actual != requested_slug:
            log(f"   ⚠️  Redirect detectado: /{requested_slug} → /{actual}")
            return True
    except Exception:
        pass
    return False


def _parse_next_data_json(raw: str, currency: str, base_url: str) -> List[dict]:
    d = json.loads(raw)

    def _find_products(obj, depth=0):
        if depth > 10 or not obj or not isinstance(obj, (dict, list)):
            return None
        # Rutas directas
        if isinstance(obj, dict):
            for key in ("products", "items", "productList", "productItems", "result"):
                v = obj.get(key)
                if isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
                    first = v[0]
                    has_name = first.get("name") or first.get("title") or first.get("displayName")
                    has_id   = first.get("url")  or first.get("productId") or first.get("id") or first.get("modelId")
                    if has_name and has_id:
                        return v
        if isinstance(obj, list) and len(obj) > 0 and isinstance(obj[0], dict):
            first = obj[0]
            has_name = first.get("name") or first.get("title") or first.get("displayName")
            has_id   = first.get("url")  or first.get("productId") or first.get("id")
            if has_name and has_id:
                return obj
        vals = obj.values() if isinstance(obj, dict) else obj
        for v in vals:
            r = _find_products(v, depth + 1)
            if r:
                return r
        return None

    items = _find_products(d) or []
    results = []
    for p in items:
        full = final = 0.0
        pd_ = p.get("priceData") or {}
        if pd_.get("prices"):
            orig  = next((x for x in pd_["prices"] if x.get("type") in ("original", "standard")), None)
            sale  = next((x for x in pd_["prices"] if x.get("type") in ("sale", "current")),       None)
            full  = float(orig.get("value") or orig.get("amount") or 0) if orig else 0.0
            final = float(sale.get("value") or sale.get("amount") or 0) if sale else full
        elif p.get("pricingInformation"):
            pi    = p["pricingInformation"]
            full  = float(pi.get("standardPrice") or pi.get("originalPrice") or 0)
            final = float(pi.get("currentPrice")  or pi.get("salePrice")     or full)
        elif p.get("price"):
            pr = p["price"]
            if isinstance(pr, dict):
                full  = float(pr.get("regular") or pr.get("original") or pr.get("current") or pr.get("value") or 0)
                final = float(pr.get("current") or pr.get("sale")     or pr.get("value")   or full)
            else:
                full = final = float(pr or 0)
        elif p.get("pricing"):
            pi    = p["pricing"]
            full  = float(pi.get("standard") or pi.get("regular") or pi.get("original") or 0)
            final = float(pi.get("sale")     or pi.get("current") or full)

        name = p.get("title") or p.get("name") or p.get("displayName") or p.get("productName") or ""
        url  = p.get("url")   or p.get("link") or p.get("pdpUrl")      or p.get("href")        or ""
        if url and not url.startswith("http"):
            url = base_url + url
        if name and (full > 0 or final > 0):
            results.append({"name": name, "url": url,
                             "full_price": full, "final_price": final, "currency": currency})
    return results


# Proxy AR para requests (fallback sin browser)
AR_PROXIES = {
    "http":  f"http://{AR_USER}:{AR_PASS}@{AR_HOST}:{{port}}",
    "https": f"http://{AR_USER}:{AR_PASS}@{AR_HOST}:{{port}}",
}
AR_HEADERS = {
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language":    "es-AR,es;q=0.9",
    "Accept":             "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "sec-ch-ua-platform": '"Windows"',
}


def scrape_ar_requests_fallback(url: str, port: int) -> List[dict]:
    """
    Fallback: intenta con Site Unblocker primero (mejor bypass Akamai),
    si falla usa proxy residencial AR.
    """
    slug = url.rstrip("/").split("adidas.com.ar/")[-1] if "adidas.com.ar/" in url else ""
    # ── Intento 1: Site Unblocker (mismo que US, pero apuntando a .com.ar) ──
    ub_proxies = {
        "http":  f"http://{UB_USER}:{UB_PASS}@{UB_HOST}:{UB_PORT}",
        "https": f"http://{UB_USER}:{UB_PASS}@{UB_HOST}:{UB_PORT}",
    }
    headers_ar = {**AR_HEADERS, "Accept-Language": "es-AR,es;q=0.9"}
    try:
        r = requests.get(url, headers=headers_ar, proxies=ub_proxies,
                         verify=False, timeout=50)
        log(f"   📊 fallback AR (SiteUnblocker): {r.status_code} | {len(r.text):,} bytes")
        if r.status_code == 200:
            results = extract_next_data_html(r.text, currency="ARS", base_url=BASE_AR,
                                             requested_slug=slug)
            if results:
                return results
    except Exception as e:
        log(f"   ⚠️  fallback AR SiteUnblocker error: {str(e)[:80]}")

    # ── Intento 2: proxy residencial AR ──
    ar_proxies = {
        "http":  f"http://{AR_USER}:{AR_PASS}@{AR_HOST}:{port}",
        "https": f"http://{AR_USER}:{AR_PASS}@{AR_HOST}:{port}",
    }
    try:
        r = requests.get(url, headers=headers_ar, proxies=ar_proxies,
                         verify=False, timeout=40)
        log(f"   📊 fallback AR (residencial): {r.status_code} | {len(r.text):,} bytes")
        if r.status_code == 200:
            return extract_next_data_html(r.text, currency="ARS", base_url=BASE_AR,
                                          requested_slug=slug)
    except Exception as e:
        log(f"   ⚠️  fallback AR residencial error: {str(e)[:80]}")
    return []


# ─────────────────────────────────────────────────────────────────
# SCRAPER AR — Playwright + proxy residencial
# ─────────────────────────────────────────────────────────────────
# Pool de puertos usado — para no repetir IPs en rebuilds
_used_ar_ports: List[int] = []

async def build_ar_browser(pw, exclude_ports: List[int] = None):
    available = [p for p in AR_PORTS if p not in (exclude_ports or [])]
    if not available:
        available = AR_PORTS  # Si se agotaron, reciclar
    port  = random.choice(available)
    proxy = {"server": f"http://{AR_HOST}:{port}", "username": AR_USER, "password": AR_PASS}
    ua    = random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ])
    browser = await pw.chromium.launch(
        headless=HEADLESS, proxy=proxy,
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox",
              "--disable-dev-shm-usage"],
    )
    context = await browser.new_context(
        locale="es-AR", timezone_id="America/Argentina/Buenos_Aires",
        user_agent=ua, viewport={"width": 1920, "height": 1080},
        extra_http_headers={
            "Accept-Language": "es-AR,es;q=0.9",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
    )
    await context.add_init_script(STEALTH_JS)
    async def _block(route, req):
        await route.abort() if req.resource_type in ("image", "media", "font") else await route.continue_()
    await context.route("**/*", _block)
    page = await context.new_page()
    log(f"   🌐 Browser AR OK — {AR_HOST}:{port}")
    return browser, context, page, port


async def warmup_ar(page) -> bool:
    # Intentar /ayuda primero, luego homepage como alternativa
    urls_warmup = [f"{BASE_AR}/ayuda", BASE_AR]
    for attempt, wurl in enumerate(urls_warmup):
        try:
            await page.goto(wurl, wait_until="domcontentloaded", timeout=25_000)
            await asyncio.sleep(random.uniform(1.0, 2.0))
            log("   ✅ Warmup AR OK")
            return True
        except Exception as e:
            log(f"   ⚠️  Warmup AR fallo (intento {attempt+1}): {str(e)[:60]}")
            await asyncio.sleep(1)
    log("   ⚠️  Warmup AR falló — continuando igual (no bloqueante)")
    return False


async def wait_next_data_ar(page, max_wait: int = TO_NEXT_DATA_AR) -> List[dict]:
    deadline     = time.time() + max_wait
    attempt      = 0
    sleep_time   = 1.5
    while time.time() < deadline:
        attempt += 1
        try:
            products = await page.evaluate(NEXT_DATA_JS)
            if products:
                log(f"   ✅ __NEXT_DATA__ OK — {len(products)} productos (intento {attempt})")
                return products
            # Dar más tiempo entre intentos tarde para no saturar
            if attempt > 6:
                sleep_time = 2.5
            log(f"   ⏳ vacío (intento {attempt})")
        except Exception as e:
            err = str(e)[:80]
            log(f"   ⚠️  eval: {err}")
            # Si el contexto fue destruido (navegación), salir inmediato
            if "Execution context was destroyed" in err or "Target closed" in err:
                break
        await asyncio.sleep(sleep_time)
    log(f"   ❌ __NEXT_DATA__ timeout {max_wait}s")
    return []


async def get_page_html(page) -> str:
    """Obtiene el HTML completo de la página actual para fallback parsing."""
    try:
        return await page.content()
    except Exception:
        return ""


async def _rebuild_browser_ar(pw, used_ports: List[int], reason: str = ""):
    """Cierra el browser actual y abre uno nuevo con IP fresca. Devuelve (page, used_ports, port)."""
    log(f"   🔄 Rebuild browser AR{f' — {reason}' if reason else ''}...")
    await asyncio.sleep(random.uniform(1.5, 3.0))
    browser2, _, page2, port2 = await build_ar_browser(pw, exclude_ports=used_ports)
    used_ports.append(port2)
    # Warmup no bloqueante — si falla igual continuamos
    await warmup_ar(page2)
    return page2, used_ports, port2


# Slugs amplios AR que requieren filtro por franchise
BROAD_SLUGS_AR = {
    "zapatillas", "zapatillas-running", "botines", "zapatillas-trail",
    "zapatillas-campus", "zapatillas-forum", "zapatillas-grand_court",
    "botines-predator", "copa_pure", "botines-f50", "predator",
    "football-shoes", "samba", "campus", "forum",
    # Slugs reales adidas.com.ar (con guiones) que son páginas de categoría amplia
    "zapatillas-supernova", "zapatillas-questar", "zapatillas-duramo",
    "zapatillas-court", "zapatillas-rivalry", "zapatillas-terrex-agravic",
    "zapatillas-adizero", "zapatillas-forum", "zapatillas-campus",
}


def _filter_broad(products: List[dict], franchise: str, slug: str) -> Optional[List[dict]]:
    """Filtra productos si estamos en página amplia. Retorna None si debe descartarse."""
    tokens = _get_tokens(franchise)

    if slug not in BROAD_SLUGS_AR and not slug.endswith("football-shoes"):
        # Aun en páginas específicas aplicar filtro de botines para no tomar producto equivocado
        if franchise.upper() in BOTINES_FILTER_TOKENS:
            filtered = [p for p in products if franchise_matches_text(p.get("name", ""), tokens)]
            log(f"   🔍 Filtro botines '{franchise}': {len(filtered)}/{len(products)}")
            if filtered:
                return filtered
            # Si no matchea nada con tokens específicos, descartar (evitar precio equivocado)
            log(f"   ⚠️  Sin match de botines en página específica — descartando")
            return None
        return products

    filtered = [p for p in products if franchise_matches_text(p.get("name", ""), tokens)]
    log(f"   🔍 Filtro '{franchise}': {len(filtered)}/{len(products)}")
    if filtered:
        return filtered
    if slug in ("zapatillas", "botines", "football-shoes"):
        log(f"   ⚠️  Filtro sin resultados en página muy amplia — descartando")
        return None
    return products  # páginas semi-amplias: usar todos antes que N/D


async def scrape_ar_franchise(
    page, franchise: str,
    pw=None,
    used_ports=None,
    consecutive_timeouts: List[int] = None,
) -> Tuple[Optional[dict], Any, List[int]]:
    """
    Retorna (producto_o_None, page_actual, used_ports).
    page_actual puede ser diferente si hubo rebuild.
    consecutive_timeouts es una lista de 1 entero [n] compartida por el loop principal.
    """
    if used_ports is None:
        used_ports = []
    if consecutive_timeouts is None:
        consecutive_timeouts = [0]

    slugs = get_urls_ar(franchise)
    log(f"   AR URLs: {slugs}")

    sparse_kw = ["adios_pro", "adios pro", "speedportal", "elite", "copa_pure"]
    wait_time = TO_NEXT_DATA_AR + 10 if any(k in franchise.lower() for k in sparse_kw) else TO_NEXT_DATA_AR

    # ── Rebuild preventivo si venimos de 2+ timeouts seguidos ──
    if pw and consecutive_timeouts[0] >= 2:
        log(f"   ⚠️  {consecutive_timeouts[0]} timeouts consecutivos — rebuild preventivo de IP")
        try:
            await page.context.browser.close()
        except Exception:
            pass
        page, used_ports, _ = await _rebuild_browser_ar(pw, used_ports, reason="timeouts consecutivos")
        consecutive_timeouts[0] = 0

    current_port = used_ports[-1] if used_ports else AR_PORTS[0]

    for slug in slugs:
        url = f"{BASE_AR}/{slug}" if not slug.startswith("http") else slug
        log(f"\n   🔗 AR → {url}")
        try:
            await page.goto(url, wait_until="commit", timeout=TO_NAV_AR)
            await asyncio.sleep(random.uniform(1.5, 2.5))

            title = await page.title()
            log(f"   📄 '{title[:55]}' | {slug}")

            # ── Detectar redirect AR (slug con _ → /zapatillas genérico) ──
            try:
                current_url = page.url
                actual_slug = current_url.rstrip("/").split("adidas.com.ar/")[-1] if "adidas.com.ar/" in current_url else ""
                if actual_slug and actual_slug != slug:
                    log(f"   ⚠️  Browser redirect: /{slug} → /{actual_slug} — saltando slug")
                    continue
            except Exception:
                pass

            # ── Detectar challenge Akamai ──
            if is_challenge_page(title):
                log(f"   🚫 Challenge Akamai — rebuild con nueva IP...")
                if pw:
                    try:
                        await page.context.browser.close()
                    except Exception:
                        pass
                    page, used_ports, current_port = await _rebuild_browser_ar(pw, used_ports, reason="challenge")
                    consecutive_timeouts[0] = 0
                    await page.goto(url, wait_until="commit", timeout=TO_NAV_AR)
                    await asyncio.sleep(random.uniform(2.0, 3.0))
                    title = await page.title()
                    log(f"   📄 '{title[:55]}' | retry post-challenge")
                    if is_challenge_page(title):
                        log(f"   🚫 Sigue bloqueado — saltando slug")
                        continue
                else:
                    continue

            # ── Título vacío: esperar hidratación ──
            if title.strip() in ("", "adidas"):
                log(f"   ⏳ Título vacío/genérico — esperando hidratación (+4s)")
                await asyncio.sleep(4.0)
                title = await page.title()

            consecutive_timeouts[0] = 0  # cargó algo, reset

            products = await wait_next_data_ar(page, max_wait=wait_time)

            # ── Fallback 1: parsear HTML de la página ya cargada ──
            if not products:
                log(f"   🔄 JS vacío — intentando fallback HTML parsing...")
                html = await get_page_html(page)
                if html:
                    products = extract_next_data_html(html, currency="ARS", base_url=BASE_AR,
                                                      requested_slug=slug)
                    if products:
                        log(f"   ✅ HTML fallback OK — {len(products)} productos")

            # ── Fallback 2: requests directo con misma IP ──
            if not products:
                log(f"   🔄 Fallback requests AR (puerto {current_port})...")
                products = await asyncio.get_event_loop().run_in_executor(
                    None, scrape_ar_requests_fallback, url, current_port
                )
                if products:
                    log(f"   ✅ Requests fallback OK — {len(products)} productos")

            if not products:
                # Título genérico y sin datos = IP degradada → rebuild
                if title.strip() in ("", "adidas"):
                    log(f"   ⚠️  Título genérico y sin productos — IP degradada")
                    consecutive_timeouts[0] += 1
                    if pw:
                        log(f"   🔄 Rebuild por IP degradada...")
                        try:
                            await page.context.browser.close()
                        except Exception:
                            pass
                        page, used_ports, current_port = await _rebuild_browser_ar(pw, used_ports, reason="IP degradada sin datos")
                        consecutive_timeouts[0] = 0
                        # Retry slug con nueva IP
                        try:
                            await page.goto(url, wait_until="commit", timeout=TO_NAV_AR)
                            await asyncio.sleep(random.uniform(2.0, 3.5))
                            title2 = await page.title()
                            log(f"   📄 Retry '{title2[:55]}' | {slug}")
                            if not is_challenge_page(title2):
                                products2 = await wait_next_data_ar(page, max_wait=wait_time)
                                if not products2:
                                    html2 = await get_page_html(page)
                                    if html2:
                                        products2 = extract_next_data_html(html2, currency="ARS", base_url=BASE_AR,
                                                                           requested_slug=slug)
                                if products2:
                                    filtered2 = _filter_broad(products2, franchise, slug)
                                    if filtered2 is not None:
                                        best = pick_best_product(filtered2, franchise)
                                        if best:
                                            return best, page, used_ports
                        except PWTimeout:
                            log(f"   ⏱️  Timeout en retry post-rebuild: {url}")
                            consecutive_timeouts[0] += 1
                        except Exception as e2:
                            log(f"   ⚠️  Error retry: {str(e2)[:80]}")
                continue

            # ── Filtrar si es página amplia ──
            filtered = _filter_broad(products, franchise, slug)
            if filtered is None:
                continue
            products = filtered

            best = pick_best_product(products, franchise)
            if best:
                consecutive_timeouts[0] = 0
                return best, page, used_ports

        except PWTimeout:
            log(f"   ⏱️  Timeout: {url}")
            consecutive_timeouts[0] += 1
            # Fallback inmediato via requests cuando Playwright timeout
            log(f"   🔄 Timeout browser — intentando requests fallback...")
            products_fb = await asyncio.get_event_loop().run_in_executor(
                None, scrape_ar_requests_fallback, url, current_port
            )
            if products_fb:
                log(f"   ✅ Requests fallback post-timeout OK — {len(products_fb)} productos")
                filtered_fb = _filter_broad(products_fb, franchise, slug)
                if filtered_fb is not None:
                    best = pick_best_product(filtered_fb, franchise)
                    if best:
                        consecutive_timeouts[0] = 0
                        return best, page, used_ports
            # Rebuild si timeout en URL amplia
            if pw and slug in ("zapatillas-running", "zapatillas", "botines") and consecutive_timeouts[0] >= 2:
                log(f"   🔄 Timeout en URL fallback — rebuild urgente de IP")
                try:
                    await page.context.browser.close()
                except Exception:
                    pass
                page, used_ports, current_port = await _rebuild_browser_ar(pw, used_ports, reason="timeout en fallback")
                consecutive_timeouts[0] = 0

        except Exception as e:
            err = str(e)[:120]
            if "ERR_TUNNEL" in err:
                log(f"   ⚠️  ERR_TUNNEL (proxy caído momentáneamente)")
                consecutive_timeouts[0] += 1
            elif "ERR_ABORTED" in err:
                log(f"   ⚠️  ERR_ABORTED — navegación interrumpida")
            else:
                log(f"   ⚠️  Error: {err}")
        await asyncio.sleep(random.uniform(1.0, 2.0))

    log(f"   ❌ AR sin resultado: {franchise}")
    return None, page, used_ports


# ─────────────────────────────────────────────────────────────────
# SCRAPER US — requests + Site Unblocker
# ─────────────────────────────────────────────────────────────────
US_PROXIES = {
    "http":  f"http://{UB_USER}:{UB_PASS}@{UB_HOST}:{UB_PORT}",
    "https": f"http://{UB_USER}:{UB_PASS}@{UB_HOST}:{UB_PORT}",
}
US_HEADERS = {
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language":    "en-US,en;q=0.9",
    "Accept":             "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "sec-ch-ua":          '"Chromium";v="124", "Google Chrome";v="124"',
    "sec-ch-ua-mobile":   "?0",
    "sec-ch-ua-platform": '"Windows"',
}


def extract_next_data_us(html: str) -> List[dict]:
    """Extrae productos US del HTML usando múltiples estrategias."""
    BASE_US_URL = "https://www.adidas.com"

    # Reusar el parser genérico que ya maneja múltiples rutas
    results = extract_next_data_html(html, currency="USD", base_url=BASE_US_URL)
    if results:
        return results

    # Fallback específico US: buscar __NEXT_DATA__ con id alternativo
    for pat in [
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        r'<script[^>]+id=["\']__NEXT_DATA__["\']>(.*?)</script>',
    ]:
        m = re.search(pat, html, re.DOTALL)
        if m:
            try:
                results = _parse_next_data_json(m.group(1), "USD", BASE_US_URL)
                if results:
                    return results
            except Exception as e:
                log(f"   ⚠️  parse US fallback: {e}")

    return []


# URLs US que son páginas amplias y requieren filtro por gama
US_BROAD_SLUGS = {"football-shoes", "soccer-shoes", "cleats", "f50-soccer-shoes",
                  "predator-soccer-shoes", "running-shoes"}


def _apply_us_filter(products: List[dict], franchise: str, slug_us: str) -> Optional[List[dict]]:
    """Filtra productos US en páginas amplias. None = descartar esta URL."""
    is_broad = any(b in slug_us for b in US_BROAD_SLUGS)
    # Botines: siempre filtrar con tokens específicos
    if franchise.upper() in BOTINES_FILTER_TOKENS:
        tokens   = BOTINES_FILTER_TOKENS[franchise.upper()]
        filtered = [p for p in products if franchise_matches_text(p.get("name", ""), tokens)]
        log(f"   🔍 Filtro botines US '{franchise}': {len(filtered)}/{len(products)}")
        if filtered:
            return filtered
        log(f"   ⚠️  Sin match botines en US — saltando")
        return None
    if not is_broad:
        return products
    tokens   = _get_tokens(franchise)
    filtered = [p for p in products if franchise_matches_text(p.get("name", ""), tokens)]
    log(f"   🔍 Filtro US '{franchise}': {len(filtered)}/{len(products)}")
    if filtered:
        return filtered
    log(f"   ⚠️  Sin match en página amplia US — saltando")
    return None


def _scrape_us_url(url: str, franchise: str, max_retries: int = 3) -> Optional[dict]:
    """Intenta obtener el mejor producto de una URL US con reintentos."""
    slug_us = url.split('/us/')[-1]

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, headers=US_HEADERS, proxies=US_PROXIES,
                             verify=False, timeout=TO_REQ_US)
            log(f"   📊 {r.status_code} | {len(r.text):,} bytes")
            if r.status_code == 404:
                return None  # URL definitivamente no existe
            if r.status_code != 200:
                time.sleep(random.uniform(2.0, 4.0))
                continue
            products = extract_next_data_us(r.text)
            if products:
                log(f"   ✅ {len(products)} productos US")
                filtered = _apply_us_filter(products, franchise, slug_us)
                if filtered is None:
                    return None
                return pick_best_product(filtered, franchise)
            else:
                log(f"   ⚠️  sin __NEXT_DATA__ US (intento {attempt})")
                if attempt < max_retries:
                    time.sleep(random.uniform(3.0, 5.0))
        except requests.exceptions.Timeout:
            log(f"   ⏱️  US Timeout (intento {attempt})")
            if attempt < max_retries:
                time.sleep(random.uniform(3.0, 5.0))
        except Exception as e:
            log(f"   ⚠️  US Error: {str(e)[:100]}")
            break
    return None


def scrape_us_franchise(franchise: str) -> Optional[dict]:
    urls = get_urls_us(franchise)
    log(f"   US URLs: {[u.split('/us/')[-1] for u in urls]}")
    for url in urls:
        slug_us = url.split('/us/')[-1]
        log(f"\n   🔗 US → {slug_us}")
        best = _scrape_us_url(url, franchise)
        if best:
            return best
        time.sleep(random.uniform(1.5, 3.0))
    log(f"   ❌ US sin resultado: {franchise}")
    return None


# ─────────────────────────────────────────────────────────────────
# SCRAPING PARALELO AR + US
# ─────────────────────────────────────────────────────────────────
async def scrape_all(franchises: List[str]) -> Dict[str, Dict]:
    results: Dict[str, Dict] = {f: {"ar": None, "us": None} for f in franchises}

    async def run_ar(pw):
        browser = page = None
        used_ports: List[int] = []
        try:
            browser, _, page, port = await build_ar_browser(pw)
            used_ports.append(port)
            ok = await warmup_ar(page)
            if not ok:
                log("   ⚠️  Warmup falló — reintentando con otro puerto...")
                try: await browser.close()
                except Exception: pass
                browser, _, page, port = await build_ar_browser(pw, exclude_ports=used_ports)
                used_ports.append(port)
                await warmup_ar(page)

            log("\n" + "="*55)
            log(f"🏁 AR — {len(franchises)} franquicias")
            log("="*55)
            consecutive_timeouts = [0]   # contador mutable compartido
            for i, fr in enumerate(franchises):
                log(f"\n[AR {i+1}/{len(franchises)}] {fr}")
                result, page, used_ports = await scrape_ar_franchise(
                    page, fr, pw=pw, used_ports=used_ports,
                    consecutive_timeouts=consecutive_timeouts,
                )
                results[fr]["ar"] = result
                await asyncio.sleep(random.uniform(2.0, 4.0))

        except Exception as e:
            log(f"❌ Error fatal AR: {e}")
            import traceback; traceback.print_exc()
        finally:
            if browser:
                try: await browser.close()
                except Exception: pass

    def run_us():
        log("\n" + "="*55)
        log(f"🏁 US — {len(franchises)} franquicias")
        log("="*55)
        for i, fr in enumerate(franchises):
            log(f"\n[US {i+1}/{len(franchises)}] {fr}")
            results[fr]["us"] = scrape_us_franchise(fr)
            time.sleep(random.uniform(2.0, 4.0))

    async with async_playwright() as pw:
        loop    = asyncio.get_event_loop()
        ar_task = asyncio.ensure_future(run_ar(pw))
        us_task = loop.run_in_executor(None, run_us)
        await asyncio.gather(ar_task, us_task)

    return results


# ─────────────────────────────────────────────────────────────────
# CONSTRUCCIÓN DE FILAS
# ─────────────────────────────────────────────────────────────────
def build_rows(
    comparativa: pd.DataFrame,
    df_sb: pd.DataFrame,
    scrape_results: Dict[str, Dict],
    fx: float,
    run_date: str,
) -> Tuple[List[Dict], List[Dict]]:

    rows1: List[Dict] = []
    rows2: List[Dict] = []

    for _, comp_row in comparativa.iterrows():
        nike_fr   = str(comp_row.get("NIKE",      "")).strip()
        adidas_fr = str(comp_row.get("ADIDAS",    "")).strip()
        categoria = str(comp_row.get("Categoria", "")).strip()
        # Normalizar nombre de categoría fútbol
        if re.match(r"^f[uú]tbol$", categoria, re.I):
            categoria = "FOOTBALL/SOCCER"
        if not nike_fr or not adidas_fr:
            continue

        ar_p = scrape_results.get(adidas_fr, {}).get("ar")
        us_p = scrape_results.get(adidas_fr, {}).get("us")

        ar_price  = ar_p.get("full_price") if ar_p else None
        ar_name   = ar_p.get("name", "")   if ar_p else ""
        ar_url    = ar_p.get("url",  "")   if ar_p else ""
        us_price  = us_p.get("full_price") if us_p else None
        us_name   = us_p.get("name", "")   if us_p else ""

        # Nike del StatusBooks — filtrar siempre a BU=FW; si es fútbol también Category=FOOTBALL/SOCCER
        df_fw = df_sb[df_sb["BU"].astype(str).str.contains("FW", case=False, na=False)].copy()
        if categoria.upper() == "FOOTBALL/SOCCER":
            df_fw = df_fw[df_fw["Category"].astype(str).str.contains("FOOTBALL|SOCCER", case=False, na=False)]
        stylecolor, mk_name = find_nike_stylecolor(nike_fr, df_fw)
        nike_price: Optional[float] = None
        if stylecolor:
            m = df_fw[df_fw["Product Code"].str.upper() == stylecolor.upper()]
            if not m.empty:
                nike_price = safe_float(m.iloc[0].get("_price"))
        mk_name = mk_name or ""

        # Log vacíos para diagnóstico
        if not stylecolor:
            log(f"   ⚠️  [build_rows] Sin StyleColor Nike para franchise '{nike_fr}'")
        if ar_p is None:
            log(f"   ⚠️  [build_rows] Sin precio AR para Adidas '{adidas_fr}'")
        if us_p is None:
            log(f"   ⚠️  [build_rows] Sin precio US para Adidas '{adidas_fr}'")

        # ── OUTPUT 1: Nike AR vs Adidas AR ────────────────────────
        nk_shp  = shipping_nike_ar(nike_price)
        ad_shp  = shipping_adidas_ar(ar_price)
        nk_tot  = (nike_price + nk_shp) if nike_price is not None else None
        ad_tot  = (ar_price   + ad_shp) if ar_price   is not None else None

        gap     = (ar_price - nike_price) / nike_price if ar_price and nike_price else None
        gap_shp = (ad_tot   - nk_tot)    / nk_tot     if ad_tot   and nk_tot     else None

        rows1.append({
            "Fecha":                          run_date,
            "Categoria":                      categoria,
            "StyleColor":                     stylecolor or "",
            "Marketing Name (Nike)":          mk_name,
            "Franchise Nike":                 nike_fr,
            "Franchise Adidas":               adidas_fr,
            "Adidas Product Name":            ar_name,
            "Adidas PDP (AR)":                ar_url,
            "Nike Full Price (ARS)":          nike_price,
            "Adidas Full Price (ARS)":        ar_price,
            "Gap %":                          gap,
            "BML Full Price":                 bml(ar_price, nike_price),
            "Nike Price + Shipping (ARS)":    nk_tot,
            "Adidas Price + Shipping (ARS)":  ad_tot,
            "Gap % con Shipping":             gap_shp,
            "BML + Shp":                      bml(ad_tot, nk_tot),
        })

        # ── OUTPUT 2: Adidas AR vs Adidas US ──────────────────────
        ar_usd      = ar_price / fx                         if ar_price and fx > 0 else None
        us_iva      = us_price * (1 + IVA)                  if us_price else None
        us_iva_bf   = us_price * (1 + IVA) * (1 + BF)      if us_price else None
        shp_ar_usd  = shipping_adidas_ar(ar_price) / fx     if fx > 0 else 0.0
        shp_us_usd  = shipping_us(us_price)
        ar_shp      = (ar_usd    + shp_ar_usd)              if ar_usd  else None
        us_shp      = (us_iva_bf + shp_us_usd)              if us_price else None

        us_url  = us_p.get("url",  "")   if us_p else ""
        rows2.append({
            "Fecha":                   run_date,
            "Categoria":               categoria,
            "Franchise":               adidas_fr,
            "Adidas AR Product Name":  ar_name,
            "Adidas US Product Name":  us_name,
            "PDP ARG":                 ar_url,
            "PDP US":                  us_url,
            "Retail ARG (ARS)":        ar_price,
            "FX ARS/USD":              fx,
            "ARG (USD)":               ar_usd,
            "USA Full (USD)":          us_price,
            "Dif FP vs USA":           ar_usd / us_price - 1    if ar_usd and us_price  else None,
            "USA + 21% IVA":           us_iva,
            "Dif FP + IVA":            ar_usd / us_iva - 1      if ar_usd and us_iva    else None,
            "BML c/ IVA":              bml(ar_usd, us_iva),
            "USA + 21% + BF 8% (USD)": us_iva_bf,
            "Dif FP + IVA + BF":       ar_usd / us_iva_bf - 1   if ar_usd and us_iva_bf else None,
            "BML c/ IVA + BF":         bml(ar_usd, us_iva_bf),
            "AR + Shp":                ar_shp,
            "US + Shp":                us_shp,
            "Dif + Shp":               ar_shp / us_shp - 1      if ar_shp and us_shp    else None,
            "BML + Shp":               bml(ar_shp, us_shp),
        })

    return rows1, rows2


# ─────────────────────────────────────────────────────────────────
# EXCEL + CSV
# ─────────────────────────────────────────────────────────────────
COLS_OUT1 = [
    "Fecha", "Categoria", "StyleColor", "Marketing Name (Nike)",
    "Franchise Nike", "Franchise Adidas",
    "Adidas Product Name", "Adidas PDP (AR)",
    "Nike Full Price (ARS)", "Adidas Full Price (ARS)", "Gap %", "BML Full Price",
    "Nike Price + Shipping (ARS)", "Adidas Price + Shipping (ARS)", "Gap % con Shipping", "BML + Shp",
]

COLS_OUT2 = [
    "Fecha", "Categoria", "Franchise",
    "Adidas AR Product Name", "Adidas US Product Name",
    "PDP ARG", "PDP US",
    "Retail ARG (ARS)", "FX ARS/USD", "ARG (USD)",
    "USA Full (USD)", "Dif FP vs USA",
    "USA + 21% IVA", "Dif FP + IVA", "BML c/ IVA",
    "USA + 21% + BF 8% (USD)", "Dif FP + IVA + BF", "BML c/ IVA + BF",
    "AR + Shp", "US + Shp", "Dif + Shp", "BML + Shp",
]

HDR_BG    = "#1F4E79"
BML_COLS1 = {"BML Full Price", "BML + Shp"}
BML_COLS2 = {"BML c/ IVA", "BML c/ IVA + BF", "BML + Shp"}


def _add_fmts(wb):
    base = {"align": "left", "valign": "vcenter"}
    return {
        "hdr":  wb.add_format({"bold": True, "font_color": "white", "bg_color": HDR_BG,
                                "align": "center", "valign": "vcenter", "border": 1}),
        "txt":  wb.add_format({**base}),
        "ars":  wb.add_format({**base, "num_format": '"$" #,##0'}),
        "usd":  wb.add_format({**base, "num_format": '"USD" #,##0.00'}),
        "pct":  wb.add_format({**base, "num_format": "0.00%"}),
        "fx":   wb.add_format({**base, "num_format": "#,##0.00"}),
        "link": wb.add_format({**base, "font_color": "blue", "underline": 1}),
        # Formatos BML con color de fondo — se aplican al escribir la celda, no como condicional
        "beat": wb.add_format({**base, "bg_color": "#C6EFCE", "bold": True}),
        "meet": wb.add_format({**base, "bg_color": "#FFEB9C", "bold": True}),
        "lose": wb.add_format({**base, "bg_color": "#FFC7CE", "bold": True}),
        "nd":   wb.add_format({**base, "bg_color": "#D9D9D9"}),
        "zebra":wb.add_format({"bg_color": "#F2F2F2"}),
    }


def _write_sheet(wb, fmts, name: str, rows: List[Dict], cols: List[str], bml_cols: set):
    # Columnas que contienen "%" en el nombre pero deben mostrarse como USD (no %)
    USD_OVERRIDE = {"USA + 21% IVA", "USA + 21% + BF 8% (USD)"}
    # pct_cols tiene prioridad sobre usd_cols — los "Dif" y "Gap" son siempre %
    pct_cols  = {c for c in cols if ("%" in c or c.startswith("Dif") or c.startswith("Gap")) and c not in USD_OVERRIDE}
    ars_cols  = {c for c in cols if "ARS" in c and "Shipping" not in c} - pct_cols
    ship_cols = {c for c in cols if "Shipping" in c} - pct_cols
    usd_cols  = {c for c in cols if any(k in c for k in ("USD","IVA","BF","AR + Shp","US + Shp","ARG (USD)"))} - pct_cols
    link_cols = {c for c in cols if "PDP" in c or "Link" in c or "URL" in c or c in ("PDP ARG","PDP US")}
    fx_cols   = {"FX ARS/USD"}

    ws = wb.add_worksheet(name)
    ws.freeze_panes(1, 0)
    ws.autofilter(0, 0, 0, len(cols) - 1)
    ws.set_row(0, 22)
    for j, c in enumerate(cols):
        ws.write(0, j, c, fmts["hdr"])

    def _is_num(x):
        if x is None: return False
        if isinstance(x, (int, float)): return True
        try: float(x); return True
        except (ValueError, TypeError): return False

    def _bml_fmt(v):
        """Retorna el formato de color según valor BML."""
        if v is None: return fmts["nd"]
        s = str(v).upper().strip()
        if s == "BEAT": return fmts["beat"]
        if s == "MEET": return fmts["meet"]
        if s == "LOSE": return fmts["lose"]
        return fmts["nd"]

    for i, row in enumerate(rows, start=1):
        ws.set_row(i, 18)
        for j, c in enumerate(cols):
            v = row.get(c)
            if c in bml_cols:
                # Escribir BML con color directo — no depender de conditional_format
                ws.write(i, j, v if v is not None else "N/D", _bml_fmt(v))
            elif c in link_cols:
                if isinstance(v, str) and v.startswith("http"):
                    ws.write_url(i, j, v, fmts["link"], string="Open")
                else:
                    ws.write(i, j, v or "", fmts["txt"])
            elif c in ars_cols or c in ship_cols:
                ws.write_number(i, j, float(v), fmts["ars"])  if _is_num(v) else ws.write(i, j, v if v is not None else "", fmts["txt"])
            elif c in usd_cols:
                ws.write_number(i, j, float(v), fmts["usd"])  if _is_num(v) else ws.write(i, j, v if v is not None else "", fmts["txt"])
            elif c in pct_cols:
                ws.write_number(i, j, float(v), fmts["pct"])  if _is_num(v) else ws.write(i, j, v if v is not None else "", fmts["txt"])
            elif c in fx_cols:
                ws.write_number(i, j, float(v), fmts["fx"])   if _is_num(v) else ws.write(i, j, v if v is not None else "", fmts["txt"])
            else:
                ws.write(i, j, v if v is not None else "", fmts["txt"])

    n = len(rows)
    ws.conditional_format(1, 0, n, len(cols)-1,
                          {"type": "formula", "criteria": "=MOD(ROW(),2)=0", "format": fmts["zebra"]})

    widths = {c: max(12, min(60, len(c)+2)) for c in cols}
    for row in rows[:150]:
        for c in cols:
            v = row.get(c)
            if v is not None:
                widths[c] = max(widths[c], min(60, len(str(v))+2))
    for j, c in enumerate(cols):
        ws.set_column(j, j, widths[c])


def write_excel(path: str, rows1: List[Dict], rows2: List[Dict]):
    log(f"📝 Escribiendo Excel: {path}")
    wb   = xlsxwriter.Workbook(path)
    fmts = _add_fmts(wb)
    _write_sheet(wb, fmts, "Nike AR vs Adidas AR",   rows1, COLS_OUT1, BML_COLS1)
    _write_sheet(wb, fmts, "Adidas AR vs Adidas US", rows2, COLS_OUT2, BML_COLS2)
    wb.close()
    log("   ✅ Excel OK")


def write_csv(path: str, rows: List[Dict], cols: List[str]):
    log(f"📄 CSV: {path}")
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns: df[c] = None
    df[cols].to_csv(path, index=False, encoding="utf-8-sig")
    log("   ✅ CSV OK")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
async def main():
    start    = time.time()
    ts       = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_date = dt.datetime.now().strftime("%Y-%m-%d")

    log("=" * 60)
    log("🚀 ADIDAS AR + US — Price Monitoring")
    log(f"   Season: {SEASON} | FX: {FX_MODE}")
    log("=" * 60)

    df_sb       = load_statusbooks(STATUSBOOKS_PATH, SEASON)
    comparativa = load_comparativa(COMPARATIVA_PATH)
    fx          = get_fx()

    franchises = list(comparativa["ADIDAS"].dropna().unique())
    log(f"\n📦 {len(franchises)} franquicias: {franchises}")

    log("\n" + "="*60)
    log("🕷️  Scraping AR + US en paralelo...")
    log("="*60)
    scrape_results = await scrape_all(franchises)

    log("\n" + "="*60)
    log("🧮 Calculando outputs...")
    rows1, rows2 = build_rows(comparativa, df_sb, scrape_results, fx, run_date)

    out_xlsx = f"Adidas_Monitoring_{SEASON}_{ts}.xlsx"
    out_csv1 = f"Adidas_NikeAR_vs_AdidasAR_{SEASON}_{ts}.csv"
    out_csv2 = f"Adidas_AdidasAR_vs_AdidasUS_{SEASON}_{ts}.csv"

    write_excel(out_xlsx, rows1, rows2)
    write_csv(out_csv1, rows1, COLS_OUT1)
    write_csv(out_csv2, rows2, COLS_OUT2)

    elapsed = time.time() - start
    ok_ar = sum(1 for r in rows1 if r.get("Adidas Full Price (ARS)") is not None)
    ok_us = sum(1 for r in rows2 if r.get("USA Full (USD)")          is not None)

    log("\n" + "="*60)
    log(f"🎉 Completado en {elapsed/60:.1f} min")
    log(f"   AR: {ok_ar}/{len(rows1)} | US: {ok_us}/{len(rows2)}")
    log(f"   📊 {out_xlsx}")
    log(f"   📄 {out_csv1}")
    log(f"   📄 {out_csv2}")
    log("="*60)

    log("\nRESUMEN:")
    for r1, r2 in zip(rows1, rows2):
        fr   = r1.get("Franchise Adidas", "")
        p_ar = r1.get("Adidas Full Price (ARS)")
        p_us = r2.get("USA Full (USD)")
        b1   = r1.get("BML Full Price", "N/D")
        b2   = r2.get("BML + Shp",      "N/D")
        ar_s = f"ARS {p_ar:>10,.0f}" if p_ar else "        SIN DATO"
        us_s = f"USD {p_us:>8.2f}"   if p_us else "    SIN DATO"
        log(f"  {fr:25s} | {ar_s} | {us_s} | AR:{b1:4s} US:{b2:4s}")


if __name__ == "__main__":
    asyncio.run(main())
