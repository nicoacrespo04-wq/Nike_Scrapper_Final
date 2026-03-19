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
#   - AR : proxy.smartproxy.net:3120      (Playwright + proxy residencial)
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

# Proxy AR — Decodo Site Unblocker (validado)
AR_HOST  = "unblock.decodo.com"
AR_PORTS = [60000]
AR_USER  = "U0000358219"
AR_PASS  = "PW_13c62a6853fe2bb0a6b377b55a4e8d8ec"

# Proxy US — Site Unblocker
UB_HOST  = "unblock.decodo.com"
UB_PORT  = 60000
UB_USER  = "U0000358219"
UB_PASS  = "PW_13c62a6853fe2bb0a6b377b55a4e8d8ec"

BASE_AR  = "https://www.adidas.com.ar"
BASE_US  = "https://www.adidas.com/us"

HEADLESS    = True
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
TO_NAV_AR       = 35_000   # ms — goto con wait_until="commit"
TO_WARMUP_AR    = 40_000   # ms
TO_NEXT_DATA_AR = 18       # segundos de polling __NEXT_DATA__
TO_REQ_US       = 45       # segundos requests


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


def find_nike_stylecolor(nike_franchise: str, df_sb: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    tokens = parse_franchise_tokens(nike_franchise)
    if not tokens:
        return None, None
    for _, row in df_sb.iterrows():
        sb_fr = str(row.get("Franchise", "")).strip()
        if franchise_matches_text(sb_fr, tokens):
            sc = str(row.get("Product Code", "")).strip().upper()
            mn = str(row.get("Marketing Name", "")).strip()
            if sc and row.get("_price", 0) > 0:
                return sc, mn
    return None, None


def find_nike_botin_by_gama(nike_gama: str, df_sb: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """
    Matching de botines Nike por GAMA (SILO BOTINES).
    Requiere:
      - Categoria contiene FOOTBALL/SOCCER
      - SILO BOTINES coincida con la gama buscada
      - Excluye Marketing Name con 'PROMINA'
      - Precio > 0
    """
    gama = str(nike_gama or "").strip().upper()
    if not gama:
        return None, None
    
    # Filtra por categoria futbol + SILO BOTINES + precio
    df_botines = df_sb[
        (df_sb["Category"].astype(str).str.contains("FOOTBALL|SOCCER", case=False, na=False)) &
        (df_sb["_price"] > 0)
    ].copy()
    
    if df_botines.empty:
        return None, None
    
    # Normalizar SILO BOTINES si existe, sino asignar vacío
    if "SILO BOTINES" not in df_botines.columns:
        if "SILO BOTIN" in df_botines.columns:
            df_botines["_silo"] = df_botines["SILO BOTIN"].astype(str).str.strip().str.upper()
        else:
            df_botines["_silo"] = ""
    else:
        df_botines["_silo"] = df_botines["SILO BOTINES"].astype(str).str.strip().str.upper()
    
    # Filtra por GAMA exacta
    df_botines = df_botines[df_botines["_silo"] == gama].copy()
    
    if df_botines.empty:
        return None, None
    
    # Excluye PROMINA en marketing name
    df_botines = df_botines[
        ~df_botines["Marketing Name"].astype(str).str.contains("PROMINA", case=False, na=False)
    ].copy()
    
    if df_botines.empty:
        return None, None
    
    # Retorna el primero encontrado
    row = df_botines.iloc[0]
    sc = str(row.get("Product Code", "")).strip().upper()
    mn = str(row.get("Marketing Name", "")).strip()
    return (sc, mn) if sc else (None, None)


def score_product(name: str, franchise: str) -> float:
    if is_blocked_name(name):
        return -999.0
    tokens = parse_franchise_tokens(franchise)
    if not tokens:
        return 0.0
    s = sum(2.0 for tok in tokens if tok in name.lower())
    s += 0.5
    return s


def pick_best_product(products: List[dict], franchise: str) -> Optional[dict]:
    if not products:
        return None
    valid = [p for p in products if not is_blocked_name(p.get("name", ""))]
    if not valid:
        log(f"   ⚠️  Todos bloqueados (ACCOUNT-PORTAL-DISABLE)")
        return None
    best = max(valid, key=lambda p: score_product(p.get("name", ""), franchise))
    s   = score_product(best.get("name", ""), franchise)
    log(f"   🎯 Best: '{best.get('name','')[:50]}' | score={s:.1f} | {best.get('currency','?')} {best.get('full_price',0):,.0f}")
    return best


# ─────────────────────────────────────────────────────────────────
# URL MAPS
# ─────────────────────────────────────────────────────────────────
FRANCHISE_URLS_AR: Dict[str, List[str]] = {
    "ADIZERO ADIOS PRO":   ["zapatillas-adizero", "zapatillas-running"],
    "ULTRABOOST":          ["zapatillas-ultraboost", "ultraboost", "zapatillas-running"],
    'ADIZERO "BOSTON"':    ["adizero-adizero_boston", "zapatillas-adizero", "zapatillas-running"],
    "ADISTAR":             ["zapatillas-adistar", "adistar", "zapatillas-running"],
    "PUREBOOST":           ["pureboost", "zapatillas-running"],
    'SUPERNOVA "STRIDE"':  ["zapatillas-supernova", "supernova", "zapatillas-running"],
    "QUESTAR":             ["zapatillas-questar", "questar", "zapatillas-running"],
    "RESPONSE":            ["zapatillas-response", "zapatillas-running"],
    "DURAMO SL":           ["zapatillas-duramo_sl", "zapatillas-duramo", "zapatillas-running"],
    "RUNFALCON":           ["zapatillas-runfalcon", "zapatillas-running"],
    "EQ AGRAVIC":          ["zapatillas-terrex_agravic", "zapatillas-trail", "zapatillas-running"],
    'SAMBA "DECON"':       ["samba_decon", "samba", "zapatillas"],
    "FORUM LOW":           ["zapatillas-forum", "forum", "zapatillas"],
    "SAMBA":               ["samba", "zapatillas"],
    "CAMPUS":              ["zapatillas-campus", "campus", "zapatillas"],
    'RIVALRY "LOW"':       ["zapatillas-rivalry_low", "zapatillas-rivalry", "zapatillas"],
    "VL COURT 3.0":        ["zapatillas-vl_court", "vl_court", "zapatillas"],
    "GRAND COURT":         ["zapatillas-grand_court", "grand_court", "zapatillas"],
    "COURT BASE":          ["zapatillas-grand_court_base", "zapatillas-grand_court", "zapatillas"],
    "SPEEDPORTAL":         ["botines-x_speedportal", "x_speedportal", "botines-f50", "botines"],
    "CLUB":                ["botines-f50", "botines"],
    "LEAGUE":              ["botines-f50", "botines"],
    "PRO":                 ["botines-predator", "predator", "botines"],
    "ELITE":               ["botines-predator", "predator", "botines"],
}

FRANCHISE_URLS_US: Dict[str, List[str]] = {
    "ADIZERO ADIOS PRO":   [f"{BASE_US}/adizero_adios_pro-running-shoes"],
    "ULTRABOOST":          [f"{BASE_US}/ultraboost-shoes", f"{BASE_US}/ultraboost-running-shoes"],
    'ADIZERO "BOSTON"':    [f"{BASE_US}/adizero_boston-shoes", f"{BASE_US}/adizero_boston-running-shoes"],
    "ADISTAR":             [f"{BASE_US}/adistar-shoes", f"{BASE_US}/adistar-running-shoes"],
    "PUREBOOST":           [f"{BASE_US}/pureboost-shoes", f"{BASE_US}/pureboost-running-shoes"],
    'SUPERNOVA "STRIDE"':  [f"{BASE_US}/supernova_stride-shoes", f"{BASE_US}/supernova-shoes"],
    "QUESTAR":             [f"{BASE_US}/questar-shoes"],
    "RESPONSE":            [f"{BASE_US}/response-shoes", f"{BASE_US}/response-running-shoes"],
    "DURAMO SL":           [f"{BASE_US}/duramo_sl-shoes", f"{BASE_US}/duramo-shoes"],
    "RUNFALCON":           [f"{BASE_US}/runfalcon-shoes"],
    "EQ AGRAVIC":          [f"{BASE_US}/terrex_agravic-trail-running-shoes", f"{BASE_US}/terrex-trail-running-shoes"],
    'SAMBA "DECON"':       [f"{BASE_US}/samba_decon-shoes", f"{BASE_US}/samba-shoes"],
    "FORUM LOW":           [f"{BASE_US}/forum_low-shoes", f"{BASE_US}/forum-shoes"],
    "SAMBA":               [f"{BASE_US}/samba-shoes"],
    "CAMPUS":              [f"{BASE_US}/campus-shoes"],
    'RIVALRY "LOW"':       [f"{BASE_US}/rivalry_low-shoes", f"{BASE_US}/rivalry-shoes"],
    "VL COURT 3.0":        [f"{BASE_US}/vl_court-shoes"],
    "GRAND COURT":         [f"{BASE_US}/grand_court-shoes"],
    "COURT BASE":          [f"{BASE_US}/grand_court_base-shoes", f"{BASE_US}/grand_court-shoes"],
    "SPEEDPORTAL":         [f"{BASE_US}/x_speedportal-soccer-shoes", f"{BASE_US}/f50-soccer-shoes", f"{BASE_US}/football-shoes"],
    "CLUB":                [f"{BASE_US}/f50_club-soccer-shoes", f"{BASE_US}/f50-soccer-shoes", f"{BASE_US}/football-shoes"],
    "LEAGUE":              [f"{BASE_US}/f50_league-soccer-shoes", f"{BASE_US}/f50-soccer-shoes", f"{BASE_US}/football-shoes"],
    "PRO":                 [f"{BASE_US}/predator_pro-soccer-shoes", f"{BASE_US}/predator-soccer-shoes", f"{BASE_US}/football-shoes"],
    "ELITE":               [f"{BASE_US}/predator_elite-soccer-shoes", f"{BASE_US}/predator-soccer-shoes", f"{BASE_US}/football-shoes"],
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
    const s = document.getElementById('__NEXT_DATA__');
    if (!s || !s.textContent) return [];
    try {
        const d = JSON.parse(s.textContent);
        let items = (
            d?.props?.pageProps?.products ||
            d?.props?.pageProps?.searchResult?.products ||
            d?.props?.pageProps?.plpData?.products ||
            d?.props?.pageProps?.initialData?.searchResult?.products ||
            []
        );
        if (!items.length) {
            function find(o, depth) {
                if (depth > 8 || !o || typeof o !== 'object') return null;
                if (Array.isArray(o) && o.length >= 1 && typeof o[0] === 'object'
                    && o[0]?.name && (o[0]?.url || o[0]?.productId || o[0]?.id)) return o;
                for (const v of Object.values(o)) { const r = find(v, depth+1); if (r) return r; }
                return null;
            }
            items = find(d, 0) || [];
        }
        if (!items.length) return [];
        return items.map(p => {
            let full = 0, final = 0;
            if (p.priceData?.prices) {
                const orig = p.priceData.prices.find(x => x.type === 'original');
                const sale = p.priceData.prices.find(x => x.type === 'sale');
                full  = orig ? parseFloat(orig.value)  : 0;
                final = sale ? parseFloat(sale.value)  : full;
            } else if (p.pricingInformation) {
                full  = parseFloat(p.pricingInformation.standardPrice || 0);
                final = parseFloat(p.pricingInformation.currentPrice  || full);
            } else if (p.price) {
                if (typeof p.price === 'object') {
                    full  = parseFloat(p.price.regular || p.price.current || p.price.value || 0);
                    final = parseFloat(p.price.current || p.price.value  || full);
                } else { full = final = parseFloat(p.price) || 0; }
            } else if (p.pricing) {
                full  = parseFloat(p.pricing.standard || p.pricing.regular || 0);
                final = parseFloat(p.pricing.sale     || p.pricing.current || full);
            }
            let url = p.url || p.link || p.productLink || p.pdpUrl || '';
            if (url && !url.startsWith('http')) url = 'https://www.adidas.com.ar' + url;
            return { name: p.title || p.name || p.displayName || '',
                     url, full_price: full, final_price: final, currency: 'ARS' };
        }).filter(p => p.name && (p.full_price > 0 || p.final_price > 0));
    } catch(e) { return []; }
}
"""

AR_DOM_PRODUCTS_JS = """
() => {
    const normPrice = (txt) => {
        if (!txt) return 0;
        const cleaned = String(txt)
            .replace(/\s+/g, ' ')
            .replace(/\./g, '')
            .replace(/,/g, '.')
            .replace(/[^\d.]/g, '');
        const n = parseFloat(cleaned);
        return Number.isFinite(n) ? n : 0;
    };

    const pickName = (card) => {
        const sel = [
            '[data-auto-id="product-title"]',
            '[data-auto-id="product-name"]',
            '[data-testid="product-card-primary-link"] span',
            'h3',
            'h4',
            'a[title]'
        ];
        for (const s of sel) {
            const el = card.querySelector(s);
            const t = (el?.textContent || el?.getAttribute?.('title') || '').trim();
            if (t) return t;
        }
        return '';
    };

    const pickUrl = (card) => {
        const a = card.querySelector('a[href]');
        if (!a) return '';
        const href = a.getAttribute('href') || '';
        if (!href) return '';
        return href.startsWith('http') ? href : ('https://www.adidas.com.ar' + href);
    };

    const cards = Array.from(document.querySelectorAll(
        '[data-auto-id="product-tile"], article[data-testid*="product"], article, .gl-product-card'
    ));
    const out = [];
    const seen = new Set();

    for (const card of cards) {
        const name = pickName(card);
        const url = pickUrl(card);
        if (!name || !url || seen.has(url)) continue;

        const priceNodes = Array.from(card.querySelectorAll(
            '[data-auto-id="product-price"], [data-testid*="price"], .gl-price-item, [class*="price"]'
        ));
        const nums = [];
        for (const node of priceNodes) {
            const n = normPrice(node.textContent || '');
            if (n > 0) nums.push(n);
        }
        if (!nums.length) continue;

        const full = Math.max(...nums);
        const final = Math.min(...nums);
        out.push({ name, url, full_price: full, final_price: final, currency: 'ARS' });
        seen.add(url);
    }

    return out;
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
        "Mozilla/5.0 (MacintFosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    ])
    browser = await pw.chromium.launch(
        headless=HEADLESS, proxy=proxy,
        args=["--disable-blink-features=AutomationControlled", "--no-sandbox",
              "--disable-dev-shm-usage", "--ignore-certificate-errors"],
    )
    context = await browser.new_context(
        locale="es-AR", timezone_id="America/Argentina/Buenos_Aires",
        user_agent=ua, viewport={"width": 1920, "height": 1080},
        ignore_https_errors=True,
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
    warmup_urls = [
        f"{BASE_AR}/zapatillas",
        BASE_AR,
    ]
    for attempt, warmup_url in enumerate(warmup_urls, start=1):
        try:
            await page.goto(warmup_url, wait_until="commit", timeout=TO_WARMUP_AR)
            await asyncio.sleep(random.uniform(1.5, 2.5))
            log(f"   ✅ Warmup AR OK — {warmup_url}")
            return True
        except Exception as e:
            log(f"   ⚠️  Warmup AR fallo (intento {attempt}): {str(e)[:60]}")
            await asyncio.sleep(2)
    return False


async def wait_next_data_ar(page, max_wait: int = TO_NEXT_DATA_AR) -> List[dict]:
    deadline = time.time() + max_wait
    attempt  = 0
    while time.time() < deadline:
        attempt += 1
        try:
            products = await page.evaluate(NEXT_DATA_JS)
            if products:
                log(f"   ✅ __NEXT_DATA__ OK — {len(products)} productos (intento {attempt})")
                return products

            # Fallback: en algunas PLPs AR no hidrata __NEXT_DATA__, pero sí renderiza cards en DOM.
            dom_products = await page.evaluate(AR_DOM_PRODUCTS_JS)
            if dom_products:
                log(f"   ✅ DOM fallback AR OK — {len(dom_products)} productos (intento {attempt})")
                return dom_products

            log(f"   ⏳ vacío (intento {attempt})")
        except Exception as e:
            log(f"   ⚠️  eval: {str(e)[:60]}")
        await asyncio.sleep(1.5)
    log(f"   ❌ __NEXT_DATA__ timeout {max_wait}s")
    return []


async def _rebuild_browser_ar(pw, used_ports: List[int], reason: str = ""):
    """Cierra el browser actual y abre uno nuevo con IP fresca. Devuelve (page, used_ports)."""
    log(f"   🔄 Rebuild browser AR{f' — {reason}' if reason else ''}...")
    await asyncio.sleep(random.uniform(3.0, 5.0))
    browser2, _, page2, port2 = await build_ar_browser(pw, exclude_ports=used_ports)
    used_ports.append(port2)
    ok = await warmup_ar(page2)
    if not ok:
        log("   ⚠️  Warmup post-rebuild falló — continuando igual")
    return page2, used_ports


async def scrape_ar_franchise(
    page, franchise: str,
    pw=None,
    used_ports=None,
    consecutive_timeouts: List[int] = None,   # mutable counter compartido entre franquicias
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
    wait_time = 25 if any(k in franchise.lower() for k in sparse_kw) else TO_NEXT_DATA_AR

    # ── Si venimos de demasiados timeouts consecutivos, rebuild preventivo ──
    if pw and consecutive_timeouts[0] >= 1:
        log(f"   ⚠️  {consecutive_timeouts[0]} timeouts consecutivos — rebuild preventivo de IP")
        try:
            old_browser = page.context.browser
            await old_browser.close()
        except Exception:
            pass
        page, used_ports = await _rebuild_browser_ar(pw, used_ports, reason="timeouts consecutivos")
        consecutive_timeouts[0] = 0

    got_something = False  # hubo al menos 1 carga exitosa (aunque vacía de productos)

    for slug in slugs:
        url = f"{BASE_AR}/{slug}" if not slug.startswith("http") else slug
        log(f"\n   🔗 AR → {url}")
        try:
            await page.goto(url, wait_until="commit", timeout=TO_NAV_AR)
            await asyncio.sleep(random.uniform(1.5, 2.5))

            title = await page.title()
            log(f"   📄 '{title[:55]}' | {slug}")

            # ── Detectar challenge ──
            if is_challenge_page(title):
                log(f"   🚫 Challenge Akamai — rebuild con nueva IP...")
                if pw:
                    try:
                        old_browser = page.context.browser
                        await old_browser.close()
                    except Exception:
                        pass
                    page, used_ports = await _rebuild_browser_ar(pw, used_ports, reason="challenge")
                    consecutive_timeouts[0] = 0
                    await page.goto(url, wait_until="commit", timeout=TO_NAV_AR)
                    await asyncio.sleep(random.uniform(2.0, 3.0))
                    title = await page.title()
                    log(f"   📄 '{title[:55]}' | retry")
                    if is_challenge_page(title):
                        log(f"   🚫 Sigue bloqueado — saltando slug")
                        continue
                else:
                    continue

            # ── Título vacío = JS todavía no cargó, darle más tiempo ──
            if title == "" or title.lower() == "adidas" and not got_something:
                log(f"   ⏳ Título vacío/genérico — esperando hidratación extra (+3s)")
                await asyncio.sleep(3.0)
                title = await page.title()

            got_something = True
            consecutive_timeouts[0] = 0  # reset: cargó algo

            products = await wait_next_data_ar(page, max_wait=wait_time)
            if not products:
                # ── Vacío persistente: IP throttled (página carga pero sin datos) ──
                if title.strip() in ("", "adidas"):
                    log(f"   ⚠️  Título genérico y sin productos — IP degradada")
                    consecutive_timeouts[0] += 1
                    # Rebuild inmediato si ya acumulamos 1+ fallo con título genérico
                    if pw and consecutive_timeouts[0] >= 1:
                        log(f"   🔄 Rebuild por IP degradada (sin datos persistente)")
                        try:
                            old_browser = page.context.browser
                            await old_browser.close()
                        except Exception:
                            pass
                        page, used_ports = await _rebuild_browser_ar(pw, used_ports, reason="IP degradada sin datos")
                        consecutive_timeouts[0] = 0
                        # Reintentar el slug actual con la nueva IP
                        try:
                            await page.goto(url, wait_until="commit", timeout=TO_NAV_AR)
                            await asyncio.sleep(random.uniform(2.0, 3.5))
                            title2 = await page.title()
                            log(f"   📄 Retry '{title2[:55]}' | {slug}")
                            if not is_challenge_page(title2):
                                products2 = await wait_next_data_ar(page, max_wait=wait_time)
                                if products2:
                                    got_something = True
                                    if slug in BROAD_SLUGS or slug.endswith("football-shoes"):
                                        tokens   = parse_franchise_tokens(franchise)
                                        filtered = [p for p in products2 if franchise_matches_text(p.get("name",""), tokens)]
                                        log(f"   🔍 Filtro retry '{franchise}': {len(filtered)}/{len(products2)}")
                                        if filtered:
                                            products2 = filtered
                                        elif slug in ("zapatillas", "botines", "football-shoes"):
                                            continue
                                    best = pick_best_product(products2, franchise)
                                    if best:
                                        return best, page, used_ports
                        except PWTimeout:
                            log(f"   ⏱️  Timeout en retry post-rebuild: {url}")
                        except Exception as e2:
                            log(f"   ⚠️  Error retry: {str(e2)[:80]}")
                continue

            # ── Filtrar si cayó en categoría amplia ──
            BROAD_SLUGS = {
                "zapatillas", "zapatillas-running", "botines", "zapatillas-trail",
                "zapatillas-campus", "zapatillas-forum", "zapatillas-grand_court",
                "botines-predator", "copa_pure", "botines-f50", "predator",
                "football-shoes",
            }
            if slug in BROAD_SLUGS or slug.endswith("football-shoes"):
                tokens   = parse_franchise_tokens(franchise)
                filtered = [p for p in products if franchise_matches_text(p.get("name", ""), tokens)]
                log(f"   🔍 Filtro '{franchise}': {len(filtered)}/{len(products)}")
                if filtered:
                    products = filtered
                # Si filtro no encontró nada, usamos todos (mejor que N/D)
                # salvo para páginas muy amplias donde el resultado sería ruidoso
                elif slug in ("zapatillas", "botines", "football-shoes"):
                    log(f"   ⚠️  Filtro sin resultados en página amplia — descartando")
                    continue

            best = pick_best_product(products, franchise)
            if best:
                consecutive_timeouts[0] = 0
                return best, page, used_ports

        except PWTimeout:
            log(f"   ⏱️  Timeout: {url}")
            consecutive_timeouts[0] += 1
            # ── Rebuild inmediato si el timeout es en fallback (URL amplia) ──
            # porque eso indica IP completamente muerta
            if pw and slug in ("zapatillas-running", "zapatillas", "botines") and consecutive_timeouts[0] >= 2:
                log(f"   🔄 Timeout en URL fallback — rebuild urgente de IP")
                try:
                    old_browser = page.context.browser
                    await old_browser.close()
                except Exception:
                    pass
                page, used_ports = await _rebuild_browser_ar(pw, used_ports, reason="timeout en fallback")
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
    try:
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if not m:
            return []
        d = json.loads(m.group(1))
        items: List[Any] = (
            d.get("props", {}).get("pageProps", {}).get("products") or
            d.get("props", {}).get("pageProps", {}).get("searchResult", {}).get("products") or
            d.get("props", {}).get("pageProps", {}).get("plpData", {}).get("products") or
            []
        )
        if not items:
            def _find(obj, depth=0):
                if depth > 8 or not obj or not isinstance(obj, (dict, list)): return None
                if isinstance(obj, list) and len(obj) >= 1 and isinstance(obj[0], dict):
                    if obj[0].get("name") and (obj[0].get("url") or obj[0].get("productId") or obj[0].get("id")):
                        return obj
                if isinstance(obj, dict):
                    for v in obj.values():
                        r = _find(v, depth + 1)
                        if r: return r
                return None
            items = _find(d) or []
        if not items:
            return []
        results = []
        for p in items:
            full = final = 0.0
            pd_ = p.get("priceData") or {}
            if pd_.get("prices"):
                orig  = next((x for x in pd_["prices"] if x.get("type") == "original"), None)
                sale  = next((x for x in pd_["prices"] if x.get("type") == "sale"),     None)
                full  = float(orig["value"]) if orig else 0.0
                final = float(sale["value"]) if sale else full
            elif p.get("pricingInformation"):
                pi    = p["pricingInformation"]
                full  = float(pi.get("standardPrice") or 0)
                final = float(pi.get("currentPrice")  or full)
            elif p.get("price"):
                pr = p["price"]
                if isinstance(pr, dict):
                    full  = float(pr.get("regular") or pr.get("current") or pr.get("value") or 0)
                    final = float(pr.get("current") or pr.get("value") or full)
                else:
                    full = final = float(pr or 0)
            name = p.get("title") or p.get("name") or p.get("displayName") or ""
            url  = p.get("url")   or p.get("link") or p.get("pdpUrl")      or ""
            if url and not url.startswith("http"):
                url = "https://www.adidas.com" + url
            if name and (full > 0 or final > 0):
                results.append({"name": name, "url": url,
                                 "full_price": full, "final_price": final, "currency": "USD"})
        return results
    except Exception as e:
        log(f"   ⚠️  parse US: {e}")
        return []


# URLs US que son páginas amplias y requieren filtro por gama
US_BROAD_SLUGS = {"football-shoes", "soccer-shoes", "cleats"}

def scrape_us_franchise(franchise: str) -> Optional[dict]:
    urls = get_urls_us(franchise)
    log(f"   US URLs: {[u.split('/us/')[-1] for u in urls]}")
    for url in urls:
        slug_us = url.split('/us/')[-1]
        log(f"\n   🔗 US → {slug_us}")
        is_broad = any(b in slug_us for b in US_BROAD_SLUGS)
        try:
            r = requests.get(url, headers=US_HEADERS, proxies=US_PROXIES,
                             verify=False, timeout=TO_REQ_US)
            log(f"   📊 {r.status_code} | {len(r.text):,} bytes")
            if r.status_code != 200:
                continue
            products = extract_next_data_us(r.text)
            if products:
                log(f"   ✅ {len(products)} productos US")
                # Filtrar en páginas amplias para no tomar producto equivocado
                if is_broad:
                    tokens   = parse_franchise_tokens(franchise)
                    filtered = [p for p in products if franchise_matches_text(p.get("name",""), tokens)]
                    log(f"   🔍 Filtro US '{franchise}': {len(filtered)}/{len(products)}")
                    if filtered:
                        products = filtered
                    else:
                        log(f"   ⚠️  Sin match en página amplia US — saltando")
                        continue
                best = pick_best_product(products, franchise)
                if best:
                    return best
            else:
                log("   ⚠️  sin __NEXT_DATA__ US")
        except requests.exceptions.Timeout:
            log(f"   ⏱️  US Timeout")
        except Exception as e:
            log(f"   ⚠️  US Error: {str(e)[:100]}")
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
        if not nike_fr or not adidas_fr:
            continue

        ar_p = scrape_results.get(adidas_fr, {}).get("ar")
        us_p = scrape_results.get(adidas_fr, {}).get("us")

        ar_price  = ar_p.get("full_price") if ar_p else None
        ar_name   = ar_p.get("name", "")   if ar_p else ""
        ar_url    = ar_p.get("url",  "")   if ar_p else ""
        us_price  = us_p.get("full_price") if us_p else None
        us_name   = us_p.get("name", "")   if us_p else ""

        # Nike del StatusBooks
        # Para botines: matching por GAMA (SILO BOTINES)
        # Para otros productos: matching por franquicia
        is_botin = categoria.upper() in ("FUTBOL", "FOOTBALL", "BOTINES")
        if is_botin:
            stylecolor, mk_name = find_nike_botin_by_gama(nike_fr, df_sb)
        else:
            stylecolor, mk_name = find_nike_stylecolor(nike_fr, df_sb)
        
        nike_price: Optional[float] = None
        if stylecolor:
            m = df_sb[df_sb["Product Code"].str.upper() == stylecolor.upper()]
            if not m.empty:
                nike_price = safe_float(m.iloc[0].get("_price"))
        mk_name = mk_name or ""

        # ── OUTPUT 1: Nike AR vs Adidas AR ────────────────────────
        nk_shp  = shipping_nike_ar(nike_price)
        ad_shp  = shipping_adidas_ar(ar_price)
        nk_tot  = (nike_price + nk_shp) if nike_price is not None else None
        ad_tot  = (ar_price   + ad_shp) if ar_price   is not None else None

        gap     = (ar_price - nike_price) / nike_price if ar_price and nike_price else None
        gap_shp = (ad_tot   - nk_tot)    / nk_tot     if ad_tot   and nk_tot     else None

        rows1.append({
            "Fecha":                   run_date,
            "Categoria":               categoria,
            "StyleColor":              stylecolor or "",
            "Marketing Name (Nike)":   mk_name,
            "Franchise Nike":          nike_fr,
            "Franchise Adidas":        adidas_fr,
            "Adidas Product Name":     ar_name,
            "Adidas PDP (AR)":         ar_url,
            "Nike Full Price (ARS)":   nike_price,
            "Adidas Full Price (ARS)": ar_price,
            "Gap %":                   gap,
            "BML Full Price":          bml(ar_price, nike_price),
            "Nike Shipping (ARS)":     nk_shp  if nike_price is not None else None,
            "Adidas Shipping (ARS)":   ad_shp  if ar_price   is not None else None,
            "Gap % con Shipping":      gap_shp,
            "BML + Shp":               bml(ad_tot, nk_tot),
        })

        # ── OUTPUT 2: Adidas AR vs Adidas US ──────────────────────
        ar_usd      = ar_price / fx                         if ar_price and fx > 0 else None
        us_iva      = us_price * (1 + IVA)                  if us_price else None
        us_iva_bf   = us_price * (1 + IVA) * (1 + BF)      if us_price else None
        shp_ar_usd  = shipping_adidas_ar(ar_price) / fx     if fx > 0 else 0.0
        shp_us_usd  = shipping_us(us_price)
        ar_shp      = (ar_usd  + shp_ar_usd)               if ar_usd  else None
        us_shp      = (us_price + shp_us_usd) * (1+IVA) * (1+BF) if us_price else None

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
    "Nike Shipping (ARS)", "Adidas Shipping (ARS)", "Gap % con Shipping", "BML + Shp",
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
    return {
        "hdr":  wb.add_format({"bold": True, "font_color": "white", "bg_color": HDR_BG,
                                "align": "center", "valign": "vcenter", "border": 1}),
        "txt":  wb.add_format({"align": "left",  "valign": "vcenter"}),
        "ars":  wb.add_format({"num_format": '"$" #,##0',          "align": "left", "valign": "vcenter"}),
        "usd":  wb.add_format({"num_format": '"USD" #,##0.00',     "align": "left", "valign": "vcenter"}),
        "pct":  wb.add_format({"num_format": "0.00%",              "align": "left", "valign": "vcenter"}),
        "fx":   wb.add_format({"num_format": "#,##0.00",           "align": "left", "valign": "vcenter"}),
        "link": wb.add_format({"font_color": "blue", "underline": 1, "align": "left", "valign": "vcenter"}),
        "beat": wb.add_format({"bg_color": "#C6EFCE"}),
        "meet": wb.add_format({"bg_color": "#FFEB9C"}),
        "lose": wb.add_format({"bg_color": "#FFC7CE"}),
        "nd":   wb.add_format({"bg_color": "#D9D9D9"}),
        "zebra":wb.add_format({"bg_color": "#F2F2F2"}),
    }


def _write_sheet(wb, fmts, name: str, rows: List[Dict], cols: List[str], bml_cols: set):
    ars_cols  = {c for c in cols if "ARS" in c and "Shipping" not in c}
    ship_cols = {c for c in cols if "Shipping" in c}
    usd_cols  = {c for c in cols if any(k in c for k in ("USD","IVA","BF","AR + Shp","US + Shp","ARG (USD)"))}
    pct_cols  = {c for c in cols if "%" in c or c.startswith("Dif") or c.startswith("Gap")}
    link_cols = {c for c in cols if "PDP" in c or "Link" in c or "URL" in c or c in ("PDP ARG","PDP US")}
    fx_cols   = {"FX ARS/USD"}

    ws = wb.add_worksheet(name)
    ws.freeze_panes(1, 0)
    ws.autofilter(0, 0, 0, len(cols) - 1)
    ws.set_row(0, 22)
    for j, c in enumerate(cols):
        ws.write(0, j, c, fmts["hdr"])

    for i, row in enumerate(rows, start=1):
        ws.set_row(i, 18)
        for j, c in enumerate(cols):
            v = row.get(c)
            def _is_num(x):
                if x is None: return False
                if isinstance(x, (int, float)): return True
                try: float(x); return True
                except (ValueError, TypeError): return False
            if c in link_cols:
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
    for bc in bml_cols:
        if bc not in cols: continue
        j  = cols.index(bc)
        cl = xlsxwriter.utility.xl_col_to_name(j)
        ws.conditional_format(1,j,n,j, {"type":"formula","criteria":f'=UPPER(${cl}2)="BEAT"',"format":fmts["beat"]})
        ws.conditional_format(1,j,n,j, {"type":"formula","criteria":f'=UPPER(${cl}2)="MEET"',"format":fmts["meet"]})
        ws.conditional_format(1,j,n,j, {"type":"formula","criteria":f'=UPPER(${cl}2)="LOSE"',"format":fmts["lose"]})
        ws.conditional_format(1,j,n,j, {"type":"formula","criteria":f'=UPPER(${cl}2)="N/D"', "format":fmts["nd"]})

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
