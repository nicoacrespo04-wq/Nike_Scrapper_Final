import os
import re
import time
import gc

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import json
import random
import datetime as dt

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
OPENSPORTS_PLP_URL = "https://www.opensports.com.ar/marcas/nike.html?product_list_limit=36"

CP_AMBA = "1425"

OPENSPORTS_STD_SHIPPING_ARS = 0.0      # ajustar con el valor real
OPENSPORTS_FREE_SHIP_FROM_ARS = 0.0    # ajustar con el valor real

NIKE_STD_SHIPPING_ARS = 8899.0
NIKE_FREE_SHIP_FROM_ARS = 99000.0

NIKE_CUOTAS_SIMPLE_MODE = True
NIKE_CUOTAS_ALL = 3
NIKE_CUOTAS_HIGH = 6
NIKE_CUOTAS_HIGH_FROM = 79000.0

BML_THRESHOLD_PCT = 0.02

SEASON = "SP26"
STATUSBOOKS_FILE = "StatusBooks NDDC ARG SP26.xlsb"
STATUSBOOKS_SHEET = "Books NDDC"

HEADLESS = False
AGENTS = max(2, int(os.getenv("AGENTS", "2")))

# 0 = sin limite; 40 para debug rapido
DEBUG_LIMIT = int(os.getenv("DEBUG_LIMIT", "40"))

MAX_PLP_PAGES = 50          # Magento 2: máximo de páginas a recorrer
PLP_STAGNATION_ROUNDS = 3
PLP_SCROLL_PIXELS = 1400

PDP_WAIT_MS = 25_000
PLP_WAIT_MS = 45_000

CACHE_PATH = "opensports_cache.json"

AUX_ASSORT_FILE = "01 Lista actualizada a nivel GRUPO DE ARTÍCULOS total.xlsx"
AUX_ASSORT_SHEET = "Primera Calidad"
AUX_COL_STYLE = "Material Nike"
AUX_COL_NAME = "Nombre material"
AUX_COL_GRUPO = "Grupo de carga"
AUX_COL_ANIO = "Año"

REFRESH_CACHED = True
VALID_PRICE_MIN = 0.0

BROWSER_RESET_EVERY = 90
RESET_SLEEP_SECONDS = 8

TS = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DATE = dt.datetime.now().strftime("%Y-%m-%d")

OUT_RAW_CSV = f"opensports_vs_nike_raw_{TS}.csv"
OUT_VISUAL_XLSX = f"opensports_vs_nike_visual_{TS}.xlsx"


# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────
def human_pause(a=0.25, b=0.85):
    time.sleep(random.uniform(a, b))


def log(msg: str):
    print(msg, flush=True)


def _is_fatal_nav_error(e: Exception) -> bool:
    s = f"{type(e).__name__}: {e}".lower()
    tokens = [
        "net::err_insufficient_resources", "net::err_aborted",
        "connection closed while reading from the driver",
        "target closed", "browser has been closed",
        "page.goto: target closed", "page.goto: browser has been closed",
        "page.goto: navigation failed", "page.goto: net::",
    ]
    return any(t in s for t in tokens)


def _reset_triplet(worker_id, pw, browser, context, page, reason=""):
    log(f"🧹 (W{worker_id}) RESET POR ERROR: {reason}".strip())
    for obj in (page, context, browser):
        try:
            obj.close()
        except Exception:
            pass
    gc.collect()
    time.sleep(RESET_SLEEP_SECONDS + random.uniform(1.0, 3.0))
    return new_browser_context(pw)


def normalize_stylecolor(style: str) -> str:
    if not style:
        return ""
    style = str(style).strip().upper()
    style = style.replace("_", "-").replace("/", "-").replace(" ", "")
    style = re.sub(r"-+", "-", style).strip("-")
    if style.startswith("NI") and len(style) >= 3:
        style = style[2:]

    m = re.match(r"^([A-Z]{2}\d{4})(\d{3})$", style)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m = re.match(r"^([A-Z]{2}\d{4})(\d{3})-(?:000|UNICO|UNI)$", style)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    m = re.match(r"^([A-Z]{2}\d{4})-(\d{3})$", style)
    if m:
        return f"{m.group(1)}-{m.group(2)}"

    return style


def stylecolor_variants(style: str) -> list:
    base = normalize_stylecolor(style)
    if not base:
        return []

    out = [base]
    compact = base.replace("-", "")
    if compact and compact not in out:
        out.append(compact)

    m = re.match(r"^([A-Z]{2}\d{4})-(\d{3})$", base)
    if m:
        core = m.group(1)
        color = m.group(2)
        extra = [
            f"{core}{color}-000",
            f"{core}{color}-UNICO",
            f"NI{core}-{color}",
            f"NI{core}{color}-000",
            f"NI{base}",
        ]
        for k in extra:
            if k not in out:
                out.append(k)

    return out


def first_map_match(d: dict, style: str):
    if not isinstance(d, dict):
        return None
    for key in stylecolor_variants(style):
        if key in d:
            return d[key]
    return None


def extract_stylecolor_from_url(url: str) -> str:
    """
    OpenSports URLs (Magento 2):
      /zapatillas-nike-downshifter-13-fd6454-001.html  -> FD6454-001
      /remera-nike-dri-fit-dv9831-010.html             -> DV9831-010
      /zapatillas-nike-air-max-sc-cw4555-002.html      -> CW4555-002
    El stylecolor esta al FINAL del slug antes de .html, separado por guion.
    Patron 1: XX9999-999 (con guion entre style y color)
    Patron 2: XX999999   (todo junto, sin guion)
    """
    path = url.split("?")[0].rstrip("/")
    path = re.sub(r"\.html$", "", path, flags=re.I)

    # Patron 1: stylecolor con guion explícito al final, ej: -fd6454-001 o -cw4555-002
    m = re.search(r"-([A-Za-z]{2}\d{4,6})-(\d{3})(?:-\d+)?$", path)
    if m:
        return f"{m.group(1)}-{m.group(2)}".upper()

    # Patron 2: todo junto al final, ej: -fd6454001
    m2 = re.search(r"-([A-Za-z]{0,2}[0-9]{6,9})$", path)
    if m2:
        return m2.group(1).upper()

    return ""


def parse_money_ar_to_float(s: str) -> float:
    """
    Parsea precios en formato argentino.
    AR: punto = separador de miles, coma = decimal
      '$126.000'     -> 126000.0
      '$126.000,50'  -> 126000.5
      '$64.999'      -> 64999.0
      '126000'       -> 126000.0
    """
    if s is None:
        return 0.0
    s = str(s).strip()
    if not s:
        return 0.0
    # Quitar símbolo $, espacios no-break y espacios normales
    s = re.sub(r"[$\u00a0\s]", "", s)
    cleaned = re.sub(r"[^\d\.,]", "", s)
    if not cleaned:
        return 0.0

    # Caso 1: tiene coma -> AR decimal: puntos=miles, coma=decimal
    if "," in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
        try:
            return float(cleaned)
        except Exception:
            return 0.0

    # Caso 2: tiene puntos pero NO coma
    dot_count = cleaned.count(".")
    if dot_count >= 1:
        parts = cleaned.split(".")
        last_part = parts[-1]
        # Si la última parte tiene exactamente 2 dígitos Y hay más de un trozo -> decimal
        # Ej: "126.50" -> 126.50  (pero "126.000" -> 126000)
        if dot_count == 1 and len(last_part) == 2:
            try:
                return float(cleaned)  # decimal genuino
            except Exception:
                return 0.0
        # En todos los demás casos (126.000, 1.234.567, etc.) -> miles AR
        cleaned = cleaned.replace(".", "")
        try:
            return float(cleaned)
        except Exception:
            return 0.0

    # Caso 3: solo dígitos
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
    if s in ("", "-") or s.lower() in {"nan", "none", "null"}:
        return 0.0
    s2 = re.sub(r"[^0-9\.\-]", "", s.replace(",", "."))
    if s2 in ("", "-", "."):
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


def _safe_str(x):
    try:
        if x is None or pd.isna(x):
            return ""
    except Exception:
        pass
    return str(x).strip()


# ─────────────────────────────────────────────────────────────────
# STATUS BOOKS
# ─────────────────────────────────────────────────────────────────
def _read_excel_any_noheader(path: str, sheet: str):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsb":
        return pd.read_excel(path, sheet_name=sheet, header=None, dtype=str, engine="pyxlsb")
    return pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)


def load_statusbooks_map(path: str, sheet: str):
    log("\n📚 Cargando Status Books...")
    if not os.path.exists(path):
        raise FileNotFoundError(f"No encuentro: {path} (SEASON={SEASON})")

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
        raise ValueError("No encontre fila header 'Product Code'.")

    headers = raw.iloc[header_row, :].fillna("").astype(str).str.strip().tolist()
    sb = raw.iloc[header_row + 1:, :].copy()
    sb.columns = headers
    sb.columns = [c if c else f"__EMPTY__{i}" for i, c in enumerate(sb.columns)]
    log(f"   ✅ {path} | Sheet: {sheet} | Filas: {len(sb)} | HeaderRow: {header_row + 1}")

    COL_CODE = "Product Code"
    COL_NAME = "Marketing Name"
    COL_DIV = "BU"
    COL_CAT = "Category"
    COL_FR = "Franchise"
    COL_SALE = "SALE"
    COL_STOCK = "STOCK BL (Inventario Brandlive)"
    COL_SSN = "SSN VTA"

    full_price_col_idx = None
    if str(SEASON) in sb.columns:
        full_price_col_idx = list(sb.columns).index(str(SEASON))
        log(f"   🧠 Full Price columna SEASON exacta: '{SEASON}'")
    else:
        group_row = header_row - 1
        if group_row >= 0:
            top = raw.iloc[group_row, :].fillna("").astype(str)
            match_idxs = [i for i, val in enumerate(top.tolist())
                          if any(p in str(val) for p in [f"({SEASON})", str(SEASON)])]
            if match_idxs:
                full_price_col_idx = match_idxs[0]
                log(f"   ⚠️ FALLBACK columna #{full_price_col_idx + 1}")

    missing = [c for c in [COL_CODE, COL_NAME, COL_DIV, COL_CAT, COL_FR, COL_SALE, COL_STOCK]
               if c not in sb.columns]
    if missing:
        raise KeyError(f"Faltan columnas en StatusBooks: {missing}")
    if full_price_col_idx is None:
        raise KeyError(f"No pude detectar columna Full Price para SEASON={SEASON}.")

    work = pd.DataFrame()
    work["ProductCode"] = sb[COL_CODE].astype(str).str.strip().str.upper()
    work["MarketingName"] = sb[COL_NAME].astype(str).str.strip()
    work["SB_Division"] = sb[COL_DIV].astype(str).str.strip()
    work["SB_Category"] = sb[COL_CAT].astype(str).str.strip()
    work["SB_Franchise"] = sb[COL_FR].astype(str).str.strip()
    work["SB_SSN_VTA"] = sb[COL_SSN].astype(str).str.strip() if COL_SSN in sb.columns else ""

    sb = sb.reset_index(drop=True)
    work = work.reset_index(drop=True)
    nike_full_series = sb.iloc[:, full_price_col_idx].apply(parse_money_ar_to_float)
    work["NikeFullPrice"] = nike_full_series.to_numpy()

    _vals = pd.to_numeric(work["NikeFullPrice"], errors="coerce")
    _med = _vals[_vals > 0].median()
    if pd.notna(_med) and _med < 2000:
        work["NikeFullPrice"] = (_vals * 1000.0).to_numpy()
        log("   🧠 NikeFullPrice en $k -> x1000")
    else:
        work["NikeFullPrice"] = _vals.to_numpy()

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
        for alias in stylecolor_variants(code):
            sb_map[alias] = data
        sb_map[code] = data
        if code.startswith("NI"):
            sb_map[code[2:]] = data
        else:
            sb_map["NI" + code] = data

    log(f"   ✅ Product Codes validos (Final>0 y StockBL>0): {len(work)}")
    log(f"   ✅ Keys en mapa (con/sin NI): {len(sb_map)}")
    return sb_map


# ─────────────────────────────────────────────────────────────────
# AUX ASSORT MAP
# ─────────────────────────────────────────────────────────────────
def _map_grupo_to_bu(grupo: str) -> str:
    g = str(grupo or "").strip().lower()
    if g == "accesorios":
        return "EQ"
    if g == "calzado":
        return "FW"
    if g == "indumentaria":
        return "APP"
    return ""


def load_aux_assort_map(path: str, sheet: str) -> dict:
    if not os.path.exists(path):
        log(f"⚠️ No encuentro AUX_ASSORT_FILE: {path}. Continuo sin completado auxiliar.")
        return {}
    try:
        df = pd.read_excel(path, sheet_name=sheet, dtype=str)
    except Exception as e:
        log(f"⚠️ No pude leer AUX_ASSORT_FILE: {e}. Continuo sin completado auxiliar.")
        return {}

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    year_col = next((c for c in [AUX_COL_ANIO, "AÑO", "Año", "ANIO", "Anio", "anio"]
                     if c in df.columns), None)
    missing = [c for c in [AUX_COL_STYLE, AUX_COL_NAME, AUX_COL_GRUPO] if c not in df.columns]
    if missing:
        log(f"⚠️ AUX_ASSORT_FILE: faltan columnas {missing}. Continuo sin completado auxiliar.")
        return {}

    out = {}
    for _, r in df.iterrows():
        sc = normalize_stylecolor(_safe_str(r.get(AUX_COL_STYLE)).upper())
        if not sc:
            continue
        payload = {
            "SB_MarketingName": _safe_str(r.get(AUX_COL_NAME)),
            "SB_Division": _map_grupo_to_bu(_safe_str(r.get(AUX_COL_GRUPO))),
            "Aux_Año": _safe_str(r.get(year_col)) if year_col else "",
        }
        for alias in stylecolor_variants(sc):
            out.setdefault(alias, payload)
        out[sc] = payload
    log(f"✅ AUX map cargado: {len(out)} items")
    return out


# ─────────────────────────────────────────────────────────────────
# BROWSER
# ─────────────────────────────────────────────────────────────────
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


# ─────────────────────────────────────────────────────────────────
# OVERLAYS / POSTAL CODE
# ─────────────────────────────────────────────────────────────────
def try_close_overlays(page):
    candidates = [
        "button:has-text('Aceptar')", "button:has-text('ACEPTAR')",
        "button:has-text('Entendido')", "button:has-text('OK')",
        "button:has-text('Cerrar')", "button[aria-label='Cerrar']",
        "[aria-label='close']", ".modal-close", ".close",
        "button:has-text('No, gracias')", "button:has-text('Rechazar')",
        "[data-testid='modal-close']",
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
            if any(k in ph for k in ["código postal", "codigo postal", "cp", "postal"]):
                inp.click(timeout=1500)
                human_pause(0.2, 0.5)
                inp.fill(cp)
                human_pause(0.2, 0.5)
                inp.press("Enter")
                human_pause(0.8, 1.2)
                return
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────
# PLP — recoleccion de links (Magento 2 con paginacion)
# ─────────────────────────────────────────────────────────────────
def collect_plp_links_opensports(page) -> list:
    """
    Extrae <a href> de cards de producto en la PLP de OpenSports (Magento 2).
    Los links de producto terminan en .html y tienen el stylecolor embebido.
    """
    base = "https://www.opensports.com.ar"
    items = page.evaluate(
        """
        () => {
            const out = [];
            const specific = [
                ...document.querySelectorAll('.product-item-link[href]'),
                ...document.querySelectorAll('.product-item a[href]'),
                ...document.querySelectorAll('.products-grid a[href]'),
                ...document.querySelectorAll('.product-items a[href]'),
            ];
            const all = Array.from(document.querySelectorAll('a[href]'));
            const anchors = [...specific, ...all];
            for (const a of anchors) {
                const href = (a.getAttribute('href') || '').trim();
                if (href) out.push(href);
            }
            return out;
        }
        """
    )

    seen = set()
    links = []
    for href in items:
        href = (href or "").strip()
        if not href:
            continue
        href_low = href.lower()
        if href_low.startswith("javascript") or href_low.startswith("#"):
            continue
        if href_low.startswith("mailto:") or href_low.startswith("tel:"):
            continue

        if href.startswith("http"):
            full_url = href
        elif href.startswith("/"):
            full_url = base + href
        else:
            full_url = base + "/" + href

        if not full_url.lower().endswith(".html"):
            continue
        full_low = full_url.lower()
        if any(x in full_low for x in ["/marcas/", "/category/", "/search", "/account",
                                        "/cart", "/login", "/checkout", "/wishlist",
                                        "/customer/", "/cms/"]):
            continue

        if not extract_stylecolor_from_url(full_url):
            continue

        if full_url not in seen:
            seen.add(full_url)
            links.append(full_url)

    return links


def _get_next_page_url(page) -> str:
    """Botón 'Siguiente' de Magento 2: <a class="action next" href="...?p=N">"""
    try:
        nxt = page.locator("a.action.next[href]").first
        if nxt.count() > 0 and nxt.is_visible():
            href = nxt.get_attribute("href") or ""
            return href.strip()
    except Exception:
        pass
    try:
        nxt = page.locator("a:has-text('Siguiente')[href]").first
        if nxt.count() > 0 and nxt.is_visible():
            href = nxt.get_attribute("href") or ""
            return href.strip()
    except Exception:
        pass
    return ""


def collect_pdp_links_from_plp(page, plp_url: str) -> list:
    """
    Recorre todas las páginas de la PLP de OpenSports (Magento 2).
    En cada página hace scroll para lazy-load y recolecta links.
    Avanza con el botón 'Siguiente' hasta que no haya más páginas.
    """
    log(f"\n🌐 Abriendo PLP: {plp_url}")
    page.goto(plp_url, wait_until="domcontentloaded", timeout=PLP_WAIT_MS)
    human_pause(1.5, 2.5)
    try_close_overlays(page)
    human_pause(0.8, 1.2)

    all_links = []
    seen = set()
    current_url = plp_url
    last_page = 1

    for page_i in range(1, MAX_PLP_PAGES + 1):
        log(f"   📄 Página {page_i}: {current_url}")

        for _ in range(4):
            try:
                page.mouse.wheel(0, PLP_SCROLL_PIXELS)
            except Exception:
                pass
            human_pause(0.3, 0.6)
        try_close_overlays(page)
        human_pause(0.5, 0.9)

        try:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            human_pause(0.8, 1.2)
        except Exception:
            pass

        links_now = collect_plp_links_opensports(page)
        new_added = 0
        for u in links_now:
            if u not in seen:
                seen.add(u)
                all_links.append(u)
                new_added += 1

        log(f"   ✅ Página {page_i}: +{new_added} nuevos | Total acumulado: {len(all_links)}")

        next_url = _get_next_page_url(page)
        if not next_url:
            log(f"   🏁 No hay más páginas después de página {page_i}. PLP completa.")
            last_page = page_i
            break

        try:
            page.goto(next_url, wait_until="domcontentloaded", timeout=PLP_WAIT_MS)
            human_pause(1.2, 2.0)
            try_close_overlays(page)
            current_url = next_url
            last_page = page_i + 1
        except PWTimeoutError:
            log(f"   ⚠️ Timeout navegando a página {page_i + 1}. Deteniendo paginación.")
            break
        except Exception as e:
            log(f"   ⚠️ Error navegando a página {page_i + 1}: {e}. Deteniendo paginación.")
            break

    log(f"\n✅ PLP completa: {len(all_links)} PDPs únicos en {last_page} páginas.")
    return all_links


# ─────────────────────────────────────────────────────────────────
# PDP — extraccion de precios (Magento 2)
# ─────────────────────────────────────────────────────────────────
def _extract_opensports_prices(page) -> tuple:
    """
    Retorna (final_price, full_price).
    Capas: 1) data-price-box Magento  2) DOM clases Magento  3) JSON-LD  4) Meta  5) DOM genérico
    """
    # CAPA 1: Magento 2 data-price-box (más confiable)
    try:
        prices = page.evaluate(
            """
            () => {
                function parseArPrice(txt) {
                    if (!txt) return 0;
                    let s = txt.replace(/[$\\u00a0\\s]/g, "");
                    if (s.includes(",")) {
                        s = s.replace(/\\./g, "").replace(",", ".");
                    } else {
                        s = s.replace(/\\./g, "");
                    }
                    const v = parseFloat(s);
                    return isNaN(v) ? 0 : v;
                }

                // Magento 2: data-role=priceBox con data-price-amount
                const box = document.querySelector('[data-role="priceBox"]');
                if (box) {
                    const finalEl = box.querySelector('[data-price-type="finalPrice"]');
                    const regularEl = box.querySelector('[data-price-type="oldPrice"], [data-price-type="basePrice"]');
                    const finalAmt = finalEl ? parseFloat(finalEl.getAttribute('data-price-amount') || '0') : 0;
                    const regularAmt = regularEl ? parseFloat(regularEl.getAttribute('data-price-amount') || '0') : 0;
                    if (finalAmt > 500) return { finalPrice: finalAmt, fullPrice: regularAmt || finalAmt };
                }

                // DOM clases Magento 2
                function extractPrice(sel) {
                    const el = document.querySelector(sel);
                    if (!el) return 0;
                    const txt = (el.innerText || el.textContent || "").trim();
                    return parseArPrice(txt);
                }

                let finalPrice = 0;
                for (const sel of [
                    '.price-wrapper[data-price-type="finalPrice"] .price',
                    '.special-price .price',
                    '[data-price-type="finalPrice"] .price',
                    '.price-final_price .price',
                    '.regular-price .price',
                    '.price-box .price',
                ]) {
                    const v = extractPrice(sel);
                    if (v > 500) { finalPrice = v; break; }
                }

                let fullPrice = 0;
                for (const sel of [
                    '.old-price .price',
                    '.price-wrapper[data-price-type="oldPrice"] .price',
                    '[data-price-type="oldPrice"] .price',
                    '.price-wrapper[data-price-type="basePrice"] .price',
                ]) {
                    const v = extractPrice(sel);
                    if (v > 500) { fullPrice = v; break; }
                }

                return { finalPrice, fullPrice };
            }
            """
        )
        fp = float(prices.get("finalPrice") or 0)
        fup = float(prices.get("fullPrice") or 0)
        if fp > 0:
            return fp, fup
    except Exception:
        pass

    # CAPA 2: JSON-LD
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
            if price and price > 0:
                return float(price), 0.0
    except Exception:
        pass

    # CAPA 3: Meta tags
    for sel in [
        "meta[property='product:price:amount']",
        "meta[property='og:price:amount']",
        "meta[itemprop='price']",
    ]:
        try:
            m = page.locator(sel).first
            if m.count() > 0:
                p = parse_money_ar_to_float(m.get_attribute("content") or "")
                if p > 0:
                    return float(p), 0.0
        except Exception:
            continue

    # CAPA 4: DOM genérico
    try:
        vals = page.evaluate(
            """
            () => {
                const priceRegex = /\\$\\s*\\d+(?:\\.\\d{3})*/;
                const nodes = Array.from(document.querySelectorAll("span,div,p,strong,b"));
                const out = [];
                for (const el of nodes) {
                    const txt = (el.innerText || "").trim();
                    if (!txt || txt.length > 40) continue;
                    if (!priceRegex.test(txt)) continue;
                    const tag = (el.tagName || "").toLowerCase();
                    if (tag === "del" || tag === "s") continue;
                    const style = window.getComputedStyle(el);
                    if ((style.textDecorationLine || "").includes("line-through")) continue;
                    let cur = el; let bad = false;
                    for (let i = 0; i < 6 && cur; i++) {
                        const t = (cur.tagName || "").toLowerCase();
                        if (t === "del" || t === "s") { bad = true; break; }
                        if ((window.getComputedStyle(cur).textDecorationLine || "").includes("line-through")) {
                            bad = true; break;
                        }
                        cur = cur.parentElement;
                    }
                    if (bad) continue;
                    out.push(txt);
                }
                return out.slice(0, 20);
            }
            """
        )
        if vals:
            bad_tokens = ("cuota", "cuotas", "x ", " x", "sin interés", "interés", "cft", "tea")
            filtered = []
            for sv in vals:
                if any(t in str(sv).lower() for t in bad_tokens):
                    continue
                v = parse_money_ar_to_float(sv)
                if v > 0:
                    filtered.append(v)
            if filtered:
                return float(max(filtered)), 0.0
    except Exception:
        pass

    return 0.0, 0.0


def extract_max_cuotas_opensports(page) -> int:
    """Extrae máximo de cuotas sin interés en PDP Magento 2."""
    try:
        page.wait_for_selector(
            "[class*='installment'], [class*='cuota'], [class*='payment'], [class*='financ']",
            timeout=3000,
        )
    except Exception:
        pass

    for selector in [
        "[class*='installment']", "[class*='cuota']",
        "[class*='payment']", "[class*='financ']",
        ".price-box", ".product-info-price",
    ]:
        try:
            els = page.locator(selector).all()
            for el in els[:5]:
                txt = el.inner_text()
                nums = [int(x) for x in re.findall(r"(\d+)\s*cuotas?\s*sin\s*inter[eé]s", txt, flags=re.I)]
                if nums:
                    return max(nums)
        except Exception:
            pass

    try:
        full_text = page.inner_text("body")
        nums = [int(x) for x in re.findall(r"(\d+)\s*cuotas?\s*sin\s*inter[eé]s", full_text, flags=re.I)]
        if nums:
            return max(nums)
    except Exception:
        pass

    return 0


def scrape_pdp(page, url: str) -> dict:
    page.goto(url, wait_until="domcontentloaded", timeout=PDP_WAIT_MS)
    human_pause(0.5, 1.2)
    try_close_overlays(page)

    style_raw = extract_stylecolor_from_url(url)
    style_norm = normalize_stylecolor(style_raw)

    name = ""
    try:
        h1 = page.locator("h1.page-title, h1").first
        if h1.count() > 0:
            name = h1.inner_text().strip()
    except Exception:
        pass
    if not name:
        try:
            name = page.title()
        except Exception:
            pass

    price_final = 0.0
    full_price = 0.0
    for _try in range(2):
        price_final, full_price = _extract_opensports_prices(page)
        if price_final > 0:
            break
        try:
            page.wait_for_timeout(500)
        except Exception:
            pass

    if price_final <= 0:
        log(f"   ⚠️ (NO_PRICE_DOM) precio no encontrado: {url}")

    max_cuotas = extract_max_cuotas_opensports(page)

    if full_price <= 0 or (price_final > 0 and full_price < price_final):
        full_price = float(price_final)
    if price_final and full_price and full_price > price_final * 3.0:
        full_price = float(price_final)

    sale_dec = 0.0
    if full_price > 0 and price_final > 0 and full_price >= price_final:
        sale_dec = max(0.0, min(1.0, (full_price - price_final) / full_price))

    return {
        "Retailer": "OpenSports",
        "OpenSports_URL": url,
        "OpenSports_Name": name,
        "OpenSports_StyleColor_Raw": style_raw,
        "OpenSports_StyleColor_Norm": style_norm,
        "OpenSports_FullPrice": float(full_price),
        "OpenSportsSaleDecimal": float(sale_dec),
        "OpenSports_FinalPrice": float(price_final),
        "OpenSports_MaxCuotasSinInteres": int(max_cuotas),
        "OpenSports_LastUpdated": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


    """
    Extrae <a href> que apunten a PDPs de Sporting.
    URL real: /zapatillas-nike-downshifter-13-de-hombre-fd6454001-001/
    El stylecolor Nike esta embebido al FINAL del slug.
    Patron JS: termina en -DIGITOS o -DIGITOS-SUFIJO (con o sin slash final).
    """
    base = "https://www.sporting.com.ar"
    items = page.evaluate(
        """
        () => {
            const out = [];

            // 1) Selectores más específicos de card (VTEX Sporting)
            const specific = [
              ...document.querySelectorAll('a[class*="clearLink"][href]'),
              ...document.querySelectorAll('article a[href]'),
              ...document.querySelectorAll('section a[href]')
            ];

            // 2) Fallback amplio
            const all = Array.from(document.querySelectorAll('a[href]'));

            const anchors = [...specific, ...all];
            for (const a of anchors) {
                const href = (a.getAttribute('href') || '').trim();
                if (!href) continue;
                out.push(href);
            }

            return out;
        }
        """
    )

    total_anchors = len(items)
    seen = set()
    links = []
    for href in items:
        href = (href or "").strip()
        if not href:
            continue

        href_low = href.lower()
        if href_low.startswith("javascript") or href_low.startswith("#"):
            continue
        if href_low.startswith("mailto:") or href_low.startswith("tel:"):
            continue

        if href.startswith("http"):
            full_url = href
        elif href.startswith("/"):
            full_url = base + href
        else:
            full_url = base + "/" + href

        full_low = full_url.lower()
        if any(x in full_low for x in ["/search", "/account", "/cart", "/login"]):
            continue

        # Filtro final por patrón de stylecolor en URL
        if not extract_stylecolor_from_url(full_url):
            continue

        if full_url not in seen:
            seen.add(full_url)
            links.append(full_url)


# ─────────────────────────────────────────────────────────────────
# BML / SHIPPING / CUOTAS
# ─────────────────────────────────────────────────────────────────
def _bml_label(comp: float, nike: float) -> str:
    try:
        c, n = float(comp), float(nike)
    except Exception:
        return ""
    if c <= 0 or n <= 0:
        return ""
    if c < n * 0.98:
        return "Lose"
    if n < c * 0.98:
        return "Beat"
    return "Meet"


def _ship_opensports(price: float) -> float:
    try:
        p = float(price)
    except Exception:
        return 0.0
    if p <= 0:
        return 0.0
    return 0.0 if p >= OPENSPORTS_FREE_SHIP_FROM_ARS else OPENSPORTS_STD_SHIPPING_ARS


def _ship_nike(price: float) -> float:
    try:
        p = float(price)
    except Exception:
        return 0.0
    if p <= 0:
        return 0.0
    return 0.0 if p >= NIKE_FREE_SHIP_FROM_ARS else NIKE_STD_SHIPPING_ARS


def _nike_cuotas(nike_final: float) -> int:
    try:
        return int(NIKE_CUOTAS_HIGH) if float(nike_final) >= NIKE_CUOTAS_HIGH_FROM else int(NIKE_CUOTAS_ALL)
    except Exception:
        return int(NIKE_CUOTAS_ALL)


def _gender_from_category(cat: str) -> str:
    s = str(cat or "").strip().lower()
    if any(k in s for k in ["mujer", "women", "dama", "femenino", "female"]):
        return "Mujer"
    if any(k in s for k in ["hombre", "men", "caballero", "masculino", "male"]):
        return "Hombre"
    if any(k in s for k in ["niño", "nino", "kids", "infantil", "junior", "niña", "youth"]):
        return "Kids"
    if "unisex" in s:
        return "Unisex"
    return ""


# ─────────────────────────────────────────────────────────────────
# BUILD TEMPLATE DF
# ─────────────────────────────────────────────────────────────────
def build_template_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    def _sanitize_nike_price(col):
        """Corrige precios Nike del SB que vengan en $k en vez de pesos completos."""
        def _fix(row):
            try:
                comp = float(row.get("OpenSports_FinalPrice") or 0)
            except Exception:
                comp = 0.0
            try:
                raw = row.get(col)
                nk = float(raw) if raw not in (None, "", "nan") and pd.notna(raw) else 0.0
            except Exception:
                nk = 0.0
            # Si el precio Nike es < 2000 y el precio Sporting sugiere que estamos en pesos reales -> x1000
            if nk > 0 and nk < 2000 and comp > 10000:
                nk = nk * 1000.0
            return nk
        return df.apply(_fix, axis=1)

    df["NikeFinalPrice"] = _sanitize_nike_price("NikeFinalPrice")
    df["NikeFullPrice"] = _sanitize_nike_price("NikeFullPrice")

    base_cols = [
        "OpenSports_StyleColor_Norm", "SB_ProductCode", "SB_MarketingName", "SB_Category",
        "SB_Division", "SB_Franchise", "PLP_PrimaryCategoria", "OpenSports_URL",
        "OpenSports_FullPrice", "OpenSportsSaleDecimal", "OpenSports_FinalPrice",
        "NikeFullPrice", "NikeSaleDecimal", "NikeFinalPrice",
        "OpenSports_MaxCuotasSinInteres", "OpenSports_LastUpdated",
        "SB_SSN_VTA", "Aux_Año", "InStatusBooks",
    ]
    for c in base_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df["StyleColor"] = df["OpenSports_StyleColor_Norm"].astype(str).fillna("").str.strip()
    df["ProductCode"] = df["SB_ProductCode"].astype(str).fillna("").str.strip()
    df["Marketing Name"] = df["SB_MarketingName"].astype(str).fillna("").str.strip()
    df["Category"] = df["SB_Category"].astype(str).fillna("").str.strip()
    df["Division"] = df["SB_Division"].astype(str).fillna("").str.strip()
    df["Franchise"] = df["SB_Franchise"].astype(str).fillna("").str.strip()
    df["Gender"] = df["PLP_PrimaryCategoria"].apply(_gender_from_category)
    df["Link PDP Competitor"] = df["OpenSports_URL"].astype(str).fillna("").str.strip()

    df["Competitor Full Price"] = pd.to_numeric(df["OpenSports_FullPrice"], errors="coerce")
    df["Competitor Markdown"] = pd.to_numeric(df["OpenSportsSaleDecimal"], errors="coerce")
    df["Competitor Final Price"] = pd.to_numeric(df["OpenSports_FinalPrice"], errors="coerce")

    _m = df["Competitor Full Price"].isna() | (df["Competitor Full Price"] <= 0)
    if _m.any():
        df.loc[_m, "Competitor Full Price"] = df.loc[_m, "Competitor Final Price"]
        df.loc[_m, "Competitor Markdown"] = 0.0

    df["Nike Full Price"] = pd.to_numeric(df["NikeFullPrice"], errors="coerce")
    df["Nike Markdown"] = pd.to_numeric(df["NikeSaleDecimal"], errors="coerce")
    df["Nike Final Price"] = pd.to_numeric(df["NikeFinalPrice"], errors="coerce")

    dex_full = df["Competitor Full Price"].replace(0, pd.NA)
    df["Competitor vs Nike"] = (df["Competitor Final Price"] - df["Nike Final Price"]) / dex_full

    df["BML Full Price"] = df.apply(
        lambda r: _bml_label(r.get("Competitor Full Price"), r.get("Nike Full Price")), axis=1
    )
    df["BML Final Price"] = df.apply(
        lambda r: _bml_label(r.get("Competitor Final Price"), r.get("Nike Final Price")), axis=1
    )

    df["Competitor Shipping"] = df["Competitor Final Price"].apply(
        lambda x: _ship_opensports(float(x)) if pd.notna(x) else 0.0
    )
    df["Nike Shipping"] = df["Nike Final Price"].apply(
        lambda x: _ship_nike(float(x)) if pd.notna(x) else 0.0
    )
    df["Nike Price + Shipping"] = df["Nike Final Price"] + df["Nike Shipping"]
    df["Competitor Price + Shipping"] = df["Competitor Final Price"] + df["Competitor Shipping"]

    df["BML with Shipping"] = df.apply(
        lambda r: _bml_label(r.get("Competitor Price + Shipping"), r.get("Nike Price + Shipping")), axis=1
    )

    df["Cuotas Competitor"] = pd.to_numeric(
        df["OpenSports_MaxCuotasSinInteres"], errors="coerce"
    ).fillna(0).astype(int)

    df["Cuotas Nike"] = df.apply(
        lambda r: _nike_cuotas(float(r.get("Nike Final Price")))
        if (r.get("InStatusBooks") is True and pd.notna(r.get("Nike Final Price")))
        else pd.NA,
        axis=1,
    )

    def _bml_cuotas(row):
        if row.get("InStatusBooks") is not True:
            return ""
        nk = row.get("Cuotas Nike")
        if pd.isna(nk):
            return ""
        try:
            v = int(float(row.get("Cuotas Competitor"))) if pd.notna(row.get("Cuotas Competitor")) else 0
            nk = int(float(nk))
        except Exception:
            return ""
        if v == nk:
            return "Meet"
        return "Beat" if nk > v else "Lose"

    df["BML Cuotas"] = df.apply(_bml_cuotas, axis=1)

    def _season_row(row):
        if row.get("InStatusBooks") is True:
            v = _safe_str(row.get("SB_SSN_VTA"))
            return v if v else "no registrado"
        v2 = _safe_str(row.get("Aux_Año"))
        return v2 if v2 else "no registrado"

    df["Season"] = df.apply(_season_row, axis=1)
    df["Fecha Corrida"] = RUN_DATE
    df["NDDC"] = df.get("InStatusBooks").apply(lambda x: "NDDC" if x is True else "")
    df["Competitor"] = "OpenSports"
    df["Last Update Competitor"] = df["OpenSports_LastUpdated"].astype(str).fillna("").str.strip()

    cols = [
        "StyleColor", "ProductCode", "Marketing Name", "Category", "Division", "Franchise", "Gender",
        "Link PDP Competitor",
        "Competitor Full Price", "Competitor Markdown", "Competitor Final Price",
        "Nike Full Price", "Nike Markdown", "Nike Final Price",
        "Competitor vs Nike",
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
        "Competitor vs Nike",
        "Competitor Shipping", "Nike Shipping", "Nike Price + Shipping", "Competitor Price + Shipping",
        "Cuotas Competitor", "Cuotas Nike",
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


# ─────────────────────────────────────────────────────────────────
# EXCEL OUTPUT
# ─────────────────────────────────────────────────────────────────
def write_visual_xlsx(df: pd.DataFrame, path: str):
    df = df.copy()
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="Output", index=False)
        wb = writer.book
        ws = writer.sheets["Output"]

        header_fmt = wb.add_format({"bold": True, "font_color": "white", "bg_color": "#111827", "border": 1})
        money_fmt = wb.add_format({"num_format": "$#,##0", "border": 1})
        pct_fmt = wb.add_format({"num_format": "0.0%", "border": 1})
        text_fmt = wb.add_format({"border": 1})
        link_fmt = wb.add_format({"font_color": "blue", "underline": 1, "border": 1})
        beat_fmt = wb.add_format({"bg_color": "#C6EFCE", "font_color": "#006100", "border": 1})
        meet_fmt = wb.add_format({"bg_color": "#FFEB9C", "font_color": "#9C6500", "border": 1})
        lose_fmt = wb.add_format({"bg_color": "#FFC7CE", "font_color": "#9C0006", "border": 1})

        for col_i, name in enumerate(df.columns):
            ws.write(0, col_i, name, header_fmt)

        col_idx = {c: i for i, c in enumerate(df.columns)}
        nrows = len(df)

        if "Marketing Name" in col_idx:
            ws.set_column(col_idx["Marketing Name"], col_idx["Marketing Name"], 35, text_fmt)
        if "Fecha Corrida" in col_idx:
            ws.set_column(col_idx["Fecha Corrida"], col_idx["Fecha Corrida"], 14, text_fmt)

        def set_col_fmt(colname, fmt):
            if colname in col_idx:
                i = col_idx[colname]
                ws.set_column(i, i, 18, fmt)

        for c in [
            "Competitor Full Price", "Competitor Final Price",
            "Nike Full Price", "Nike Final Price",
            "Competitor Shipping", "Nike Shipping",
            "Nike Price + Shipping", "Competitor Price + Shipping",
        ]:
            set_col_fmt(c, money_fmt)

        for c in ["Competitor Markdown", "Nike Markdown", "Competitor vs Nike"]:
            set_col_fmt(c, pct_fmt)

        if "Link PDP Competitor" in col_idx:
            i = col_idx["Link PDP Competitor"]
            ws.set_column(i, i, 55, link_fmt)

        for c in ["BML Final Price", "BML Full Price", "BML with Shipping", "BML Cuotas"]:
            if c not in col_idx:
                continue
            i = col_idx[c]
            ws.set_column(i, i, 18, text_fmt)
            for val, fmt in [("Beat", beat_fmt), ("Meet", meet_fmt), ("Lose", lose_fmt)]:
                ws.conditional_format(1, i, nrows, i, {
                    "type": "text", "criteria": "containing", "value": val, "format": fmt,
                })

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
    log(f"🏁 STAGE: writing XLSX (FAST) -> {final_path}")
    write_fast_xlsx(df, final_path)
    log(f"🏁 STAGE: XLSX (FAST) written -> {final_path}")

    tmp_visual = final_path.replace(".xlsx", "__VISUAL_TMP__.xlsx")
    log(f"🏁 STAGE: writing XLSX (VISUAL, timeout={timeout_sec}s) -> {tmp_visual}")
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
            log(f"⚠️ XLSX VISUAL timeout. Se mantiene FAST.")
            return
        if os.path.exists(tmp_visual) and os.path.getsize(tmp_visual) > 0:
            try:
                os.replace(tmp_visual, final_path)
                log("✅ XLSX VISUAL OK.")
            except Exception as e:
                log(f"⚠️ No pude reemplazar XLSX VISUAL: {e}")
        else:
            log("⚠️ XLSX VISUAL no se genero. Se mantiene FAST.")
    finally:
        try:
            if os.path.exists(tmp_visual):
                os.remove(tmp_visual)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    log("🚀 OPENSPORTS vs NIKE – INDESTRUCTIBLE MODE")
    log(f"🧪 Headless={HEADLESS} | REFRESH_CACHED={REFRESH_CACHED}")
    log(f"🏷️  SEASON={SEASON}")
    log(f"📚 StatusBooks: {STATUSBOOKS_FILE}")
    log(f"🗃️  CACHE: {CACHE_PATH}")
    log(f"📄 RAW CSV: {OUT_RAW_CSV}")
    log(f"📄 XLSX: {OUT_VISUAL_XLSX}")
    log(f"🔢 DEBUG_LIMIT={DEBUG_LIMIT} (0=sin limite) | AGENTS={AGENTS}")

    cache = load_cache(CACHE_PATH)
    if not isinstance(cache, dict):
        cache = {}
    log(f"✅ Cache cargado: {len(cache)} stylecolors")

    sb_map = load_statusbooks_map(STATUSBOOKS_FILE, STATUSBOOKS_SHEET)
    aux_map = load_aux_assort_map(AUX_ASSORT_FILE, AUX_ASSORT_SHEET)

    # ── STAGE 1: recolectar PDPs desde las PLPs ──
    log(f"\n📦 Stage 1: recolectando PDPs desde PLP (Magento 2 paginado)...")
    style_meta = {}

    with sync_playwright() as pw:
        browser, context, page = new_browser_context(pw)
        try:
            links = collect_pdp_links_from_plp(page, OPENSPORTS_PLP_URL)
        except PWTimeoutError:
            log(f"⚠️ Timeout en PLP. Continuo con links recolectados.")
            links = []
        except Exception as e:
            log(f"⚠️ Error en PLP: {e}. Continuo.")
            links = []

        for u in links:
            style_raw = extract_stylecolor_from_url(u)
            style_norm = normalize_stylecolor(style_raw)
            if not style_norm:
                continue
            if style_norm not in style_meta:
                style_meta[style_norm] = {
                    "last_url": u,
                    "categorias": {"Nike"},
                    "primary_cat": "Nike",
                    "source_plp": OPENSPORTS_PLP_URL,
                }
            else:
                style_meta[style_norm]["last_url"] = u

            try:
                obj.close()
            except Exception:
                pass

    all_styles = list(style_meta.keys())
    log(f"✅ Stylecolors unicos: {len(all_styles)}")

    # ── STAGE 2: scraping PDPs ──
    log("\n🧾 Stage 2: scraping PDPs + update JSON ...")

    styles_ordered = list(all_styles)
    if DEBUG_LIMIT and DEBUG_LIMIT > 0:
        styles_ordered = styles_ordered[:DEBUG_LIMIT]
        all_styles = styles_ordered
        log(f"🧪 DEBUG_LIMIT activo: {len(styles_ordered)} stylecolors")

    style_to_idx = {st: i for i, st in enumerate(styles_ordered, start=1)}
    cache_lock = threading.Lock()

    def _process_styles_worker(worker_id: int, styles_chunk: list) -> dict:
        local_processed = 0
        local_scraped_new = 0
        local_skipped_safe = 0
        local_skipped_not_in_sb = 0
        local_errors = 0

        with sync_playwright() as pw:
            browser, context, page = new_browser_context(pw)

            for style_norm in styles_chunk:
                idx = style_to_idx.get(style_norm, 0)
                meta = style_meta[style_norm]
                url = meta["last_url"]

                with cache_lock:
                    if style_norm not in cache:
                        cache[style_norm] = {}

                    cache[style_norm]["PLP_Categorias"] = " | ".join(sorted(meta["categorias"]))
                    cache[style_norm]["PLP_PrimaryCategoria"] = meta["primary_cat"]
                    cache[style_norm]["PLP_SourcePLP"] = meta["source_plp"]
                    cache[style_norm]["OpenSports_URL"] = url
                    cache[style_norm]["OpenSports_StyleColor_Norm"] = style_norm

                    sb = first_map_match(sb_map, style_norm)

                    if not sb:
                        cache[style_norm]["InStatusBooks"] = False
                        local_skipped_not_in_sb += 1
                        aux = first_map_match(aux_map, style_norm)
                        # Fallback a AUX quando no está en StatusBooks
                        cache[style_norm]["SB_ProductCode"] = style_norm
                        cache[style_norm]["SB_MarketingName"] = (aux.get("SB_MarketingName") if aux else "") or style_norm
                        cache[style_norm]["SB_Division"] = (aux.get("SB_Division") if aux else "") or "Unknown"
                        cache[style_norm]["SB_Category"] = "Non-NDDC (from AUX)" if aux else "Non-NDDC"
                        cache[style_norm]["SB_Franchise"] = ""
                        cache[style_norm]["SB_SSN_VTA"] = "" # No SSN para productos auxiliares
                        cache[style_norm]["Aux_Año"] = (aux.get("Aux_Año") if aux else "") or ""
                        cache[style_norm]["NikeFullPrice"] = None
                        cache[style_norm]["NikeSaleDecimal"] = None
                        cache[style_norm]["NikeFinalPrice"] = None
                        atomic_write_json(CACHE_PATH, cache)
                        aux_marker = f"(AUX)" if aux else f"(NO_AUX)"
                        log(f"[{idx}/{len(all_styles)}] ⚠️ (W{worker_id}) {style_norm}: fuera de SB {aux_marker} -> se scrapea igual")
                    else:
                        cache[style_norm]["InStatusBooks"] = True
                        for k, v in sb.items():
                            cache[style_norm][k] = v

                    already_has_valid = float(cache[style_norm].get("OpenSports_FinalPrice", 0.0)) > VALID_PRICE_MIN

                if (not REFRESH_CACHED) and already_has_valid:
                    with cache_lock:
                        local_skipped_safe += 1
                        _nk = cache[style_norm].get("NikeFinalPrice", 0.0)
                        nike_final = float(_nk) if _nk not in (None, "", "nan") else 0.0
                        sport_price = float(cache[style_norm].get("OpenSports_FinalPrice", 0.0))
                        cache[style_norm]["DiffPctNike"] = float((sport_price - nike_final) / nike_final) if nike_final else None
                        atomic_write_json(CACHE_PATH, cache)
                    log(f"[{idx}/{len(all_styles)}] ✅ (W{worker_id}) SAFE-SKIP {style_norm}")
                    local_processed += 1
                else:
                    log(f"[{idx}/{len(all_styles)}] ➡️ (W{worker_id}) SCRAPE {style_norm} | {url}")
                    try:
                        row = None
                        for _attempt in range(2):
                            try:
                                row = scrape_pdp(page, url)
                                break
                            except PWTimeoutError:
                                if _attempt == 0:
                                    browser, context, page = _reset_triplet(
                                        worker_id, pw, browser, context, page, reason="Timeout en scrape_pdp"
                                    )
                                    continue
                                raise
                            except Exception as _e:
                                if _attempt == 0 and _is_fatal_nav_error(_e):
                                    browser, context, page = _reset_triplet(
                                        worker_id, pw, browser, context, page, reason=f"{_e}"
                                    )
                                    continue
                                raise

                        sport_price = float((row or {}).get("OpenSports_FinalPrice", 0.0))

                        with cache_lock:
                            for k, v in row.items():
                                cache[style_norm][k] = v
                            cache[style_norm]["OpenSports_PriceValid"] = bool(sport_price > VALID_PRICE_MIN)
                            _nk = cache[style_norm].get("NikeFinalPrice", 0.0)
                            has_nike = _nk not in (None, "", "nan")
                            nike_final = float(_nk) if has_nike else 0.0
                            cache[style_norm]["DiffPctNike"] = float((sport_price - nike_final) / nike_final) if nike_final else None
                            atomic_write_json(CACHE_PATH, cache)

                        local_scraped_new += 1
                        local_processed += 1

                        delta_str = f"Δ%={((sport_price - nike_final) / nike_final):.3%}" if nike_final else "Δ%=N/A"
                        nike_display = f"{nike_final:,.2f}" if has_nike else "N/A"
                        log(
                            f"   🧾 OpenSportsFinal={sport_price:,.2f} | NikeFinal={nike_display} | "
                            f"{delta_str} | Cuotas={cache.get(style_norm, {}).get('OpenSports_MaxCuotasSinInteres', 0)}"
                        )

                    except PWTimeoutError:
                        with cache_lock:
                            local_errors += 1
                            cache[style_norm]["OpenSports_Error"] = "Timeout"
                            cache[style_norm]["OpenSports_LastErrorAt"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            atomic_write_json(CACHE_PATH, cache)
                        log("   ⚠️ Timeout en PDP. Sigo.")
                        local_processed += 1

                    except Exception as e:
                        with cache_lock:
                            local_errors += 1
                            cache[style_norm]["OpenSports_Error"] = f"{type(e).__name__}: {e}"
                            cache[style_norm]["OpenSports_LastErrorAt"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            atomic_write_json(CACHE_PATH, cache)
                        log(f"   ⚠️ Error en PDP: {e}. Sigo.")
                        local_processed += 1

                if local_processed > 0 and (local_processed % BROWSER_RESET_EVERY == 0):
                    log(f"\n🧹 (W{worker_id}) RESET PERIODICO: {local_processed} items -> reinicio browser...")
                    with cache_lock:
                        try:
                            atomic_write_json(CACHE_PATH, cache)
                        except Exception as e:
                            log(f"⚠️ No pude guardar cache antes de reset: {e}")
                    for obj in (page, context, browser):
                        try:
                            obj.close()
                        except Exception:
                            pass
                    time.sleep(RESET_SLEEP_SECONDS)
                    browser, context, page = new_browser_context(pw)

            for obj in (page, context, browser):
                try:
                    obj.close()
                except Exception:
                    pass

        return {
            "processed": local_processed,
            "scraped_new": local_scraped_new,
            "skipped_safe": local_skipped_safe,
            "skipped_not_in_sb": local_skipped_not_in_sb,
            "errors": local_errors,
        }

    # Ejecutar workers
    proc_total = scraped_total = skipped_safe_total = skipped_not_in_sb_total = errors_total = 0

    if AGENTS <= 1:
        log("🧵 AGENTS=1 -> modo secuencial.")
        st = _process_styles_worker(worker_id=1, styles_chunk=styles_ordered)
        proc_total = st["processed"]
        scraped_total = st["scraped_new"]
        skipped_safe_total = st["skipped_safe"]
        skipped_not_in_sb_total = st["skipped_not_in_sb"]
        errors_total = st["errors"]
    else:
        log(f"🧵 Stage 2 paralelo: AGENTS={AGENTS}...")
        chunks = [styles_ordered[i::AGENTS] for i in range(AGENTS)]
        with ThreadPoolExecutor(max_workers=AGENTS) as ex:
            futs = [ex.submit(_process_styles_worker, wi + 1, ch) for wi, ch in enumerate(chunks) if ch]
            for fut in as_completed(futs):
                try:
                    st = fut.result()
                except Exception as e:
                    errors_total += 1
                    log(f"⚠️ Worker caido en Stage 2: {type(e).__name__}: {e}. Continuo con los demas.")
                    continue
                proc_total += st["processed"]
                scraped_total += st["scraped_new"]
                skipped_safe_total += st["skipped_safe"]
                skipped_not_in_sb_total += st["skipped_not_in_sb"]
                errors_total += st["errors"]

    # ── STAGE 3: exportar ──
    log("\n📤 Stage 3: exportando reporte ...")
    cache = load_cache(CACHE_PATH)

    rows = []
    for style_norm, rec in cache.items():
        if not isinstance(rec, dict):
            continue
        if not rec.get("OpenSports_URL"):
            continue
        if not rec.get("SB_ProductCode"):
            rec["SB_ProductCode"] = style_norm
        # Incluir tanto SB como non-SB
        if not rec.get("InStatusBooks"):
            # Asegurar que los campos mínimos estén presentes para productos no-SB
            rec.setdefault("SB_MarketingName", style_norm)
            rec.setdefault("SB_Division", "Unknown")
            rec.setdefault("SB_Category", "Non-NDDC")
            rec.setdefault("Aux_Año", "")
        rows.append(rec)

    df = pd.DataFrame(rows)
    if not df.empty and "OpenSports_StyleColor_Norm" in df.columns:
        df = df.sort_values("OpenSports_StyleColor_Norm").reset_index(drop=True)

    df_out = build_template_df(df)

    if not df_out.empty and "Last Update Competitor" in df_out.columns:
        _lu = df_out["Last Update Competitor"].astype(str).fillna("").str.strip()
        _lu_date = _lu.str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False).fillna(_lu.str.slice(0, 10))
        _before = len(df_out)
        # Mantener filas actualizadas hoy, o que no tienen fecha (ej. scrape con error en non-SB)
        _mask_today = _lu_date == RUN_DATE
        _mask_no_date = _lu_date.isna() | (_lu_date.str.strip() == "") | (_lu_date == "nan")
        df_out = df_out[_mask_today | _mask_no_date].copy()
        log(f"🧪 Filtro 'actualizados hoy o sin fecha': {_before} -> {len(df_out)} filas")

    df_out.to_csv(OUT_RAW_CSV, index=False, encoding="utf-8-sig")
    log(f"📄 RAW CSV: {OUT_RAW_CSV}")

    safe_write_excel_with_timeout(df_out, OUT_VISUAL_XLSX, timeout_sec=360)
    log(f"📄 XLSX: {OUT_VISUAL_XLSX}")

    log("\n📌 Resumen:")
    log(f"   ✅ Stylecolors detectados (PLP): {len(all_styles)}")
    log(f"   ✅ Procesados esta corrida: {proc_total}")
    log(f"   ✅ Scrapes nuevos: {scraped_total}")
    log(f"   ✅ SAFE skips: {skipped_safe_total}")
    log(f"   ⚠️ No en SB / Fallback AUX: {skipped_not_in_sb_total}")
    log(f"   ⚠️ Errores en JSON: {errors_total}")
    log(f"   📊 TOTAL en output: {len(df_out)} filas ({len([r for r in rows if not r.get('InStatusBooks')])} non-SB)")
    log("\n✅ DONE")


if __name__ == "__main__":
    main()
