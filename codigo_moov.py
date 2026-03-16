import os
import re
import time
import gc

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import math
import json
import random
import datetime as dt
from urllib.parse import urljoin

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

LINKS_XLSX = "Links Retail.xlsx"
LINKS_SHEET = "Moov"
LINKS_COL_CATEGORY = "Categoria"
LINKS_COL_LINK = "LINK"

CP_AMBA = "1425"

NIKE_STD_SHIPPING_ARS = 8999.0
NIKE_FREE_SHIP_FROM_ARS = 99000.0
Moov_STD_SHIPPING_ARS = 6899.0
Moov_FREE_SHIP_FROM_ARS = 169000.0

NIKE_CUOTAS_SIMPLE_MODE = True
NIKE_CUOTAS_ALL = 3
NIKE_CUOTAS_HIGH = 6
NIKE_CUOTAS_HIGH_FROM = 79000.0

BML_THRESHOLD_PCT = 0.02


SEASON = "SP26"
STATUSBOOKS_FILE = "StatusBooks NDDC ARG SP26.xlsb"
STATUSBOOKS_SHEET = "Books NDDC"

Headless = True
# --- Performance (multi-agent) ---
AGENTS = max(2, int(os.getenv('AGENTS', '2')))  # set 1 to disable parallelism

# --- Debug: limitar cantidad de productos a procesar (0 = sin límite) ---
DEBUG_LIMIT = int(os.getenv('DEBUG_LIMIT', '40'))


MAX_PLP_SCROLL_ROUNDS = 18
PLP_STAGNATION_ROUNDS = 4
PLP_SCROLL_PIXELS = 1400

PDP_WAIT_MS = 25_000
PLP_WAIT_MS = 35_000

CACHE_PATH = "moov_cache.json"

# --- Aux NDDC/Assort map (for products NOT in StatusBooks) ---
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
RUN_DATETIME = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

OUT_RAW_CSV = f"moov_vs_nike_raw_{TS}.csv"
OUT_VISUAL_XLSX = f"moov_vs_nike_visual_{TS}.xlsx"


def human_pause(a=0.25, b=0.85):
    time.sleep(random.uniform(a, b))


def log(msg: str):
    print(msg, flush=True)


def _is_fatal_nav_error(e: Exception) -> bool:
    s = f"{type(e).__name__}: {e}"
    s_low = s.lower()
    # Playwright / Chromium transient fatals
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
        log(f"🧹 (W{worker_id}) RESET POR ERROR: {reason}".strip())
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
    if not style:
        return ""
    style = str(style).strip().upper()
    if style.startswith("NI") and len(style) >= 3:
        return style[2:]
    return style


def extract_stylecolor_from_url(url: str) -> str:
    m = re.search(r"/([A-Za-z0-9]{5,20}-[A-Za-z0-9]{2,10})\.html", url)
    return m.group(1).upper() if m else ""


def parse_money_ar_to_float(s: str) -> float:
    if s is None:
        return 0.0
    s = str(s).strip()
    if not s:
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

    if cleaned.count(".") == 1 and re.search(r"\.\d{2}$", cleaned):
        try:
            return float(cleaned)
        except Exception:
            return 0.0

    cleaned = cleaned.replace(".", "")
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
        log(f"ℹ️ Excel con múltiples sheets. Usando la primera: '{sheet_name}'")
    else:
        df = raw

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Detectar columna de año (puede venir como 'Año', 'AÑO', etc.)
    year_col = None
    for cand in [AUX_COL_ANIO, 'AÑO', 'Año', 'ANIO', 'Anio', 'anio']:
        if cand in df.columns:
            year_col = cand
            break

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
        raise ValueError("El Excel de links quedó vacío luego de limpiar. Revisá contenido.")

    return out


def _read_excel_any_noheader(path: str, sheet: str):
    ext = os.path.splitext(path)[1].lower()
    if ext == ".xlsb":
        return pd.read_excel(path, sheet_name=sheet, header=None, dtype=str, engine="pyxlsb")
    return pd.read_excel(path, sheet_name=sheet, header=None, dtype=str)


def load_statusbooks_map(path: str, sheet: str):
    log("\n📚 Cargando Status Books...")

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
            "Revisá el StatusBooks: quizás cambió el nombre del campo."
        )

    headers = raw.iloc[header_row, :].fillna("").astype(str).str.strip().tolist()
    sb = raw.iloc[header_row + 1 :, :].copy()
    sb.columns = headers

    sb.columns = [c if c else f"__EMPTY__{i}" for i, c in enumerate(sb.columns)]

    log(f"   ✅ Archivo: {path} | Sheet: {sheet} | Filas: {len(sb)} | HeaderRow: {header_row+1}")

    COL_CODE = "Product Code"
    COL_NAME = "Marketing Name"
    COL_DIV = "BU"
    COL_CAT = "Category"
    COL_FR = "Franchise"
    
    # 🧠 PRIMARY: Buscar columna exacta con header = SEASON (ej. "SP26")
    full_price_col_idx = None
    if str(SEASON) in sb.columns:
        full_price_col_idx = list(sb.columns).index(str(SEASON))
        col_name = str(sb.columns[full_price_col_idx])
        log(f"   🧠 Full Price por SEASON exacta: '{SEASON}' -> usando columna header='{col_name}'")
    else:
        log(f"   ❌ No encontré columna header exacta '{SEASON}' en el StatusBooks.")
        # FALLBACK: Buscar en la fila anterior (celdas mergeadas) patrones con "(SP26)" o "SP26"
        group_row = header_row - 1
        if group_row >= 0:
            top = raw.iloc[group_row, :].fillna("").astype(str)
            season_patterns = [f"({SEASON})", str(SEASON)]
            match_idxs = []
            for i, val in enumerate(top.tolist()):
                v = str(val)
                if any(p in v for p in season_patterns):
                    match_idxs.append(i)
            if match_idxs:
                full_price_col_idx = match_idxs[0]
                try:
                    col_name = str(sb.columns[full_price_col_idx])
                except Exception:
                    col_name = ""
                log(f"   ⚠️ FALLBACK: Usando método antiguo - detecté columna #{full_price_col_idx+1} (header='{col_name}') en fila anterior")

    COL_SALE = "SALE"
    COL_STOCK = "STOCK BL (Inventario Brandlive)"
    COL_SSN = "SSN VTA"

    missing = [c for c in [COL_CODE, COL_NAME, COL_DIV, COL_CAT, COL_FR, COL_SALE, COL_STOCK] if c not in sb.columns]
    if missing:
        raise KeyError(
            f"Faltan columnas en StatusBooks (header detectado en fila {header_row+1}): {missing}. "
            f"Ejemplo de columnas detectadas (primeras 40): {list(sb.columns)[:40]}"
        )

    # Validación columna Full Price (detectada primero por SEASON exacto, luego por fallback)
    if full_price_col_idx is None:
        raise KeyError(
            f"No pude detectar la columna de Full Price para SEASON={SEASON}. "
            f"Busqué primero un header exacto '{SEASON}', y luego en la fila superior (celdas mergeadas) patrones como '({SEASON})'. "
            f"HeaderRow detectado: {header_row+1}. "
            f"Ejemplo de headers (primeras 40): {list(sb.columns)[:40]}"
        )
    else:
        try:
            col_name = str(sb.columns[full_price_col_idx])
        except Exception:
            col_name = ""

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
        log("⚠️ StatusBooks: no encontré columna 'SSN VTA'. Season por fila quedará vacío/no registrado para NDDC.")

    sb = sb.reset_index(drop=True)
    work = work.reset_index(drop=True)
    nike_full_series = sb.iloc[:, full_price_col_idx].apply(parse_money_ar_to_float)
    work["NikeFullPrice"] = nike_full_series.to_numpy()
    # 🧠 Algunos StatusBooks expresan precios en $k (miles). Si el nivel luce como miles, escalamos x1000.
    _vals = pd.to_numeric(work["NikeFullPrice"], errors="coerce")
    _med = _vals[_vals > 0].median()
    if pd.notna(_med) and _med < 2000:
        work["NikeFullPrice"] = (_vals * 1000.0).to_numpy()
        log("   🧠 NikeFullPrice parece estar en $k -> multiplico x1000")
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
        sb_map[code] = data
        if code.startswith("NI"):
            sb_map[code[2:]] = data
        else:
            sb_map["NI" + code] = data

    log(f"   ✅ Product Codes válidos (Final>0 y StockBL>0): {len(work)}")
    log(f"   ✅ Keys en mapa (incluye variantes con/sin NI): {len(sb_map)}")
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


def _safe_str(x):
    try:
        if x is None or pd.isna(x):
            return ''
    except Exception:
        pass
    return str(x).strip()


def load_aux_assort_map(path: str, sheet: str) -> dict:
    """Mapa auxiliar para completar campos cuando el producto NO está en StatusBooks.
    Matching por StyleColor via columna 'Material Nike'.
    """
    if not os.path.exists(path):
        log(f"⚠️ No encuentro AUX_ASSORT_FILE: {path}. Continuo sin completado auxiliar.")
        return {}
    try:
        df = pd.read_excel(path, sheet_name=sheet, dtype=str)
    except Exception as e:
        log(f"⚠️ No pude leer AUX_ASSORT_FILE ({path} / sheet={sheet}): {e}. Continuo sin completado auxiliar.")
        return {}

    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]

    # Detectar columna de año
    year_col = None
    for cand in [AUX_COL_ANIO, 'AÑO', 'Año', 'ANIO', 'Anio', 'anio']:
        if cand in df.columns:
            year_col = cand
            break

    missing = [c for c in [AUX_COL_STYLE, AUX_COL_NAME, AUX_COL_GRUPO] if c not in df.columns]
    if missing:
        log(f"⚠️ AUX_ASSORT_FILE: faltan columnas {missing}. Detectadas: {list(df.columns)[:40]}. Continuo sin completado auxiliar.")
        return {}

    out = {}
    for _, r in df.iterrows():
        sc = normalize_stylecolor(_safe_str(r.get(AUX_COL_STYLE)).upper())
        if not sc:
            continue
        name = _safe_str(r.get(AUX_COL_NAME))
        grupo = _safe_str(r.get(AUX_COL_GRUPO))
        bu = _map_grupo_to_bu(grupo)
        anio = _safe_str(r.get(year_col)) if year_col else ""
        out[sc] = {"SB_MarketingName": name, "SB_Division": bu, "Aux_Año": anio}
    log(f"✅ AUX map cargado: {len(out)} items (sheet='{sheet}')")
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


def collect_plp_links_nike(page) -> list[str]:
    base = "https://www.Moov.com.ar"
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
        if not is_nike:
            abs_u = urljoin(base, href)
            sc = extract_stylecolor_from_url(abs_u)
            if sc.upper().startswith("NI"):
                is_nike = True

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
        btn = page.locator("button.more", has_text=re.compile(r"quiero\s+ver\s+m[aá]s", re.I)).first
        if btn.count() == 0:
            btn = page.locator("button.btn.more", has_text=re.compile(r"quiero\s+ver\s*m[aá]s", re.I)).first
        if btn.count() == 0:
            return False

        try_close_overlays(page)

        try:
            btn.scroll_into_view_if_needed(timeout=2000)
        except Exception:
            pass

        if not btn.is_visible():
            try:
                page.mouse.wheel(0, 1600)
                human_pause(0.3, 0.7)
            except Exception:
                pass
            try:
                btn.scroll_into_view_if_needed(timeout=2000)
            except Exception:
                pass

        if not btn.is_visible():
            return False

        try:
            if btn.is_disabled():
                return False
        except Exception:
            pass

        btn.click(timeout=4000)
        return True
    except Exception:
        return False


def collect_pdp_links_from_plp_no_loadmore(page, plp_url: str):
    log(f"\n🌐 Abriendo PLP: {plp_url}")
    page.goto(plp_url, wait_until="domcontentloaded", timeout=PLP_WAIT_MS)
    human_pause(0.8, 1.4)
    try_close_overlays(page)
    try_set_postal_code(page, CP_AMBA)

    all_links = []
    seen = set()
    stagnation = 0
    prev_total_unique = 0

    for r in range(1, MAX_PLP_SCROLL_ROUNDS + 1):
        try:
            page.mouse.wheel(0, 2200)
        except Exception:
            pass
        human_pause(0.4, 0.9)
        try_close_overlays(page)

        links_now = collect_plp_links_nike(page)
        new_added = 0
        for u in links_now:
            if u not in seen:
                seen.add(u)
                all_links.append(u)
                new_added += 1

        total_unique = len(seen)

        if total_unique <= prev_total_unique and new_added == 0:
            stagnation += 1
        else:
            stagnation = 0
        prev_total_unique = total_unique

        if stagnation >= PLP_STAGNATION_ROUNDS:
            log("   ✅ Stop PLP: estancada (sin nuevos productos).")
            break

        clicked = _click_quiero_ver_mas(page)
        if not clicked:
            log("   ✅ Stop PLP: no encontré / no pude clickear 'Quiero ver más'.")
            break

        t0 = time.time()
        target = total_unique + 1
        while time.time() - t0 < 12:
            human_pause(0.25, 0.55)
            try_close_overlays(page)
            after = len(collect_plp_links_nike(page))
            if after >= target:
                break

    return all_links


def extract_price_final_by_label(page) -> float:
    """FINAL PRICE (precio visible principal) — scoped al box principal.
    Regla: span.value[data-js-marketing-price] dentro de #price-reload.
    """
    try:
        box = page.locator("div.prices#price-reload").first
        if box.count() == 0:
            # fallback por si el id cambia levemente
            box = page.locator("div.prices").first

        loc = box.locator("span.value[data-js-marketing-price]").first
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

        loc2 = box.locator("span.sales span.value").first
        if loc2.count() > 0:
            content = (loc2.get_attribute("content") or "").strip()
            if content:
                v = parse_money_ar_to_float(content)
                if v > 0:
                    return float(v)
            txt = (loc2.inner_text() or "").strip()
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


def extract_price_from_visible_avoid_strike(page) -> float:
    try:
        vals = page.evaluate(
            """
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

                let cur = el;
                let bad = false;
                for (let i=0; i<6 && cur; i++){
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
            """
        )
        if vals:
            parsed = [parse_money_ar_to_float(v) for v in vals]
            parsed = [p for p in parsed if p > 0]
            if not parsed:
                return 0.0

            # Evitar falsos "final price" por cuotas/financiación (e.g. "12 cuotas de $19.999")
            bad_tokens = ("cuota", "cuotas", "x ", " x", "sin interés", "interés", "cft", "tea", "tem")
            filtered = []
            for s in vals:
                ss = str(s).lower()
                if any(t in ss for t in bad_tokens):
                    continue
                v = parse_money_ar_to_float(s)
                if v and v > 0:
                    filtered.append(v)

            candidates = filtered if filtered else parsed

            # El precio final real suele ser el "principal" del box (más alto), no la cuota (más bajo)
            return float(max(candidates)) if candidates else 0.0
    except Exception:
        pass
    return 0.0


def extract_full_price_from_strike(page) -> float:
    """FULL PRICE (precio tachado) — scoped al box principal.
    Regla: dentro de #price-reload, buscar <del> y adentro span.value (priorizar @content).
    Si no existe, no hay descuento → devolver 0.0 (caller setea full=final).
    """
    try:
        box = page.locator("div.prices#price-reload").first
        if box.count() == 0:
            box = page.locator("div.prices").first

        loc = box.locator("del span.value").first
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
    try:
        page.wait_for_selector("div.installments", timeout=2500)
        nums = []

        cont = page.locator("div.installments").first
        if cont.count() > 0:
            txt = (cont.inner_text() or "").strip()
            nums += [int(x) for x in re.findall(r"(\d+)\s*cuotas?\s*sin\s*inter[eé]s", txt, flags=re.I)]

        ps = page.locator("div.installments p")
        cnt = ps.count()
        for i in range(cnt):
            t = (ps.nth(i).inner_text() or "").strip()
            if not t:
                continue
            nums += [int(x) for x in re.findall(r"(\d+)\s*cuotas?\s*sin\s*inter[eé]s", t, flags=re.I)]

        if nums:
            return max(nums)
    except Exception:
        pass

    try:
        heading = page.locator("text=/Cuotas\\s+habituales/i").first
        if heading.count() == 0:
            return 0
        container = heading.locator("xpath=ancestor::*[self::div or self::section][1]").first
        if container.count() == 0:
            container = heading.locator("xpath=ancestor::*[1]").first

        txt = (container.inner_text() or "").strip()
        nums = [int(n) for n in re.findall(r"(\\d+)\\s*cuotas?\\s*sin\\s*inter[eé]s", txt, flags=re.I)]
        return max(nums) if nums else 0
    except Exception:
        return 0


def scrape_pdp(page, url: str) -> dict:
    page.goto(url, wait_until="domcontentloaded", timeout=PDP_WAIT_MS)
    human_pause(0.5, 1.1)
    try_close_overlays(page)
    try_set_postal_code(page, CP_AMBA)

    style_raw = extract_stylecolor_from_url(url)
    style_norm = normalize_stylecolor(style_raw)

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

    price_final = 0.0
    full_price = 0.0

    # --- FIX PRECIOS (DOM ONLY / scoped) ---
    # Final: span.value[data-js-marketing-price] dentro de #price-reload (o contenedor equivalente).
    # Full: solo si existe <del> dentro del mismo contenedor; si no, Full=Final y Markdown=0.
    # Si no existe contenedor / precio DOM: NO fallback global (evita contaminación) => precio 0.0 + log NO_PRICE_DOM.
    for _try in range(2):
        try:
            price_final = extract_price_final_by_label(page)
            full_price = extract_full_price_from_strike(page)
            if price_final > 0:
                break
        except Exception:
            pass
        # wait corto por si el DOM tardó en pintar el box de precio
        try:
            page.wait_for_timeout(350)
        except Exception:
            pass

    if price_final <= 0:
        try:
            log(f"   ⚠️ (NO_PRICE_DOM) precio DOM no encontrado")
        except Exception:
            pass
        price_final = 0.0
        full_price = 0.0

    max_cuotas = extract_max_cuotas_habituales(page)

    if full_price <= 0 or (price_final > 0 and full_price < price_final):
        full_price = float(price_final)

    # Sanity check: evita full_price inflado por números ajenos al precio (cuotas, variantes, etc.)
    RATIO_MAX_FULL_VS_FINAL = 3.0
    if price_final and full_price and full_price > (price_final * RATIO_MAX_FULL_VS_FINAL):
        full_price = float(price_final)

    sale_dec = 0.0
    if full_price > 0 and price_final > 0 and full_price >= price_final:
        sale_dec = (float(full_price) - float(price_final)) / float(full_price)
        sale_dec = max(0.0, min(1.0, float(sale_dec)))

    return {
        "Retailer": "Moov",
        "Moov_URL": url,
        "Moov_Name": name,
        "Moov_StyleColor_Raw": style_raw,
        "Moov_StyleColor_Norm": style_norm,
        "Moov_FullPrice": float(full_price),
        "MoovSaleDecimal": float(sale_dec),
        "Moov_FinalPrice": float(price_final),
        "Moov_MaxCuotasSinInteres": int(max_cuotas),
        "Moov_LastUpdated": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def _bml_label_from_prices(comp_price: float, nike_price: float) -> str:
    """BML (Beat/Meet/Lose) según tu fórmula:
    IF(Comp < Nike*0.98 -> LOSE, IF(Nike < Comp*0.98 -> BEAT, else MEET)
    """
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
        return 0.0 if p >= float(NIKE_FREE_SHIP_FROM_ARS) else float(NIKE_STD_SHIPPING_ARS)
    except Exception:
        return 0.0


def _ship_for_price_Moov(price: float) -> float:
    try:
        p = float(price)
    except Exception:
        return 0.0
    if p <= 0:
        return 0.0
    try:
        return 0.0 if p >= float(Moov_FREE_SHIP_FROM_ARS) else float(Moov_STD_SHIPPING_ARS)
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
    if any(k in s for k in ['niño','nino','kids','infantil','junior','niña','nina','youth']):
        return 'Kids'
    if 'unisex' in s:
        return 'Unisex'
    return ''


def build_template_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # --- Saneamiento de precios: evita casos donde NikeFinal quede en escala incorrecta ---
    def _sanitize_nike_price(row):
        try:
            moov = float(row.get('Moov_FinalPrice') or 0) if pd.notna(row.get('Moov_FinalPrice')) else 0.0
        except Exception:
            moov = 0.0
        try:
            nk = float(row.get('NikeFinalPrice') or 0) if pd.notna(row.get('NikeFinalPrice')) else 0.0
        except Exception:
            nk = 0.0
        # Si Nike viene en escala $k (ej. 78.40) y competitor está en ARS (119999), escalamos.
        if nk > 0 and nk < 2000 and moov > 50000:
            nk = nk * 1000.0
        # Si sigue siendo implausible, lo anulamos para no generar % absurdos
        if nk > 0 and moov > 0 and (nk < moov * 0.05 or nk > moov * 20):
            return 0.0
        return nk


    df['NikeFinalPrice'] = df.apply(_sanitize_nike_price, axis=1)

    base_cols = [
        'Moov_StyleColor_Norm','SB_ProductCode', 'SB_MarketingName', 'SB_Category','SB_MarketingName','SB_Division','SB_Franchise',
        'PLP_PrimaryCategoria','Moov_URL',
        'Moov_FullPrice','MoovSaleDecimal','Moov_FinalPrice',
        'NikeFullPrice','NikeSaleDecimal','NikeFinalPrice', 'FechaCorrida',
        'Moov_MaxCuotasSinInteres','Moov_LastUpdated','SB_SSN_VTA','Aux_Año','InStatusBooks'
    ]
    for c in base_cols:
        if c not in df.columns:
            df[c] = pd.NA

    df['StyleColor'] = df['Moov_StyleColor_Norm'].astype(str).fillna('').str.strip()
    df['ProductCode'] = df['SB_ProductCode'].astype(str).fillna('').str.strip()
    df['Marketing Name'] = df['SB_MarketingName'].astype(str).fillna('').str.strip()
    df['Category'] = df['SB_Category'].astype(str).fillna('').str.strip()
    df['Division'] = df['SB_Division'].astype(str).fillna('').str.strip()
    df['Franchise'] = df['SB_Franchise'].astype(str).fillna('').str.strip()
    df['Gender'] = df['PLP_PrimaryCategoria'].apply(_gender_from_category)
    df['Link PDP Competitor'] = df['Moov_URL'].astype(str).fillna('').str.strip()

    df['Moov Full Price'] = pd.to_numeric(df['Moov_FullPrice'], errors='coerce')
    df['Moov Markdown'] = pd.to_numeric(df['MoovSaleDecimal'], errors='coerce')
    df['Moov Final Price'] = pd.to_numeric(df['Moov_FinalPrice'], errors='coerce')

    _m_full_missing = df['Moov Full Price'].isna() | (df['Moov Full Price'] <= 0)
    if _m_full_missing.any():
        df.loc[_m_full_missing, 'Moov Full Price'] = df.loc[_m_full_missing, 'Moov Final Price']
        df.loc[_m_full_missing, 'Moov Markdown'] = 0.0

    df['Nike Full Price'] = pd.to_numeric(df['NikeFullPrice'], errors='coerce')
    df['Nike Markdown'] = pd.to_numeric(df['NikeSaleDecimal'], errors='coerce')
    df['Nike Final Price'] = pd.to_numeric(df['NikeFinalPrice'], errors='coerce')

    dex_full = df['Moov Full Price'].replace(0, pd.NA)
    df['Moov vs Nike (Full Price)'] = (df['Moov Full Price'] - df['Nike Full Price']) / dex_full

    nike_full_den = df['Nike Full Price'].replace(0, pd.NA)
    diff_pct_full_nike = (df['Moov Full Price'] - df['Nike Full Price']) / nike_full_den
    df['Moov vs Nike Full (BML)'] = df.apply(lambda r: _bml_label_from_prices(r.get('Moov Full Price'), r.get('Nike Full Price')), axis=1)

    dex = df['Moov Final Price'].replace(0, pd.NA).replace(0, pd.NA)
    df['Moov vs Nike'] = (df['Moov Final Price'] - df['Nike Final Price']) / dex

    nike = df['Nike Final Price'].replace(0, pd.NA)
    diff_pct_nike = (df['Moov Final Price'] - df['Nike Final Price']) / nike
    df['Moov vs Nike (BML)'] = df.apply(lambda r: _bml_label_from_prices(r.get('Moov Final Price'), r.get('Nike Final Price')), axis=1)

    df['Moov Shipping'] = df['Moov Final Price'].apply(lambda x: _ship_for_price_Moov(float(x)) if pd.notna(x) else 0.0)
    df['Nike Shipping'] = df['Nike Final Price'].apply(lambda x: _ship_for_price_nike(float(x)) if pd.notna(x) else 0.0)
    df['Nike Price + Shipping'] = df['Nike Final Price'] + df['Nike Shipping']
    df['Moov Price + Shipping'] = df['Moov Final Price'] + df['Moov Shipping']

    diff_pct_ship = (df['Moov Price + Shipping'] - df['Nike Price + Shipping']) / df['Nike Price + Shipping'].replace(0, pd.NA)
    df['BML with Shipping'] = df.apply(lambda r: _bml_label_from_prices(r.get('Moov Price + Shipping'), r.get('Nike Price + Shipping')), axis=1)

    df['Cuotas Moov'] = pd.to_numeric(df['Moov_MaxCuotasSinInteres'], errors='coerce').fillna(0).astype(int)

    df['Cuotas Nike'] = df.apply(lambda r: _nike_cuotas_for_price(float(r.get('Nike Final Price'))) if (r.get('InStatusBooks') is True and pd.notna(r.get('Nike Final Price'))) else pd.NA, axis=1)

    def _bml_cuotas_row(row):
        v = row.get('Cuotas Moov')
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

    def _season_row(row):
        if row.get('InStatusBooks') is True:
            v = _safe_str(row.get('SB_SSN_VTA'))
            return v if v else 'no registrado'
        v2 = _safe_str(row.get('Aux_Año'))
        return v2 if v2 else 'no registrado'
    df['Season'] = df.apply(_season_row, axis=1)
    df['Fecha Corrida'] = RUN_DATE

    df['NDDC'] = df.get('InStatusBooks').apply(lambda x: 'NDDC' if x is True else '')

    df['Competitor'] = 'Moov'
    df['Last Update Competitor'] = df['Moov_LastUpdated'].astype(str).fillna('').str.strip()

    df['Link PDP Competitor'] = df['Link PDP Competitor'].astype(str).fillna('').str.strip()

    df['Competitor Full Price'] = df['Moov Full Price']
    df['Competitor Markdown'] = df['Moov Markdown']
    df['Competitor Final Price'] = df['Moov Final Price']

    df['Competitor vs Nike'] = df['Moov vs Nike']

    df['BML Final Price'] = df['Moov vs Nike (BML)']
    df['BML Full Price'] = df['Moov vs Nike Full (BML)']

    df['Competitor Shipping'] = df['Moov Shipping']
    df['Competitor Price + Shipping'] = df['Moov Price + Shipping']

    df['Cuotas Competitor'] = pd.to_numeric(df['Cuotas Moov'], errors='coerce').fillna(0).astype(int)

    cols = [
        'StyleColor','ProductCode','Marketing Name','Category','Division','Franchise','Gender',
        'Link PDP Competitor',
        'Competitor Full Price','Competitor Markdown','Competitor Final Price',
        'Nike Full Price','Nike Markdown','Nike Final Price',
        'Competitor vs Nike',
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

    num_cols = [
        'Competitor Full Price','Competitor Markdown','Competitor Final Price',
        'Nike Full Price','Nike Markdown','Nike Final Price',
        'Competitor vs Nike',
        'Competitor Shipping','Nike Shipping','Nike Price + Shipping','Competitor Price + Shipping',
        'Cuotas Competitor','Cuotas Nike'
    ]
    for c in num_cols:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors='coerce')
    return out


def write_visual_xlsx(df: pd.DataFrame, path: str):
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

        for c in ['Moov Full Price','Moov Final Price','Nike Full Price','Nike Final Price',
                  'Moov Shipping','Nike Shipping','Nike Price + Shipping','Moov Price + Shipping']:
            set_col_format(c, money_fmt)

        for c in ['Moov Markdown','Nike Markdown','Moov vs Nike (Full Price)','Moov vs Nike']:
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
    """Escritura XLSX rápida y segura (sin formato pesado)."""
    df = df.copy()
    # Mantener misma info del output; sheet simple
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="DATA")


def _visual_xlsx_worker(df: pd.DataFrame, tmp_path: str):
    """Worker separado para evitar cuelgues del proceso principal."""
    try:
        write_visual_xlsx(df, tmp_path)
    except Exception:
        # No levantar al padre; el padre decide fallback
        pass


def safe_write_excel_with_timeout(df: pd.DataFrame, final_path: str, timeout_sec: int = 360):
    """Siempre produce XLSX. Intenta 'visual' con timeout; si falla, deja el FAST."""
    # 1) FAST siempre (garantía)
    log(f"🏁 STAGE: writing XLSX (FAST) -> {final_path}")
    write_fast_xlsx(df, final_path)
    log(f"🏁 STAGE: XLSX (FAST) written -> {final_path}")

    # 2) VISUAL best-effort con timeout en proceso separado
    tmp_visual = final_path.replace(".xlsx", "__VISUAL_TMP__.xlsx")
    log(f"🏁 STAGE: writing XLSX (VISUAL, timeout={timeout_sec}s) -> {tmp_visual}")

    try:
        import multiprocessing as mp

        ctx = mp.get_context("spawn")  # Windows-safe
        p = ctx.Process(target=_visual_xlsx_worker, args=(df, tmp_visual), daemon=True)
        p.start()
        p.join(timeout_sec)

        if p.is_alive():
            try:
                p.terminate()
            except Exception:
                pass
            log(f"⚠️ XLSX VISUAL timeout ({timeout_sec}s). Se mantiene XLSX FAST.")
            return

        # Si terminó, validar archivo antes de reemplazar
        if os.path.exists(tmp_visual) and os.path.getsize(tmp_visual) > 0:
            try:
                os.replace(tmp_visual, final_path)
                log("✅ XLSX VISUAL OK (reemplazó FAST).")
            except Exception as e:
                log(f"⚠️ No pude reemplazar XLSX VISUAL (se mantiene FAST): {e}")
        else:
            log("⚠️ XLSX VISUAL no se generó correctamente. Se mantiene FAST.")

    finally:
        # limpiar tmp si quedó
        try:
            if os.path.exists(tmp_visual):
                os.remove(tmp_visual)
        except Exception:
            pass



def main():
    log("🚀 MOOV vs NIKE – INDESTRUCTIBLE MODE")
    log(f"🧪 Headless={HEADLESS} | REFRESH_CACHED={REFRESH_CACHED}")
    log(f"🏷️ SEASON={SEASON}")
    log(f"📚 StatusBooks: {STATUSBOOKS_FILE}")
    log(f"🗃️ CACHE: {CACHE_PATH} (fuente de verdad)")
    log(f"📄 Export RAW: {OUT_RAW_CSV}")
    log(f"📄 Export XLSX: {OUT_VISUAL_XLSX}")

    cache = load_cache(CACHE_PATH)
    if not isinstance(cache, dict):
        cache = {}
    log(f"✅ Cache cargado: {len(cache)} stylecolors")

    plps_df = load_plps_from_links_excel(LINKS_XLSX, LINKS_SHEET)
    log(f"✅ PLPs cargados: {len(plps_df)}")

    sb_map = load_statusbooks_map(STATUSBOOKS_FILE, STATUSBOOKS_SHEET)
    aux_map = load_aux_assort_map(AUX_ASSORT_FILE, AUX_ASSORT_SHEET)

    log("\n📦 Stage 1: recolectando PDPs desde PLPs (Nike only)...")
    style_meta = {}

    with sync_playwright() as pw:
        browser, context, page = new_browser_context(pw)

        for _, r in plps_df.iterrows():
            cat = str(r[LINKS_COL_CATEGORY]).strip()
            plp = str(r[LINKS_COL_LINK]).strip()

            try:
                links = collect_pdp_links_from_plp_no_loadmore(page, plp)
            except PWTimeoutError:
                log(f"⚠️ Timeout en PLP: {plp} | Salteo.")
                continue
            except Exception as e:
                log(f"⚠️ Error en PLP: {plp} | {e} | Salteo.")
                continue

            log(f"✅ PDPs Nike en '{cat}': {len(links)}")
            for u in links:
                style_raw = extract_stylecolor_from_url(u)
                style_norm = normalize_stylecolor(style_raw)
                if not style_norm:
                    continue

                if style_norm not in style_meta:
                    style_meta[style_norm] = {
                        "last_url": u,
                        "categorias": set([cat]),
                        "primary_cat": cat,
                        "source_plp": plp,
                    }
                else:
                    style_meta[style_norm]["last_url"] = u
                    style_meta[style_norm]["categorias"].add(cat)

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

    all_styles = list(style_meta.keys())
    log(f"\n✅ Total stylecolors únicos detectados (sin NI): {len(all_styles)}")

    log("\n🧾 Stage 2: scraping PDPs + update JSON (DB) ...")

    # style_to_idx se calcula luego de aplicar DEBUG_LIMIT
    styles_ordered = list(all_styles)
    if DEBUG_LIMIT and DEBUG_LIMIT > 0:
        styles_ordered = styles_ordered[:DEBUG_LIMIT]
        all_styles = styles_ordered
        log(f"🧪 DEBUG_LIMIT activo: procesando solo {len(styles_ordered)} stylecolors")


    style_to_idx = {st: i for i, st in enumerate(styles_ordered, start=1)}
    # 🔒 Lock + acumuladores DEBEN estar dentro de main (mismo scope que el +=)
    cache_lock = threading.Lock()

    # ✅ Inicialización de acumuladores (evita UnboundLocalError en modo paralelo)
    processed_this_run = 0
    scraped_new_this_run = 0
    skipped_safe = 0
    skipped_not_in_sb = 0
    errors = 0

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

                    cache[style_norm]["PLP_Categorias"] = " | ".join(sorted(list(meta["categorias"])))
                    cache[style_norm]["PLP_PrimaryCategoria"] = meta["primary_cat"]
                    cache[style_norm]["PLP_SourcePLP"] = meta["source_plp"]
                    cache[style_norm]["Moov_URL"] = url
                    cache[style_norm]["Moov_StyleColor_Norm"] = style_norm

                    sb = sb_map.get(style_norm) or sb_map.get("NI" + style_norm)

                    if not sb:
                        # No está en StatusBooks (NDDC). Igual lo incluimos en el universe del retailer.
                        cache[style_norm]["InStatusBooks"] = False
                        local_skipped_not_in_sb += 1

                        # Completo con AUX (si existe)
                        aux = aux_map.get(style_norm) if isinstance(aux_map, dict) else None
                        cache[style_norm]["SB_ProductCode"] = style_norm  # ProductCode del output para no-NDDC
                        cache[style_norm]["SB_MarketingName"] = (aux.get("SB_MarketingName") if aux else "") or ""
                        cache[style_norm]["SB_Division"] = (aux.get("SB_Division") if aux else "") or ""
                        cache[style_norm]["SB_Category"] = ""
                        cache[style_norm]["SB_Franchise"] = ""

                        # Nike.* vacío para no-NDDC (así quedan BMLs/comparaciones vacías)
                        cache[style_norm]["NikeFullPrice"] = None
                        cache[style_norm]["NikeSaleDecimal"] = None
                        cache[style_norm]["NikeFinalPrice"] = None

                        atomic_write_json(CACHE_PATH, cache)
                        log(f"[{idx}/{len(all_styles)}] ⚠️ (W{worker_id}) {style_norm}: fuera de StatusBooks -> se incluye por assortment (aux={'YES' if aux else 'NO'}).")
                    else:
                        cache[style_norm]["InStatusBooks"] = True
                        for k, v in sb.items():
                            cache[style_norm][k] = v

                    already_has_valid = float(cache[style_norm].get("Moov_FinalPrice", 0.0)) > VALID_PRICE_MIN

                if (not REFRESH_CACHED) and already_has_valid:
                    with cache_lock:
                        local_skipped_safe += 1

                        _nk = cache[style_norm].get("NikeFinalPrice", 0.0)
                        nike_final = float(_nk) if _nk not in (None, "", "nan") else 0.0
                        dex_price = float(cache[style_norm].get("Moov_FinalPrice", 0.0))
                        if nike_final:
                            pct_nike = ((dex_price - nike_final) / nike_final)
                            pct_over_dex = ((dex_price - nike_final) / dex_price) if dex_price else 0.0
                            cache[style_norm]["DiffPctNike"] = float(pct_nike)
                            cache[style_norm]["Moov_vs_Nike_pct"] = float(pct_over_dex)
                        else:
                            cache[style_norm]["DiffPctNike"] = None
                            cache[style_norm]["Moov_vs_Nike_pct"] = None
                            pct_nike = 0.0
                            pct_over_dex = 0.0

                        atomic_write_json(CACHE_PATH, cache)

                    log(f"[{idx}/{len(all_styles)}] ✅ (W{worker_id}) SAFE-SKIP {style_norm} | ya cacheado (MoovFinal={dex_price:,.2f})")
                    local_processed += 1
                else:
                    log(f"[{idx}/{len(all_styles)}] ➡️ (W{worker_id}) SCRAPE {style_norm} | {url}")
                    try:
                        row = None
                        dex_price = 0.0
                        # Reintento interno con reset si el browser/driver muere o se queda sin recursos
                        for _attempt in range(2):
                            try:
                                row = scrape_pdp(page, url)
                                dex_price = float((row or {}).get("Moov_FinalPrice", 0.0))
                                break
                            except PWTimeoutError as _te:
                                if _attempt == 0:
                                    browser, context, page = _reset_triplet(worker_id, pw, browser, context, page, reason=f"Timeout en scrape_pdp -> reset y reintento")
                                    continue
                                raise
                            except Exception as _e:
                                if _attempt == 0 and _is_fatal_nav_error(_e):
                                    browser, context, page = _reset_triplet(worker_id, pw, browser, context, page, reason=f"{_e}")
                                    continue
                                raise

                        with cache_lock:
                            for k, v in row.items():
                                cache[style_norm][k] = v

                            cache[style_norm]["Moov_PriceValid"] = bool(dex_price > VALID_PRICE_MIN)

                            _nk = cache[style_norm].get("NikeFinalPrice", 0.0)
                            nike_final = float(_nk) if _nk not in (None, "", "nan") else 0.0
                            if nike_final:
                                pct_nike = ((dex_price - nike_final) / nike_final)
                                pct_over_dex = ((dex_price - nike_final) / dex_price) if dex_price else 0.0
                                cache[style_norm]["DiffPctNike"] = float(pct_nike)
                                cache[style_norm]["Moov_vs_Nike_pct"] = float(pct_over_dex)
                            else:
                                cache[style_norm]["DiffPctNike"] = None
                                cache[style_norm]["Moov_vs_Nike_pct"] = None
                                pct_nike = 0.0
                                pct_over_dex = 0.0

                            atomic_write_json(CACHE_PATH, cache)

                        local_scraped_new += 1
                        local_processed += 1
                        log(
                            f"   🧾 MoovFinal={dex_price:,.2f} | NikeFinal={nike_final:,.2f} | Δ%(Nike)={pct_nike:.3%} | Δ%(overDex)={pct_over_dex:.3%} | "
                            f"CuotasHabitualesMax={cache.get(style_norm, {}).get('Moov_MaxCuotasSinInteres', 0)}"
                        )

                    except PWTimeoutError:
                        with cache_lock:
                            local_errors += 1
                            cache[style_norm]["Moov_Error"] = "Timeout"
                            cache[style_norm]["Moov_LastErrorAt"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            atomic_write_json(CACHE_PATH, cache)
                        log("   ⚠️ Timeout en PDP. Guardado error en JSON y sigo.")
                        local_processed += 1

                    except Exception as e:
                        with cache_lock:
                            local_errors += 1
                            cache[style_norm]["Moov_Error"] = f"{type(e).__name__}: {e}"
                            cache[style_norm]["Moov_LastErrorAt"] = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            atomic_write_json(CACHE_PATH, cache)
                        log(f"   ⚠️ Error en PDP: {e}. Guardado error en JSON y sigo.")
                        local_processed += 1

                if local_processed > 0 and (local_processed % BROWSER_RESET_EVERY == 0):
                    log(f"\n🧹 (W{worker_id}) RESET EXTREMO: procesados {local_processed} items -> guardo cache, cierro browser/context, espero {RESET_SLEEP_SECONDS}s, reabro limpio...\n")
                    with cache_lock:
                        try:
                            atomic_write_json(CACHE_PATH, cache)
                        except Exception as e:
                            log(f"⚠️ (W{worker_id}) No pude guardar cache antes de reset: {e} (sigo igual).")

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

    # Acumuladores para resumen (se completan en secuencial o paralelo)
    proc_total = 0
    scraped_total = 0
    skipped_safe_total = 0
    skipped_not_in_sb_total = 0
    errors_total = 0

    if AGENTS <= 1:
        log("🧵 AGENTS=1 -> Stage 2 en modo secuencial (sin paralelismo).")
        stats = _process_styles_worker(worker_id=1, styles_chunk=styles_ordered)
        processed_this_run = stats["processed"]
        scraped_new_this_run = stats["scraped_new"]
        skipped_safe = stats["skipped_safe"]
        skipped_not_in_sb = stats["skipped_not_in_sb"]
        errors = stats["errors"]
        proc_total = processed_this_run
        scraped_total = scraped_new_this_run
        skipped_safe_total = skipped_safe
        skipped_not_in_sb_total = skipped_not_in_sb
        errors_total = errors
    else:
        log(f"🧵 Stage 2 paralelo: AGENTS={AGENTS}...")
        
        # Acumuladores locales para el bloque paralelo
        proc_total = 0
        scraped_total = 0
        skipped_safe_total = 0
        skipped_not_in_sb_total = 0
        errors_total = 0
        
        chunks = [styles_ordered[i::AGENTS] for i in range(AGENTS)]
        
        log("🏁 STAGE: waiting workers join")
        with ThreadPoolExecutor(max_workers=AGENTS) as ex:
            futs = [ex.submit(_process_styles_worker, wi + 1, ch) for wi, ch in enumerate(chunks) if ch]
            for fut in as_completed(futs):
                st = fut.result()
                proc_total += st["processed"]
                scraped_total += st["scraped_new"]
                skipped_safe_total += st["skipped_safe"]
                skipped_not_in_sb_total += st["skipped_not_in_sb"]
                errors_total += st["errors"]
        log("🏁 STAGE: workers joined")
    
    # Transferir los resultados a las variables principales
    processed_this_run = proc_total
    scraped_new_this_run = scraped_total
    skipped_safe = skipped_safe_total
    skipped_not_in_sb = skipped_not_in_sb_total
    errors = errors_total

    log("🏁 STAGE: queue finished")
    log("🏁 STAGE: writing output")
    log("\n📤 Stage 3: exportando reporte desde moov_cache.json ...")
    cache = load_cache(CACHE_PATH)

    rows = []
    for style_norm, rec in cache.items():
        if not isinstance(rec, dict):
            continue
        # incluir TODO el assortment Nike del retailer (NDDC y no-NDDC)
        if not rec.get("Moov_URL"):
            continue
        if not rec.get("SB_ProductCode"):
            # fallback: si no tiene SB_ProductCode, uso stylecolor
            rec["SB_ProductCode"] = style_norm
        rows.append(rec)

    df = pd.DataFrame(rows)

    if not df.empty and "Moov_StyleColor_Norm" in df.columns:
        df = df.sort_values(by=["Moov_StyleColor_Norm"], ascending=True).reset_index(drop=True)
    df_out = build_template_df(df)

    if not df_out.empty and "Last Update Competitor" in df_out.columns:
        _lu = df_out["Last Update Competitor"].astype(str).fillna("").str.strip()
        _lu_date = _lu.str.extract(r"(\d{4}-\d{2}-\d{2})", expand=False)
        _lu_date = _lu_date.fillna(_lu.str.slice(0, 10))
        _before = len(df_out)
        df_out = df_out[_lu_date == RUN_DATE].copy()
        log(f"🧪 Filtro 'solo actualizados hoy': {_before} -> {len(df_out)} filas (RUN_DATE={RUN_DATE})")

    df_out.to_csv(OUT_RAW_CSV, index=False, encoding="utf-8-sig")
    log(f"📄 RAW CSV exportado desde JSON: {OUT_RAW_CSV}")

    safe_write_excel_with_timeout(df_out, OUT_VISUAL_XLSX, timeout_sec=360)
    log(f"📄 Visual XLSX exportado desde JSON: {OUT_VISUAL_XLSX}")
    log("🏁 STAGE: output written")

    log("\n📌 Resumen:")
    log(f"   ✅ Stylecolors detectados (PLPs): {len(all_styles)}")
    log(f"   ✅ Procesados esta corrida: {processed_this_run}")
    log(f"   ✅ Scrapes realizados esta corrida: {scraped_new_this_run}")
    log(f"   ✅ SAFE skips esta corrida: {skipped_safe}")
    log(f"   ⚠️ No estaban en SB (o sin stock BL): {skipped_not_in_sb}")
    log(f"   ⚠️ Errores PDP guardados en JSON: {errors}")
    log("\n✅ DONE")
    log("\n✅ FINALIZADO (indestructible)")


if __name__ == "__main__":
    main()
