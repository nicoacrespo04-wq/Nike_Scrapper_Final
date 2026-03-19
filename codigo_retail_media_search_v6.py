# ============================================================
# RETAIL MEDIA SEARCH v6
#
# Combina:
#   - JS de deteccion de cards de inspector_v2 (function() syntax, sin arrow)
#   - Arquitectura 1 browser session por retailer (como search_5)
#   - 30 queries por retailer
#   - MercadoLibre con proxy residencial Decodo
#   - Output Excel heatmap + CSV
#
# Flujo por retailer:
#   1. Abrir browser (con proxy si aplica)
#   2. Warmup: cargar home, cerrar modal CP
#   3. Descubrir mejor selector CSS UNA sola vez (con primera query)
#   4. Iterar 30 queries: buscar, scroll, extraer cards, contar Nike
#   5. Cerrar browser
# ============================================================

import datetime as dt
import json
import random
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import pandas as pd
import xlsxwriter
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# --- CONFIG ---
HEADLESS      = False
TOP_N         = 20
PAGE_WAIT_MS  = 35_000
SCROLL_ROUNDS = 6
SCROLL_PX     = 1_200
WORKERS       = 3

TS       = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_DATE = dt.datetime.now().strftime("%Y-%m-%d")
OUT_XLSX = f"retail_media_search_v6_{TS}.xlsx"
OUT_CSV  = f"retail_media_search_v6_{TS}.csv"

# --- PROXY RESIDENCIAL (MercadoLibre) ---
RESI_HOST  = "proxy.smartproxy.net"
RESI_PORTS = [3120]
RESI_USER  = "smart-hysjlehcrm30"
RESI_PASS  = "GmpKHg6LdhAbs9Tx"

def proxy_resi() -> dict:
    port = random.choice(RESI_PORTS)
    return {"server": f"http://{RESI_HOST}:{port}",
            "username": RESI_USER, "password": RESI_PASS}

# --- 30 BUSQUEDAS ---
SEARCH_QUERIES = [
    # Running (5)
    "zapatillas running",
    "zapatillas running hombre",
    "zapatillas running mujer",
    "zapatillas para correr",
    "zapatillas entrenamiento",
    # Training (3)
    "zapatillas training",
    "zapatillas gym",
    "zapatillas gimnasio",
    # Futbol (4)
    "botines de futbol",
    "botines futbol hombre",
    "botines futsal",
    "botines niño",
    # Lifestyle (5)
    "zapatillas blancas",
    "zapatillas negras",
    "sneakers",
    # Kids (1)
    "zapatillas ninos",
    # Indumentaria (5)
    "remera deportiva",
    "campera deportiva",
    "buzo deportivo",
    "short deportivo",
    "pantalon deportivo",
    "ropa deportiva",
    # Outdoor (4)
    "zapatillas trail",
    "zapatillas trekking",
    "zapatillas tenis",
    "zapatillas padel",
    # Accesorios (2)
    "mochila deportiva",
    "medias deportivas",
    "bolso deportivo",

    # Basquet (1)
    "zapatillas basquet",
]

assert len(SEARCH_QUERIES) == 30, f"Expected 30 queries, got {len(SEARCH_QUERIES)}"

# --- RETAILERS ---
CP_POSTAL = "1425"

RETAILERS = {
    "Moov": {
        "type":         "vtex",
        "base":         "https://www.moov.com.ar",
        "search_input": 'input[placeholder*="Buscar"],input[class*="searchInput"],input[type="search"]',
        "search_url":   "https://www.moov.com.ar/buscar?q={q}",
        "cp_modal":     True,
        "proxy":        None,
    },
    "Dexter": {
        "type":         "vtex",
        "base":         "https://www.dexter.com.ar",
        "search_input": 'input[placeholder*="Buscar"],input[class*="searchInput"],input[type="search"]',
        "search_url":   "https://www.dexter.com.ar/buscar?q={q}",
        "cp_modal":     True,
        "proxy":        None,
    },
    "StockCenter": {
        "type":         "vtex",
        "base":         "https://www.stockcenter.com.ar",
        "search_input": 'input[placeholder*="Buscar"],input[class*="searchInput"],input[type="search"]',
        "search_url":   "https://www.stockcenter.com.ar/buscar?q={q}",
        "cp_modal":     True,
        "proxy":        None,
    },
    "Sporting": {
        "type":         "vtex",
        "base":         "https://www.sporting.com.ar",
        "search_input": 'input[placeholder*="Buscar"],input[class*="searchInput"],input[type="search"]',
        "search_url":   "https://www.sporting.com.ar/{q_slug}",
        "cp_modal":     True,
        "proxy":        None,
    },
    "SoloDeportes": {
        "type":         "magento",
        "base":         "https://www.solodeportes.com.ar",
        "search_input": 'input[placeholder*="Buscar"],input#search,input[name="q"],input[type="search"]',
        "search_url":   "https://www.solodeportes.com.ar/catalogsearch/result/?q={q}",
        "cp_modal":     False,
        "proxy":        None,
    },
    "DigitalSport": {
        "type":         "digitalsport",
        "base":         "https://www.digitalsport.com.ar",
        "search_input": 'input[name="q"],input[placeholder*="Buscar"],input[type="search"]',
        "search_url":   "https://www.digitalsport.com.ar/buscar?q={q}",
        "cp_modal":     False,
        "proxy":        None,
    },
    "OpenSports": {
        "type":         "magento",
        "base":         "https://www.opensports.com.ar",
        "search_input": 'input#search,input[name="q"],input[placeholder*="Buscar"]',
        "search_url":   "https://www.opensports.com.ar/catalogsearch/result/?q={q}",
        "cp_modal":     False,
        "proxy":        None,
    },
    "MercadoLibre": {
        "type":         "meli",
        "base":         "https://www.mercadolibre.com.ar",
        "search_input": 'input[placeholder*="Buscar"],input#cb1-edit,input[type="text"][class*="nav-search"]',
        "search_url":   "https://listado.mercadolibre.com.ar/{q_slug}",
        "cp_modal":     False,
        "proxy":        "residencial",
    },
}

KNOWN_BRANDS = [
    "nike", "adidas", "puma", "asics", "new balance", "under armour",
    "reebok", "fila", "saucony", "brooks", "mizuno", "hoka", "on running",
    "salomon", "columbia", "lacoste", "vans", "converse", "jordan",
    "topper", "penalty", "olympikus",
]

import re as _re
_PRICE_DISPLAY_PATTERNS = _re.compile(
    r"(price|sales|tax|discount|promo|badge|stamp|label|tag|"
    r"breadcrumb|banner|nav|menu|header|footer|modal|overlay|"
    r"swiper|carousel|slider|toast|notification)$",
    _re.IGNORECASE,
)

COLS_RAW = [
    "Retailer", "Query", "Total_Cards", "Nike_Cards", "Nike_Share",
    "Nike_Positions", "Best_Selector", "Status", "URL", "Error",
]

# ============================================================
# UTILS
# ============================================================
def log(msg: str):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def human_pause(a=0.3, b=0.9):
    time.sleep(random.uniform(a, b))

def scroll_page(page, rounds=SCROLL_ROUNDS, px=SCROLL_PX):
    for _ in range(rounds):
        page.mouse.wheel(0, px)
        human_pause(0.4, 0.8)

def slugify(q: str) -> str:
    """Convierte query a slug sin acentos para URLs."""
    q = unicodedata.normalize("NFKD", q).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", "-", q.strip().lower())

# ============================================================
# JS CONSTANTS — VERBATIM de inspector_v2 (function() syntax)
# IMPORTANTE: No usar arrow functions ni const/let — Playwright
# falla con "Malformed arrow function parameter list"
# ============================================================

_COLLECT_FN = """
    function collectStrings(el) {
        var parts = [];
        el.querySelectorAll('input.productGtmData,[class*="productGtmData"],input[class*="GtmData"]').forEach(function(inp) {
            try {
                var raw = inp.value || inp.getAttribute('value') || '';
                if (raw) {
                    var obj = JSON.parse(raw);
                    ['item_brand','item_name','affiliation','brand','name','category'].forEach(function(k) {
                        if (obj[k]) parts.push(String(obj[k]));
                    });
                }
            } catch(e) {}
        });
        el.querySelectorAll('[data-product-name],[data-brand],[data-sku]').forEach(function(n) {
            ['data-product-name','data-brand','data-sku'].forEach(function(a) {
                var v = n.getAttribute(a); if (v) parts.push(v);
            });
        });
        el.querySelectorAll('img').forEach(function(img) {
            var alt = (img.getAttribute('alt') || '').trim();
            if (alt && alt.length > 3) parts.push(alt);
        });
        el.querySelectorAll('a[href]').forEach(function(a) {
            var href = a.getAttribute('href') || '';
            if (href && href.indexOf('javascript') < 0) parts.push(href);
        });
        var nameSels = ['.tile-body a','.product-name','.product-title',
            '[class*="productName"]','[class*="product-name"]','[class*="ProductName"]',
            '.product-item-name a','h2','h3','.name','.title'];
        for (var i = 0; i < nameSels.length; i++) {
            var ne = el.querySelector(nameSels[i]);
            if (ne) { var t = (ne.innerText||ne.textContent||'').trim(); if (t.length > 3) { parts.push(t); break; } }
        }
        var full = (el.innerText||el.textContent||'').trim();
        if (full) parts.push(full.slice(0, 300));
        [el].concat(Array.from(el.querySelectorAll('[aria-label]'))).forEach(function(n) {
            var v = (n.getAttribute('aria-label')||'').trim(); if (v) parts.push(v);
        });
        return parts.join(' ').toLowerCase();
    }
"""

# Extrae cards con selector dado — arg = [selector, top_n]
_EXTRACT_JS = (
    "function(arg) {\n"
    + _COLLECT_FN
    + """
    var sel = arg[0], top_n = arg[1];
    var items = [];
    try { items = Array.from(document.querySelectorAll(sel)); } catch(e) {}
    var results = [];
    for (var i = 0; i < Math.min(items.length, top_n); i++) {
        var combined = collectStrings(items[i]);
        results.push({ position: i+1, combined: combined.slice(0, 800) });
    }
    return results;
}"""
)

# Fallback: links con imagen — arg = [top_n]
_LINK_FALLBACK_JS = (
    "function(arg) {\n"
    + _COLLECT_FN
    + """
    var top_n = arg[0];
    var seen = {};
    var items = Array.from(document.querySelectorAll('a[href]')).filter(function(a) {
        var href = a.getAttribute('href') || '';
        var key = href.split('?')[0];
        if (seen[key]) return false;
        var valid = a.querySelector('img') !== null
            && href.indexOf('-') >= 0 && href.length > 20
            && !href.match(/busca|search|account|login|cart|javascript|wishlist|#|category|marca/);
        if (valid) seen[key] = true;
        return valid;
    });
    var results = [];
    for (var i = 0; i < Math.min(items.length, top_n); i++) {
        var combined = collectStrings(items[i]);
        results.push({ position: i+1, combined: combined.slice(0, 800) });
    }
    return results;
}"""
)

# Auto-discovery de selectores en el DOM — sin argumentos
_DISCOVER_JS = """
function() {
    var priceRe = /\\$\\s*\\d{1,3}([.,]\\d{3})*/;
    var priceEls = [];
    document.querySelectorAll('*').forEach(function(el) {
        if (el.children.length > 0) return;
        var txt = (el.innerText||el.textContent||'').trim();
        if (priceRe.test(txt)) priceEls.push(el);
    });
    var depthMap = {};
    priceEls.forEach(function(el) {
        var node = el;
        for (var i = 0; i < 6; i++) {
            node = node && node.parentElement;
            if (!node) break;
            var cls = node.className || '';
            if (typeof cls !== 'string' || cls.length < 2) continue;
            var key = node.tagName.toLowerCase() + '|' + cls.trim().split(/\\s+/).slice(0,3).join(' ');
            if (!depthMap[key]) depthMap[key] = { count:0, tag:node.tagName.toLowerCase(), cls:cls.trim() };
            depthMap[key].count++;
        }
    });
    var priceCandidate = Object.values(depthMap)
        .filter(function(v) { return v.count >= 3; })
        .sort(function(a,b) { return b.count - a.count; })
        .slice(0, 8)
        .map(function(v) {
            var firstCls = v.cls.split(/\\s+/)[0];
            var sel = v.tag + (firstCls ? '.' + firstCls : '');
            try { return { strategy:'price-parent', sel:sel, count:document.querySelectorAll(sel).length }; }
            catch(e) { return { strategy:'price-parent', sel:sel, count:0 }; }
        });
    var knownSels = [
        'div.product-tile','li.grid-tile',
        'article.vtex-product-summary-2-x-element',
        'article[class*="productSummary"]',
        'div[class*="vtex-product-summary"]',
        'div.vtex-search-result-3-x-galleryItem',
        'div[class*="galleryItem"]',
        'section[class*="product-summary"]',
        'div[class*="ProductSummary"]',
        'div:has(> input.productGtmData)',
        'li:has(> div > input.productGtmData)',
        'li.product-item','div.product-item',
        '.products-grid li','li.ui-search-layout__item',
        '[class*="product-card"]','[class*="productCard"]',
        'a.product[productid]','div[class*="shelf"]',
    ];
    var knownResults = knownSels.map(function(sel) {
        try { return { strategy:'known', sel:sel, count:document.querySelectorAll(sel).length }; }
        catch(e) { return { strategy:'known', sel:sel, count:0 }; }
    }).filter(function(r) { return r.count >= 2; }).sort(function(a,b) { return b.count - a.count; });
    var gtmInputs = Array.from(document.querySelectorAll('input.productGtmData,input[class*="GtmData"]'));
    var gtmSample = null;
    if (gtmInputs.length >= 2) {
        var parent = gtmInputs[0].closest('article,li,div[class]') || gtmInputs[0].parentElement;
        if (parent) {
            var firstCls = (parent.className||'').trim().split(/\\s+/)[0];
            var gsel = parent.tagName.toLowerCase() + (firstCls ? '.'+firstCls : '');
            gtmSample = { strategy:'gtmData', sel:gsel, count:gtmInputs.length };
        }
    }
    var repeatMap = {};
    document.querySelectorAll('a').forEach(function(a) {
        if (!a.querySelector('img')) return;
        var parent = a.closest('li,article,div[class]');
        if (!parent) return;
        var firstCls = (parent.className||'').trim().split(/\\s+/)[0];
        if (!firstCls) return;
        var key = parent.tagName.toLowerCase() + '.' + firstCls;
        repeatMap[key] = (repeatMap[key] || 0) + 1;
    });
    var repeatCandidates = Object.entries(repeatMap)
        .filter(function(e) { return e[1] >= 3; })
        .sort(function(a,b) { return b[1] - a[1]; })
        .slice(0, 5)
        .map(function(e) {
            try { return { strategy:'img-link-repeat', sel:e[0], count:document.querySelectorAll(e[0]).length }; }
            catch(ex) { return { strategy:'img-link-repeat', sel:e[0], count:e[1] }; }
        });
    return { byPrice:priceCandidate, byKnown:knownResults.slice(0,10), byGtm:gtmSample, byRepeat:repeatCandidates };
}
"""

# JS MercadoLibre — function() syntax (sin arrow functions)
_MELI_EXTRACT_JS = (
    "function(arg) {\n"
    + _COLLECT_FN
    + """
    var top_n = arg[0];
    var selectors = [
        'li.ui-search-layout__item',
        'li[class*="ui-search-layout__item"]',
        'div.ui-search-result__wrapper',
        'li.results-item',
        '.poly-card',
    ];
    var items = [];
    for (var s = 0; s < selectors.length; s++) {
        var found = Array.from(document.querySelectorAll(selectors[s]));
        if (found.length >= 4) { items = found; break; }
    }
    var results = [];
    for (var i = 0; i < Math.min(items.length, top_n); i++) {
        var el = items[i];
        var titleEl = el.querySelector('h2, .poly-component__title, .ui-search-item__title, [class*="title"]');
        var title = titleEl ? (titleEl.innerText || titleEl.textContent || '').trim() : '';
        var combined = collectStrings(el);
        var full = (title + ' ' + combined).toLowerCase();
        results.push({ position: i+1, combined: full.slice(0, 800) });
    }
    return results;
}"""
)

# ============================================================
# PICK BEST SELECTOR — verbatim de inspector_v2
# ============================================================
def _is_price_display(sel: str) -> bool:
    last_part = sel.split(".")[-1].split("[")[0]
    return bool(_PRICE_DISPLAY_PATTERNS.search(last_part))

def pick_best_selector(discovery: dict) -> Optional[str]:
    # 1. GTM - mas confiable en VTEX
    g = discovery.get("byGtm")
    if g and g.get("count", 0) >= 3:
        return g["sel"]
    # 2. byKnown — rango 4-90
    for c in discovery.get("byKnown", []):
        cnt = c.get("count", 0)
        if 4 <= cnt <= 90:
            return c["sel"]
    # 2b. byKnown fallback: hasta 200
    for c in discovery.get("byKnown", []):
        cnt = c.get("count", 0)
        if 90 < cnt <= 200:
            return c["sel"]
    # 3. byRepeat
    for c in discovery.get("byRepeat", []):
        cnt = c.get("count", 0)
        if 4 <= cnt <= 80 and not _is_price_display(c["sel"]):
            return c["sel"]
    # 4. byPrice
    for c in discovery.get("byPrice", []):
        cnt = c.get("count", 0)
        if 4 <= cnt <= 80 and not _is_price_display(c["sel"]):
            return c["sel"]
    return None

# ============================================================
# CP MODAL — verbatim de inspector_v2
# ============================================================
def dismiss_cp_modal(page) -> bool:
    for sel in [
        'input[placeholder*="postal"]','input[placeholder*="Postal"]',
        'input[placeholder*="codigo"]','input[placeholder*="Codigo"]',
        'input[id*="postal"]','input[id*="zipcode"]',
        'input[name*="postal"]','input[name*="zipcode"]',
    ]:
        try:
            inp = page.locator(sel).first
            if inp.count() > 0 and inp.is_visible(timeout=1500):
                inp.fill(CP_POSTAL)
                human_pause(0.3, 0.6)
                confirmed = False
                for bsel in ['button:has-text("Confirmar")','button:has-text("Aceptar")',
                             'button:has-text("OK")','button:has-text("Continuar")',
                             'button[class*="confirm"]','button[class*="submit"]']:
                    try:
                        btn = page.locator(bsel).first
                        if btn.count() > 0 and btn.is_visible(timeout=800):
                            btn.click(); confirmed = True; break
                    except Exception: pass
                if not confirmed:
                    inp.press("Enter")
                human_pause(0.5, 1.0)
                log(f"      CP modal cerrado con {CP_POSTAL}")
                return True
        except Exception: pass
    return False

# ============================================================
# BUSQUEDA — verbatim de inspector_v2
# ============================================================
def do_search(page, retailer_name: str, cfg: Dict, query: str) -> str:
    input_sel    = cfg.get("search_input", 'input[type="search"]')
    base_url     = cfg["base"]
    search_url_t = cfg.get("search_url", "")

    def _try_input() -> bool:
        for sel in [s.strip() for s in input_sel.split(",")]:
            try:
                inp = page.locator(sel).first
                if inp.count() > 0 and inp.is_visible(timeout=2000):
                    inp.click(timeout=2000)
                    human_pause(0.2, 0.4)
                    inp.fill("")
                    inp.type(query, delay=40)
                    human_pause(0.3, 0.6)
                    inp.press("Enter")
                    log(f"      Input encontrado: {sel}")
                    return True
            except Exception: continue
        return False

    if _try_input():
        pass
    else:
        log(f"      Input no encontrado - recargando home")
        try:
            page.goto(base_url, wait_until="domcontentloaded", timeout=PAGE_WAIT_MS)
            human_pause(2.0, 3.0)
            dismiss_cp_modal(page)
        except Exception: pass

        if not _try_input():
            if search_url_t:
                q_enc  = quote_plus(query)
                q_slug = slugify(query)
                search_url = search_url_t.replace("{q}", q_enc).replace("{q_slug}", q_slug)
                log(f"      URL fallback: {search_url}")
                try:
                    page.goto(search_url, wait_until="domcontentloaded", timeout=PAGE_WAIT_MS)
                except Exception: pass
            else:
                log(f"      Sin URL template para {retailer_name}")
                return page.url

    try: page.wait_for_load_state("load", timeout=PAGE_WAIT_MS)
    except Exception: pass
    human_pause(1.5, 2.5)
    dismiss_cp_modal(page)
    return page.url

# ============================================================
# BROWSER FACTORY
# ============================================================
def _make_browser(pw, proxy_cfg: Optional[dict] = None):
    launch_kwargs = dict(
        headless=HEADLESS,
        args=["--disable-blink-features=AutomationControlled",
              "--no-sandbox", "--disable-dev-shm-usage"],
    )
    if proxy_cfg:
        launch_kwargs["proxy"] = proxy_cfg

    browser = pw.chromium.launch(**launch_kwargs)
    context = browser.new_context(
        locale="es-AR",
        timezone_id="America/Argentina/Buenos_Aires",
        viewport={"width": 1440, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        extra_http_headers={"Accept-Language": "es-AR,es;q=0.9"},
    )
    page = context.new_page()
    return browser, context, page

def _get_proxy_for_retailer(cfg: Dict) -> Optional[dict]:
    p = cfg.get("proxy")
    if p == "residencial":
        return proxy_resi()
    return None

def _rotate_browser(pw, browser, context, page, cfg: Dict):
    """Cierra el browser actual y abre uno nuevo (rotando IP para MeLi)."""
    try: page.close(); context.close(); browser.close()
    except Exception: pass
    time.sleep(2)
    proxy_cfg = _get_proxy_for_retailer(cfg)
    return _make_browser(pw, proxy_cfg)

# ============================================================
# CLICK LOAD MORE (para MeLi)
# ============================================================
def _click_load_more(page, current_count: int, top_n: int) -> bool:
    if current_count >= top_n:
        return False
    btn_selectors = [
        'button:has-text("Ver mas")', 'button:has-text("Cargar mas")',
        'button:has-text("Ver todos")', 'button:has-text("Mostrar mas")',
        'a:has-text("Ver mas")', 'a:has-text("Cargar mas")',
        '[class*="showMore"]', '[class*="loadMore"]', '[class*="show-more"]',
        '[class*="load-more"]', '[class*="verMas"]',
        'button[class*="paging"]', 'button[data-testid*="load"]',
    ]
    for sel in btn_selectors:
        try:
            btn = page.locator(sel).first
            if btn.count() > 0 and btn.is_visible(timeout=800):
                btn.scroll_into_view_if_needed(timeout=800)
                btn.click(timeout=1500)
                human_pause(1.5, 2.5)
                return True
        except Exception:
            continue
    return False

# ============================================================
# EXTRACTORES DE CARDS
# ============================================================
def extract_cards_generic(page, best_sel: Optional[str], top_n: int) -> List[Dict]:
    """
    Extrae cards usando best_sel (descubierto por _DISCOVER_JS).
    Fallback a link+img si < 3 cards.
    """
    cards = []
    if best_sel:
        try:
            cards = page.evaluate(_EXTRACT_JS, [best_sel, top_n]) or []
        except Exception as e:
            log(f"      extract_cards_generic error con sel '{best_sel}': {e}")

    if len(cards) < 3:
        log(f"      Fallback link+img...")
        try:
            cards = page.evaluate(_LINK_FALLBACK_JS, [top_n]) or []
        except Exception as e:
            log(f"      Fallback error: {e}")

    return cards

def extract_cards_meli(page, top_n: int) -> List[Dict]:
    """MercadoLibre: usa selectores hardcodeados + click 'Ver mas'."""
    try:
        cards = page.evaluate(_MELI_EXTRACT_JS, [top_n]) or []
        # Intentar cargar mas resultados
        for _ in range(2):
            if len(cards) >= top_n:
                break
            if not _click_load_more(page, len(cards), top_n):
                break
            scroll_page(page, rounds=3)
            human_pause(1.0, 1.5)
            cards = page.evaluate(_MELI_EXTRACT_JS, [top_n]) or []
        return cards
    except Exception as e:
        log(f"      extract_cards_meli error: {e}")
        return []

# ============================================================
# SCORE PAGINA ACTUAL (sin volver a navegar)
# ============================================================
def _score_current_page(page, retailer_name: str, cfg: Dict,
                        query: str, best_sel: Optional[str]) -> Dict:
    """
    Extrae y puntua cards de la pagina ya cargada.
    Usado para la primera query (despues del discovery).
    """
    is_meli = cfg["type"] == "meli"
    url = page.url

    result = {
        "Retailer": retailer_name, "Query": query, "URL": url,
        "Status": "OK", "Total_Cards": 0, "Nike_Cards": 0,
        "Nike_Share": None, "Nike_Positions": "",
        "Best_Selector": best_sel or "link-fallback", "Error": "",
    }

    try:
        if is_meli:
            cards = extract_cards_meli(page, TOP_N)
        else:
            cards = extract_cards_generic(page, best_sel, TOP_N)

        total = len(cards)
        nike_cards = [c for c in cards if "nike" in c.get("combined", "")]
        nike_count = len(nike_cards)
        nike_pos   = ",".join(str(c["position"]) for c in nike_cards)

        result["Total_Cards"]    = total
        result["Nike_Cards"]     = nike_count
        result["Nike_Share"]     = round(nike_count / total, 4) if total > 0 else None
        result["Nike_Positions"] = nike_pos

        log(f"      {total} cards | Nike: {nike_count}/{total} | Pos: [{nike_pos}]")
    except Exception as e:
        result["Status"] = "ERROR"
        result["Error"]  = str(e)[:120]
        log(f"      ERROR en _score_current_page: {e}")

    return result

# ============================================================
# EXTRACT AND SCORE (navega + extrae)
# ============================================================
def _extract_and_score(page, retailer_name: str, cfg: Dict,
                       query: str, best_sel: Optional[str]) -> Dict:
    """Navega a la query y extrae/puntua cards."""
    is_meli = cfg["type"] == "meli"

    result = {
        "Retailer": retailer_name, "Query": query, "URL": "",
        "Status": "OK", "Total_Cards": 0, "Nike_Cards": 0,
        "Nike_Share": None, "Nike_Positions": "",
        "Best_Selector": best_sel or "link-fallback", "Error": "",
    }

    try:
        url = do_search(page, retailer_name, cfg, query)
        result["URL"] = url
        log(f"      URL: {url[:80]}")

        scroll_page(page)
        human_pause(0.8, 1.2)

        if is_meli:
            cards = extract_cards_meli(page, TOP_N)
        else:
            cards = extract_cards_generic(page, best_sel, TOP_N)

        # Scroll extra si 0 cards
        if len(cards) == 0:
            log(f"      0 cards - scroll extra...")
            scroll_page(page, rounds=4)
            human_pause(1.0, 1.5)
            if is_meli:
                cards = extract_cards_meli(page, TOP_N)
            else:
                cards = extract_cards_generic(page, best_sel, TOP_N)

        total = len(cards)
        nike_cards = [c for c in cards if "nike" in c.get("combined", "")]
        nike_count = len(nike_cards)
        nike_pos   = ",".join(str(c["position"]) for c in nike_cards)

        result["Total_Cards"]    = total
        result["Nike_Cards"]     = nike_count
        result["Nike_Share"]     = round(nike_count / total, 4) if total > 0 else None
        result["Nike_Positions"] = nike_pos

        # Log marcas detectadas
        brand_hits: Dict[str, int] = {}
        for card in cards:
            combined = card.get("combined", "")
            for b in KNOWN_BRANDS:
                if b in combined:
                    brand_hits[b] = brand_hits.get(b, 0) + 1
        brands_str = " | ".join(f"{b}:{n}" for b, n in
                                sorted(brand_hits.items(), key=lambda x: -x[1])[:6])

        log(f"      {total} cards | Nike: {nike_count}/{total} | Pos: [{nike_pos}]")
        if brands_str:
            log(f"         marcas: {brands_str}")

    except PWTimeout:
        result["Status"] = "TIMEOUT"
        result["Error"]  = "Timeout"
        log(f"      TIMEOUT")
    except Exception as e:
        result["Status"] = "ERROR"
        result["Error"]  = str(e)[:120]
        log(f"      ERROR: {str(e)[:80]}")

    return result

# ============================================================
# WORKER POR RETAILER
# ============================================================
def run_retailer_worker(retailer_name: str, retailer_cfg: Dict,
                        queries: List[str]) -> List[Dict]:
    is_meli    = retailer_cfg["type"] == "meli"
    proxy_type = retailer_cfg.get("proxy") or "directo"

    log(f"\n{'='*60}")
    log(f"[{retailer_name}] START — {len(queries)} busquedas | proxy: {proxy_type}")
    log(f"{'='*60}")

    rows: List[Dict] = []

    with sync_playwright() as pw:
        proxy_cfg = _get_proxy_for_retailer(retailer_cfg)
        browser, context, page = _make_browser(pw, proxy_cfg)

        # --- Warmup ---
        try:
            page.goto(retailer_cfg["base"], wait_until="domcontentloaded",
                      timeout=PAGE_WAIT_MS)
            human_pause(1.5, 2.5)
            if retailer_cfg.get("cp_modal"):
                dismiss_cp_modal(page)
            log(f"   [{retailer_name}] Warmup OK")
        except Exception as e:
            log(f"   [{retailer_name}] Warmup fallo: {str(e)[:60]}")

        # --- Descubrir selector (solo para no-MeLi) ---
        best_sel = None
        start_idx = 0

        if not is_meli:
            # Primera query: navegar + scroll + discovery
            log(f"\n   [{retailer_name}] [1/{len(queries)}] Discovery con '{queries[0]}'")
            try:
                do_search(page, retailer_name, retailer_cfg, queries[0])
                scroll_page(page)
                human_pause(0.5, 1.0)

                discovery = page.evaluate(_DISCOVER_JS)

                # Log discovery results
                for c in discovery.get("byKnown", [])[:4]:
                    log(f"      [known]  '{c['sel']}' -> {c['count']}")
                if discovery.get("byGtm"):
                    g = discovery["byGtm"]
                    log(f"      [gtm]    '{g['sel']}' -> {g['count']}")
                for c in discovery.get("byRepeat", [])[:2]:
                    log(f"      [repeat] '{c['sel']}' -> {c['count']}")

                best_sel = pick_best_selector(discovery)
                log(f"   [{retailer_name}] MEJOR SELECTOR: '{best_sel}'")

                # Puntuar query[0] (ya estamos en la pagina)
                row0 = _score_current_page(
                    page, retailer_name, retailer_cfg, queries[0], best_sel)
                rows.append(row0)
                start_idx = 1

            except Exception as e:
                log(f"   [{retailer_name}] Error en discovery: {e}")
                # Igual intentamos las queries desde 0
                start_idx = 0

        # --- Loop de queries ---
        consecutive_empty = 0
        reset_interval = 10 if is_meli else 12

        for i, query in enumerate(queries[start_idx:], start=start_idx):
            log(f"\n   [{retailer_name}] [{i+1}/{len(queries)}] '{query}'")

            # Re-discovery si 3 vacios consecutivos (solo no-MeLi)
            if not is_meli and consecutive_empty >= 3:
                log(f"   [{retailer_name}] Re-discovery tras {consecutive_empty} vacios")
                try:
                    discovery = page.evaluate(_DISCOVER_JS)
                    new_sel = pick_best_selector(discovery)
                    if new_sel:
                        best_sel = new_sel
                        log(f"   [{retailer_name}] Nuevo selector: '{best_sel}'")
                except Exception: pass
                consecutive_empty = 0

            row = _extract_and_score(page, retailer_name, retailer_cfg, query, best_sel)
            rows.append(row)

            if row["Total_Cards"] == 0:
                consecutive_empty += 1
            else:
                consecutive_empty = 0

            human_pause(1.5, 3.0)

            # Reset preventivo
            if (i + 1) % reset_interval == 0 and (i + 1) < len(queries):
                log(f"   [{retailer_name}] Reset preventivo ({i+1}/{len(queries)})")
                browser, context, page = _rotate_browser(
                    pw, browser, context, page, retailer_cfg)
                # Warmup del nuevo browser
                try:
                    page.goto(retailer_cfg["base"], wait_until="domcontentloaded",
                              timeout=PAGE_WAIT_MS)
                    human_pause(1.5, 2.0)
                    if retailer_cfg.get("cp_modal"):
                        dismiss_cp_modal(page)
                except Exception: pass

        try: page.close(); context.close(); browser.close()
        except Exception: pass

    # Resumen del worker
    ok         = sum(1 for r in rows if r["Status"] == "OK")
    tot_nike   = sum(r["Nike_Cards"]  for r in rows)
    tot_cards  = sum(r["Total_Cards"] for r in rows)
    avg_share  = round(tot_nike / tot_cards, 3) if tot_cards > 0 else 0
    log(f"\n   [{retailer_name}] DONE {ok}/{len(queries)} OK | Nike share: {avg_share:.1%}")
    return rows

# ============================================================
# EXCEL OUTPUT
# ============================================================
def write_outputs(all_rows: List[Dict]):
    log(f"\nEscribiendo outputs...")

    # CSV
    df_raw = pd.DataFrame(all_rows)
    for c in COLS_RAW:
        if c not in df_raw.columns:
            df_raw[c] = None
    df_raw[COLS_RAW].to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    log(f"   CSV: {OUT_CSV}")

    # Excel
    wb = xlsxwriter.Workbook(OUT_XLSX)

    hdr_fmt  = wb.add_format({"bold": True, "font_color": "white", "bg_color": "#1F4E79",
                               "align": "center", "valign": "vcenter", "border": 1, "font_size": 10})
    txt_fmt  = wb.add_format({"align": "left", "valign": "vcenter", "font_size": 9})
    pct_fmt  = wb.add_format({"num_format": "0%", "align": "center", "valign": "vcenter", "font_size": 9})
    num_fmt  = wb.add_format({"align": "center", "valign": "vcenter", "font_size": 9})
    url_fmt  = wb.add_format({"font_color": "blue", "underline": 1, "font_size": 9})
    gray_fmt = wb.add_format({"bg_color": "#D9D9D9", "align": "center", "font_size": 9,
                               "num_format": "0%"})
    sub_hdr  = wb.add_format({"bold": True, "bg_color": "#BDD7EE", "align": "center",
                               "valign": "vcenter", "border": 1, "font_size": 10})
    zebra    = wb.add_format({"bg_color": "#F2F2F2", "font_size": 9})

    def _share_fmt(share):
        if share is None:
            return wb.add_format({"bg_color": "#D9D9D9", "align": "center",
                                   "font_size": 9, "num_format": "0%"})
        r = int(255 * (1 - share))
        g = int(200 * share + 55)
        b = 80
        hex_c = f"#{r:02X}{g:02X}{b:02X}"
        font_c = "white" if share < 0.3 or share > 0.85 else "black"
        return wb.add_format({"bg_color": hex_c, "font_color": font_c,
                               "align": "center", "valign": "vcenter", "bold": True,
                               "num_format": "0%", "font_size": 9})

    share_fmts = {i: _share_fmt(i / 10) for i in range(11)}

    def get_share_fmt(share):
        if share is None:
            return gray_fmt
        bucket = min(10, int(share * 10 + 0.5))
        return share_fmts[bucket]

    retailer_names = list(RETAILERS.keys())

    # --- HOJA 1: HEATMAP SHARE ---
    ws1 = wb.add_worksheet("Heatmap Nike Share")
    ws1.freeze_panes(1, 1)
    ws1.set_zoom(90)
    ws1.write(0, 0, "Busqueda", hdr_fmt)
    for j, ret in enumerate(retailer_names):
        ws1.write(0, j + 1, ret, hdr_fmt)
        ws1.set_column(j + 1, j + 1, 14)
    ws1.set_column(0, 0, 30)
    ws1.set_row(0, 22)

    pivot: Dict[str, Dict[str, Optional[float]]] = {}
    for row in all_rows:
        q   = row["Query"]
        ret = row["Retailer"]
        if q not in pivot: pivot[q] = {}
        pivot[q][ret] = row.get("Nike_Share")

    for i, query in enumerate(SEARCH_QUERIES, start=1):
        ws1.write(i, 0, query, txt_fmt)
        ws1.set_row(i, 16)
        for j, ret in enumerate(retailer_names):
            share = pivot.get(query, {}).get(ret)
            fmt   = get_share_fmt(share)
            if share is not None:
                ws1.write_number(i, j + 1, share, fmt)
            else:
                ws1.write(i, j + 1, "N/D", gray_fmt)

    avg_row = len(SEARCH_QUERIES) + 2
    ws1.write(avg_row, 0, "PROMEDIO", sub_hdr)
    ws1.set_row(avg_row, 18)
    for j, ret in enumerate(retailer_names):
        shares = [pivot.get(q, {}).get(ret) for q in SEARCH_QUERIES
                  if pivot.get(q, {}).get(ret) is not None]
        avg = sum(shares) / len(shares) if shares else None
        fmt = get_share_fmt(avg)
        if avg is not None: ws1.write_number(avg_row, j + 1, avg, fmt)
        else: ws1.write(avg_row, j + 1, "N/D", gray_fmt)

    tot_row = avg_row + 1
    ws1.write(tot_row, 0, "TOTAL ABSOLUTO", sub_hdr)
    ws1.set_row(tot_row, 18)
    for j, ret in enumerate(retailer_names):
        ret_rows = [r for r in all_rows if r["Retailer"] == ret]
        tn = sum(r["Total_Cards"] for r in ret_rows)
        tk = sum(r["Nike_Cards"]  for r in ret_rows)
        share = tk / tn if tn > 0 else None
        fmt = get_share_fmt(share)
        if share is not None: ws1.write_number(tot_row, j + 1, share, fmt)
        else: ws1.write(tot_row, j + 1, "N/D", gray_fmt)

    # --- HOJA 2: HEATMAP CARDS ABSOLUTOS ---
    ws2 = wb.add_worksheet("Heatmap Nike Cards")
    ws2.freeze_panes(1, 1)
    ws2.set_zoom(90)
    ws2.write(0, 0, "Busqueda", hdr_fmt)
    for j, ret in enumerate(retailer_names):
        ws2.write(0, j + 1, ret, hdr_fmt)
        ws2.set_column(j + 1, j + 1, 12)
    ws2.set_column(0, 0, 30)

    pivot2: Dict[str, Dict[str, Optional[int]]] = {}
    for row in all_rows:
        q   = row["Query"]
        ret = row["Retailer"]
        if q not in pivot2: pivot2[q] = {}
        pivot2[q][ret] = row.get("Nike_Cards")

    for i, query in enumerate(SEARCH_QUERIES, start=1):
        ws2.write(i, 0, query, txt_fmt)
        ws2.set_row(i, 16)
        for j, ret in enumerate(retailer_names):
            val = pivot2.get(query, {}).get(ret)
            if val is not None:
                share = val / TOP_N
                fmt = get_share_fmt(share)
                ws2.write_number(i, j + 1, val, fmt)
            else:
                ws2.write(i, j + 1, "N/D", gray_fmt)

    # --- HOJA 3: RESUMEN POR RETAILER ---
    ws3 = wb.add_worksheet("Resumen Retailers")
    ws3.freeze_panes(1, 0)
    ws3.set_zoom(90)
    cols3 = ["Retailer", "OK", "Total Cards", "Total Nike",
             "Share Promedio", "Share Total", "Mejor Query", "Peor Query"]
    for j, c in enumerate(cols3):
        ws3.write(0, j, c, hdr_fmt)
    ws3.set_row(0, 22)
    for j, w in enumerate([16, 8, 14, 12, 16, 14, 32, 32]):
        ws3.set_column(j, j, w)

    for i, ret in enumerate(retailer_names, start=1):
        rr = [r for r in all_rows if r["Retailer"] == ret and r["Status"] == "OK"]
        ok       = len(rr)
        tot_c    = sum(r["Total_Cards"] for r in rr)
        tot_n    = sum(r["Nike_Cards"]  for r in rr)
        shares   = [r["Nike_Share"] for r in rr if r["Nike_Share"] is not None]
        avg_s    = sum(shares) / len(shares) if shares else None
        abs_s    = tot_n / tot_c if tot_c > 0 else None
        top_q    = max(rr, key=lambda r: r.get("Nike_Share") or 0) if rr else None
        worst_q  = min(rr, key=lambda r: r.get("Nike_Share") if r.get("Nike_Share") is not None else 999) if rr else None
        ws3.write(i, 0, ret, txt_fmt)
        ws3.write_number(i, 1, ok, num_fmt)
        ws3.write_number(i, 2, tot_c, num_fmt)
        ws3.write_number(i, 3, tot_n, num_fmt)
        if avg_s is not None: ws3.write_number(i, 4, avg_s, get_share_fmt(avg_s))
        else: ws3.write(i, 4, "N/D", gray_fmt)
        if abs_s is not None: ws3.write_number(i, 5, abs_s, get_share_fmt(abs_s))
        else: ws3.write(i, 5, "N/D", gray_fmt)
        ws3.write(i, 6, top_q["Query"] if top_q else "", txt_fmt)
        ws3.write(i, 7, worst_q["Query"] if worst_q else "", txt_fmt)
        ws3.set_row(i, 18)

    # --- HOJA 4: RAW DATA ---
    ws4 = wb.add_worksheet("Raw Data")
    ws4.freeze_panes(1, 0)
    ws4.autofilter(0, 0, 0, len(COLS_RAW) - 1)
    for j, c in enumerate(COLS_RAW):
        ws4.write(0, j, c, hdr_fmt)
    for j, w in enumerate([14, 28, 12, 12, 12, 20, 30, 10, 60, 40]):
        ws4.set_column(j, j, w)
    ws4.set_row(0, 22)

    for i, row in enumerate(all_rows, start=1):
        ws4.set_row(i, 16)
        base_fmt = zebra if i % 2 == 0 else txt_fmt
        ws4.write(i, 0, row.get("Retailer", ""),      base_fmt)
        ws4.write(i, 1, row.get("Query", ""),          base_fmt)
        ws4.write_number(i, 2, row.get("Total_Cards", 0) or 0, num_fmt)
        ws4.write_number(i, 3, row.get("Nike_Cards",  0) or 0, num_fmt)
        share = row.get("Nike_Share")
        if share is not None: ws4.write_number(i, 4, share, pct_fmt)
        else: ws4.write(i, 4, "N/D", gray_fmt)
        ws4.write(i, 5, row.get("Nike_Positions", ""), base_fmt)
        ws4.write(i, 6, row.get("Best_Selector", ""),  base_fmt)
        ws4.write(i, 7, row.get("Status", ""),          base_fmt)
        url = row.get("URL", "")
        if url: ws4.write_url(i, 8, url, url_fmt, string=url[:60])
        ws4.write(i, 9, row.get("Error", ""),           base_fmt)

    wb.close()
    log(f"   Excel: {OUT_XLSX}")

# ============================================================
# MAIN
# ============================================================
def main():
    start = time.time()
    log("=" * 65)
    log("RETAIL MEDIA SEARCH v6 — Visibilidad Nike")
    log(f"   {len(SEARCH_QUERIES)} busquedas x {len(RETAILERS)} retailers")
    log(f"   Top {TOP_N} cards | {WORKERS} workers en paralelo")
    log("=" * 65)

    retailer_items = list(RETAILERS.items())
    all_rows: List[Dict] = []
    lock = threading.Lock()

    def worker_task(args):
        name, cfg = args
        return run_retailer_worker(name, cfg, SEARCH_QUERIES)

    log(f"\nLanzando {len(retailer_items)} retailers con {WORKERS} workers...")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(worker_task, item): item[0]
                   for item in retailer_items}
        for future in as_completed(futures):
            name = futures[future]
            try:
                rows = future.result()
                with lock:
                    all_rows.extend(rows)
                log(f"   OK {name} ({len(rows)} filas)")
            except Exception as e:
                log(f"   FATAL {name}: {e}")
                import traceback; traceback.print_exc()

    # Ordenar para output consistente
    order       = {n: i for i, n in enumerate(RETAILERS.keys())}
    query_order = {q: i for i, q in enumerate(SEARCH_QUERIES)}
    all_rows.sort(key=lambda r: (
        order.get(r.get("Retailer", ""), 99),
        query_order.get(r.get("Query", ""), 99),
    ))

    write_outputs(all_rows)

    elapsed = time.time() - start
    log("\n" + "=" * 65)
    log(f"Completado en {elapsed/60:.1f} min")
    log(f"\n{'Retailer':<15} {'OK':>4}  {'Avg Share':>10}  {'Abs Share':>10}")
    log(f"{'-'*45}")
    for ret in RETAILERS:
        rr     = [r for r in all_rows if r["Retailer"] == ret and r["Status"] == "OK"]
        ok     = len(rr)
        shares = [r["Nike_Share"] for r in rr if r["Nike_Share"] is not None]
        avg    = sum(shares) / len(shares) if shares else 0
        tot_k  = sum(r["Nike_Cards"]  for r in rr)
        tot_c  = sum(r["Total_Cards"] for r in rr)
        abs_s  = tot_k / tot_c if tot_c > 0 else 0
        log(f"  {ret:<15} {ok:>4}  {avg:>10.1%}  {abs_s:>10.1%}")
    log(f"\n   {OUT_XLSX}")
    log(f"   {OUT_CSV}")
    log("=" * 65)


if __name__ == "__main__":
    main()
