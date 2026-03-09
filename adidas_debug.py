# ============================================================
# ADIDAS DEBUG v2 — AR + US
# Objetivo: entrar a la PLP de cada franquicia en AR y US
#           y extraer 1 precio full por franquicia
#
# URL patterns:
#   AR: adidas.com.ar/zapatillas-{slug}  |  adidas.com.ar/{slug}
#   US: adidas.com/us/{slug}-shoes       |  adidas.com/us/{slug}-running-shoes
# ============================================================

import asyncio
import json
import random
import re
import time
import datetime as dt
from urllib.parse import quote
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
import nest_asyncio
nest_asyncio.apply()

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
# AR proxy — gate.decodo.com
DECODO_USER  = "spyrndvq0x"
DECODO_PASS  = "8eOzLZZj3i3b=mcoc8"
AR_HOST      = "gate.decodo.com"       # o ar.decodo.com
AR_PORTS     = list(range(10001, 10011))
US_HOST      = "us.decodo.com"
US_PORTS     = list(range(10001, 10011))

# Site Unblocker — para US (maneja anti-bot automáticamente)
UNBLOCKER_HOST = "unblock.decodo.com"
UNBLOCKER_PORT = 60000
UNBLOCKER_USER = "U0000358219"
UNBLOCKER_PASS = "PW_13c62a6853fe2bb0a6b377b55a4e8d8ec"

HEADLESS = False
BASE_AR  = "https://www.adidas.com.ar"
BASE_US  = "https://www.adidas.com/us"

# ── Mapa de franquicias → URLs a intentar (en orden) ──────────
# Aprendizajes del debug:
#   - "zapatillas-{slug}" funciona para muchas franquicias
#   - Para sub-franquicias: "{cat}-{subfranquicia}" (ej: adizero-adizero_boston)
#   - Fallback por categoría amplia: zapatillas-running, zapatillas, botines
#   - ADIOS PRO: tiene pocos productos, necesitar wait más largo (25s)
FRANCHISE_URLS = {
    # Running ──────────────────────────────────────────────────
    "ADIZERO ADIOS PRO": [
        f"{BASE_AR}/adizero-adios_pro",           # correcto según sitemap
        f"{BASE_AR}/zapatillas-adizero",           # adizero amplio como fallback
        f"{BASE_AR}/zapatillas-running",
    ],
    "ULTRABOOST": [
        f"{BASE_AR}/zapatillas-ultraboost",        # ✅ confirmado
        f"{BASE_AR}/ultraboost",
        f"{BASE_AR}/zapatillas-running",
    ],
    "ADIZERO BOSTON": [
        f"{BASE_AR}/adizero-adizero_boston",       # ✅ confirmado
        f"{BASE_AR}/zapatillas-adizero_boston",
        f"{BASE_AR}/zapatillas-running",
    ],
    "ADISTAR": [
        f"{BASE_AR}/zapatillas-adistar",
        f"{BASE_AR}/adistar",
        f"{BASE_AR}/zapatillas-running",
    ],
    "PUREBOOST": [
        f"{BASE_AR}/zapatillas-pureboost",
        f"{BASE_AR}/pureboost",
        f"{BASE_AR}/zapatillas-running",
    ],
    'SUPERNOVA "STRIDE"': [
        f"{BASE_AR}/zapatillas-supernova",
        f"{BASE_AR}/supernova",
        f"{BASE_AR}/zapatillas-running",
    ],
    "SUPERNOVA STRIDE": [
        f"{BASE_AR}/zapatillas-supernova",
        f"{BASE_AR}/supernova",
        f"{BASE_AR}/zapatillas-running",
    ],
    "QUESTAR": [
        f"{BASE_AR}/zapatillas-questar",
        f"{BASE_AR}/questar",
        f"{BASE_AR}/zapatillas-running",
    ],
    "RESPONSE": [
        f"{BASE_AR}/zapatillas-response",
        f"{BASE_AR}/zapatillas-running",
    ],
    "DURAMO SL": [
        f"{BASE_AR}/zapatillas-duramo_sl",
        f"{BASE_AR}/zapatillas-duramo",
        f"{BASE_AR}/zapatillas-running",
    ],
    "RUNFALCON": [
        f"{BASE_AR}/zapatillas-runfalcon",         # ✅ confirmado via fallback+filtro
        f"{BASE_AR}/zapatillas-running",
    ],
    # Sportswear ───────────────────────────────────────────────
    "EQ AGRAVIC": [
        f"{BASE_AR}/zapatillas-terrex",            # EQ Agravic es trail, suele estar en Terrex
        f"{BASE_AR}/terrex",
        f"{BASE_AR}/zapatillas-eq",
        f"{BASE_AR}/zapatillas",
    ],
    'SAMBA "DECON"': [
        f"{BASE_AR}/samba",                        # ✅ /samba funciona
        f"{BASE_AR}/zapatillas-samba",
        f"{BASE_AR}/zapatillas",
    ],
    "SAMBA DECON": [
        f"{BASE_AR}/samba",
        f"{BASE_AR}/zapatillas-samba",
        f"{BASE_AR}/zapatillas",
    ],
    "SAMBA": [
        f"{BASE_AR}/samba",                        # ✅ confirmado
        f"{BASE_AR}/zapatillas-samba",
        f"{BASE_AR}/zapatillas",
    ],
    "FORUM LOW": [
        f"{BASE_AR}/zapatillas-forum",             # ✅ confirmado
        f"{BASE_AR}/forum",
        f"{BASE_AR}/zapatillas",
    ],
    "CAMPUS": [
        f"{BASE_AR}/zapatillas-campus",            # ✅ confirmado
        f"{BASE_AR}/campus",
        f"{BASE_AR}/zapatillas",
    ],
    'RIVALRY "LOW"': [
        f"{BASE_AR}/zapatillas-rivalry",
        f"{BASE_AR}/zapatillas",
    ],
    "RIVALRY LOW": [
        f"{BASE_AR}/zapatillas-rivalry",
        f"{BASE_AR}/zapatillas",
    ],
    "VL COURT 3.0": [
        f"{BASE_AR}/zapatillas-vl_court",
        f"{BASE_AR}/zapatillas-grand_court",       # fallback court
        f"{BASE_AR}/zapatillas",
    ],
    "GRAND COURT": [
        f"{BASE_AR}/zapatillas-grand_court",
        f"{BASE_AR}/zapatillas",
    ],
    "COURT BASE": [
        f"{BASE_AR}/zapatillas-court_base",
        f"{BASE_AR}/zapatillas-grand_court",
        f"{BASE_AR}/zapatillas",
    ],
    # Fútbol ───────────────────────────────────────────────────
    "SPEEDPORTAL": [
        f"{BASE_AR}/botines-x_speedportal",
        f"{BASE_AR}/x_speedportal",
        f"{BASE_AR}/botines-predator",
        f"{BASE_AR}/botines",
    ],
    "CLUB": [
        f"{BASE_AR}/botines-copa_pure",
        f"{BASE_AR}/botines-copa",
        f"{BASE_AR}/botines",
    ],
    "LEAGUE": [
        f"{BASE_AR}/botines-copa",
        f"{BASE_AR}/botines",
    ],
    "PRO": [
        f"{BASE_AR}/botines-predator",
        f"{BASE_AR}/botines",
    ],
    "ELITE": [
        f"{BASE_AR}/botines-predator",
        f"{BASE_AR}/botines",
    ],
}

# Categoría fallback AR por tipo
CATEGORY_FALLBACK_AR = {
    "running":    f"{BASE_AR}/zapatillas-running",
    "sportswear": f"{BASE_AR}/zapatillas",
    "futbol":     f"{BASE_AR}/botines",
}

# ── US: adidas.com/us — patrón {slug}-shoes / {slug}-running-shoes ──
# Confirmado en Google: adidas.com/us/adizero_boston-shoes
#                       adidas.com/us/ultraboost-shoes
#                       adidas.com/us/samba-shoes
FRANCHISE_URLS_US = {
    # Running
    "ADIZERO ADIOS PRO": [
        f"{BASE_US}/adizero_adios_pro-shoes",
        f"{BASE_US}/adizero_adios_pro-running-shoes",
        f"{BASE_US}/adizero-running-shoes",
    ],
    "ULTRABOOST": [
        f"{BASE_US}/ultraboost-shoes",
        f"{BASE_US}/ultraboost-running-shoes",
    ],
    "ADIZERO BOSTON": [
        f"{BASE_US}/adizero_boston-shoes",
        f"{BASE_US}/adizero_boston-running-shoes",
        f"{BASE_US}/adizero-adizero_boston-running-shoes",
    ],
    'ADIZERO "BOSTON"': [
        f"{BASE_US}/adizero_boston-shoes",
        f"{BASE_US}/adizero_boston-running-shoes",
    ],
    "ADISTAR": [
        f"{BASE_US}/adistar-shoes",
        f"{BASE_US}/adistar-running-shoes",
    ],
    "PUREBOOST": [
        f"{BASE_US}/pureboost-shoes",
        f"{BASE_US}/pureboost-running-shoes",
    ],
    "SUPERNOVA STRIDE": [
        f"{BASE_US}/supernova_stride-shoes",
        f"{BASE_US}/supernova-shoes",
        f"{BASE_US}/supernova-running-shoes",
    ],
    'SUPERNOVA "STRIDE"': [
        f"{BASE_US}/supernova_stride-shoes",
        f"{BASE_US}/supernova-shoes",
        f"{BASE_US}/supernova-running-shoes",
    ],
    "QUESTAR": [
        f"{BASE_US}/questar-shoes",
        f"{BASE_US}/questar-running-shoes",
    ],
    "RESPONSE": [
        f"{BASE_US}/response-shoes",
        f"{BASE_US}/response-running-shoes",
    ],
    "DURAMO SL": [
        f"{BASE_US}/duramo_sl-shoes",
        f"{BASE_US}/duramo-shoes",
        f"{BASE_US}/duramo-running-shoes",
    ],
    "RUNFALCON": [
        f"{BASE_US}/runfalcon-shoes",
        f"{BASE_US}/runfalcon-running-shoes",
    ],
    # Sportswear
    "EQ AGRAVIC": [
        f"{BASE_US}/terrex_agravic-shoes",
        f"{BASE_US}/terrex-trail-running-shoes",
    ],
    "SAMBA": [
        f"{BASE_US}/samba-shoes",
        f"{BASE_US}/samba-originals-shoes",
    ],
    'SAMBA "DECON"': [
        f"{BASE_US}/samba_decon-shoes",
        f"{BASE_US}/samba-shoes",
    ],
    "FORUM LOW": [
        f"{BASE_US}/forum_low-shoes",
        f"{BASE_US}/forum-shoes",
    ],
    "CAMPUS": [
        f"{BASE_US}/campus-shoes",
        f"{BASE_US}/campus_00s-shoes",
    ],
    "RIVALRY LOW": [
        f"{BASE_US}/rivalry_low-shoes",
        f"{BASE_US}/rivalry-shoes",
    ],
    'RIVALRY "LOW"': [
        f"{BASE_US}/rivalry_low-shoes",
        f"{BASE_US}/rivalry-shoes",
    ],
    "VL COURT 3.0": [
        f"{BASE_US}/vl_court-shoes",
        f"{BASE_US}/vl_court_3.0-shoes",
        f"{BASE_US}/grand_court-shoes",
    ],
    "GRAND COURT": [
        f"{BASE_US}/grand_court-shoes",
    ],
    "COURT BASE": [
        f"{BASE_US}/grand_court_base-shoes",
        f"{BASE_US}/grand_court-shoes",
    ],
    # Fútbol
    "SPEEDPORTAL": [
        f"{BASE_US}/x_speedportal-soccer-shoes",
        f"{BASE_US}/x_speedportal-shoes",
        f"{BASE_US}/soccer-shoes",
    ],
    "CLUB": [
        f"{BASE_US}/copa_pure-soccer-shoes",
        f"{BASE_US}/copa-soccer-shoes",
        f"{BASE_US}/soccer-shoes",
    ],
    "LEAGUE": [
        f"{BASE_US}/copa_pure-soccer-shoes",
        f"{BASE_US}/copa-soccer-shoes",
        f"{BASE_US}/soccer-shoes",
    ],
    "PRO": [
        f"{BASE_US}/predator-soccer-shoes",
        f"{BASE_US}/soccer-shoes",
    ],
    "ELITE": [
        f"{BASE_US}/predator-soccer-shoes",
        f"{BASE_US}/soccer-shoes",
    ],
}

# Fallback US por categoría
CATEGORY_FALLBACK_US = {
    "running":    f"{BASE_US}/running-shoes",
    "sportswear": f"{BASE_US}/originals-shoes",
    "futbol":     f"{BASE_US}/soccer-shoes",
}

USER_AGENTS_EN = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined, configurable: true });
    Object.defineProperty(navigator, 'plugins', { get: () => { const p = [{name:'Chrome PDF Plugin'},{name:'Chrome PDF Viewer'},{name:'Native Client'}]; p.refresh=()=>{}; p.item=i=>p[i]; p.namedItem=n=>p.find(x=>x.name===n); Object.defineProperty(p,'length',{get:()=>p.length}); return p; }, configurable: true });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en','es'], configurable: true });
    if (!window.chrome) { window.chrome = { app:{}, csi:()=>{}, loadTimes:()=>({}), runtime:{connect:()=>{},sendMessage:()=>{}} }; }
    const getParam = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(p) {
        if (p===37445) return 'Intel Inc.';
        if (p===37446) return 'Intel Iris OpenGL Engine';
        return getParam.call(this,p);
    };
    ['__playwright','__pwInitScripts','__pw_manual'].forEach(k=>{ try{delete window[k];}catch(e){} });
}
"""

# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────
def log(msg):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

async def pause(a=1.0, b=2.5):
    await asyncio.sleep(random.uniform(a, b))


# ─────────────────────────────────────────────────────────────────
# BROWSER — acepta site "AR" o "US", con Site Unblocker para US
# ─────────────────────────────────────────────────────────────────
async def build_browser(pw, site: str = "AR", port: int = None, use_unblocker: bool = False):
    if site == "US":
        locale, tz = "en-US", "America/New_York"
        accept_lang = "en-US,en;q=0.9"
        if use_unblocker:
            # Site Unblocker — maneja anti-bot automáticamente
            proxy = {
                "server":   f"http://{UNBLOCKER_HOST}:{UNBLOCKER_PORT}",
                "username": UNBLOCKER_USER,
                "password": UNBLOCKER_PASS,
            }
            proxy_label = f"SiteUnblocker:{UNBLOCKER_PORT}"
        else:
            if port is None:
                port = US_PORTS[0]
            proxy = {"server": f"http://{US_HOST}:{port}", "username": DECODO_USER, "password": DECODO_PASS}
            proxy_label = f"{US_HOST}:{port}"
    else:
        locale, tz = "es-AR", "America/Argentina/Buenos_Aires"
        accept_lang = "es-AR,es;q=0.9,en-US;q=0.8"
        if port is None:
            port = AR_PORTS[0]
        proxy = {"server": f"http://{AR_HOST}:{port}", "username": DECODO_USER, "password": DECODO_PASS}
        proxy_label = f"{AR_HOST}:{port}"

    ua = random.choice(USER_AGENTS_EN)

    browser = await pw.chromium.launch(
        headless=HEADLESS,
        proxy=proxy,
        args=[
            "--ignore-certificate-errors",
            "--disable-blink-features=AutomationControlled",
            "--no-sandbox",
        ],
    )
    context = await browser.new_context(
        locale=locale,
        timezone_id=tz,
        user_agent=ua,
        viewport={"width": 1920, "height": 1080},
        ignore_https_errors=True,
        extra_http_headers={
            "Accept-Language": accept_lang,
            "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
    )
    await context.add_init_script(STEALTH_JS)

    async def _block(route, req):
        if req.resource_type in ("image", "media"):
            await route.abort()
        else:
            await route.continue_()
    await context.route("**/*", _block)

    page = await context.new_page()
    log(f"🌐 Browser {site} OK — proxy {proxy_label} | UA: {ua[:50]}...")
    return browser, context, page


# ─────────────────────────────────────────────────────────────────
# WARMUP — soporta AR y US
# ─────────────────────────────────────────────────────────────────
async def warmup(page, site: str = "AR") -> bool:
    """Entra a una página estática para establecer cookies."""
    # /us/help no carga JS completo (imagen confirmó nav vacío)
    # /us/shoes fuerza hidratación completa de Next.js
    warmup_url = f"{BASE_AR}/ayuda" if site == "AR" else f"{BASE_US}/shoes"
    # Site Unblocker necesita más tiempo para resolver el challenge inicial
    timeout = 40_000 if site == "AR" else 60_000
    try:
        log(f"   🔥 Warmup {site} → {warmup_url.split('.com')[1]}")
        await page.goto(warmup_url, wait_until="domcontentloaded", timeout=timeout)
        await pause(2, 4)
        for sel in [
            "[data-auto-id='cookie-consent-accept']",
            "button:has-text('Accept All')", "button:has-text('Accept')",
            "button:has-text('Aceptar')", "#onetrust-accept-btn-handler",
        ]:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0 and await btn.is_visible():
                    await btn.click()
                    await pause(0.5, 1.0)
                    break
            except Exception:
                pass
        log(f"   ✅ Warmup {site} OK — {page.url.split('.com')[1][:50]}")
        return True
    except Exception as e:
        log(f"   ⚠️ Warmup {site} falló: {str(e)[:100]}")
        return False


# ─────────────────────────────────────────────────────────────────
# EXTRACCIÓN __NEXT_DATA__ con polling
# ─────────────────────────────────────────────────────────────────
async def wait_for_next_data(page, max_wait=15) -> List[dict]:
    """Polling cada 1.5s hasta que __NEXT_DATA__ tenga productos."""
    deadline = time.time() + max_wait
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            products = await page.evaluate("""
                () => {
                    try {
                        const s = document.getElementById('__NEXT_DATA__');
                        if (!s) return [];
                        const d = JSON.parse(s.textContent);

                        // Buscar en rutas conocidas
                        let items = (
                            d?.props?.pageProps?.products ||
                            d?.props?.pageProps?.searchResult?.products ||
                            d?.props?.pageProps?.plpData?.products ||
                            d?.props?.pageProps?.initialData?.searchResult?.products ||
                            []
                        );

                        // Si no, buscar recursivamente
                        if (!items.length) {
                            function find(obj, depth) {
                                if (depth > 7 || !obj || typeof obj !== 'object') return null;
                                if (Array.isArray(obj) && obj.length >= 1 &&
                                    typeof obj[0] === 'object' && obj[0]?.name &&
                                    (obj[0]?.url || obj[0]?.link || obj[0]?.productId)) return obj;
                                for (const v of Object.values(obj)) {
                                    const r = find(v, depth + 1);
                                    if (r) return r;
                                }
                                return null;
                            }
                            items = find(d, 0) || [];
                        }

                        if (!items.length) return [];

                        return items.map(p => {
                            let full = 0, final = 0;
                            // Múltiples estructuras de precio según site/versión
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
                                } else {
                                    full = final = parseFloat(p.price) || 0;
                                }
                            } else if (p.pricing) {
                                full  = parseFloat(p.pricing.standard || p.pricing.regular || 0);
                                final = parseFloat(p.pricing.sale || p.pricing.current || full);
                            }
                            // URL relativa → absoluta (AR o US)
                            let url = p.url || p.link || p.productLink || p.pdpUrl || '';
                            if (url && !url.startsWith('http')) {
                                const isUS = window.location.hostname === 'www.adidas.com';
                                url = (isUS ? 'https://www.adidas.com' : 'https://www.adidas.com.ar') + url;
                            }
                            return {
                                name:        p.title || p.name || p.displayName || p.productName || '',
                                url:         url,
                                full_price:  full,
                                final_price: final,
                                product_id:  p.id || p.productId || p.sku || p.articleNumber || '',
                            };
                        }).filter(p => p.name && (p.final_price > 0 || p.full_price > 0));
                    } catch(e) { return []; }
                }
            """)
            if products:
                log(f"   ✅ __NEXT_DATA__ OK — {len(products)} productos (intento {attempt})")
                return products
            log(f"   ⏳ __NEXT_DATA__ vacío (intento {attempt}), esperando 1.5s...")
        except Exception as e:
            log(f"   ⚠️ Error evaluate: {e}")
        await asyncio.sleep(1.5)
    log(f"   ❌ __NEXT_DATA__ nunca tuvo productos (timeout {max_wait}s)")
    return []


# ─────────────────────────────────────────────────────────────────
# US: esperar que los precios carguen en el DOM (lazy load)
# Los precios en US se cargan DESPUÉS del __NEXT_DATA__ via API interna
# ─────────────────────────────────────────────────────────────────
async def wait_for_us_prices(page, max_wait: int = 45) -> List[dict]:
    """
    Para adidas.com/us: espera que aparezcan los price elements en DOM.
    Los precios son lazy-loaded y no están en __NEXT_DATA__ al momento del commit.
    """
    log(f"   💲 [US] Esperando precios en DOM (hasta {max_wait}s)...")
    deadline = time.time() + max_wait
    attempt  = 0

    while time.time() < deadline:
        attempt += 1
        try:
            products = await page.evaluate("""
                () => {
                    // Selectores de precio US adidas.com
                    const priceSelectors = [
                        '[data-auto-id="product-card-price"]',
                        '[class*="price_large"]',
                        '[class*="ProductPrice"]',
                        '[data-testid="product-price"]',
                        'div[class*="price"]',
                        'span[class*="price"]',
                    ];
                    // Selectores de product card
                    const cardSelectors = [
                        '[data-auto-id="product-card"]',
                        '[data-testid="plp-product-card"]',
                        'article[class*="product"]',
                        '[class*="ProductCard"]',
                        'div[class*="product-card"]',
                    ];

                    let cards = [];
                    for (const sel of cardSelectors) {
                        cards = Array.from(document.querySelectorAll(sel));
                        if (cards.length >= 1) break;
                    }
                    if (!cards.length) return null; // sin cards todavía

                    const results = [];
                    let pricesFound = 0;

                    for (const card of cards.slice(0, 20)) {
                        // Nombre
                        const nameEl = card.querySelector('p[class*="title"], [data-auto-id="product-card-title"], h3, h2, [class*="title"]');
                        const name = nameEl ? nameEl.innerText.trim() : '';
                        if (!name) continue;

                        // Precio — múltiples estrategias
                        let price = 0, fullPrice = 0;
                        for (const psel of priceSelectors) {
                            const el = card.querySelector(psel);
                            if (el) {
                                const txt = el.innerText.replace(/[^0-9.]/g, '');
                                const p   = parseFloat(txt);
                                if (p > 0) { price = p; fullPrice = p; pricesFound++; break; }
                            }
                        }
                        // Si hay 2 precios (tachado + actual), buscar ambos
                        const allPriceEls = [];
                        for (const psel of priceSelectors) {
                            const els = Array.from(card.querySelectorAll(psel));
                            if (els.length) { allPriceEls.push(...els); break; }
                        }
                        if (allPriceEls.length >= 2) {
                            const prices = allPriceEls.map(e => parseFloat(e.innerText.replace(/[^0-9.]/g,''))).filter(Boolean);
                            if (prices.length >= 2) {
                                fullPrice = Math.max(...prices);
                                price     = Math.min(...prices);
                                pricesFound++;
                            }
                        }

                        // URL
                        const linkEl = card.querySelector('a[href]');
                        let url = linkEl ? linkEl.href : '';

                        results.push({ name, url, full_price: fullPrice, final_price: price });
                    }

                    // Solo retornar si al menos 1 precio encontrado
                    if (pricesFound === 0) return null;
                    return results.filter(p => p.name);
                }
            """)

            if products:
                log(f"   ✅ [US] DOM precios OK — {len(products)} productos (intento {attempt})")
                return products
            log(f"   ⏳ [US] sin precios en DOM (intento {attempt}), esperando 2s...")
        except Exception as e:
            log(f"   ⚠️ [US] error DOM: {str(e)[:80]}")
        await asyncio.sleep(2.0)

    log(f"   ❌ [US] Precios no cargaron en {max_wait}s")
    return []


async def extract_dom(page) -> List[dict]:
    try:
        products = await page.evaluate("""
            () => {
                const selectors = [
                    "[data-auto-id='product-card']",
                    "article.product-card",
                    "[data-testid='plp-product-card']",
                    "[class*='ProductCard']",
                    ".product-grid-item",
                ];
                let cards = [];
                for (const sel of selectors) {
                    cards = Array.from(document.querySelectorAll(sel));
                    if (cards.length) break;
                }
                return cards.slice(0, 20).map(card => {
                    const nameEl = card.querySelector("[class*='title'], [data-auto-id*='title'], h2, h3");
                    const priceEl = card.querySelector("[data-auto-id='product-price'], [class*='price'], [class*='Price']");
                    const linkEl  = card.querySelector('a[href]');
                    const name  = nameEl  ? nameEl.innerText.trim()  : '';
                    const ptxt  = priceEl ? priceEl.innerText.trim() : '';
                    const url   = linkEl  ? (linkEl.href.startsWith('http') ? linkEl.href : 'https://www.adidas.com.ar' + linkEl.getAttribute('href')) : '';
                    // Parse AR price
                    let price = 0;
                    const m = ptxt.replace(/[$\\u00a0\\s]/g,'').replace(/\\./g,'').replace(',','.');
                    price = parseFloat(m) || 0;
                    return { name, url, full_price: price, final_price: price, price_text: ptxt };
                }).filter(p => p.name);
            }
        """)
        if products:
            log(f"   ✅ DOM fallback — {len(products)} productos")
        return products or []
    except Exception as e:
        log(f"   ⚠️ DOM fallback error: {e}")
        return []


# ─────────────────────────────────────────────────────────────────
# SCORING simple
# ─────────────────────────────────────────────────────────────────
def pick_best(products: List[dict], franchise: str) -> Optional[dict]:
    tokens = re.sub(r'["\'\(\)]', '', franchise).lower().split()
    def score(p):
        name = (p.get("name") or "").lower()
        s = sum(2.0 if tok in name else 0.0 for tok in tokens if len(tok) > 2)
        if p.get("final_price", 0) > 0:
            s += 0.5
        return s
    scored = sorted(products, key=score, reverse=True)
    best = scored[0] if scored else None
    if best:
        currency = "USD" if best.get("currency") == "USD" else "ARS"
        log(f"   🎯 Best: '{best.get('name')}' | score={score(best):.1f} | {currency} {best.get('final_price'):,.0f}")
    return best


# ─────────────────────────────────────────────────────────────────
# SCRAPE UNA FRANQUICIA — soporta AR y US
# ─────────────────────────────────────────────────────────────────
async def scrape_franchise(page, franchise: str, categoria: str, site: str = "AR") -> Optional[dict]:
    """
    Intenta entrar a la PLP de la franquicia via árbol de URLs.
    Retorna el mejor producto encontrado (solo necesitamos 1 precio full).
    """
    clean = re.sub(r'["\']', '', franchise).strip().upper()

    if site == "US":
        url_map   = FRANCHISE_URLS_US
        fallbacks = CATEGORY_FALLBACK_US
        base      = BASE_US
        domain_check = "adidas.com"
        def url_suffix(u): return u.split('/us/')[1] if '/us/' in u else u.split('.com/')[1]
    else:
        url_map   = FRANCHISE_URLS
        fallbacks = CATEGORY_FALLBACK_AR
        base      = BASE_AR
        domain_check = "adidas.com.ar"
        def url_suffix(u): return u.split('.ar/')[1] if '.ar/' in u else u

    urls_to_try = url_map.get(franchise, url_map.get(clean, []))

    # Construir dinámicamente si no está en el mapa
    if not urls_to_try:
        slug = re.sub(r'["\']', '', franchise).lower().strip().replace(' ', '_')
        slug_g = slug.replace('_', '-')
        if site == "US":
            urls_to_try = [
                f"{base}/{slug}-shoes",
                f"{base}/{slug_g}-shoes",
                f"{base}/{slug_g}-running-shoes",
            ]
        else:
            urls_to_try = [
                f"{base}/zapatillas-{slug}",
                f"{base}/{slug_g}",
            ]
        fb = fallbacks.get(categoria.lower())
        if fb:
            urls_to_try.append(fb)

    log(f"   📋 [{site}] URLs ({len(urls_to_try)}): {[url_suffix(u) for u in urls_to_try]}")

    for url in urls_to_try:
        log(f"\n   🔗 [{site}] {url}")
        try:
            nav_timeout = 60_000 if site == "US" else 35_000
            await page.goto(url, wait_until="domcontentloaded", timeout=nav_timeout)
            await pause(1.5, 3.0) if site == "US" else await pause(1.0, 2.0)

            current_url = page.url
            if domain_check not in current_url:
                log(f"   ⚠️ Redirigió fuera: {current_url[:80]}")
                continue

            try:
                title = await page.title()
                content_short = await page.evaluate("document.body ? document.body.innerText.slice(0, 200) : ''")
                if "404" in title or "not found" in title.lower():
                    log(f"   ⚠️ 404")
                    continue
                if len(content_short.strip()) < 50:
                    log(f"   ⚠️ Página vacía")
                    continue
                log(f"   📄 '{title[:55]}' | {url_suffix(current_url)[:55]}")
            except Exception:
                pass

            # US via unblocker: precios cargan lazy en DOM, no en __NEXT_DATA__
            # AR: precios están en __NEXT_DATA__
            if site == "US":
                # Primero intentar __NEXT_DATA__ (puede que sí tenga precios)
                products = await wait_for_next_data(page, max_wait=20)
                if not products:
                    log("   🔄 [US] __NEXT_DATA__ sin precios → esperando DOM lazy...")
                    products = await wait_for_us_prices(page, max_wait=45)
            else:
                if any(x in url for x in ["adios_pro", "speedportal", "elite", "copa_pure"]):
                    wait_time = 25
                else:
                    wait_time = 15
                products = await wait_for_next_data(page, max_wait=wait_time)

            if not products:
                log("   🔄 DOM fallback...")
                products = await extract_dom(page)

            if products:
                # Marcar moneda
                for p in products:
                    p["currency"] = "USD" if site == "US" else "ARS"

                franchise_clean = re.sub(r'["\']', '', franchise)
                tokens = [t for t in franchise_clean.lower().split() if len(t) > 2]
                broad_endings = ["-shoes", "-running-shoes", "-soccer-shoes", "/zapatillas-running", "/zapatillas", "/botines"]
                is_broad = any(url.endswith(x) for x in broad_endings)
                slug_check = re.sub(r'["\']', '', franchise).lower().replace(' ', '_').replace(' ', '-')
                slug_in_url = slug_check.split('_')[0] in url or slug_check.split('-')[0] in url

                if is_broad or not slug_in_url:
                    filtered = [p for p in products if any(tok in (p.get("name") or "").lower() for tok in tokens)]
                    log(f"   🔍 Filtro '{franchise}': {len(filtered)}/{len(products)}")
                    if filtered:
                        return pick_best(filtered, franchise)
                    log(f"   ℹ️ Sin match — mejor de {len(products)}")
                    return pick_best(products, franchise)
                else:
                    return pick_best(products, franchise)

        except PWTimeoutError:
            log(f"   ⚠️ Timeout")
            continue
        except Exception as e:
            log(f"   ⚠️ Error: {str(e)[:100]}")
            continue

    return None


# ─────────────────────────────────────────────────────────────────
# MAIN DEBUG — AR + US en paralelo (2 browsers simultáneos)
# ─────────────────────────────────────────────────────────────────
DEBUG_FRANCHISES = [
    # Running (10)
    {"adidas": "ADIZERO ADIOS PRO",   "nike": "VAPORFLY",        "categoria": "Running"},
    {"adidas": "ULTRABOOST",          "nike": "PEGASUS PLUS",    "categoria": "Running"},
    {"adidas": 'ADIZERO "BOSTON"',    "nike": "ZOOM FLY",        "categoria": "Running"},
    {"adidas": "ADISTAR",             "nike": "VOMERO",          "categoria": "Running"},
    {"adidas": "PUREBOOST",           "nike": "PEGASUS",         "categoria": "Running"},
    {"adidas": 'SUPERNOVA "STRIDE"',  "nike": "WINFLO",          "categoria": "Running"},
    {"adidas": "QUESTAR",             "nike": "QUEST",           "categoria": "Running"},
    {"adidas": "RESPONSE",            "nike": "DOWNSHIFTER",     "categoria": "Running"},
    {"adidas": "DURAMO SL",           "nike": "REVOLUTION",      "categoria": "Running"},
    {"adidas": "RUNFALCON",           "nike": "DEFY",            "categoria": "Running"},
    # Sportswear (9)
    {"adidas": "EQ AGRAVIC",          "nike": "AIRMAX DN",       "categoria": "Sportswear"},
    {"adidas": 'SAMBA "DECON"',       "nike": "DUNK",            "categoria": "Sportswear"},
    {"adidas": "FORUM LOW",           "nike": "AIR FORCE 1",     "categoria": "Sportswear"},
    {"adidas": "SAMBA",               "nike": "KILLSHOT",        "categoria": "Sportswear"},
    {"adidas": "CAMPUS",              "nike": "CORTEZ",          "categoria": "Sportswear"},
    {"adidas": 'RIVALRY "LOW"',       "nike": "NIKE AIR MAX SC", "categoria": "Sportswear"},
    {"adidas": "VL COURT 3.0",        "nike": "COURT VISION",    "categoria": "Sportswear"},
    {"adidas": "GRAND COURT",         "nike": "COURT SHOT",      "categoria": "Sportswear"},
    {"adidas": "COURT BASE",          "nike": "CHARGE",          "categoria": "Sportswear"},
    # Fútbol (5)
    {"adidas": "SPEEDPORTAL",         "nike": "PREMIER",         "categoria": "FUTBOL"},
    {"adidas": "CLUB",                "nike": "ACADEMY",         "categoria": "FUTBOL"},
    {"adidas": "LEAGUE",              "nike": "CLUB",            "categoria": "FUTBOL"},
    {"adidas": "PRO",                 "nike": "PRO",             "categoria": "FUTBOL"},
    {"adidas": "ELITE",               "nike": "ELITE",           "categoria": "FUTBOL"},
]


async def run_site(pw, site: str, rows: list, results: list):
    """Corre todas las franquicias para un site (AR o US)."""
    use_unblocker = (site == "US")  # US siempre usa Site Unblocker
    browser, context, page = await build_browser(
        pw, site=site,
        port=AR_PORTS[0] if site == "AR" else US_PORTS[0],
        use_unblocker=use_unblocker,
    )

    if not await warmup(page, site=site):
        log(f"⚠️ Warmup {site} falló — reintentando...")
        await browser.close()
        browser, context, page = await build_browser(
            pw, site=site,
            port=AR_PORTS[1] if site == "AR" else US_PORTS[1],
            use_unblocker=use_unblocker,
        )
        await warmup(page, site=site)

    log(f"\n{'='*60}")
    log(f"🏁 Iniciando {len(rows)} franquicias — {site}")
    log(f"{'='*60}")

    for i, row in enumerate(rows):
        franchise = row["adidas"]
        nike      = row["nike"]
        categoria = row["categoria"]

        log(f"\n[{i+1}/{len(rows)}] [{site}] Nike: {nike} ↔ Adidas: {franchise} ({categoria})")
        result = await scrape_franchise(page, franchise, categoria, site=site)

        if result:
            price = result.get("final_price", 0)
            full  = result.get("full_price", 0)
            curr  = "USD" if site == "US" else "ARS"
            log(f"   ✅ {curr} {price:,.0f} (full: {full:,.0f}) | {result.get('name','')[:40]}")
            results.append({
                "site": site, "franchise": franchise, "nike": nike,
                "categoria": categoria, "status": "OK",
                "name": result.get("name",""),
                "url": result.get("url",""),
                "full_price": full, "final_price": price,
                "currency": curr,
            })
        else:
            log(f"   ❌ FALLO: {franchise}")
            results.append({"site": site, "franchise": franchise, "nike": nike,
                            "categoria": categoria, "status": "FAIL"})

        await asyncio.sleep(random.uniform(2.0, 3.5))

    await browser.close()


async def main():
    log("=" * 60)
    log("🧪 ADIDAS DEBUG v2 — AR + US por árbol de categorías")
    log("=" * 60)

    results_ar: list = []
    results_us: list = []

    async with async_playwright() as pw:
        # Ejecutar AR y US en paralelo
        await asyncio.gather(
            run_site(pw, "AR", DEBUG_FRANCHISES, results_ar),
            run_site(pw, "US", DEBUG_FRANCHISES, results_us),
        )

    all_results = results_ar + results_us

    # Resumen
    log("\n" + "="*60)
    log("📊 RESUMEN DEBUG")
    log("="*60)
    for site in ["AR", "US"]:
        rs = [r for r in all_results if r["site"] == site]
        ok   = sum(1 for r in rs if r["status"] == "OK")
        fail = sum(1 for r in rs if r["status"] == "FAIL")
        log(f"\n  [{site}] ✅ {ok}/{len(rs)}   ❌ {fail}/{len(rs)}")
        for r in rs:
            st    = "✅" if r["status"] == "OK" else "❌"
            curr  = r.get("currency", site)
            price = f"{curr} {r.get('final_price',0):>10,.0f}" if r["status"] == "OK" else f"{'—':>14}"
            name  = r.get("name", "—")[:38]
            log(f"    {st} {r['franchise']:25s} | {price} | {name}")

    with open("adidas_debug_results.json", "w", encoding="utf-8") as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    log("\n💾 adidas_debug_results.json")


if __name__ == "__main__":
    asyncio.run(main())
