# ============================================================
# ADIDAS DEBUG US — Site Unblocker
# Estrategia:
#   1. Primero valida con requests puro (sin browser) que el
#      unblocker llega a adidas.com/us y extrae __NEXT_DATA__
#   2. Si requests funciona → úsalo directamente (más rápido)
#   3. Si no → Playwright con SSL fix para unblocker
#
# Credenciales Site Unblocker Decodo:
#   unblock.decodo.com:60000  U0000358219  PW_13c62a6853...
# ============================================================

import asyncio
import json
import random
import re
import time
import datetime as dt
import requests
import urllib3
from typing import List, Optional

from playwright.async_api import async_playwright, TimeoutError as PWTimeoutError
import nest_asyncio
nest_asyncio.apply()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────
UNBLOCKER_HOST = "unblock.decodo.com"
UNBLOCKER_PORT = 60000
UNBLOCKER_USER = "U0000358219"
UNBLOCKER_PASS = "PW_13c62a6853fe2bb0a6b377b55a4e8d8ec"

PROXY_URL = f"http://{UNBLOCKER_USER}:{UNBLOCKER_PASS}@{UNBLOCKER_HOST}:{UNBLOCKER_PORT}"
PROXIES   = {"http": PROXY_URL, "https": PROXY_URL}

BASE_US   = "https://www.adidas.com/us"
HEADLESS  = False

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "sec-ch-ua":       '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile":"?0",
    "sec-ch-ua-platform": '"Windows"',
}

# ── URL map US — patrón confirmado en Google ──────────────────
FRANCHISE_URLS_US = {
    "ADIZERO ADIOS PRO":  [f"{BASE_US}/adizero_adios_pro-running-shoes"],
    "ULTRABOOST":         [f"{BASE_US}/ultraboost-shoes", f"{BASE_US}/ultraboost-running-shoes"],
    'ADIZERO "BOSTON"':   [f"{BASE_US}/adizero_boston-shoes", f"{BASE_US}/adizero_boston-running-shoes"],
    "ADISTAR":            [f"{BASE_US}/adistar-shoes", f"{BASE_US}/adistar-running-shoes"],
    "PUREBOOST":          [f"{BASE_US}/pureboost-shoes", f"{BASE_US}/pureboost-running-shoes"],
    'SUPERNOVA "STRIDE"': [f"{BASE_US}/supernova_stride-shoes", f"{BASE_US}/supernova-shoes"],
    "QUESTAR":            [f"{BASE_US}/questar-shoes"],
    "RESPONSE":           [f"{BASE_US}/response-shoes", f"{BASE_US}/response-running-shoes"],
    "DURAMO SL":          [f"{BASE_US}/duramo_sl-shoes", f"{BASE_US}/duramo-shoes"],
    "RUNFALCON":          [f"{BASE_US}/runfalcon-shoes"],
    "EQ AGRAVIC":         [f"{BASE_US}/terrex_agravic-shoes", f"{BASE_US}/terrex-trail-running-shoes"],
    'SAMBA "DECON"':      [f"{BASE_US}/samba_decon-shoes", f"{BASE_US}/samba-shoes"],
    "FORUM LOW":          [f"{BASE_US}/forum_low-shoes", f"{BASE_US}/forum-shoes"],
    "SAMBA":              [f"{BASE_US}/samba-shoes"],
    "CAMPUS":             [f"{BASE_US}/campus-shoes"],
    'RIVALRY "LOW"':      [f"{BASE_US}/rivalry_low-shoes", f"{BASE_US}/rivalry-shoes"],
    "VL COURT 3.0":       [f"{BASE_US}/vl_court-shoes"],
    "GRAND COURT":        [f"{BASE_US}/grand_court-shoes"],
    "COURT BASE":         [f"{BASE_US}/grand_court_base-shoes", f"{BASE_US}/grand_court-shoes"],
    "SPEEDPORTAL":        [f"{BASE_US}/x_speedportal-soccer-shoes", f"{BASE_US}/soccer-shoes"],
    "CLUB":               [f"{BASE_US}/copa_pure-soccer-shoes", f"{BASE_US}/soccer-shoes"],
    "LEAGUE":             [f"{BASE_US}/copa_pure-soccer-shoes", f"{BASE_US}/soccer-shoes"],
    "PRO":                [f"{BASE_US}/predator-soccer-shoes", f"{BASE_US}/soccer-shoes"],
    "ELITE":              [f"{BASE_US}/predator-soccer-shoes", f"{BASE_US}/soccer-shoes"],
}

DEBUG_FRANCHISES = [
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
    {"adidas": "EQ AGRAVIC",          "nike": "AIRMAX DN",       "categoria": "Sportswear"},
    {"adidas": 'SAMBA "DECON"',       "nike": "DUNK",            "categoria": "Sportswear"},
    {"adidas": "FORUM LOW",           "nike": "AIR FORCE 1",     "categoria": "Sportswear"},
    {"adidas": "SAMBA",               "nike": "KILLSHOT",        "categoria": "Sportswear"},
    {"adidas": "CAMPUS",              "nike": "CORTEZ",          "categoria": "Sportswear"},
    {"adidas": 'RIVALRY "LOW"',       "nike": "NIKE AIR MAX SC", "categoria": "Sportswear"},
    {"adidas": "VL COURT 3.0",        "nike": "COURT VISION",    "categoria": "Sportswear"},
    {"adidas": "GRAND COURT",         "nike": "COURT SHOT",      "categoria": "Sportswear"},
    {"adidas": "COURT BASE",          "nike": "CHARGE",          "categoria": "Sportswear"},
    {"adidas": "SPEEDPORTAL",         "nike": "PREMIER",         "categoria": "FUTBOL"},
    {"adidas": "CLUB",                "nike": "ACADEMY",         "categoria": "FUTBOL"},
    {"adidas": "LEAGUE",              "nike": "CLUB",            "categoria": "FUTBOL"},
    {"adidas": "PRO",                 "nike": "PRO",             "categoria": "FUTBOL"},
    {"adidas": "ELITE",               "nike": "ELITE",           "categoria": "FUTBOL"},
]

# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────
def log(msg):
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)

def parse_price_usd(text: str) -> float:
    """'$189.95' → 189.95"""
    clean = re.sub(r'[^0-9.]', '', text)
    try:
        return float(clean)
    except Exception:
        return 0.0

def pick_best(products: List[dict], franchise: str) -> Optional[dict]:
    tokens = re.sub(r'["\']', '', franchise).lower().split()
    def score(p):
        name = (p.get("name") or "").lower()
        s = sum(2.0 if t in name else 0.0 for t in tokens if len(t) > 2)
        if p.get("final_price", 0) > 0:
            s += 0.5
        return s
    scored = sorted(products, key=score, reverse=True)
    best = scored[0] if scored else None
    if best:
        log(f"   🎯 Best: '{best.get('name')}' | score={score(best):.1f} | USD {best.get('final_price'):,.2f}")
    return best


# ─────────────────────────────────────────────────────────────────
# ESTRATEGIA 1: requests puro con Site Unblocker
# ─────────────────────────────────────────────────────────────────
def extract_next_data_from_html(html: str) -> List[dict]:
    """Extrae productos del __NEXT_DATA__ JSON embebido en el HTML."""
    try:
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if not m:
            return []
        d = json.loads(m.group(1))

        items = (
            d.get("props", {}).get("pageProps", {}).get("products") or
            d.get("props", {}).get("pageProps", {}).get("searchResult", {}).get("products") or
            d.get("props", {}).get("pageProps", {}).get("plpData", {}).get("products") or
            []
        )

        # Búsqueda recursiva si no encontró
        if not items:
            def find_products(obj, depth=0):
                if depth > 8 or not obj or not isinstance(obj, (dict, list)):
                    return None
                if isinstance(obj, list) and len(obj) >= 1:
                    if isinstance(obj[0], dict) and obj[0].get("name") and (
                        obj[0].get("url") or obj[0].get("productId") or obj[0].get("id")
                    ):
                        return obj
                if isinstance(obj, dict):
                    for v in obj.values():
                        r = find_products(v, depth + 1)
                        if r:
                            return r
                return None
            items = find_products(d) or []

        if not items:
            return []

        results = []
        for p in items:
            # Precio
            full = final = 0.0
            pd = p.get("priceData") or {}
            if pd.get("prices"):
                orig = next((x for x in pd["prices"] if x.get("type") == "original"), None)
                sale = next((x for x in pd["prices"] if x.get("type") == "sale"), None)
                full  = float(orig["value"]) if orig else 0.0
                final = float(sale["value"]) if sale else full
            elif p.get("pricingInformation"):
                pi = p["pricingInformation"]
                full  = float(pi.get("standardPrice") or 0)
                final = float(pi.get("currentPrice") or full)
            elif p.get("price"):
                pr = p["price"]
                if isinstance(pr, dict):
                    full  = float(pr.get("regular") or pr.get("current") or pr.get("value") or 0)
                    final = float(pr.get("current") or pr.get("value") or full)
                else:
                    full = final = float(pr or 0)

            name = p.get("title") or p.get("name") or p.get("displayName") or ""
            url  = p.get("url") or p.get("link") or p.get("pdpUrl") or ""
            if url and not url.startswith("http"):
                url = "https://www.adidas.com" + url

            if name and (full > 0 or final > 0):
                results.append({
                    "name": name, "url": url,
                    "full_price": full, "final_price": final,
                    "currency": "USD",
                })
        return results
    except Exception as e:
        log(f"   ⚠️ parse __NEXT_DATA__: {e}")
        return []


def scrape_requests(url: str, franchise: str) -> Optional[dict]:
    """Intenta scraping con requests puro via Site Unblocker."""
    try:
        log(f"   📡 [requests] GET {url.split('/us/')[1]}")
        r = requests.get(
            url, headers=HEADERS, proxies=PROXIES,
            verify=False, timeout=45,
        )
        log(f"   📊 Status: {r.status_code} | Size: {len(r.text):,} bytes")

        if r.status_code != 200:
            log(f"   ⚠️ HTTP {r.status_code}")
            return None

        # Intentar __NEXT_DATA__
        products = extract_next_data_from_html(r.text)
        if products:
            log(f"   ✅ [requests/__NEXT_DATA__] {len(products)} productos")
            return pick_best(products, franchise)

        # Fallback: buscar precios en HTML crudo con regex
        # US adidas usa formato "$189.95" en el HTML
        price_pattern = re.compile(r'\$(\d{2,4}(?:\.\d{2})?)')
        name_pattern  = re.compile(r'"(?:title|name|displayName)"\s*:\s*"([^"]{5,80})"')

        prices = [float(m) for m in price_pattern.findall(r.text) if 10 < float(m) < 1000]
        names  = list(dict.fromkeys(name_pattern.findall(r.text)))[:10]  # dedup, primeros 10

        if prices and names:
            log(f"   ✅ [requests/regex] {len(names)} nombres, precios: {sorted(set(prices))[:5]}")
            # Devolver el primer nombre relevante con el primer precio
            tokens = re.sub(r'["\']', '', franchise).lower().split()
            best_name = next(
                (n for n in names if any(t in n.lower() for t in tokens if len(t) > 2)),
                names[0]
            )
            best_price = sorted(set(prices))[0]  # precio más bajo = full en USD
            return {
                "name": best_name, "url": url,
                "full_price": best_price, "final_price": best_price,
                "currency": "USD", "method": "regex",
            }

        log(f"   ⚠️ [requests] sin productos extraíbles")
        log(f"   🔍 Preview HTML: {r.text[500:800]!r}")
        return None

    except requests.exceptions.Timeout:
        log(f"   ⚠️ [requests] Timeout")
        return None
    except Exception as e:
        log(f"   ⚠️ [requests] Error: {str(e)[:120]}")
        return None


# ─────────────────────────────────────────────────────────────────
# ESTRATEGIA 2: Playwright + SSL fix para Site Unblocker
# ─────────────────────────────────────────────────────────────────
STEALTH_JS = """
() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => undefined, configurable: true });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'], configurable: true });
    if (!window.chrome) { window.chrome = { app:{}, runtime:{} }; }
    ['__playwright','__pwInitScripts'].forEach(k=>{ try{delete window[k];}catch(e){} });
}
"""

async def scrape_playwright(pw, url: str, franchise: str) -> Optional[dict]:
    """Playwright con SSL deshabilitado para Site Unblocker."""
    proxy = {
        "server":   f"http://{UNBLOCKER_HOST}:{UNBLOCKER_PORT}",
        "username": UNBLOCKER_USER,
        "password": UNBLOCKER_PASS,
    }
    browser = None
    try:
        browser = await pw.chromium.launch(
            headless=HEADLESS,
            proxy=proxy,
            args=[
                "--ignore-certificate-errors",
                "--ignore-ssl-errors",
                "--disable-web-security",
                "--allow-running-insecure-content",
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
            ],
        )
        context = await browser.new_context(
            locale="en-US",
            timezone_id="America/New_York",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            ignore_https_errors=True,
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
            },
        )
        await context.add_init_script(STEALTH_JS)

        # Bloquear solo imágenes
        async def _block(route, req):
            await route.abort() if req.resource_type in ("image", "media") else await route.continue_()
        await context.route("**/*", _block)

        page = await context.new_page()
        log(f"   🌐 [playwright] GET {url.split('/us/')[1]}")

        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        await asyncio.sleep(3)

        title = await page.title()
        log(f"   📄 Title: '{title[:60]}'")

        # Polling __NEXT_DATA__ 20s
        deadline = time.time() + 20
        products = []
        attempt  = 0
        while time.time() < deadline:
            attempt += 1
            try:
                products = await page.evaluate("""
                    () => {
                        const s = document.getElementById('__NEXT_DATA__');
                        if (!s) return [];
                        try {
                            const d = JSON.parse(s.textContent);
                            let items = d?.props?.pageProps?.products || [];
                            if (!items.length) {
                                function find(o,depth){
                                    if(depth>7||!o||typeof o!=='object') return null;
                                    if(Array.isArray(o)&&o.length>=1&&typeof o[0]==='object'&&o[0]?.name&&(o[0]?.url||o[0]?.productId)) return o;
                                    for(const v of Object.values(o)){const r=find(v,depth+1);if(r)return r;}
                                    return null;
                                }
                                items = find(d,0)||[];
                            }
                            return items.map(p=>{
                                let full=0,final=0;
                                const pd=p.priceData||{};
                                if(pd.prices){
                                    const orig=pd.prices.find(x=>x.type==='original');
                                    const sale=pd.prices.find(x=>x.type==='sale');
                                    full=orig?parseFloat(orig.value):0;
                                    final=sale?parseFloat(sale.value):full;
                                } else if(p.price){
                                    full=final=typeof p.price==='object'?parseFloat(p.price.current||p.price.value||0):parseFloat(p.price)||0;
                                }
                                let url=p.url||p.link||'';
                                if(url&&!url.startsWith('http'))url='https://www.adidas.com'+url;
                                return {name:p.title||p.name||'',url,full_price:full,final_price:final};
                            }).filter(p=>p.name&&(p.full_price>0||p.final_price>0));
                        } catch(e){return [];}
                    }
                """)
                if products:
                    log(f"   ✅ [playwright/__NEXT_DATA__] {len(products)} productos (intento {attempt})")
                    break
            except Exception:
                pass
            await asyncio.sleep(1.5)

        # Si no hay productos en __NEXT_DATA__, esperar precios en DOM
        if not products:
            log("   🔄 [playwright] esperando precios lazy en DOM (30s)...")
            deadline2 = time.time() + 30
            attempt2  = 0
            while time.time() < deadline2:
                attempt2 += 1
                try:
                    products = await page.evaluate("""
                        () => {
                            const cardSels = ['[data-auto-id="product-card"]','[data-testid="plp-product-card"]','article[class*="product"]','[class*="ProductCard"]'];
                            let cards = [];
                            for(const s of cardSels){ cards=Array.from(document.querySelectorAll(s)); if(cards.length) break; }
                            if(!cards.length) return null;
                            const priceSels = ['[data-auto-id="product-card-price"]','[class*="price_large"]','[class*="ProductPrice"]','[data-testid="product-price"]','span[class*="price"]'];
                            let pricesFound = 0;
                            const results = [];
                            for(const card of cards.slice(0,20)){
                                const nameEl = card.querySelector('[data-auto-id="product-card-title"],p[class*="title"],h3,h2');
                                const name = nameEl ? nameEl.innerText.trim() : '';
                                if(!name) continue;
                                let price=0, full=0;
                                for(const ps of priceSels){
                                    const els = Array.from(card.querySelectorAll(ps));
                                    if(els.length){
                                        const prices = els.map(e=>parseFloat(e.innerText.replace(/[^0-9.]/g,''))).filter(Boolean);
                                        if(prices.length){ full=Math.max(...prices); price=Math.min(...prices); pricesFound++; break; }
                                    }
                                }
                                if(price>0){
                                    const linkEl=card.querySelector('a[href]');
                                    results.push({name,url:linkEl?linkEl.href:'',full_price:full,final_price:price});
                                }
                            }
                            return pricesFound>0 ? results : null;
                        }
                    """)
                    if products:
                        log(f"   ✅ [playwright/DOM] {len(products)} productos con precio (intento {attempt2})")
                        break
                    log(f"   ⏳ sin precios DOM (intento {attempt2})")
                except Exception as e:
                    log(f"   ⚠️ DOM eval: {str(e)[:60]}")
                await asyncio.sleep(2)

        if products:
            for p in products:
                p["currency"] = "USD"
            return pick_best(products, franchise)

        log("   ❌ [playwright] sin productos")
        return None

    except PWTimeoutError:
        log(f"   ⚠️ [playwright] Timeout")
        return None
    except Exception as e:
        log(f"   ⚠️ [playwright] Error: {str(e)[:120]}")
        return None
    finally:
        if browser:
            try:
                await browser.close()
            except Exception:
                pass


# ─────────────────────────────────────────────────────────────────
# SCRAPE UNA FRANQUICIA — requests primero, playwright como fallback
# ─────────────────────────────────────────────────────────────────
async def scrape_franchise_us(pw, franchise: str) -> Optional[dict]:
    clean = re.sub(r'["\']', '', franchise).strip().upper()
    urls  = FRANCHISE_URLS_US.get(franchise) or FRANCHISE_URLS_US.get(clean) or []

    if not urls:
        slug = re.sub(r'["\']', '', franchise).lower().replace(' ', '_')
        urls = [f"{BASE_US}/{slug}-shoes", f"{BASE_US}/{slug}-running-shoes"]

    log(f"   URLs: {[u.split('/us/')[1] for u in urls]}")

    for url in urls:
        log(f"\n   ── Franquicia: {franchise} | {url.split('/us/')[1]}")

        # ── Intento 1: requests puro (más rápido, sin browser overhead)
        log(f"   [1/2] requests + Site Unblocker")
        result = scrape_requests(url, franchise)
        if result:
            return result

        # ── Intento 2: Playwright con SSL fix
        log(f"   [2/2] Playwright + SSL fix + Site Unblocker")
        result = await scrape_playwright(pw, url, franchise)
        if result:
            return result

        # Pequeña pausa entre URLs
        await asyncio.sleep(2)

    return None


# ─────────────────────────────────────────────────────────────────
# VALIDACIÓN RÁPIDA — 3 franquicias para probar ambas estrategias
# ─────────────────────────────────────────────────────────────────
QUICK_TEST = [
    {"adidas": "ADIZERO ADIOS PRO",   "nike": "VAPORFLY",     "categoria": "Running"},
    {"adidas": "ULTRABOOST",          "nike": "PEGASUS PLUS", "categoria": "Running"},
    {"adidas": "SAMBA",               "nike": "KILLSHOT",     "categoria": "Sportswear"},
]

FULL_RUN = True  # ← cambiado a True para las 24 franquicias


async def main():
    log("=" * 60)
    log("🧪 ADIDAS US DEBUG — Site Unblocker (requests + playwright)")
    log("=" * 60)

    rows    = DEBUG_FRANCHISES if FULL_RUN else QUICK_TEST
    results = []

    async with async_playwright() as pw:
        for i, row in enumerate(rows):
            franchise = row["adidas"]
            nike      = row["nike"]
            log(f"\n{'='*50}")
            log(f"[{i+1}/{len(rows)}] Nike: {nike} ↔ Adidas: {franchise}")

            result = await scrape_franchise_us(pw, franchise)

            if result:
                method = result.get("method", "__NEXT_DATA__")
                log(f"   ✅ USD {result.get('final_price'):,.2f} | {result.get('name','')[:40]} [{method}]")
                results.append({"franchise": franchise, "status": "OK", **result})
            else:
                log(f"   ❌ FALLO: {franchise}")
                results.append({"franchise": franchise, "status": "FAIL"})

            await asyncio.sleep(random.uniform(2, 4))

    # Resumen
    log("\n" + "=" * 60)
    log("📊 RESUMEN US")
    log("=" * 60)
    ok   = sum(1 for r in results if r["status"] == "OK")
    fail = sum(1 for r in results if r["status"] == "FAIL")
    log(f"✅ {ok}/{len(results)}   ❌ {fail}/{len(results)}")
    for r in results:
        st    = "✅" if r["status"] == "OK" else "❌"
        price = f"USD {r.get('final_price',0):>8,.2f}" if r["status"] == "OK" else f"{'—':>12}"
        name  = r.get("name", "—")[:38]
        log(f"  {st} {r['franchise']:25s} | {price} | {name}")

    with open("adidas_us_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    log("\n💾 adidas_us_results.json")


if __name__ == "__main__":
    asyncio.run(main())
