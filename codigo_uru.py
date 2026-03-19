import datetime as dt
import glob
import math
import os
import re
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
import xlsxwriter

import codigo_usa as us

SEASON = "SP26"
STATUSBOOKS_URU_PATH = "StatusBooks NDDC URU SP26.xlsx"
LINKS_PLP_PATH = "Links PLP Scrapping.xlsx"

FX_MODE = "oficial"
TASA_CAMBIO_URU_FALLBACK = 43.5

IVA = 0.21
BANK_FEES = 0.08

URU_FREE_SHIP_THRESHOLD_UYU = 4000.0
URU_SHIPPING_UYU = 250.0
US_FREE_SHIP_THRESHOLD_USD = 50.0
US_SHIPPING_USD = 5.0

TEMPLATE_COLS_URU = [
    "fecha", "Division", "Franchise", "Gender", "GAMA BOTINES", "PLATO",
    "Category", "Marketing Name (UY)", "Style", "Product Code",
    "Nike US Product Name", "PDP USA",
    "Retail URU (UYU)", "FX UYU/USD", "URU (USD)",
    "Retail ARG (ARS)", "FX ARS/USD", "ARG (USD)",
    "USA Full (USD)",
    "Dif URU vs US", "Dif ARG vs US", "Dif URU vs ARG",
    "URU + Shp (USD)", "ARG + Shp (USD)", "US + Shp (USD)",
    "Dif URU+Shp vs US+Shp", "Dif ARG+Shp vs US+Shp",
]


def log(msg: str):
    ts = dt.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, str):
            s = x.strip()
            if s == "" or s.upper() in {"#N/A", "NAN", "NONE", "NULL"}:
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


def normalize_upper(s: str) -> str:
    return str(s or "").strip().upper()


def extract_plato_from_marketing_name(marketing_name: str) -> str:
    name = normalize_upper(marketing_name)
    patterns = [
        (r"SG-PRO", "SG-PRO"),
        (r"\bTF\b", "TF"),
        (r"\bFG\b", "FG"),
        (r"\bIC\b", "IC"),
    ]
    for pattern, value in patterns:
        if re.search(pattern, name):
            return value
    return ""


def get_uyu_usd_venta() -> float:
    try:
        r = requests.get("https://dolarapi.com/v1/cotizaciones/usd/uyu", timeout=15)
        r.raise_for_status()
        data = r.json()
        for k in ("venta", "promedio", "compra"):
            if isinstance(data, dict) and isinstance(data.get(k), (int, float)) and data[k] > 0:
                return float(data[k])
    except Exception:
        pass
    log(f"WARNING no se pudo obtener UYU/USD, uso fallback={TASA_CAMBIO_URU_FALLBACK}")
    return float(TASA_CAMBIO_URU_FALLBACK)


def resolve_statusbooks_uru_path(configured_path: str) -> str:
    candidates = [
        configured_path,
        "StatusBooks NDDC URU SP26.xlsx",
        "StatusBooks_NDDC_URU_SP26.xlsx",
        os.path.join("..", "Data", "Stylecolor", "StatusBooks NDDC URU SP26 - x dia (8).xlsx"),
        os.path.join("..", "Data", "Stylecolor", "StatusBooks NDDC URU SP26 - x dia.xlsx"),
        os.path.join("..", "Data", "Stylecolor", "StatusBooks NDDC URU SP26 - x dia (1).xlsx"),
    ]
    for p in candidates:
        if p and os.path.isfile(p):
            return p

    roots = [
        os.getcwd(),
        os.path.abspath(os.path.join(os.getcwd(), "..")),
        os.path.abspath(os.path.join(os.getcwd(), "..", "Data")),
    ]
    found: List[str] = []
    for root in roots:
        if not os.path.isdir(root):
            continue
        found.extend(glob.glob(os.path.join(root, "**", "*StatusBooks*URU*.xls*"), recursive=True))

    found = [p for p in found if os.path.isfile(p)]
    if not found:
        raise FileNotFoundError("No encuentro StatusBooks URU. Revisa STATUSBOOKS_URU_PATH.")

    found.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return found[0]


def load_statusbooks_uru_for_scrape(path: str) -> pd.DataFrame:
    log(f"Cargando StatusBooks URU desde: {path}")
    df = pd.read_excel(path, sheet_name="Books NDDC", header=6)
    df.columns = [str(c).strip() for c in df.columns]

    required = [
        "Product Code", "Marketing Name", "BU", "Category", "Gender", "Franchise", "Retail Price"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"StatusBooks URU missing columns: {missing}")

    if "SILO BOTINES" not in df.columns:
        if "SILO BOTIN" in df.columns:
            df["SILO BOTINES"] = df["SILO BOTIN"]
        else:
            df["SILO BOTINES"] = ""

    if "PLATO" not in df.columns:
        df["PLATO"] = ""

    if "STOCK BL (Inventario Brandlive)" not in df.columns:
        df["STOCK BL (Inventario Brandlive)"] = 1.0

    df["Product Code"] = df["Product Code"].astype(str).fillna("").map(lambda s: s.strip().upper())
    df["Style"] = df["Product Code"].map(lambda s: s[:6] if len(s) >= 6 else s)

    # URU no trae suela confiable como columna; se deriva del Marketing Name si falta.
    df["PLATO"] = df["PLATO"].astype(str).fillna("").map(lambda s: s.strip())
    missing_plato = df["PLATO"].eq("")
    if missing_plato.any():
        df.loc[missing_plato, "PLATO"] = df.loc[missing_plato, "Marketing Name"].apply(extract_plato_from_marketing_name)

    df["_price"] = df["Retail Price"].apply(safe_float)
    df = df[df["_price"].notna() & (df["_price"] > 0)].copy()

    df["_stock"] = df["STOCK BL (Inventario Brandlive)"].apply(lambda x: safe_float(x) or 0.0)
    df["_stock_style_sum"] = df.groupby("Style")["_stock"].transform("sum")
    df = df[df["_stock_style_sum"] > 0].copy()

    norm_cols = [
        "Style", "Product Code", "Marketing Name", "BU", "Category", "Gender", "Franchise", "SILO BOTINES", "PLATO"
    ]
    for c in norm_cols:
        df[c] = df[c].astype(str).fillna("").map(lambda s: s.strip())

    log(f"StatusBooks URU listo: {len(df):,} filas")
    return df


def build_arg_price_map() -> Dict[str, float]:
    try:
        log("Cargando ARG pricing...")
        df_arg = us.load_statusbooks_filtered(us.STATUSBOOKS_PATH, us.SEASON)
        first = df_arg.sort_index().groupby("Style")["_price"].first()
        out: Dict[str, float] = {}
        for style, price in first.to_dict().items():
            st = str(style).strip().upper()
            if st and price is not None and price > 0:
                out[st] = float(price)
        log(f"   {len(out)} styles con precio ARG")
        return out
    except Exception as e:
        log(f"WARNING Error cargando ARG pricing: {e}")
        return {}


def build_uru_style_info_map(df_sb_uru: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    g = df_sb_uru.sort_index().groupby("Style").first(numeric_only=False)
    for style, row in g.iterrows():
        st = str(style).strip().upper()
        if not st:
            continue
        out[st] = {
            "Retail URU (UYU)": safe_float(row.get("_price")),
            "Product Code": str(row.get("Product Code", "")).strip(),
            "Marketing Name (UY)": str(row.get("Marketing Name", "")).strip(),
            "Franchise": str(row.get("Franchise", "")).strip(),
            "Gender": str(row.get("Gender", "")).strip(),
            "GAMA BOTINES": str(row.get("SILO BOTINES", "")).strip(),
            "PLATO": str(row.get("PLATO", "")).strip(),
            "Category": str(row.get("Category", "")).strip(),
        }
    return out


def make_us_like_row(
    *,
    run_date: str,
    division: str,
    franchise: str,
    category: str,
    marketing_uy: str,
    style: str,
    us_name: str,
    pdp: str,
    retail_uyu: Optional[float],
    us_full: Optional[float],
    gender: str,
    silo: str,
    plato: str,
    product_code: str,
) -> Dict[str, Any]:
    return {
        "fecha": run_date,
        "Division": division,
        "Franchise": franchise,
        "Gender": gender,
        "GAMA BOTINES": silo,
        "PLATO": plato,
        "Category": category,
        "Marketing Name (UY)": marketing_uy,
        "Style": style,
        "Product Code": product_code,
        "Nike US Product Name": us_name,
        "PDP USA": pdp,
        # nombre heredado por compatibilidad con outputs de codigo_usa
        "Retail ARG (ARS)": retail_uyu,
        "USA Full (USD)": us_full,
    }


def build_football_output_uru(
    session: requests.Session,
    df_sb: pd.DataFrame,
    style_price_map: Dict[str, float],
    style_meta_map: Dict[str, Dict[str, str]],
    links_sheets: Dict[str, pd.DataFrame],
    run_date: str,
    style_info_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if "Football" not in links_sheets:
        return []

    df_links = links_sheets["Football"].copy()
    df_links.columns = pd.Index([str(c).strip() for c in df_links.columns])

    expected = ["Franchise", "Gender", "SILO BOTINES", "PLATO"]
    missing = [c for c in expected if c not in df_links.columns]
    if missing:
        log(f"Football sheet missing columns: {missing}")
        return []

    df_fb = us.filter_football_sb(df_sb)
    df_fb["_F_FRANCHISE"] = df_fb["Franchise"].apply(us.normalize_upper)
    df_fb["_F_GENDER"] = df_fb["Gender"].apply(us.normalize_upper)
    df_fb["_F_SILO"] = df_fb["SILO BOTINES"].apply(us.normalize_upper)
    df_fb["_F_PLATO"] = df_fb["PLATO"].apply(lambda x: us.normalize_plato(x))

    out_rows: List[Dict[str, Any]] = []
    log(f"\n[FOOTBALL URU] {len(df_links)} combinaciones")

    for idx, r in df_links.iterrows():
        franchise = str(r.get("Franchise", "")).strip()
        gender = str(r.get("Gender", "")).strip()
        silo = str(r.get("SILO BOTINES", "")).strip()
        plato = str(r.get("PLATO", "")).strip()

        if not (franchise and gender and plato):
            continue

        base_query = f"{franchise} {silo} {plato}".strip() if silo else f"{franchise} {plato}".strip()
        url_us = us.build_nike_search_url(base_query, market="us")

        fr_u = us.normalize_upper(franchise)
        ge_u = us.normalize_upper(gender)
        si_u = us.normalize_upper(silo)
        pl_u = us.normalize_plato(plato)

        log(f"   [{idx+1}/{len(df_links)}] {franchise} | {gender} | {silo} | {plato}")

        # intento 1: estricto (franchise+gender+plato+silo)
        m = (df_fb["_F_FRANCHISE"] == fr_u) & (df_fb["_F_GENDER"] == ge_u) & (df_fb["_F_PLATO"] == pl_u)
        if si_u:
            m = m & (df_fb["_F_SILO"] == si_u)

        styles = [str(s).strip().upper() for s in df_fb.loc[m, "Style"].dropna().unique().tolist() if str(s).strip()]
        match_method = "STRICT" if styles else ""

        # intento 2: fallback sin plato cuando URU no trae suela consistente
        if not styles:
            m2 = (df_fb["_F_FRANCHISE"] == fr_u) & (df_fb["_F_GENDER"] == ge_u)
            if si_u:
                m2 = m2 & (df_fb["_F_SILO"] == si_u)
            styles = [str(s).strip().upper() for s in df_fb.loc[m2, "Style"].dropna().unique().tolist() if str(s).strip()]
            if styles:
                match_method = "FALLBACK_NO_PLATO"

        n_candidates = len(styles)
        log(f"      Styles SB: {n_candidates} ({match_method or 'SIN_MATCH'})")

        if not styles:
            _row = make_us_like_row(
                run_date=run_date,
                division="FW",
                franchise=franchise,
                category="Football",
                marketing_uy="",
                style="",
                us_name="",
                pdp="",
                retail_uyu=None,
                us_full=None,
                gender=gender,
                silo=silo,
                plato=plato,
                product_code="",
            )
            _row["_match_status"] = "NO_STYLE_SB"
            _row["_match_method"] = ""
            _row["_candidates"] = 0
            out_rows.append(_row)
            us.human_pause(*us.SLEEP_RANGE)
            continue

        pick = None
        try:
            products = us.scrape_plp_products_paged(
                session,
                url_us,
                pages=us.FOOTBALL_MAX_PAGES,
                max_cards=us.MAX_PLP_PRODUCTS_SCAN,
            )

            by_style: Dict[str, List[Any]] = {}
            for p in products:
                if not p.style_base:
                    continue
                by_style.setdefault(p.style_base.upper(), []).append(p)

            tried = 0
            for st in styles:
                if tried >= us.FOOTBALL_MAX_STYLE_TRIES_PER_KEY:
                    break
                tried += 1

                plist = by_style.get(st, [])
                plist = [p for p in plist if (not p.sold_out_flag) and p.full_price_flag and p.price_usd and p.price_usd > 0]
                if not plist:
                    continue

                plist.sort(key=lambda x: (us.extract_max_model_number(x.title), x.price_usd or 0.0), reverse=True)
                pick = plist[0]
                break
        except Exception as e:
            log(f"      Error scraping football URU: {e}")
            pick = None

        if pick is None:
            _row = make_us_like_row(
                run_date=run_date,
                division="FW",
                franchise=franchise,
                category="Football",
                marketing_uy="",
                style="",
                us_name="",
                pdp="",
                retail_uyu=None,
                us_full=None,
                gender=gender,
                silo=silo,
                plato=plato,
                product_code="",
            )
            _row["_match_status"] = "NO_US_MATCH"
            _row["_match_method"] = match_method
            _row["_candidates"] = n_candidates
            out_rows.append(_row)
        else:
            style = str(pick.style_base or "").strip().upper()
            retail_uyu = style_price_map.get(style)
            meta = style_meta_map.get(style, {})
            info = style_info_map.get(style, {})
            marketing = str(info.get("Marketing Name (UY)") or meta.get("Marketing Name") or "").strip()
            product_code = str(info.get("Product Code", "")).strip()

            _row = make_us_like_row(
                run_date=run_date,
                division="FW",
                franchise=franchise,
                category="Football",
                marketing_uy=marketing,
                style=style,
                us_name=pick.title,
                pdp=pick.pdp_url,
                retail_uyu=float(retail_uyu) if retail_uyu is not None else None,
                us_full=float(pick.price_usd) if pick.price_usd is not None else None,
                gender=gender,
                silo=silo,
                plato=plato,
                product_code=product_code,
            )
            _row["_match_status"] = "OK"
            _row["_match_method"] = match_method
            _row["_candidates"] = n_candidates
            out_rows.append(_row)

        us.human_pause(*us.SLEEP_RANGE)

    return out_rows


def make_output_row(
    row_us_like: Dict[str, Any],
    fx_uyu_per_usd: float,
    fx_ars_per_usd: float,
    arg_price_map: Dict[str, float],
    style_info_map: Dict[str, Dict[str, Any]],
    run_date: str,
) -> Dict[str, Any]:
    style = str(row_us_like.get("Style", "")).strip().upper()
    info = style_info_map.get(style, {})

    retail_uyu = safe_float(row_us_like.get("Retail ARG (ARS)"))
    if retail_uyu is None:
        retail_uyu = safe_float(info.get("Retail URU (UYU)"))

    retail_ars = arg_price_map.get(style) if style else None
    us_full = safe_float(row_us_like.get("USA Full (USD)"))

    uru_usd = (float(retail_uyu) / fx_uyu_per_usd) if (retail_uyu is not None and fx_uyu_per_usd) else None
    arg_usd = (float(retail_ars) / fx_ars_per_usd) if (retail_ars is not None and fx_ars_per_usd) else None

    dif_uru_vs_us = (uru_usd / us_full - 1.0) if (uru_usd is not None and us_full) else None
    dif_arg_vs_us = (arg_usd / us_full - 1.0) if (arg_usd is not None and us_full) else None
    dif_uru_vs_arg = (uru_usd / arg_usd - 1.0) if (uru_usd is not None and arg_usd) else None

    ship_uru_usd = None
    if retail_uyu is not None and fx_uyu_per_usd:
        ship_uru_uyu = 0.0 if float(retail_uyu) >= URU_FREE_SHIP_THRESHOLD_UYU else URU_SHIPPING_UYU
        ship_uru_usd = ship_uru_uyu / float(fx_uyu_per_usd)

    ship_arg_usd = None
    if retail_ars is not None and fx_ars_per_usd:
        ship_arg_ars = 0.0 if float(retail_ars) >= us.ARG_FREE_SHIP_THRESHOLD_ARS else float(us.ARG_SHIPPING_ARS)
        ship_arg_usd = ship_arg_ars / float(fx_ars_per_usd)

    ship_us_usd = None
    if us_full is not None:
        ship_us_usd = 0.0 if float(us_full) >= US_FREE_SHIP_THRESHOLD_USD else US_SHIPPING_USD

    uru_plus_shp = (uru_usd + ship_uru_usd) if (uru_usd is not None and ship_uru_usd is not None) else None
    arg_plus_shp = (arg_usd + ship_arg_usd) if (arg_usd is not None and ship_arg_usd is not None) else None
    us_plus_shp = (us_full + ship_us_usd) if (us_full is not None and ship_us_usd is not None) else None

    dif_uru_shp_vs_us = (uru_plus_shp / us_plus_shp - 1.0) if (uru_plus_shp is not None and us_plus_shp) else None
    dif_arg_shp_vs_us = (arg_plus_shp / us_plus_shp - 1.0) if (arg_plus_shp is not None and us_plus_shp) else None

    row = {
        "fecha": run_date,
        "Division": str(row_us_like.get("Division", "")).strip(),
        "Franchise": str(row_us_like.get("Franchise", "")).strip() or str(info.get("Franchise", "")).strip(),
        "Gender": str(row_us_like.get("Gender", "")).strip() or str(info.get("Gender", "")).strip(),
        "GAMA BOTINES": str(row_us_like.get("GAMA BOTINES", "")).strip() or str(info.get("GAMA BOTINES", "")).strip(),
        "PLATO": str(row_us_like.get("PLATO", "")).strip() or str(info.get("PLATO", "")).strip(),
        "Category": str(row_us_like.get("Category", "")).strip() or str(info.get("Category", "")).strip(),
        "Marketing Name (UY)": str(row_us_like.get("Marketing Name (UY)", "")).strip() or str(info.get("Marketing Name (UY)", "")).strip(),
        "Style": style,
        "Product Code": str(row_us_like.get("Product Code", "")).strip() or str(info.get("Product Code", "")).strip(),
        "Nike US Product Name": str(row_us_like.get("Nike US Product Name", "")).strip(),
        "PDP USA": str(row_us_like.get("PDP USA", "")).strip(),
        "Retail URU (UYU)": retail_uyu,
        "FX UYU/USD": fx_uyu_per_usd,
        "URU (USD)": uru_usd,
        "Retail ARG (ARS)": retail_ars,
        "FX ARS/USD": fx_ars_per_usd,
        "ARG (USD)": arg_usd,
        "USA Full (USD)": us_full,
        "Dif URU vs US": dif_uru_vs_us,
        "Dif ARG vs US": dif_arg_vs_us,
        "Dif URU vs ARG": dif_uru_vs_arg,
        "URU + Shp (USD)": uru_plus_shp,
        "ARG + Shp (USD)": arg_plus_shp,
        "US + Shp (USD)": us_plus_shp,
        "Dif URU+Shp vs US+Shp": dif_uru_shp_vs_us,
        "Dif ARG+Shp vs US+Shp": dif_arg_shp_vs_us,
    }

    for c in TEMPLATE_COLS_URU:
        row.setdefault(c, None)
    return row


def convert_rows_to_output(
    rows_us_like: List[Dict[str, Any]],
    fx_uyu: float,
    fx_ars: float,
    arg_price_map: Dict[str, float],
    style_info_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    run_date = dt.datetime.now().strftime("%Y-%m-%d")
    for r in rows_us_like:
        out.append(make_output_row(r, fx_uyu, fx_ars, arg_price_map, style_info_map, run_date))
    return out


def write_xlsx(output_path: str, rows: List[Dict[str, Any]]):
    log(f"Escribiendo Excel: {output_path}")
    wb = xlsxwriter.Workbook(output_path)
    ws = wb.add_worksheet("Comparativo")

    header_fmt = wb.add_format({"bold": True, "font_color": "white", "bg_color": "#1F4E79", "border": 1})
    text_fmt = wb.add_format({"align": "left", "valign": "vcenter"})
    money_local = wb.add_format({"num_format": "#,##0", "align": "left", "valign": "vcenter"})
    money_usd = wb.add_format({"num_format": "$#,##0.00", "align": "left", "valign": "vcenter"})
    fx_fmt = wb.add_format({"num_format": "#,##0.00", "align": "left", "valign": "vcenter"})
    pct_fmt = wb.add_format({"num_format": "0.00%", "align": "left", "valign": "vcenter"})
    link_fmt = wb.add_format({"font_color": "blue", "underline": 1})

    for j, c in enumerate(TEMPLATE_COLS_URU):
        ws.write(0, j, c, header_fmt)

    local_cols = {"Retail URU (UYU)", "Retail ARG (ARS)"}
    usd_cols = {"URU (USD)", "ARG (USD)", "USA Full (USD)", "URU + Shp (USD)", "ARG + Shp (USD)", "US + Shp (USD)"}
    pct_cols = {"Dif URU vs US", "Dif ARG vs US", "Dif URU vs ARG", "Dif URU+Shp vs US+Shp", "Dif ARG+Shp vs US+Shp"}

    for i, row in enumerate(rows, start=1):
        for j, c in enumerate(TEMPLATE_COLS_URU):
            v = row.get(c)
            if c == "PDP USA":
                if isinstance(v, str) and v.startswith("http"):
                    ws.write_url(i, j, v, link_fmt, string="Open")
                else:
                    ws.write(i, j, v if v is not None else "", text_fmt)
            elif c in local_cols:
                ws.write_number(i, j, float(v), money_local) if v is not None else ws.write(i, j, "", text_fmt)
            elif c in usd_cols:
                ws.write_number(i, j, float(v), money_usd) if v is not None else ws.write(i, j, "", text_fmt)
            elif c in pct_cols:
                ws.write_number(i, j, float(v), pct_fmt) if v is not None else ws.write(i, j, "", text_fmt)
            elif c in {"FX UYU/USD", "FX ARS/USD"}:
                ws.write_number(i, j, float(v), fx_fmt) if v is not None else ws.write(i, j, "", text_fmt)
            else:
                ws.write(i, j, v if v is not None else "", text_fmt)

    for j, c in enumerate(TEMPLATE_COLS_URU):
        ws.set_column(j, j, min(60, max(10, len(c) + 2)))

    wb.close()


def main():
    start = time.time()
    log("=" * 60)
    log("Nike URU vs ARG vs US - Price Monitoring")
    log("=" * 60)

    try:
        uru_path = resolve_statusbooks_uru_path(STATUSBOOKS_URU_PATH)
        df_sb_uru = load_statusbooks_uru_for_scrape(uru_path)

        style_price_map = us.build_style_price_map(df_sb_uru)
        style_meta_map = us.build_style_meta_map(df_sb_uru)
        style_info_map = build_uru_style_info_map(df_sb_uru)
        links_sheets = us.load_links_sheets(LINKS_PLP_PATH)

        fx_uyu = get_uyu_usd_venta()
        fx_ars = us.get_usd_ars_venta(FX_MODE)
        log(f"UYU/USD = {fx_uyu:.2f}")
        log(f"ARS/USD = {fx_ars:.2f}")

        arg_price_map = build_arg_price_map()

        session = us.build_session()
        run_date = dt.datetime.now().strftime("%Y-%m-%d")
        rows_us_like: List[Dict[str, Any]] = []

        rows_us_like.extend(us.build_nonfootball_output(session, df_sb_uru, style_price_map, style_meta_map, links_sheets, fx_uyu, run_date))
        rows_us_like.extend(us.build_kids_output(session, df_sb_uru, style_price_map, style_meta_map, links_sheets, fx_uyu, run_date))
        if us.SCRAPING_APPAREL:
            rows_us_like.extend(us.build_apparel_output(session, df_sb_uru, style_price_map, style_meta_map, fx_uyu, run_date))
        if us.SCRAPING_EQUIPMENT:
            rows_us_like.extend(us.build_equipment_output(session, df_sb_uru, style_price_map, style_meta_map, fx_uyu, run_date))

        # Football con fallback de matching cuando no hay PLATO/SUELA consistente en URU.
        rows_us_like.extend(
            build_football_output_uru(
                session,
                df_sb_uru,
                style_price_map,
                style_meta_map,
                links_sheets,
                run_date,
                style_info_map,
            )
        )

        rows_out = convert_rows_to_output(rows_us_like, fx_uyu, fx_ars, arg_price_map, style_info_map)

        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_xlsx = f"Nike_URU_ARG_US_{SEASON}_{ts}.xlsx"
        out_csv = f"Nike_URU_ARG_US_{SEASON}_{ts}.csv"

        write_xlsx(out_xlsx, rows_out)
        pd.DataFrame(rows_out)[TEMPLATE_COLS_URU].to_csv(out_csv, index=False, encoding="utf-8-sig")

        elapsed = time.time() - start
        log(f"Completado en {elapsed:.1f}s")
        log(f"Excel: {out_xlsx}")
        log(f"CSV:   {out_csv}")

    except Exception as e:
        log(f"ERROR FATAL: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
