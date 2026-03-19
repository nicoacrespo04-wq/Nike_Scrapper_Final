import datetime as dt
import glob
import math
import os
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
BML_TOL = 0.02

URU_FREE_SHIP_THRESHOLD_UYU = 2000.0
URU_SHIPPING_UYU = 300.0
US_FREE_SHIP_THRESHOLD_USD = 50.0
US_SHIPPING_USD = 5.0

TEMPLATE_COLS_URU = [
    "fecha", "Division", "Franchise", "Gender", "GAMA BOTINES", "PLATO",
    "Category", "Marketing Name (AR)", "Style", "Nike US Product Name",
    "PDP USA", "Retail URU (UYU)", "FX UYU/USD", "URU (USD)", "USA Full (USD)",
    "Dif FP vs USA", "USA + 21% IVA", "Dif FP + IVA", "BML c IVA",
    "USA + 21% + BF 8% (USD)", "Dif FP + 21% + BF", "BML c IVA + BF",
    "URU + Shp", "US + Shp", "Dif", "BML + Shp", "Pais",
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


def bml_label(diff: Optional[float], tol: float = BML_TOL) -> str:
    if diff is None:
        return "NO_US_DATA"
    if abs(diff) <= tol:
        return "MEET"
    if diff < -tol:
        return "BEAT"
    return "LOSE"


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

    # URU no usa Style confiable: se deriva con LEFT 6 de Product Code.
    df["Product Code"] = df["Product Code"].astype(str).fillna("").map(lambda s: s.strip().upper())
    df["Style"] = df["Product Code"].map(lambda s: s[:6] if len(s) >= 6 else s)

    df["_price"] = df["Retail Price"].apply(safe_float)
    df = df[df["_price"].notna() & (df["_price"] > 0)].copy()

    df["_stock"] = df["STOCK BL (Inventario Brandlive)"].apply(lambda x: safe_float(x) or 0.0)
    df["_stock_style_sum"] = df.groupby("Style")["_stock"].transform("sum")
    df = df[df["_stock_style_sum"] > 0].copy()

    for c in ["Style", "Product Code", "Marketing Name", "BU", "Category", "Gender", "Franchise", "SILO BOTINES", "PLATO"]:
        df[c] = df[c].astype(str).fillna("").map(lambda s: s.strip())

    log(f"StatusBooks URU listo: {len(df):,} filas")
    return df


def make_row_uru(
    run_date: str,
    division: str,
    franchise: str,
    category: str,
    marketing: str,
    style: str,
    us_name: str,
    pdp: str,
    retail_uyu: Optional[float],
    fx_uyu_per_usd: float,
    us_full: Optional[float],
    gender: str = "",
    silo: str = "",
    plato: str = "",
) -> Dict[str, Any]:
    uru_usd = (float(retail_uyu) / fx_uyu_per_usd) if (retail_uyu is not None and fx_uyu_per_usd) else None
    dif_fp_vs_us = (uru_usd / us_full - 1.0) if (uru_usd is not None and us_full) else None

    us_iva = us_full * (1.0 + IVA) if us_full is not None else None
    dif_fp_iva = (uru_usd / us_iva - 1.0) if (uru_usd is not None and us_iva) else None
    bml_iva = bml_label(dif_fp_iva)

    us_tax_bf = us_iva * (1.0 + BANK_FEES) if us_iva is not None else None
    dif_fp_tax_bf = (uru_usd / us_tax_bf - 1.0) if (uru_usd is not None and us_tax_bf) else None
    bml_tax_bf = bml_label(dif_fp_tax_bf)

    ship_uru_usd = None
    if retail_uyu is not None and fx_uyu_per_usd:
        ship_uru_uyu = 0.0 if float(retail_uyu) >= URU_FREE_SHIP_THRESHOLD_UYU else URU_SHIPPING_UYU
        ship_uru_usd = ship_uru_uyu / float(fx_uyu_per_usd)

    ship_us_usd = None
    if us_full is not None:
        ship_us_usd = 0.0 if us_full >= US_FREE_SHIP_THRESHOLD_USD else US_SHIPPING_USD

    uru_plus_shp = (uru_usd + ship_uru_usd) if (uru_usd is not None and ship_uru_usd is not None) else None
    us_plus_shp = (us_full + ship_us_usd) if (us_full is not None and ship_us_usd is not None) else None
    dif_shp = (uru_plus_shp / us_plus_shp - 1.0) if (uru_plus_shp is not None and us_plus_shp) else None
    bml_shp = bml_label(dif_shp)

    row = {
        "fecha": run_date,
        "Division": division,
        "Franchise": franchise,
        "Gender": gender,
        "GAMA BOTINES": silo,
        "PLATO": plato,
        "Category": category,
        "Marketing Name (AR)": marketing,
        "Style": style,
        "Nike US Product Name": us_name,
        "PDP USA": pdp,
        "Retail URU (UYU)": retail_uyu,
        "FX UYU/USD": fx_uyu_per_usd,
        "URU (USD)": uru_usd,
        "USA Full (USD)": us_full,
        "Dif FP vs USA": dif_fp_vs_us,
        "USA + 21% IVA": us_iva,
        "Dif FP + IVA": dif_fp_iva,
        "BML c IVA": bml_iva,
        "USA + 21% + BF 8% (USD)": us_tax_bf,
        "Dif FP + 21% + BF": dif_fp_tax_bf,
        "BML c IVA + BF": bml_tax_bf,
        "URU + Shp": uru_plus_shp,
        "US + Shp": us_plus_shp,
        "Dif": dif_shp,
        "BML + Shp": bml_shp,
        "Pais": "UY",
    }
    for c in TEMPLATE_COLS_URU:
        row.setdefault(c, None)
    return row


def convert_rows_to_uru(rows_us: List[Dict[str, Any]], fx_uyu: float) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    run_date = dt.datetime.now().strftime("%Y-%m-%d")
    for r in rows_us:
        out.append(
            make_row_uru(
                run_date=run_date,
                division=str(r.get("Division", "")).strip(),
                franchise=str(r.get("Franchise", "")).strip(),
                category=str(r.get("Category", "")).strip(),
                marketing=str(r.get("Marketing Name (AR)", "")).strip(),
                style=str(r.get("Style", "")).strip(),
                us_name=str(r.get("Nike US Product Name", "")).strip(),
                pdp=str(r.get("PDP USA", "")).strip(),
                retail_uyu=safe_float(r.get("Retail ARG (ARS)")),
                fx_uyu_per_usd=fx_uyu,
                us_full=safe_float(r.get("USA Full (USD)")),
                gender=str(r.get("Gender", "")).strip(),
                silo=str(r.get("GAMA BOTINES", "")).strip(),
                plato=str(r.get("PLATO", "")).strip(),
            )
        )
    return out


def write_xlsx(output_path: str, rows: List[Dict[str, Any]]):
    log(f"Escribiendo Excel: {output_path}")
    wb = xlsxwriter.Workbook(output_path)
    ws = wb.add_worksheet("Correccion")

    header_fmt = wb.add_format({"bold": True, "font_color": "white", "bg_color": "#1F4E79", "border": 1})
    text_fmt = wb.add_format({"align": "left", "valign": "vcenter"})
    money_local = wb.add_format({"num_format": '#,##0', "align": "left", "valign": "vcenter"})
    money_usd = wb.add_format({"num_format": "$#,##0.00", "align": "left", "valign": "vcenter"})
    fx_fmt = wb.add_format({"num_format": "#,##0.00", "align": "left", "valign": "vcenter"})
    pct_fmt = wb.add_format({"num_format": "0.00%", "align": "left", "valign": "vcenter"})
    link_fmt = wb.add_format({"font_color": "blue", "underline": 1})

    for j, c in enumerate(TEMPLATE_COLS_URU):
        ws.write(0, j, c, header_fmt)

    local_cols = {"Retail URU (UYU)"}
    usd_cols = {"URU (USD)", "USA Full (USD)", "USA + 21% IVA", "USA + 21% + BF 8% (USD)", "URU + Shp", "US + Shp"}
    pct_cols = {"Dif FP vs USA", "Dif FP + IVA", "Dif FP + 21% + BF", "Dif"}

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
            elif c == "FX UYU/USD":
                ws.write_number(i, j, float(v), fx_fmt) if v is not None else ws.write(i, j, "", text_fmt)
            else:
                ws.write(i, j, v if v is not None else "", text_fmt)

    for j, c in enumerate(TEMPLATE_COLS_URU):
        ws.set_column(j, j, min(60, max(10, len(c) + 2)))

    wb.close()


def main():
    start = time.time()
    log("=" * 60)
    log("Nike URU vs Nike US - Price Monitoring")
    log("=" * 60)

    try:
        uru_path = resolve_statusbooks_uru_path(STATUSBOOKS_URU_PATH)
        df_sb_uru = load_statusbooks_uru_for_scrape(uru_path)

        style_price_map = us.build_style_price_map(df_sb_uru)
        style_meta_map = us.build_style_meta_map(df_sb_uru)
        links_sheets = us.load_links_sheets(LINKS_PLP_PATH)

        fx_uyu = get_uyu_usd_venta()
        log(f"UYU/USD = {fx_uyu:.2f}")

        session = us.build_session()
        rows_us: List[Dict[str, Any]] = []

        rows_us.extend(us.build_nonfootball_output(session, df_sb_uru, style_price_map, style_meta_map, links_sheets, fx_uyu, dt.datetime.now().strftime("%Y-%m-%d")))
        rows_us.extend(us.build_kids_output(session, df_sb_uru, style_price_map, style_meta_map, links_sheets, fx_uyu, dt.datetime.now().strftime("%Y-%m-%d")))
        if us.SCRAPING_APPAREL:
            rows_us.extend(us.build_apparel_output(session, df_sb_uru, style_price_map, style_meta_map, fx_uyu, dt.datetime.now().strftime("%Y-%m-%d")))
        if us.SCRAPING_EQUIPMENT:
            rows_us.extend(us.build_equipment_output(session, df_sb_uru, style_price_map, style_meta_map, fx_uyu, dt.datetime.now().strftime("%Y-%m-%d")))
        rows_us.extend(us.build_football_output(session, df_sb_uru, style_price_map, style_meta_map, links_sheets, fx_uyu, dt.datetime.now().strftime("%Y-%m-%d")))

        rows_uru = convert_rows_to_uru(rows_us, fx_uyu)

        ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_xlsx = f"Nike_US_UY_{SEASON}_{ts}.xlsx"
        out_csv = f"Nike_US_UY_{SEASON}_{ts}.csv"

        write_xlsx(out_xlsx, rows_uru)
        pd.DataFrame(rows_uru)[TEMPLATE_COLS_URU].to_csv(out_csv, index=False, encoding="utf-8-sig")

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
