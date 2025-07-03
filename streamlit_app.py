import streamlit as st
import pandas as pd
import requests
import os
import time
import json
from datetime import datetime, timezone
import pytz

# ------------------- AUTHENTICATION -------------------
password = st.text_input("Enter password", type="password")

if password != st.secrets["app_password"]:
    st.stop()

# ------------------- API CONFIG -------------------
API_KEY = st.secrets["api_key"]
HEADERS = {"accept": "application/json", "key": API_KEY}


# ------------------- DATA FETCHING -------------------
@st.cache_data
def fetch_albaranes():
    url = "https://api.holded.com/api/invoicing/v1/documents/waybill"
    response = requests.get(url, headers=HEADERS)
    return pd.DataFrame(response.json())


@st.cache_data(ttl=86400)  # Cache for 24 hours
def fetch_all_products():
    BASE_URL = "https://api.holded.com/api/invoicing/v1/products"
    PAGE_SIZE = 100
    HEADERS = {"accept": "application/json", "key": st.secrets["api_key"]}
    BACKUP_FILE = "products_backup.json"

    all_products = []
    page = 1
    max_retries = 3
    delay_seconds = 0.3  # to avoid hammering the API

    try:
        while True:
            for attempt in range(max_retries):
                try:
                    resp = requests.get(
                        BASE_URL,
                        headers=HEADERS,
                        params={"page": page, "limit": PAGE_SIZE},
                    )
                    resp.raise_for_status()

                    data = resp.json()
                    chunk = data.get("data", data) if isinstance(data, dict) else data

                    if not chunk:
                        raise ValueError("Received empty response or unexpected format.")

                    all_products.extend(chunk)

                    if len(chunk) < PAGE_SIZE:
                        raise StopIteration  # No more pages

                    page += 1
                    time.sleep(delay_seconds)
                    break  # exit retry loop on success

                except requests.exceptions.RequestException as e:
                    st.warning(f"⚠️ Attempt {attempt + 1} failed for page {page}: {e}")
                    time.sleep(2 ** attempt)  # exponential backoff

            else:
                st.error("🚨 All retries failed. Loading from backup if available.")
                raise ConnectionError("API failed after retries")

    except (ConnectionError, StopIteration, ValueError):
        # Save to disk if we fetched at least one page
        if all_products:
            with open(BACKUP_FILE, "w", encoding="utf-8") as f:
                json.dump(all_products, f, ensure_ascii=False, indent=2)
            st.success("✅ Product data fetched and cached to backup.")
        elif os.path.exists(BACKUP_FILE):
            with open(BACKUP_FILE, "r", encoding="utf-8") as f:
                all_products = json.load(f)
            st.warning("⚠️ Using local backup data (products_backup.json)")
        else:
            st.error("❌ No product data available from API or backup.")
            return []

    return all_products


# ------------------- HELPERS -------------------
def build_origin_hs_lookup(all_products):
    """Builds a lookup dict keyed by product ID with Origin & HS Code."""
    lookup = {}
    for p in all_products:
        pid = p.get("id") or p.get("productId")
        if not pid:
            continue
        origin = hs_code = None
        for attr in p.get("attributes", []):
            name = attr.get("name", "").strip().lower()
            val = attr.get("value")
            if name == "origen":
                origin = val
            elif name == "taric":
                hs_code = val
        lookup[pid] = {"Origin": origin, "HS Code": hs_code}
    return lookup


def explode_order_row(df, row_idx, products_col="products", catalog_lookup={}):
    """Explodes the products list inside one albarán row into a flat DataFrame."""
    items = df.at[row_idx, products_col] or []
    records = []

    for item in items:
        sku = item.get("sku")
        name = item.get("name")
        units = item.get("units") or item.get("quantity")
        unit_price = item.get("price") or item.get("unitPrice")
        tax = 1 + (item.get("tax", 0) / 100)
        discount = 1 - (item.get("discount", 0) / 100)

        unit_price = (
            round(unit_price * discount, 2) if units is not None and unit_price is not None else None
        )
        subtotal = (
            round(units * unit_price, 2) if units is not None and unit_price is not None else None
        )
        total = round(subtotal * tax, 2) if subtotal is not None else None

        pid = item.get("productId")
        info = catalog_lookup.get(pid, {})
        origin = info.get("Origin")
        hs_code = info.get("HS Code")
        net_w = item.get("weight") or item.get("netWeight")
        t_net_w = net_w * units if net_w is not None and units is not None else None

        records.append(
            {
                "SKU": sku,
                "Item": name,
                "Units": units,
                "Unit Price": unit_price,
                "Subtotal": subtotal,
                "Total": total,
                "Origin": origin,
                "HS Code": hs_code,
                "Net W.": net_w,
                "T. Net W.": t_net_w,
            }
        )

    return pd.DataFrame(records)


# ------------------- STREAMLIT APP -------------------

st.title("Albarán Lookup")

doc_input = st.text_input("Enter Albarán DocNumber (e.g. A250245)")

if doc_input:
    # ❗ NORMALIZE INPUT TO BE CASE-INSENSITIVE
    doc_input_norm = doc_input.strip().upper()

    albaran_df = fetch_albaranes()
    all_products = fetch_all_products()
    catalog_lookup = build_origin_hs_lookup(all_products)

    # Ensure the docNumber column is string and compare in upper-case for case-insensitive search
    albaran_df["docNumber"] = albaran_df["docNumber"].astype(str)
    match_idx = albaran_df.index[albaran_df["docNumber"].str.upper() == doc_input_norm]

    if not match_idx.empty:
        row_idx = int(match_idx[0])
        company_name = albaran_df.loc[row_idx, "contactName"]

        
        contact_id = albaran_df.at[row_idx, "contact"]
        url = f"https://api.holded.com/api/invoicing/v1/contacts/{contact_id}"
        headers = {
            "accept": "application/json",
            "key": st.secrets["api_key"],
        }

        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()


        bill_address_str = albaran_df.loc[row_idx, 'shippingData']
        
        if pd.isnull(bill_address_str):
            bill = data.get("billAddress", {})
            bill_address_str = (
                f"{bill.get('address', '')}, {bill.get('postalCode', '').strip()}, "
                f"{bill.get('city', '')}, {bill.get('province', '')}, {bill.get('country', '')}"
            )

        st.subheader(f"Shipping Info for {albaran_df.loc[row_idx, 'docNumber']}")
        st.write(f"**Client**: {company_name}")
        st.write(f"**Billing Address**: {bill_address_str}")

        result_df = explode_order_row(albaran_df, row_idx, catalog_lookup=catalog_lookup)

        st.subheader("Product Table")
        st.dataframe(result_df)

        csv = result_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"albaran_{albaran_df.loc[row_idx, 'docNumber']}.csv",
            mime="text/csv",
        )
    else:
        st.warning(f"No document found with DocNumber: {doc_input}")


