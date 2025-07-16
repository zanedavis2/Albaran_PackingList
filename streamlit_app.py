import streamlit as st
import pandas as pd
import requests
import os
import time
import json
from datetime import datetime, timezone
import pytz
import io

# ------------------- AUTHENTICATION -------------------
password = st.text_input("Enter password", type="password")

if password != st.secrets["app_password"]:
    st.stop()


refresh = st.button("🔄 Refresh Data")
if refresh:
    st.cache_data.clear()
    st.success("✅ Cache cleared. Fresh data will be loaded.")

# ------------------- API CONFIG -------------------
API_KEY = st.secrets["api_key"]
HEADERS = {"accept": "application/json", "key": API_KEY}


# ------------------- DATA FETCHING -------------------
@st.cache_data
def fetch_albaranes():
    url = "https://api.holded.com/api/invoicing/v1/documents/waybill"
    response = requests.get(url, headers=HEADERS)
    return pd.DataFrame(response.json())


@st.cache_data(ttl=45000)  # Cache for 24 hours
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
        gross_w = p.get("weight")
        subcat = "Sin linea de productos"
        for attr in p.get("attributes", []):
            name = attr.get("name", "").strip().lower()
            val = attr.get("value")
            if name == "origen":
                origin = val
            elif name == "taric":
                hs_code = val
            elif name == "product line":
                subcat = val

        
        lookup[pid] = {"Origin": origin, "HS Code": hs_code, "SubCat" : subcat, "Attributes": p.get("attributes"), "Weight" : gross_w}
    return lookup


def explode_order_row(df, row_idx, products_col="products", catalog_lookup={}):
    """Explodes the products list inside one albarán row into a flat DataFrame grouped by subcategory with subtotal and sorting."""

    items = df.at[row_idx, products_col] or []
    grouped = {}

    for item in items:
        sku = item.get("sku")
        prod_name = item.get("name")        
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
        subcategory = info.get("SubCat")
        
        gross_w = info.get("Weight")
        t_gross_w = gross_w * units if gross_w is not None and units is not None else None

        
        attributes = info.get("Attributes") or []
        
        net_weight = None
        for attr in attributes:
            name = attr.get("name", "")
            raw_value = attr.get("value")
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if name == "Peso Neto":
                net_weight = value
                
        t_net_w = net_weight * units if net_weight is not None and units is not None else None
        subcategory = info.get("SubCat")

        

        row_data = {
            "SKU": sku,
            "Item": prod_name,
            "Units": units,
            "Unit Price": unit_price,
            "Subtotal": subtotal,
            "Total": total,
            "Origin": origin,
            "HS Code": hs_code,
            "Net W.": net_weight,
            "Total W.": t_net_w,
            "Gross W.": gross_w,
            "Total Gross W. ": t_gross_w
        }

        grouped.setdefault(subcategory, []).append(row_data)

    # Build sorted output
    output = []
    subtotals = []

    
    for subcat, products in grouped.items():
        if subcat is None:
            subcat = "Sin categoría"
        else:
            subcat = str(subcat).strip()
    
        df_sub = pd.DataFrame(products)
        df_sub["Subtotal"] = pd.to_numeric(df_sub["Subtotal"], errors="coerce")
        total_subtotal = df_sub["Subtotal"].sum(min_count=1)
    
        subtotals.append((subcat, products, total_subtotal))
    
    subtotals.sort(key=lambda x: x[0])
   
        
    for subcat, products, total_subtotal in subtotals:
        output.append({
            "SKU": "",
            "Item": f"——— {subcat} ———",
            "Units": "",
            "Unit Price": "",
            "Subtotal": "",
            "Total": "",
            "Origin": "",
            "HS Code": "",
            "Net W.": "",
            "Total W.": "",
            "Gross W.": "",
            "Total Gross W. ": ""
        })
        output.extend(products)

        df_group = pd.DataFrame(products)
        for col in ["Units", "Subtotal", "Total", "Total W."]:
            df_group[col] = pd.to_numeric(df_group[col], errors="coerce")

        output.append({
            "SKU": "",
            "Item": "                                            Subtotal",
            "Units": round(df_group["Units"].sum(min_count=1), 1),
            "Unit Price": "",
            "Subtotal": round(df_group["Subtotal"].sum(min_count=1), 2),
            "Total": round(df_group["Total"].sum(min_count=1), 2),
            "Origin": "",
            "HS Code": "",
            "Net W.": "",
            "Total W.": round(df_group["Total W."].sum(min_count=1), 3),
            "Gross W.": "",
            "Total Gross W. ": ""
        })

    return pd.DataFrame(output)



def explode_order_raw(df, row_idx, products_col="products", catalog_lookup={}):
    """Returns a flat product DataFrame without grouping or subtotals."""
    items = df.at[row_idx, products_col] or []
    rows = []

    for item in items:
        sku = item.get("sku")
        prod_name = item.get("name")
        gross_w = item.get("weight")
        units = item.get("units") or item.get("quantity")        
        t_gross_w = gross_w * units if gross_w is not None and units is not None else None
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
        gross_w = info.get("Weight")
        t_gross_w = gross_w * units if gross_w is not None and units is not None else None
        
        attributes = info.get("Attributes") or []
        net_weight = None
        for attr in attributes:
            name = attr.get("name", "")
            raw_value = attr.get("value")
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if name == "Peso Neto":
                net_weight = value
                
        t_net_w = net_weight * units if net_weight is not None and units is not None else None

        rows.append({
            "SKU": sku,
            "Item": prod_name,
            "Units": units,
            "Unit Price": unit_price,
            "Subtotal": subtotal,
            "Total": total,
            "Origin": origin,
            "HS Code": hs_code,
            "Net W.": net_weight,
            "Total W.": t_net_w,
            "Gross W.": gross_w,
            "Total Gross W. ": t_gross_w
        })

    return pd.DataFrame(rows)

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
        
        if not bill_address_str or str(bill_address_str).lower() == "nan":
            bill = data.get("billAddress", {})
            address = bill.get('address') or ''
            postal_code = (bill.get('postalCode') or '').strip()
            city = bill.get('city') or ''
            province = bill.get('province') or ''
            country = bill.get('country') or ''
            bill_address_str = f"{address}, {postal_code}, {city}, {province}, {country}"


        contact_email = data.get("email")
        if not contact_email:
            contact_email = "Not available"
            
        contact_phone = data.get("phone")
        if not contact_phone:
            contact_phone = "Not available"
            
        contact_mobile = data.get("mobile")
        if not contact_mobile:
            contact_mobile = "Not available"

        space = "  "
        st.subheader(f"Shipping Info for {albaran_df.loc[row_idx, 'docNumber']}")
        st.write(f"**Client**: {company_name}")
        st.write(f"**Email**: {contact_email}")
        st.write(f"**Phone**: {contact_phone}&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;**Mobile**: {contact_mobile}", unsafe_allow_html=True)
        st.write(f"**Billing Address**: {bill_address_str}")

        flat_df = explode_order_raw(albaran_df, row_idx, catalog_lookup=catalog_lookup)
        result_df = explode_order_row(albaran_df, row_idx, catalog_lookup=catalog_lookup)
        
        def highlight_subcategories(row):
            if str(row['Item']).startswith('——'):
                return ['font-weight: bold; background-color: #f0f0f0'] * len(row)
            if str(row['Item']).strip() == "Subtotal" or "Subtotal" in str(row['Item']):
                return ['font-weight: bold; text-align: right'] * len(row)
            return [''] * len(row)
        
        def format_spanish(num_format):
            def formatter(x):
                if isinstance(x, (int, float)):
                    return num_format.format(x).replace(",", "X").replace(".", ",").replace("X", ".")
                return x
            return formatter
        
        styled_df = (
            result_df
            .style
            .apply(highlight_subcategories, axis=1)
            .format({
                "Units": format_spanish("{:,.0f}"),
                "Unit Price": format_spanish("{:,.2f}"),
                "Subtotal": format_spanish("{:,.2f}"),
                "Total": format_spanish("{:,.2f}"),
                "Net W.": format_spanish("{:,.2f}"),
                "Total W.": format_spanish("{:,.2f}"),
                "Gross W.": format_spanish("{:,.2f}"),
                "Total Gross W. ": format_spanish("{:,.2f}")
            }, na_rep="—")
        )

        styled_df_raw = (
            flat_df
            .style
            .apply(highlight_subcategories, axis=1)
            .format({
                "Units": format_spanish("{:,.0f}"),
                "Unit Price": format_spanish("{:,.2f}"),
                "Subtotal": format_spanish("{:,.2f}"),
                "Total": format_spanish("{:,.2f}"),
                "Net W.": format_spanish("{:,.2f}"),
                "Total W.": format_spanish("{:,.2f}"),
                "Gross W.": format_spanish("{:,.2f}"),
                "Total Gross W. ": format_spanish("{:,.2f}"),
            }, na_rep="—")
        )

        st.subheader("Raw Product Table")
        st.write(styled_df_raw)
        raw_csv = flat_df.to_csv(index=False).encode("utf-8-sig")
        
        st.download_button(
            label="📥 Download CSV",
            data=raw_csv,
            file_name=f"albaran_raw_{albaran_df.loc[row_idx, 'docNumber']}.csv",
            mime="text/csv",
        )
                     
        st.subheader("Product Table (Sorted)")
        st.write(styled_df)
        
        file_name = f"albaran_sorted_{albaran_df.loc[row_idx, 'docNumber']}.xlsx"
        
        excel_buffer = io.BytesIO()
        
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            result_df.to_excel(writer, index=False, sheet_name='Sheet1')
        excel_buffer.seek(0)  # Reset buffer position
        
        # Download button
        st.download_button(
            label="📥 Download Excel",
            data=excel_buffer,
            file_name=file_name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.warning(f"No document found with DocNumber: {doc_input}")


