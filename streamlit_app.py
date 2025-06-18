import os
import json, ast
import requests
import pandas as pd
from datetime import datetime, timezone
import pytz
import streamlit as st

API_KEY = os.getenv('HOLDED_API_KEY', 'acd2e9953041d758c9ebd8802719cbac')
BASE_URL = 'https://api.holded.com/api/invoicing/v1/documents/'
HEADERS = {'accept': 'application/json', 'key': API_KEY}


def fetch_doc(doc_type: str) -> pd.DataFrame:
    url = f'{BASE_URL}{doc_type}'
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return pd.DataFrame(resp.json())


def parse_from_cell(x):
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            return json.loads(x)
        except Exception:
            return ast.literal_eval(x)
    return {}


def build_dataframe() -> pd.DataFrame:
    # Presupuestos (Estimates)
    presupuesto_df = fetch_doc('estimate')
    second_df = (
        presupuesto_df[['id', 'date', 'docNumber']]
        .rename(columns={'date': 'Presupuesto Date', 'docNumber': 'Presupuesto DocNum'})
    )

    # Proformas
    proforma_raw = fetch_doc('proform')
    proforma_raw['from_dict'] = proforma_raw['from'].apply(parse_from_cell)
    mask = proforma_raw['from_dict'].apply(lambda d: d.get('docType') == 'estimate')
    proforma_df = (
        proforma_raw.loc[mask, ['date', 'docNumber', 'from_dict', 'id']]
        .assign(from_id=lambda df: df['from_dict'].apply(lambda d: d.get('id')))
    )
    proforma_df = proforma_df[['date', 'docNumber', 'from_id', 'id']].rename(columns={
        'date': 'Proforma Date',
        'docNumber': 'Proforma DocNum',
        'id': 'prof_id',
        'from_id': 'id'
    })
    second_df = second_df.merge(proforma_df, on='id', how='left')

    # Pedidos (Sales Orders)
    pedido_raw = fetch_doc('salesorder')
    pedido_raw['from_dict'] = pedido_raw['from'].apply(parse_from_cell)
    mask = pedido_raw['from_dict'].apply(lambda d: d.get('docType') == 'proform')
    pedidos_df = (
        pedido_raw.loc[mask, ['date', 'docNumber', 'from_dict', 'id']]
        .assign(from_id=lambda df: df['from_dict'].apply(lambda d: d.get('id')))
    )
    pedidos_df = pedidos_df[['date', 'docNumber', 'from_id', 'id']].rename(columns={
        'date': 'Pedido Date',
        'docNumber': 'Pedido DocNum',
        'id': 'ped_id',
        'from_id': 'prof_id'
    })
    second_df = second_df.merge(pedidos_df[['prof_id', 'Pedido Date', 'Pedido DocNum', 'ped_id']],
                                on='prof_id', how='left')

    # Main sales order info (Cliente and Total)
    main_raw = fetch_doc('salesorder')
    main_df = main_raw[['id', 'contactName', 'date', 'total', 'docNumber']].copy()
    main_df = main_df.rename(columns={
        'contactName': 'Cliente',
        'total': 'Total',
        'docNumber': 'Original Pedido DocNum'
    })
    main_df = main_df.merge(second_df.drop(columns=['id']).rename(columns={'ped_id': 'id'}),
                            on='id', how='left')
    main_df = main_df.rename(columns={'date': 'Pedido Date'})

    # Albaranes (Waybills)
    albaran_raw = fetch_doc('waybill')
    albaran_raw['fromID'] = albaran_raw['from'].apply(lambda d: d.get('id') if isinstance(d, dict) else None)
    albaran_df = albaran_raw[['id', 'fromID', 'date', 'docNumber']].rename(columns={
        'id': 'alb_id',
        'fromID': 'id',
        'date': 'Albaran Date',
        'docNumber': 'Albaran DocNum'
    })
    main_df = main_df.merge(albaran_df[['id', 'Albaran Date', 'Albaran DocNum', 'alb_id']],
                            on='id', how='left')

    # Facturas (Invoices)
    factura_raw = fetch_doc('invoice')
    factura_raw['from_dict'] = factura_raw['from'].apply(parse_from_cell)
    mask = factura_raw['from_dict'].apply(lambda d: d.get('docType') == 'waybill')
    factura_df = factura_raw.loc[mask, ['date', 'docNumber', 'from_dict']].assign(
        from_id=lambda df: df['from_dict'].apply(lambda d: d.get('id'))
    )
    factura_df = factura_df[['date', 'docNumber', 'from_id']].rename(columns={
        'date': 'Factura Date',
        'docNumber': 'Factura DocNum',
        'from_id': 'alb_id'
    })
    main_df = main_df.merge(factura_df, on='alb_id', how='left')

    # Date conversions
    date_cols = ['Presupuesto Date', 'Proforma Date', 'Pedido Date', 'Albaran Date', 'Factura Date']

    for col in date_cols:
        if col in main_df.columns:
            main_df[col] = pd.to_datetime(main_df[col], unit='s', utc=True, errors='coerce')
            main_df[col] = main_df[col].dt.tz_convert('Europe/Madrid').dt.date

    main_df['Presupuesto → Proforma'] = (main_df['Proforma Date'] - main_df['Presupuesto Date']).dt.days
    main_df['Proforma → Pedido'] = (main_df['Pedido Date'] - main_df['Proforma Date']).dt.days
    main_df['Pedido → Albaran'] = (main_df['Albaran Date'] - main_df['Pedido Date']).dt.days
    main_df['Albaran → Factura'] = (main_df['Factura Date'] - main_df['Albaran Date']).dt.days

    ordered = [
        'Cliente', 'Total',
        'Presupuesto Date',
        'Presupuesto → Proforma',
        'Proforma Date',
        'Proforma → Pedido',
        'Pedido Date',
        'Pedido → Albaran',
        'Albaran Date',
        'Albaran → Factura',
        'Factura Date',
        'Presupuesto DocNum',
        'Proforma DocNum',
        'Pedido DocNum',
        'Albaran DocNum',
        'Factura DocNum',
        'Original Pedido DocNum'
    ]
    main_df['Total'] = main_df['Total'].round(2)
    return main_df[ordered]


@st.cache_data(show_spinner='Fetching data...')
def load_data():
    return build_dataframe()


st.title('Holded Document Pipeline')

if st.button('Update'):
    load_data.clear()

df = load_data()
st.dataframe(df, use_container_width=True)


