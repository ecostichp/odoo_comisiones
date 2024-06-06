import os
from pathlib import Path
import xmlrpc.client
import pandas as pd
from datetime import datetime, timedelta

def api_params_func(test_db: bool = False) -> dict:

    api_url = os.environ.get('ODOO_URL_API')
    api_db = os.environ.get('ODOO_DB_API')
    api_test_db = os.environ.get('ODOO_DB_PRUEBA_API')
    api_username = os.environ.get('ODOO_USERNAME_API')
    api_clave = os.environ.get('ODOO_CLAVE_API')


    api_params = {}
    if test_db:
        api_params['api_db'] = api_test_db
    else:
        api_params['api_db'] = api_db


    common = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/common')
    uid = common.authenticate(api_params['api_db'], api_username, api_clave, {})
    models = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/object')


    api_params['api_clave'] = api_clave
    api_params['api_uid'] = uid
    api_params['api_models'] = models

    return api_params

def search_pay_func(mes: int) -> list[str]:
    
    if type(mes) != int or mes < 1 or mes > 12:
        raise Exception (f'El mes es incorrecto. El párametro "mes" debe ser un número entero entre 1 y 12. Escribiste: {mes}')
    

    param_dia_ini = datetime(2024, mes, 1)
    param_dia_fin = datetime(2024, mes + 1, 1) - timedelta(days= 1)

    search_pay_acc = [
        "&", "&", "&",
            ("partner_type", "=", "customer"),
            ("is_internal_transfer", "=", False),
            ("state", "=", "posted"),
        "&",
            ("date", ">=", param_dia_ini.strftime('%Y-%m-%d')),
            ("date", "<=", param_dia_fin.strftime('%Y-%m-%d')),
        ]


    param_dia_ini_pos = param_dia_ini + timedelta(hours=7)
    param_dia_fin_pos = param_dia_fin + timedelta(hours=31) - timedelta(seconds= 1)
    
    search_pay_pos = [
        "&",
            ("payment_date", ">=", param_dia_ini_pos.strftime('%Y-%m-%d %H:%M:%S')),
            ("payment_date", "<=", param_dia_fin_pos.strftime('%Y-%m-%d %H:%M:%S')),
        ]
    

    return search_pay_acc, search_pay_pos

def api_call_pay_acc_func(api_params: dict, search_pay: list[str] ) -> list[dict]:
    
    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']


    search_pay_acc = search_pay[0]


    pay_acc_fields = [
                    'name',
                    'date',
                    'partner_id',
                    'amount',
                    'ref',
                    'reconciled_invoice_ids',
                    'pos_session_id'
                    ]

    pay_acc_ids = models.execute_kw(api_db, uid, api_clave, 'account.payment', 'search', [search_pay_acc])
    pay_acc_json = models.execute_kw(api_db, uid, api_clave, 'account.payment', 'read', [pay_acc_ids], {'fields': pay_acc_fields})

    
    return pay_acc_json

def pay_acc_df_func(pay_acc_json: list[dict]) -> list[pd.DataFrame, int]:
    
    data_pay_acc = []
    data_fact_ids = []

    for pay in pay_acc_json:
        if not pay['pos_session_id']:
            new = {}
            new['id'] = pay['id']
            new['name'] = pay['name']
            new['date'] = pay['date']
            new['partner_id'] = pay['partner_id']
            new['amount'] = pay['amount']
            new['ref'] = pay['ref'] if pay['ref'] else pd.NA
            new['pay_fact_docs'] = pay['reconciled_invoice_ids'] if pay['reconciled_invoice_ids'] else pd.NA
            
            data_fact_ids += pay['reconciled_invoice_ids']

            data_pay_acc.append(new)

    pay_acc_df = pd.DataFrame(data_pay_acc)


    pay_acc_df['date'] = pd.to_datetime(pay_acc_df['date'], format='%Y-%m-%d')
    
    return pay_acc_df, data_fact_ids

def api_call_pay_pos_func(api_params: dict, search_pay: list[str] ) -> list[dict]:
    
    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']


    search_pay_pos = search_pay[1]


    pay_pos_fields = [
                    'pos_order_id',
                    'payment_date',
                    'amount',
                    'payment_method_id'
                    ]

    pay_pos_ids = models.execute_kw(api_db, uid, api_clave, 'pos.payment', 'search', [search_pay_pos])
    pay_pos_json = models.execute_kw(api_db, uid, api_clave, 'pos.payment', 'read', [pay_pos_ids], {'fields': pay_pos_fields})
    

    return pay_pos_json

def pay_pos_df_func(pay_pos_json: list[dict]) -> list[pd.DataFrame, int]:
    
    data_pay_pos = []
    data_pos_ids = set()

    for pay in pay_pos_json:
        new = {}
        new['pay_pos_id'] = pay['id']
        new['pos_order_id'] = pay['pos_order_id'][0]
        new['pos_order_name'] = pay['pos_order_id'][1]
        new['date'] = pay['payment_date']
        new['amount'] = pay['amount']
        new['payment_method'] = pay['payment_method_id'][1]
    
        data_pos_ids.add(pay['pos_order_id'][0])
        data_pay_pos.append(new)

    pay_pos_df = pd.DataFrame(data_pay_pos)

    pay_pos_df['date'] = pd.to_datetime(pay_pos_df['date'], format='%Y-%m-%d %H:%M:%S')
    
    return pay_pos_df, list(data_pos_ids)

def api_call_pos_order_func(api_params: dict, data_pos_ids: list[int] ) -> list[dict]:
    
    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']

    pos_order_fields = [
                    'name',
                    'account_move',
                    'partner_id',
                    'session_id',
                    'amount_total'
                    ]

    pos_order_json = models.execute_kw(api_db, uid, api_clave, 'pos.order', 'read', [data_pos_ids], {'fields': pos_order_fields})

    return pos_order_json

def pos_order_func(pos_order_json: list[dict], data_fact_ids: list[int]) -> list[list[int]]:
    
    data_pos_amount_cero = [] 
  
    for pos in pos_order_json:
        
        if pos['account_move']:
            data_fact_ids.append(pos['account_move'][0])

        if pos['amount_total'] == 0:
            data_pos_amount_cero.append(pos)


    pos_amount_cero_df = pd.DataFrame(data_pos_amount_cero)
    
    if not pos_amount_cero_df.empty:
        pos_amount_cero_df['session_id'] = pos_amount_cero_df['session_id'].str.get(1)
        
    
    return data_fact_ids, pos_amount_cero_df

def api_call_acccount_docs_func(api_params: dict, data_fact_ids: list[int] ) -> list[dict]:
    
    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']

    data_fact_fields = [
                    'name',
                    'partner_id',
                    'date',
                    'invoice_payments_widget',
                    'amount_total',
                    'amount_residual'
                    ]

    fact_doc_json = models.execute_kw(api_db, uid, api_clave, 'account.move', 'read', [list(set(data_fact_ids))], {'fields': data_fact_fields})

    return fact_doc_json

def pay_fact_df_func(mes: int, fact_doc_json: dict) -> pd.DataFrame:
    
    data_pay_fact = []

    for fact in fact_doc_json:
        if fact['invoice_payments_widget']:
            for pay in fact['invoice_payments_widget']['content']:
                if 'Facturas' not in pay['journal_name'] and datetime.strptime(pay['date'], '%Y-%m-%d').month == mes:
                    new = {}
                    new['fact_doc_id'] = fact['id']
                    new['fact_doc_name'] = fact['name']
                    new['fact_doc_cliente'] = fact['partner_id'][0]
                    new['fact_doc_date'] = fact['date']
                    new['fact_doc_total'] = fact['amount_total']
                    new['fact_doc_deuda'] = fact['amount_residual']
                    new['pay_journal'] = pay['journal_name']

                    if 'PdV ' in pay['ref']:
                        i = pay['ref'].find('PdV ')
                        new['ref'] = pay['ref'][i:i+12]
                    elif 'Shop' in pay['ref']:
                        i = pay['ref'].find('Shop')
                        new['ref'] = pay['ref'][i:i+9]
                    else:
                        new['ref'] = pd.NA

                    new['pay_amount'] = pay['amount']
                    new['pay_date'] = pay['date']
                    new['pay_pos'] = pay['pos_payment_name'] if pay['pos_payment_name'] else pd.NA
        
                    data_pay_fact.append(new)

    pay_fact_df = pd.DataFrame(data_pay_fact)

    pay_fact_df.loc[~pay_fact_df['pay_journal'].str.contains('Punto De Venta'), ['ref']] = pd.NA

    pay_fact_df['fact_doc_date'] = pd.to_datetime(pay_fact_df['fact_doc_date'], format='%Y-%m-%d')
    pay_fact_df['pay_date'] = pd.to_datetime(pay_fact_df['pay_date'], format='%Y-%m-%d')

    return pay_fact_df

def pagos_faltantes_check_func(pay_acc_df: pd.DataFrame, pos_amount_cero_df: pd.DataFrame, pay_pos_df: pd.DataFrame) -> bool:
    
    pagos_faltantes = []

    if not pos_amount_cero_df.empty:
        acc_cobranza_pdv_df = pay_acc_df[(~pay_acc_df['ref'].isna()) & (pay_acc_df['ref'].str[:1] != 'F')]
        pdv_cobranza_df = pos_amount_cero_df.merge(pay_pos_df, how='left', left_on='id', right_on='pos_order_id')

        for i in range(len(pdv_cobranza_df)):
            mini_df = acc_cobranza_pdv_df.loc[
                (acc_cobranza_pdv_df['ref'].str.contains(pdv_cobranza_df['name'].iloc[i])) 
                | (acc_cobranza_pdv_df['ref'].str.contains(pdv_cobranza_df['session_id'].iloc[i])) 
                & (acc_cobranza_pdv_df['amount'] == pdv_cobranza_df['amount'].iloc[i])
                & (acc_cobranza_pdv_df['partner_id'].str.get(0) == pdv_cobranza_df['partner_id'].iloc[i][0])]

            if mini_df.empty:
                new = {}
                new['pos_doc_id'] = pdv_cobranza_df.iloc[i]['id']
                new['pos_doc_name'] = pdv_cobranza_df.iloc[i]['name']
                new['cliente'] = pdv_cobranza_df.iloc[i]['partner_id'][1]
                new['monto'] = pdv_cobranza_df.iloc[i]['amount']
                new['fecha'] = pdv_cobranza_df.iloc[i]['date']- timedelta(hours=7)

                pagos_faltantes.append(new)

    pagos_faltantes_df = pd.DataFrame(pagos_faltantes)

    if pagos_faltantes_df.empty:
        return True

    archivo = 'Cobranza_PdV_Faltante'
    path = Path.home().joinpath(f'Desktop/{archivo}.xlsx')
    pagos_faltantes_df.to_excel(path)

def cobranza_mes_func(mes: int, test_db: bool = False) -> pd.DataFrame:

    api_params = api_params_func(test_db)
    search_pay = search_pay_func(mes)

    pay_acc_json = api_call_pay_acc_func(api_params, search_pay)
    pay_acc_df, data_fact_ids2 = pay_acc_df_func(pay_acc_json)

    pay_pos_json = api_call_pay_pos_func(api_params, search_pay)
    pay_pos_df, data_pos_ids = pay_pos_df_func(pay_pos_json)

    pos_order_json = api_call_pos_order_func(api_params, data_pos_ids)
    data_fact_ids, pos_amount_cero_df = pos_order_func(pos_order_json, data_fact_ids2)

    pagos_faltantes_check = pagos_faltantes_check_func(pay_acc_df, pos_amount_cero_df, pay_pos_df)

    if pagos_faltantes_check:
        fact_doc_json = api_call_acccount_docs_func(api_params, data_fact_ids)
        pay_fact_df = pay_fact_df_func(mes, fact_doc_json)
        
        return pay_fact_df

    else:
        print('Corrige los pagos de cobranza PdV faltantes, el archivo está en tu escritorio')
