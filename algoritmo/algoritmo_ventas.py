import os
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


def if_list_gt0_idex (item: dict, key: str) -> None | int:
        val = item[key]

        if val:
            if len(val) == 0:
                return None
            else:
                return val[0]
        else:
            return None


def search_fact_func(mes: int, dias: None | list[int] = None, vendedor: None | int  = None) -> list[str]:
    
    if type(mes) != int or mes < 1 or mes > 12:
        raise Exception (f'El mes es incorrecto. El párametro "mes" debe ser un número entero entre 1 y 12. Escribiste: {mes}')
    

    if dias is None:
        param_dia_ini = datetime(2024, mes, 1)
        param_dia_fin = datetime(2024, mes + 1, 1) - timedelta(days= 1)
    
    elif len(dias) != 2:
        raise Exception (f'El párametro "días_del_mes" debe ser una lista de 2 elementos. El día inicial de búsqueda se escribe en el índice 0 de la lista, el día final de búsqueda en el índice 1. La lista tiene: {len(dias)} de elementos')
    
    elif type(dias[0]) != int or type(dias[1]) != int:
        raise Exception (f'El párametro "día_inicial_de_búsqueda" y "día_final_de_búsqueda" sólo pueden ser números enteros.')

    elif dias[0] > dias[1]:
        raise Exception (f'El párametro "día_inicial_de_búsqueda" no debe ser mayor al parámetro "día_final_de_búsqueda". El día inicial de búsqueda se escribe en el índice 0 de la lista, el día final de búsqueda en el índice 1.')
    
    else:
        try:
            param_dia_ini = datetime(2024, mes, dias[0])
        except:
            raise Exception (f'El párametro "día_inicial_de_búsqueda" es incorrecto. La fecha "día: {dias[0]}, mes: {mes}, año 2024" no existe')
        try:
            param_dia_fin = datetime(2024, mes, dias[1])
        except:
            raise Exception (f'El párametro "día_final_de_búsqueda" es incorrecto. La fecha "día: {dias[1]}, mes: {mes}, año 2024" no existe')


    search_fact_all = [
        "&",
            ("state", "=", "posted"),
        "&", "&",
            ("invoice_date", ">=", param_dia_ini.strftime('%Y-%m-%d')),
            ("invoice_date", "<=", param_dia_fin.strftime('%Y-%m-%d')),
            ("journal_id", "in", [10, 90, 30, 97])
        ]

    search_fact_vendedor = [
        "&",
            ("state", "=", "posted"),
        "&", "&",
            ("invoice_date", ">=", param_dia_ini.strftime('%Y-%m-%d')),
            ("invoice_date", "<=", param_dia_fin.strftime('%Y-%m-%d')),
        "&",
            ("journal_id", "in", [10, 90, 30, 97]),
        "|",
            ("invoice_user_id", "=", vendedor),
            ("pos_order_ids.lines.sale_order_origin_id.user_id", "=", vendedor)]


    if vendedor == None:
        return search_fact_all
    
    if type(vendedor) != int:
        raise Exception (f'El párametro "Vendedor" debe ser un número entero. Escribiste: {vendedor}')
    
    return search_fact_vendedor


def api_call_func(api_params: dict, search_fact: list ) -> list[int | dict]:
    
    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']


    search_fact = search_fact


    fact_doc_fields = [
                    'name',
                    'invoice_date',
                    'state',
                    'reversed_entry_id',
                    'reversal_move_id',
                    'journal_id',
                    'company_id',
                    'invoice_origin',
                    'pos_order_ids',
                    'line_ids',
                    'partner_id',
                    'move_type',
                    'invoice_user_id'
                    ]

    fact_doc_ids1 = models.execute_kw(api_db, uid, api_clave, 'account.move', 'search', [search_fact])
    fact_doc_ids2 = models.execute_kw(api_db, uid, api_clave, 'account.move', 'search', [[("reversal_move_id", "=", fact_doc_ids1)]])
    fact_doc_ids = list(set(fact_doc_ids1) | set(fact_doc_ids2))
    fact_doc_ids.sort()

    fact_doc_json = models.execute_kw(api_db, uid, api_clave, 'account.move', 'read', [fact_doc_ids], {'fields': fact_doc_fields})

    fact_line_ids = []
    pos_doc_ids1 = []
    invoice_origin_names = []

    for fact in fact_doc_json:
        if fact['pos_order_ids']:
            pos_doc_ids1.append(fact['pos_order_ids'][0])
        
        if fact['invoice_origin']:
            if fact['invoice_origin'][:2] in ['Pd', 'Sh']:
                invoice_origin_names.append(fact['invoice_origin'])

        fact_line_ids += fact['line_ids']


    fact_line_fields = [
            'product_id',
            'quantity',
            'price_unit',
            'discount',
            'account_id',
            'price_subtotal',
        ]


    fact_line_ids.sort()
    fact_line_json = models.execute_kw(api_db, uid, api_clave, 'account.move.line', 'read', [fact_line_ids], {'fields': fact_line_fields})


    pos_line_fields = [
        'name',
        'order_id',
        'sale_order_origin_id',
        'refunded_orderline_id',
    ]

    pos_doc_ids2 = models.execute_kw(api_db, uid, api_clave, 'pos.order', 'search', [[("name", "in", invoice_origin_names)]])
    pos_doc_ids = list(set(pos_doc_ids1) | set(pos_doc_ids2))
    pos_doc_ids.sort()

    pos_line_ids1 = models.execute_kw(api_db, uid, api_clave, 'pos.order.line', 'search', [[("order_id.id", "=", pos_doc_ids)]])
    pos_line_ids2 = models.execute_kw(api_db, uid, api_clave, 'pos.order.line', 'search', [[("refund_orderline_ids", "=", pos_line_ids1)]])
    pos_line_ids = list(set(pos_line_ids1) | set(pos_line_ids2))
    pos_line_ids.sort()
    pos_line_json = models.execute_kw(api_db, uid, api_clave, 'pos.order.line', 'read', [pos_line_ids], {'fields': pos_line_fields})


    sale_doc_ids_each_line = []
    for pos in pos_line_json:
        if pos['sale_order_origin_id']:
            sale_doc_ids_each_line.append(pos['sale_order_origin_id'][0])

    sale_doc_ids = list(set(sale_doc_ids_each_line))
    sale_doc_ids.sort()


    sale_doc_fields = [
        'user_id',
    ]

    sale_doc_json = models.execute_kw(api_db, uid, api_clave, 'sale.order', 'read', [sale_doc_ids], {'fields': sale_doc_fields})

    return fact_doc_ids1, fact_doc_json, fact_line_json, pos_line_json, sale_doc_json


def fact_doc_df_fun(fact_doc_ids: list[int], fact_doc_json: list[dict]) -> pd.DataFrame:
    
    data_fact = []

    for fact in fact_doc_json:
        if fact['id'] in fact_doc_ids:
            for line in fact['line_ids']:
                new = {}
                new['fact_doc_id'] = fact['id']
                new['name'] = fact['name']
                new['invoice_date'] = fact['invoice_date']
                new['state'] = fact['state']
                new['invoice_origin'] = fact['invoice_origin']
                new['module_origin'] = None
                new['pos_doc_id'] = if_list_gt0_idex(fact, 'pos_order_ids')
                new['move_type'] = fact['move_type']
                new['reversal_move_id'] = if_list_gt0_idex(fact, 'reversal_move_id')
                new['reversed_entry_id'] = if_list_gt0_idex(fact, 'reversed_entry_id')
                new['journal_id'] = fact['journal_id'][0]
                new['company_id'] = fact['company_id'][0]
                new['partner_id'] = fact['partner_id'][0]
                new['invoice_user_id'] = fact['invoice_user_id'][0]
                new['fact_line_id'] = line

                if not fact['invoice_origin']:
                    new['module_origin'] = 'Contabilidad'
                elif fact['invoice_origin'][:2] in ['Pd', 'Sh']:
                        new['module_origin'] = 'PdV'
                elif fact['invoice_origin'][0] == 'S':
                        new['module_origin'] = 'Ventas'

                data_fact.append(new)



    fact_doc_df = pd.DataFrame(data_fact)


    fact_doc_df.loc[fact_doc_df['invoice_origin'] == False , ['invoice_origin',]] = pd.NA
    fact_doc_df['invoice_date'] = pd.to_datetime(fact_doc_df['invoice_date'], format='%Y-%m-%d')
    
    for col in fact_doc_df.columns:
         if fact_doc_df[col].dtype in ['int64', 'float64']:
              fact_doc_df[col] = fact_doc_df[col].astype('Int64')

    return fact_doc_df


def fact_line_df_fun(fact_line_json: list[dict]) -> pd.DataFrame:

    data_line_fact = []

    for fact_line in fact_line_json:
        if fact_line['account_id'] and fact_line['account_id'][0] in [85, 197]:
            new = {}
            new['fact_line_id'] = fact_line['id']
            new['product_id'] = fact_line['product_id'][0]
            new['quantity'] = fact_line['quantity']
            new['price_unit'] = fact_line['price_unit']
            new['discount'] = fact_line['discount'] / 100
            new['price_subtotal'] = fact_line['price_subtotal']

            data_line_fact.append(new)


    fact_line_df = pd.DataFrame(data_line_fact)


    fact_line_df.loc[fact_line_df['product_id'] == False, ['product_id',]] = pd.NA
    fact_line_df['fact_line_id'] = fact_line_df['fact_line_id'].astype('Int64')
    fact_line_df['product_id'] = fact_line_df['product_id'].astype('Int64')
    
    return fact_line_df


def pos_line_df_fun(pos_line_json: list[dict]) -> pd.DataFrame:

    data_pos_line = []

    for pos in pos_line_json:
        new = {}
        new['pos_line_id'] = pos['id']
        new['pos_doc_id'] = pos['order_id'][0]
        new['sale_doc_id'] = if_list_gt0_idex(pos, 'sale_order_origin_id')
        new['refunded_orderline_id'] = if_list_gt0_idex(pos, 'refunded_orderline_id')
        

        data_pos_line.append(new)

    pos_line_df = pd.DataFrame(data_pos_line)
    
    for col in pos_line_df.columns:
         if pos_line_df[col].dtype in ['int64', 'float64']:
              pos_line_df[col] = pos_line_df[col].astype('Int64')

    
    # Para copiar el 'sale_doc_id' de la línea origial de venta a la línea de devolución que no tiene.
    for id in pos_line_df.loc[(pos_line_df['sale_doc_id'].isna()) & (~pos_line_df['refunded_orderline_id'].isna())]['refunded_orderline_id'].unique():
        pos_line_df.loc[(pos_line_df['refunded_orderline_id'] == id), 'sale_doc_id'] = pos_line_df.loc[(pos_line_df['pos_line_id'] == id), 'sale_doc_id'].iloc[0]

    # Para copiar el 'sale_doc_id' de una línea hermana con el mismo pos_doc_id.
    pos_line_id_not_sale_doc, pos_doc_id_not_sale_doc = pos_line_df.loc[pos_line_df['sale_doc_id'].isna(), ['pos_line_id', 'pos_doc_id']].items()
    for i in range(len(pos_line_id_not_sale_doc[1])):
        pos_line_df.loc[(pos_line_df['pos_line_id'] == pos_line_id_not_sale_doc[1].iloc[i]), 'sale_doc_id'] = pos_line_df.loc[(pos_line_df['pos_doc_id'] == pos_doc_id_not_sale_doc[1].iloc[i]), 'sale_doc_id'].iloc[0]


    return pos_line_df


def sale_doc_df_fun(sale_doc_json: list[dict]) -> pd.DataFrame:

    data_sale_doc = []

    for sale in sale_doc_json:
        new = {}
        new['sale_doc_id'] = sale['id']
        new['salesperson_id'] = sale['user_id'][0]

        data_sale_doc.append(new)


    sale_doc_df = pd.DataFrame(data_sale_doc)
    for col in sale_doc_df.columns:
        sale_doc_df[col] = sale_doc_df[col].astype('Int64')

    return sale_doc_df


def fact_df_func(fact_doc_df: pd.DataFrame, fact_line_df: pd.DataFrame) -> pd.DataFrame:

    fact_df = fact_doc_df.merge(fact_line_df, how='left', on='fact_line_id')
    fact_df.loc[fact_df['move_type'] == 'out_refund', ['quantity', 'price_subtotal']] = fact_df.loc[fact_df['move_type'] == 'out_refund', ['quantity', 'price_subtotal']] * -1
    index_to_drop = fact_df[fact_df['product_id'].isna()].index
    fact_df.drop(index_to_drop, inplace=True)

    return fact_df


def pos_sale_df_func(pos_line_df: pd.DataFrame, sale_doc_df: pd.DataFrame) -> list[pd.DataFrame]:
    pos_sale_df = pos_line_df.merge(sale_doc_df, how='left', on='sale_doc_id')

    saleperson_FvsM = pos_sale_df.loc[:, ['pos_doc_id', 'salesperson_id']].groupby('pos_doc_id').agg({'salesperson_id':['first', 'mean']})
    saleperson_FvsM['is_different'] = saleperson_FvsM[('salesperson_id', 'first')] != saleperson_FvsM[('salesperson_id', 'mean')]

    pos_doc_equal_ids_df = saleperson_FvsM.loc[saleperson_FvsM['is_different'] == False, [('salesperson_id', 'first')]].reset_index()
    pos_doc_equal_ids_df.columns = ['pos_doc_id', 'salesperson_id']

    pos_doc_different_ids = ((saleperson_FvsM.loc[saleperson_FvsM['is_different'] == True])).reset_index()['pos_doc_id']
    pos_doc_different_ids_df = pos_sale_df.loc[pos_sale_df['pos_doc_id'].isin(pos_doc_different_ids), ['pos_line_id', 'pos_doc_id', 'salesperson_id']]

    return pos_doc_equal_ids_df, pos_doc_different_ids_df


def complete_df_func(api_params: dict, fact_df: pd.DataFrame, pos_doc_equal_ids_df: pd.Series, pos_doc_different_ids_df: pd.Series) -> pd.DataFrame:
    
    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']


    # Se complementa el campo 'salesperson_id' de todos los fact_doc_id que tienen en su contraparte pos_doc_id un mismo vendedor
    complete_df = fact_df.merge(pos_doc_equal_ids_df, how='left', on='pos_doc_id')
    
    
    # Se complementa el campo 'salesperson_id' de todos los fact_doc_id que provienen de los módulos de origen 'Contabilidad' y 'Ventas'
    complete_df.loc[complete_df['module_origin'] != 'PdV', 'salesperson_id'] = complete_df.loc[complete_df['module_origin'] != 'PdV', 'invoice_user_id']
    

    # Se complementa el campo 'salesperson_id' de todos los fact_doc_id que tienen en su contraparte pos_doc_id un vendedor diferente en alguna de sus líneas
    if not pos_doc_different_ids_df.empty:
        for id in pos_doc_different_ids_df['pos_doc_id'].unique():
            if len(complete_df.loc[complete_df['pos_doc_id'] == id]) == len(pos_doc_different_ids_df.loc[pos_doc_different_ids_df['pos_doc_id'] == id]):
                complete_df.loc[complete_df['pos_doc_id'] == id, 'salesperson_id'] = list(pos_doc_different_ids_df.loc[pos_doc_different_ids_df['pos_doc_id'] == id, 'salesperson_id'])
    
    
    # Se complementa al campo 'salesperson_id' de las líneas tipo 'out_refund' que no tienen un pos_doc_id. Se trata de buscar la línea de la factura original que coincide con el producto para copiar el 'salesperson_id' a la línea de devolución.
    fact_line_id_not_saleline, reversed_entry_id_not_saleline, product_id_not_saleline = complete_df.loc[(complete_df['salesperson_id'].isna()) & (complete_df['move_type'] == 'out_refund'), ['fact_line_id', 'reversed_entry_id', 'product_id']].items()

    for i in range(len(fact_line_id_not_saleline[1])):
        if not complete_df.loc[complete_df['fact_doc_id'] == reversed_entry_id_not_saleline[1].iloc[i]].empty:
            complete_df.loc[complete_df['fact_line_id'] == fact_line_id_not_saleline[1].iloc[i], 'salesperson_id'] = complete_df.loc[(complete_df['fact_doc_id'] == reversed_entry_id_not_saleline[1].iloc[i]) & (complete_df['product_id'] == product_id_not_saleline[1].iloc[i]), 'salesperson_id'].iloc[0]

    
    # Se complementa al campo 'salesperson_id' de las líneas restantes. No tienen un pos_doc_id y el 'invoice_origin' no está en los json. Ha que llamar a la API_Odoo de nuevo.
    pos_doc_names_not_sale_doc, product_id_not_sale_doc = complete_df.loc[complete_df['salesperson_id'].isna(), ['invoice_origin', 'product_id']].items()

    pos_line_not_found_json = models.execute_kw(api_db, uid, api_clave, 'pos.order.line', 'search_read', [["&", ("order_id.name", "in", list(pos_doc_names_not_sale_doc[1])), ("product_id", "in", list(product_id_not_sale_doc[1].astype(int)))]], {'fields': ['order_id', 'product_id', 'sale_order_line_id']})

    pos_line_not_found_ids = []
    for pos in pos_line_not_found_json:
        if pos['sale_order_line_id']:
            pos_line_not_found_ids.append( pos['sale_order_line_id'][0])

    sale_pos_line_not_found_json = models.execute_kw(api_db, uid, api_clave, 'sale.order.line', 'read', [pos_line_not_found_ids], {'fields': ['salesman_id']})

    for i in range(len(pos_doc_names_not_sale_doc[1])):
        for pos in pos_line_not_found_json:
            if pos_doc_names_not_sale_doc[1].iloc[i] == pos['order_id'][1] and product_id_not_sale_doc[1].iloc[i] == pos['product_id'][0]:
                for sale in sale_pos_line_not_found_json:
                    if pos['sale_order_line_id']:
                        if pos['sale_order_line_id'][0] == sale['id']:
                            complete_df.loc[(complete_df['salesperson_id'].isna()) & (complete_df['invoice_origin'] == pos_doc_names_not_sale_doc[1].iloc[i]) & (complete_df['product_id'] == product_id_not_sale_doc[1].iloc[i]), 'salesperson_id'] = sale['id']
            
    return complete_df


def ventas_df_func(mes: int, dias: None | list[int] = None, vendedor: None | int  = None) -> pd.DataFrame:
    api_params = api_params_func()
    search_fact = search_fact_func(mes, dias, vendedor)
    
    fact_doc_ids, fact_doc_json, fact_line_json, pos_line_json, sale_doc_json = api_call_func(api_params, search_fact)
    fact_doc_df = fact_doc_df_fun(fact_doc_ids, fact_doc_json)
    fact_line_df = fact_line_df_fun(fact_line_json)
    pos_line_df = pos_line_df_fun(pos_line_json)
    sale_doc_df = sale_doc_df_fun(sale_doc_json)

    fact_df = fact_df_func(fact_doc_df, fact_line_df)
    pos_doc_equal_ids_df, pos_doc_different_ids_df = pos_sale_df_func(pos_line_df, sale_doc_df)

    complete_df = complete_df_func(api_params, fact_df, pos_doc_equal_ids_df, pos_doc_different_ids_df)

    return complete_df
