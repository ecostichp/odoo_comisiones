import os
import xmlrpc.client
import pandas as pd

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

def api_call_purchase_orders_func(api_params: dict, compras: list[str]) -> list[ list[dict] | list[str]]:

    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']


    upper_compras = list(map(lambda x: x.upper(), set(compras)))

    fields_purchase_order = ['state', 'name', 'x_fecha_factura']
    purchase_order_ids = models.execute_kw(api_db, uid, api_clave, 'purchase.order', 'search', [[("name", "in", upper_compras)]])
    purchase_order_json = models.execute_kw(api_db, uid, api_clave, 'purchase.order', 'read', [purchase_order_ids], {'fields':fields_purchase_order})

    return purchase_order_json, upper_compras

def api_call_purchase_lines_func(api_params: dict, purchase_ids: list[int]) -> list[dict]:

    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']


    fields_purchase_lines = ['product_id', 'price_unit_discounted', 'order_id']
    purchase_lines_ids = models.execute_kw(api_db, uid, api_clave, 'purchase.order.line', 'search', [[("order_id", "in", purchase_ids)]])
    purchase_lines_json = models.execute_kw(api_db, uid, api_clave, 'purchase.order.line', 'read', [purchase_lines_ids], {'fields':fields_purchase_lines})


    return purchase_lines_json

def api_call_product_product_func(api_params: dict, data_product_ids: list[int]) -> list[dict]:

    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']


    fields_product_product = ['product_tmpl_id', 'x_factor_utilidad']
    product_product_json = models.execute_kw(api_db, uid, api_clave, 'product.product', 'read', [data_product_ids], {'fields':fields_product_product})


    return product_product_json

def purchases_check_func(purchase_order_json: list[dict], upper_compras: list[str]) -> list[bool, list[int], list[dict]]:

    data_purchase_orders = []
    purchase_ids = []
    not_state_purchase = []
    not_fecha_factura_found = []
    compra_names = []


    for p in purchase_order_json:
       
        new = {}
        new['order_id'] = p['id']
        new['fecha_factura'] = p['x_fecha_factura']
        data_purchase_orders.append(new)
        
        purchase_ids.append(p['id'])
        compra_names.append(p['name'])

        if p['state'] != 'purchase':
            not_state_purchase.append(p['name'])

        if not p['x_fecha_factura']:
            not_fecha_factura_found.append(p['name'])



    not_compra_found = []
    for c in upper_compras:
        if c not in compra_names:
            not_compra_found.append(c)


    if not_compra_found or not_state_purchase or not_fecha_factura_found:
        print('Existen errores en la lista de compras, por favor corrígelos: \n')
        
        if not_compra_found:
            print('\t Las siguientes compras no son válidas:', not_compra_found)
        if not_state_purchase:
            print('\t Las siguientes compras no tienen el estado como "Orden de compra":', not_state_purchase)
        if not_fecha_factura_found:
            print('\t Las siguientes compras no cuentan con fecha en el campo "Fecha factura":', not_fecha_factura_found)

        return False, 0, 0
    
    else:
        return True, purchase_ids, data_purchase_orders

def purchase_lines_to_dict_func(purchase_lines_json: list[dict]) -> list[dict, int]:

    data_purchase_lines = []
    data_product_ids = []

    for purchase in purchase_lines_json:
        new = {}
        new['purchase_line_id'] = purchase['id']
        new['product_id_pp'] = purchase['product_id'][0]
        new['purchase_cost'] = purchase['price_unit_discounted']
        new['order_id'] = purchase['order_id'][0]

        data_product_ids.append(purchase['product_id'][0])
        data_purchase_lines.append(new)

    data_product_ids = list(set(data_product_ids))

    return data_purchase_lines, data_product_ids

def product_product_to_dict_func(product_product_json: list[dict]) -> list[dict]:

    data_product_lines = []

    for product in product_product_json:
        new = {}
        new['product_id'] = product['product_tmpl_id'][0]
        new['product_id_pp'] = product['id']
        new['factor_utilidad'] = product['x_factor_utilidad']

        data_product_lines.append(new)

    return data_product_lines

def complete_precioycosto_df_func(data_purchase_orders: list[dict], data_purchase_lines: list[dict], data_product_lines: list[dict]) -> pd.DataFrame:
    
    purchase_orders_df = pd.DataFrame(data_purchase_orders)
    purchase_orders_df['fecha_factura'] = pd.to_datetime(purchase_orders_df['fecha_factura'], format='%Y-%m-%d')
    
    purchase_lines_df = pd.DataFrame(data_purchase_lines)
    
    purchase_df1 = purchase_lines_df.merge(purchase_orders_df, how='left', on='order_id')
    purchase_df = purchase_df1.loc[purchase_df1.sort_values(by='fecha_factura', ascending=False)['product_id_pp'].drop_duplicates().index]

    data_product_lines_df = pd.DataFrame(data_product_lines)
    
    complete_precioycosto_df = purchase_df.merge(data_product_lines_df, how='left', on='product_id_pp')

    complete_precioycosto_df['product_price'] = complete_precioycosto_df['purchase_cost'] * complete_precioycosto_df['factor_utilidad']
    
    return complete_precioycosto_df

def api_write_new_product_data_func(api_params: dict, complete_precioycosto_df: pd.DataFrame) -> str:

    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']

    purchase_line_id_items,	product_id_pp_items, purchase_cost_items, order_id_items, fecha_factura_items, product_id_items, factor_utilidad_items, product_price_items = complete_precioycosto_df.items()

    for i in range(len(purchase_line_id_items[1])):
        try:
            models.execute_kw(api_db, uid, api_clave, 'product.template', 'write', [[int(product_id_items[1].iloc[i])], {'standard_price': float(purchase_cost_items[1].iloc[i]), 'x_fecha_costo': fecha_factura_items[1].iloc[i].strftime(format='%Y-%m-%d'), 'x_tipo_costo': 'ultima_compra', 'list_price': float(product_price_items[1].iloc[i])}])
        except Exception as e:
            print('Error en el product_id', product_id_items[1].iloc[i], 'de product.template')
            print(e)
            return
    
    print('Se ingresaron con éxito los nuevos costos y precios')

def cambio_precio_costo_odoo(compras: list[str], test_db: bool = False) -> str:

    api_params = api_params_func(test_db)

    purchase_order_json, upper_compras = api_call_purchase_orders_func(api_params, compras)
    purchases_check, purchase_ids, data_purchase_orders = purchases_check_func(purchase_order_json, upper_compras)


    if purchases_check:

        purchase_lines_json = api_call_purchase_lines_func(api_params, purchase_ids)
        data_purchase_lines, data_product_ids = purchase_lines_to_dict_func(purchase_lines_json)

        product_product_json = api_call_product_product_func(api_params, data_product_ids)
        data_product_lines = product_product_to_dict_func(product_product_json)

        complete_precioycosto_df = complete_precioycosto_df_func(data_purchase_orders, data_purchase_lines, data_product_lines)

        api_write_new_product_data_func(api_params, complete_precioycosto_df)
