import os
from pathlib import Path
import xmlrpc.client
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Engine


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

def search_costo_ventas_func(mes: int) -> list[str]:
    
    if type(mes) != int or mes < 1 or mes > 12:
        raise Exception (f'El mes es incorrecto. El párametro "mes" debe ser un número entero entre 1 y 12. Escribiste: {mes}')
    
    param_dia_hr_ini = pd.Timestamp(2024, mes, 1) + pd.Timedelta(hours=7)
    
    if mes == 12:
        param_dia_hr_fin = pd.Timestamp(2025, 1, 1) + pd.Timedelta(hours=7) - pd.Timedelta(seconds= 1)
    else:
        param_dia_hr_fin = pd.Timestamp(2024, mes+1, 1) + pd.Timedelta(hours=7) - pd.Timedelta(seconds= 1)

    search_costo_ventas = [
        "&", "&",
            ("state", "in", ["purchase", "done"]),
            ("date_approve", ">=", param_dia_hr_ini.strftime('%Y-%m-%d %H:%M:%S')),
            ("date_approve", "<=", param_dia_hr_fin.strftime('%Y-%m-%d %H:%M:%S')),
        ]

    return search_costo_ventas

def _get_df_from_excel(file_name:str, file_location:str) -> pd.DataFrame:

    file_path_str = str(Path().cwd().parent.parent.joinpath(f'data/{file_location}/{file_name}'))
    df = pd.read_excel(file_path_str, dtype_backend='numpy_nullable')

    return df

def prov_locales_df_from_excel() -> list[pd.DataFrame]:
    
    file_name = 'proveedores_oficiales.xlsx'
    file_location = 'compras'

    df = _get_df_from_excel(file_name, file_location)

    prov_oficiales = df.loc[df['oficial'] == 1][['partner_id', 'partner_name']]
    prov_locales = df.loc[df['oficial'] == 0][['partner_id', 'partner_name']]

    return prov_oficiales, prov_locales

def clientes_comisiones_df_from_excel(mes:int) -> list[pd.DataFrame]:
    
    file_name = 'clientes_comisiones.xlsx'
    file_location = 'comisiones'

    df = _get_df_from_excel(file_name, file_location)

    if not df[df['partner_id_x'].duplicated()].empty:
        print(f'Hay "partner_id_x" (cientes comisionistas) que están duplicados en el archivo {file_name}. Esto en el merge va a generar una doble línea. ¡Corrígelo!')
    
    
    dia_ini = pd.Timestamp(2024, mes, 1)
    
    if mes == 12:
        dia_fin = pd.Timestamp(2024, 12, 31)
    else:
        dia_fin = pd.Timestamp(2024, mes+1, 1) - pd.Timedelta(days=1)


    clientes_comisiones = df.loc[
                                (df['fecha_alta'] <= dia_ini) 
                                & (
                                    (df['fecha_baja'] >= dia_fin) 
                                    | (df['fecha_baja'].isna()) 
                                )
                            ]
    
    return clientes_comisiones

def _ultimo_costo_sae_df_from_excel() -> list[pd.DataFrame]:
    
    file_name = 'ultimo_costo_sae.xlsx'
    file_location = 'costo_ventas'

    df = _get_df_from_excel(file_name, file_location)

    return df

def _get_db_engine(db_mode:str) -> Engine:
    
    if db_mode.lower() == 'local':

        db_file_path_str = str(Path().cwd().parent.parent.joinpath(f'data/comisiones.db'))
        engine = create_engine(f'sqlite:///{db_file_path_str}')

        return engine

    else:
        raise Exception (f'Sólo existe la base de datos "Local"')

def get_dfs_from_database(db_mode: str, mes:int) -> list[pd.DataFrame]:

    engine = _get_db_engine(db_mode)
    mes_anterior = mes -1


    if mes != 1:
    
        with engine.connect() as conn, conn.begin():
            
            try:
                ventas = pd.read_sql_table(f'ventas_{mes}_2024', conn, dtype_backend='numpy_nullable')
            except:
                print(f'No se encontró la tabla ventas_{mes}_2024 en la base de datos {db_mode}')

            try:
                ultimo_costo = pd.read_sql_table(f'ultimo_costo_{mes_anterior}_2024', conn, dtype_backend='numpy_nullable')
            except:
                print(f'No se encontró la tabla ultimo_costo_{mes_anterior}_2024 en la base de datos {db_mode}')

            try:
                compras_especiales_sin_usar = pd.read_sql_table(f'compras_especiales_sin_usar_{mes_anterior}_2024', conn, dtype_backend='numpy_nullable')
            except:
                print(f'No se encontró la tabla compras_especiales_sin_usar_{mes_anterior}_2024 en la base de datos {db_mode}')

        engine.dispose()


        if type(ventas) != pd.DataFrame or type(ultimo_costo) != pd.DataFrame or type(compras_especiales_sin_usar) != pd.DataFrame:
            return None, None, None


    else:

        with engine.connect() as conn, conn.begin():
            
            try:
                ventas = pd.read_sql_table(f'ventas_{mes}_2024', conn, dtype_backend='numpy_nullable')
            except:
                print(f'¡Cuidado, no continúes! No se encontró la tabla ventas_{mes}_2024 en la base de datos {db_mode}')
                ventas = None

            ultimo_costo = _ultimo_costo_sae_df_from_excel()
            compras_especiales_sin_usar = None

        engine.dispose()


    return ventas, ultimo_costo, compras_especiales_sin_usar

def correcciones_df_from_excel(mes:int) ->pd.DataFrame:
    
    file_name = f'correcciones_costo_ventas_{mes}_2024.xlsx'
    file_location = 'costo_ventas'

    try:
        correcciones = _get_df_from_excel(file_name, file_location)
    
    except:
        print(f'No existe el archivo {file_name}')
        return None, []
    
    line_ids_correcciones = list(
                correcciones.loc[
                        (~correcciones['line_id_correct'].isna())
                        & (correcciones['line_id_correct'] != 'SAE')
                    ]
                    ['line_id_correct']
                    .astype(int)
                    .sort_values()
                )

    return correcciones, line_ids_correcciones

def api_call_purchase_doc_func(api_params: dict, search_costo_ventas: list[str], line_ids_correcciones:list ) -> list[int, dict]:
    
    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']

    purchase_doc_fields = [
                    'name',
                    'state',
                    'partner_id',
                    'partner_ref',
                    'date_approve',
                    'x_fecha_factura',
                    'user_id',
                    'create_uid'
                    ]

    purchase_doc_ids1 = models.execute_kw(api_db, uid, api_clave, 'purchase.order', 'search', [search_costo_ventas])
    
    if len(line_ids_correcciones) != 0:
        purchase_doc_ids2 = models.execute_kw(api_db, uid, api_clave, 'purchase.order', 'search', [[("order_line", "in", line_ids_correcciones)]])
    
    else:
        purchase_doc_ids2 = set()
    
    purchase_doc_ids = list(set(purchase_doc_ids2) | set(purchase_doc_ids1))
    ids_doc_correcciones_dif_mes = list(set(purchase_doc_ids2) - set(purchase_doc_ids1))

    purchase_doc_json = models.execute_kw(api_db, uid, api_clave, 'purchase.order', 'read', [purchase_doc_ids], {'fields': purchase_doc_fields})
     
    return purchase_doc_ids, purchase_doc_json, ids_doc_correcciones_dif_mes

def purchase_doc_func(purchase_doc_json: list[dict], prov_oficiales:pd.DataFrame) -> pd.DataFrame:

    purchase_doc_data = []

    for compra in purchase_doc_json:
        new = {}
        new['order_id'] = compra['id']
        new['order_name'] = compra['name']
        new['order_state'] = compra['state']
        new['order_date'] = compra['date_approve'] if compra['date_approve'] else pd.NA
        new['partner_id'] = compra['partner_id'][0]
        new['partner_name'] = compra['partner_id'][1]
        new['partner_fact_ref'] = compra['partner_ref']
        new['partner_fact_date'] = compra['x_fecha_factura'] if compra['x_fecha_factura'] else pd.NA
        new['capturista'] = compra['create_uid'][1] if compra['create_uid'] else pd.NA
        new['vendedora'] = compra['user_id'][1] if compra['user_id'] else pd.NA

        purchase_doc_data.append(new)

    compras_doc = pd.DataFrame(purchase_doc_data)
    compras_doc['order_date'] = pd.to_datetime(compras_doc['order_date'], format='%Y-%m-%d %H:%M:%S')
    compras_doc['partner_fact_date'] = pd.to_datetime(compras_doc['partner_fact_date'], format='%Y-%m-%d')
    compras_doc['oficial'] = compras_doc['partner_id'].isin(prov_oficiales['partner_id'])

    return compras_doc


def api_call_purchase_line_func(api_params: dict, purchase_doc_ids: list[int]) -> list[dict]:
    
    api_db = api_params['api_db']
    api_clave = api_params['api_clave']
    uid = api_params['api_uid']
    models = api_params['api_models']

    purchase_line_fields = [
                        'order_id',
                        'date_approve',
                        'partner_id',
                        'product_id',
                        'product_qty',
                        'price_unit_discounted'
                        ]

    purchase_line_ids = models.execute_kw(api_db, uid, api_clave, 'purchase.order.line', 'search', [[("order_id.id", "in", purchase_doc_ids)]])
    purchase_line_json = models.execute_kw(api_db, uid, api_clave, 'purchase.order.line', 'read', [purchase_line_ids], {'fields': purchase_line_fields})
    
    return purchase_line_json

def purchase_line_func(purchase_line_json: list[dict]) -> pd.DataFrame:
    
    purchase_line_data = []

    for line in purchase_line_json:
        new = {}
        new['line_id'] = line['id']
        new['order_id'] = line['order_id'][0]
        new['order_name'] = line['order_id'][1]
        new['order_date'] = line['date_approve'] if line['date_approve'] else pd.NA
        new['partner_id'] = line['partner_id'][0]
        new['partner_name'] = line['partner_id'][1]
        new['product_id_pp'] = line['product_id'][0]
        new['product_name'] = line['product_id'][1]
        new['product_qty'] = line['product_qty']
        new['product_cost'] = line['price_unit_discounted']
        
        purchase_line_data.append(new)

    compras_line = pd.DataFrame(purchase_line_data)
    compras_line['order_date'] = pd.to_datetime(compras_line['order_date'], format='%Y-%m-%d %H:%M:%S')

    return compras_line

def compras_odoo_func(compras_doc:pd.DataFrame, compras_line:pd.DataFrame, line_ids_correcciones:list, ids_doc_correcciones_dif_mes:list) -> pd.DataFrame:
    
    compras_odoo_total = pd.merge(
                    compras_line,
                    compras_doc[['order_id', 'partner_fact_ref', 'partner_fact_date', 'capturista', 'vendedora']], 
                    how='left', 
                    on='order_id'
                )

    compras_odoo_total['order_date'] = compras_odoo_total['order_date'].dt.normalize()

    cols_to_Int64 = ['line_id', 'order_id', 'partner_id', 'product_id_pp']
    compras_odoo_total[cols_to_Int64] = compras_odoo_total[cols_to_Int64].astype('Int64')

    compras_odoo_total['product_qty'] = compras_odoo_total['product_qty'].astype('Float64')
    compras_odoo_total['vendedora'] = compras_odoo_total['vendedora'].convert_dtypes()

    compras_odoo = compras_odoo_total[~compras_odoo_total['order_id'].isin(ids_doc_correcciones_dif_mes)]
    compras_line_id_corrected = compras_odoo_total[
                                        compras_odoo_total['line_id'].isin(line_ids_correcciones)
                                    ].rename(columns={'partner_id': 'partner_id_y',
                                                      'partner_name': 'partner_name_y',
                                                      'product_name': 'product_name_y'})

    return compras_odoo, compras_line_id_corrected

def lista_capturistas_func(mes:int) -> list:
    
    lista_capturistas = [
            'Elsa Ivette Diaz Leyva',
            'Alexa Yadira Mazariegos Zunun',
            'Dulce Guadalupe Pedroza Valenzuela',
            'Mariana Araceli Carvajal Flores',
            'Rosario Martinez Zarate'
        ]

    if mes <= 6:
        return lista_capturistas
        
    else:
        return lista_capturistas + ['Patricia Flores Pantaleón']

def division_compras_especiales_func(compras_odoo:pd.DataFrame, compras_especiales_sin_usar:pd.DataFrame, lista_capturistas:list, prov_oficiales:pd.DataFrame) -> list[pd.DataFrame]:

    # Concatenación de compras especiales del mes anterior con las compras de este mes de odoo
    total_compras_odoo = pd.concat([
                compras_odoo,
                compras_especiales_sin_usar
        ])

    # Compras especiales del mes en curso que además no son de proveedores oficiales
    compras_especiales = total_compras_odoo.loc[
                (~total_compras_odoo['vendedora'].isin(lista_capturistas))
                & (~total_compras_odoo['partner_id'].isin(prov_oficiales['partner_id']))
        ]

    # Resto de compras del mes en curso, es decir, compras de venta normal
    compras_no_especiales = total_compras_odoo.loc[
                ~total_compras_odoo['line_id'].isin(compras_especiales['line_id'])
        ]

    return total_compras_odoo, compras_especiales, compras_no_especiales

def match_1_merge_func(ventas:pd.DataFrame, compras_especiales:pd.DataFrame) -> list[pd.DataFrame]:

    match_1_repetidos = pd.DataFrame([], columns=['line_id','fact_line_id'])

    compras_especiales_while = compras_especiales.copy()
    ventas_while = ventas.copy()
    repetidos_while = True

    while repetidos_while:
        
        match_1_merge = (
            pd.merge_asof(
                left = compras_especiales_while.sort_values('order_date'), 
                right = ventas_while.sort_values('invoice_date'),
                
                left_by = ['product_id_pp', 'vendedora', 'product_qty'], 
                right_by = ['product_id', 'salesperson_name', 'quantity'], 
                
                left_on = 'order_date', 
                right_on = 'invoice_date', 

                direction = 'nearest',
                tolerance = pd.Timedelta(days=3))
        )

        ids_ventas_repetidas_por_corregir = match_1_merge.loc[
                                                    (~match_1_merge['fact_line_id'].isna()) 
                                                    & (match_1_merge['fact_line_id'].duplicated())
                                            ]['fact_line_id'].drop_duplicates()
        
        if not ids_ventas_repetidas_por_corregir.empty:
            repetidos_mini_df = match_1_merge.loc[match_1_merge['fact_line_id'].isin(ids_ventas_repetidas_por_corregir)].sort_values('line_id')
            repetidos_mini_df['diff'] = abs(repetidos_mini_df['invoice_date'] - repetidos_mini_df['order_date'])

            for id in ids_ventas_repetidas_por_corregir:
                match_line_to_keep = repetidos_mini_df.loc[repetidos_mini_df['fact_line_id'] == id].sort_values('diff').reset_index().loc[:0, ['line_id', 'fact_line_id']]

                if match_1_repetidos.empty:
                    match_1_repetidos = match_line_to_keep.copy()
                else:
                    match_1_repetidos = pd.concat([match_1_repetidos, match_line_to_keep])

            compras_especiales_while = compras_especiales_while[~compras_especiales_while['line_id'].isin(match_1_repetidos['line_id'])]
            ventas_while = ventas_while[~ventas_while['fact_line_id'].isin(match_1_repetidos['fact_line_id'])]


        if ids_ventas_repetidas_por_corregir.empty:        
            print('Terminó el ciclo while')
            repetidos_while = False


    return match_1_merge, match_1_repetidos

def match_1_results_func(match_1_merge:pd.DataFrame, match_1_repetidos:pd.DataFrame, ventas:pd.DataFrame, compras_especiales:pd.DataFrame) -> list[pd.DataFrame]:

    # Match resultantes del 1er match.
    match_1 = match_1_merge.loc[~match_1_merge['fact_line_id'].isna(), ['line_id', 'fact_line_id']]

    # Después de correr el 1er match, las ventas restantes
    ventas_after_match_1 = ventas.loc[
                                    (~ventas['fact_line_id'].isin(match_1['fact_line_id']))
                                    & (~ventas['fact_line_id'].isin(match_1_repetidos['fact_line_id']))
                                ]

    # Después de correr el 1er match, las compras que tienen un product_id que sí existe en las ventas restantes.
    compras_especiales_after_match_1 = compras_especiales.loc[
                                                (~compras_especiales['line_id'].isin(match_1['line_id']))
                                                & (~compras_especiales['line_id'].isin(match_1_repetidos['line_id']))
                                                & (compras_especiales['product_id_pp'].isin(ventas_after_match_1['product_id']))
                                            ]

    # Después de correr el 1er match, resto de las compras especiales. Estan no tienen un product_id que existe en las ventas restantes y no se pueden merchar.
    compras_especiales_after_match_1_sin_linea_venta = compras_especiales.loc[
                                                                (~compras_especiales['line_id'].isin(match_1['line_id']))
                                                                & (~compras_especiales['line_id'].isin(match_1_repetidos['line_id']))
                                                                & (~compras_especiales['product_id_pp'].isin(ventas_after_match_1['product_id']))
                                                            ]
    
    return match_1, ventas_after_match_1, compras_especiales_after_match_1, compras_especiales_after_match_1_sin_linea_venta

def match_2_merge_func(ventas_after_match_1:pd.DataFrame, compras_especiales_after_match_1:pd.DataFrame) -> list[pd.DataFrame]:

    # Varias ventas para una sóla compra.
    match_2 = pd.DataFrame([], columns=['line_id','fact_line_id'])

    for i in range(len(compras_especiales_after_match_1)):
        
        linea_compra = compras_especiales_after_match_1.sort_values('order_date').iloc[i]

        mini_df = ventas_after_match_1.loc[
                    (~ventas_after_match_1['fact_line_id'].isin(match_2['fact_line_id']))
                    & (ventas_after_match_1['salesperson_name'] == linea_compra['vendedora'])
                    & (ventas_after_match_1['product_id'] == linea_compra['product_id_pp'])
                ]
        
        if not mini_df.empty:
            df_copia = mini_df.copy()
            df_copia['diff'] = abs(df_copia['invoice_date'] - linea_compra['order_date'])
            df_sort = df_copia.sort_values('diff').reset_index()
            df_sort['cumsum'] = df_sort['quantity'].cumsum()
            index = df_sort[df_sort['cumsum'] == linea_compra['product_qty']].index[0] if not df_sort[df_sort['cumsum'] == linea_compra['product_qty']].index.empty else None
            if index != None:
                df_sort.loc[:index , 'line_id'] = linea_compra['line_id']
                df_to_keep = df_sort.loc[~df_sort['line_id'].isna()]

                if match_2.empty:
                    match_2 = df_to_keep[['line_id','fact_line_id']].copy()
                else:
                    match_2 = pd.concat([
                                match_2, 
                                df_to_keep[['line_id','fact_line_id']],
                            ])


    return match_2

def match_2_results_func(match_2:pd.DataFrame, ventas_after_match_1:pd.DataFrame, compras_especiales_after_match_1:pd.DataFrame) -> list[pd.DataFrame]:

    # Después de correr el 2do match, las ventas restantes
    ventas_after_match_2 = ventas_after_match_1.loc[
                                    ~ventas_after_match_1['fact_line_id'].isin(match_2['fact_line_id'])
                                ]

    # Después de correr el 2do match, las compras que restan y que sí tendrán venta a la cual mercharse.
    compras_especiales_after_match_2 = compras_especiales_after_match_1.loc[
                                                (~compras_especiales_after_match_1['line_id'].isin(match_2['line_id']))
                                                & (compras_especiales_after_match_1['product_id_pp'].isin(ventas_after_match_2['product_id']))
                                            ]

    # Después de correr el 2do match, resto de las compras especiales. Estan no tienen un product_id que existe en las ventas restantes y no se pueden merchar.
    compras_especiales_after_match_2_sin_linea_venta = compras_especiales_after_match_1.loc[
                                                                (~compras_especiales_after_match_1['line_id'].isin(match_2['line_id']))
                                                                & (~compras_especiales_after_match_1['product_id_pp'].isin(ventas_after_match_2['product_id']))
                                                            ]
    
    return ventas_after_match_2, compras_especiales_after_match_2, compras_especiales_after_match_2_sin_linea_venta

def match_3_merge_func(ventas_after_match_2:pd.DataFrame, compras_especiales_after_match_2:pd.DataFrame) -> list[pd.DataFrame]:

    # Varias compras para una sóla venta.
    match_3 = pd.DataFrame([], columns=['line_id','fact_line_id'])
    match_3_to_errase = pd.DataFrame([], columns=['line_id','fact_line_id'])

    ventas_inside_compras_especiales_after_match_2 = ventas_after_match_2[
                                                            ventas_after_match_2['product_id'].isin(compras_especiales_after_match_2['product_id_pp'])
                                                        ]

    for i in range(len(ventas_inside_compras_especiales_after_match_2)):
        
        linea_venta = ventas_inside_compras_especiales_after_match_2.sort_values('invoice_date').iloc[i]

        mini_df = compras_especiales_after_match_2.loc[
                    (~compras_especiales_after_match_2['line_id'].isin(match_3['line_id']))
                    & (compras_especiales_after_match_2['vendedora'] == linea_venta['salesperson_name'])
                    & (compras_especiales_after_match_2['product_id_pp'] == linea_venta['product_id'])
                ]
            

        if not mini_df.empty:
        
            df_copia = mini_df.copy()
            df_copia['diff'] = abs(df_copia['order_date'] - linea_venta['invoice_date'])
            df_sort = df_copia.sort_values('diff').reset_index()
            df_sort['cumsum'] = df_sort['product_qty'].cumsum()
            index = df_sort[df_sort['cumsum'] == linea_venta['quantity']].index[0] if not df_sort[df_sort['cumsum'] == linea_venta['quantity']].index.empty else None
            
            if index != None:
                df_sort.loc[:index , 'fact_line_id'] = linea_venta['fact_line_id']
                
                #Esta línea es para poder detectar la línea de compra con el costo mayor y dejarla para la línea de venta
                df_to_keep = (
                        df_sort.loc[
                                ~df_sort['fact_line_id'].isna()
                            ]
                            .sort_values('product_cost', ascending=False)
                            .reset_index()
                        )

                if match_3.empty:
                    match_3 = df_to_keep.loc[:0, ['line_id', 'fact_line_id']].copy()
                    match_3_to_errase = df_to_keep[['line_id', 'fact_line_id']].copy()

                else:
                    match_3 = pd.concat([
                                match_3, 
                                df_to_keep.loc[:0, ['line_id', 'fact_line_id']],
                            ])
                    match_3_to_errase = pd.concat([
                                match_3_to_errase, 
                                df_to_keep[['line_id', 'fact_line_id']],
                            ])


    return match_3, match_3_to_errase

def match_3_results_func(match_3_to_errase:pd.DataFrame, ventas_after_match_2:pd.DataFrame, compras_especiales_after_match_2:pd.DataFrame) -> list[pd.DataFrame]:

    # Después de correr el 3er match, las ventas restantes
    ventas_after_match_3 = ventas_after_match_2.loc[
                                    ~ventas_after_match_2['fact_line_id'].isin(match_3_to_errase['fact_line_id'])
                                ]

    # Después de correr el 3er match, las compras que restan y que sí tendrán venta a la cual mercharse.
    compras_especiales_after_match_3 = compras_especiales_after_match_2.loc[
                                                (~compras_especiales_after_match_2['line_id'].isin(match_3_to_errase['line_id']))
                                                & (compras_especiales_after_match_2['product_id_pp'].isin(ventas_after_match_3['product_id']))
                                            ]

    # Después de correr el 3er match, resto de las compras especiales. Estan no tienen un product_id que existe en las ventas restantes y no se pueden merchar.
    compras_especiales_after_match_3_sin_linea_venta = compras_especiales_after_match_2.loc[
                                                                (~compras_especiales_after_match_2['line_id'].isin(match_3_to_errase['line_id']))
                                                                & (~compras_especiales_after_match_2['product_id_pp'].isin(ventas_after_match_3['product_id']))
                                                            ]
    
    return ventas_after_match_3, compras_especiales_after_match_3, compras_especiales_after_match_3_sin_linea_venta

def match_4_merge_func(ventas_after_match_3:pd.DataFrame, compras_especiales_after_match_3:pd.DataFrame) -> list[pd.DataFrame]:

    match_4_merge = (
        pd.merge_asof(
            left = ventas_after_match_3.sort_values('invoice_date'), 
            right = compras_especiales_after_match_3.sort_values('order_date'),
            
            left_by = ['product_id'], 
            right_by = ['product_id_pp'], 
            
            left_on = 'invoice_date', 
            right_on = 'order_date', 

            direction = 'nearest',
        )
    )

    return match_4_merge

def match_4_results_func(match_4_merge:pd.DataFrame, ventas_after_match_3:pd.DataFrame, compras_especiales_after_match_3:pd.DataFrame) -> list[pd.DataFrame]:

    # Match resultantes del 4to match.
    match_4 = match_4_merge.loc[~match_4_merge['line_id'].isna(), ['line_id', 'fact_line_id']]

    # Después de correr el 1er match, las ventas restantes
    ventas_after_match_4 = ventas_after_match_3.loc[
                                    ~ventas_after_match_3['fact_line_id'].isin(match_4['fact_line_id'])
                                ]

    # Después de correr el 4to match, las compras que tienen un product_id que sí existe en las ventas restantes.
    compras_especiales_after_match_4 = compras_especiales_after_match_3.loc[
                                                (~compras_especiales_after_match_3['line_id'].isin(match_4['line_id']))
                                                & (compras_especiales_after_match_3['product_id_pp'].isin(ventas_after_match_4['product_id']))
                                            ]

    # Después de correr el 4to match, resto de las compras especiales. Estan no tienen un product_id que existe en las ventas restantes y no se pueden merchar.
    compras_especiales_after_match_4_sin_linea_venta = compras_especiales_after_match_3.loc[
                                                                (~compras_especiales_after_match_3['line_id'].isin(match_4['line_id']))
                                                                & (~compras_especiales_after_match_3['product_id_pp'].isin(ventas_after_match_4['product_id']))
                                                            ]
    
    return match_4, ventas_after_match_4, compras_especiales_after_match_4, compras_especiales_after_match_4_sin_linea_venta

def match_compras_especiales_func(match_1:pd.DataFrame, match_1_repetidos:pd.DataFrame, match_2:pd.DataFrame, match_3:pd.DataFrame, match_4:pd.DataFrame) -> pd.DataFrame:

    match_compras_especiales = pd.concat(
                                    [
                                        match_1, 
                                        match_1_repetidos if not match_1_repetidos.empty else None, 
                                        match_2, 
                                        match_3, 
                                        match_4
                                    ]
                                )

    return match_compras_especiales

def costo_ventas_a_func(match_compras_especiales:pd.DataFrame, ventas:pd.DataFrame, total_compras_odoo:pd.DataFrame) -> pd.DataFrame:

    costo_ventas_a = (
        match_compras_especiales.merge(
            ventas,
            how='left',
            on='fact_line_id'
        ).merge(
            total_compras_odoo,
            how='left',
            on='line_id'
        )
    )

    return costo_ventas_a

def costo_ventas_b_func(ventas_after_match_4:pd.DataFrame, compras_no_especiales:pd.DataFrame, ultimo_costo:pd.DataFrame) -> pd.DataFrame:

    costo_ventas_b = (
        pd.merge_asof(
            left = ventas_after_match_4.sort_values('invoice_date'),
            right = pd.concat([
                            compras_no_especiales, 
                            ultimo_costo,
                    ]).sort_values('order_date'), 
            
            left_by = 'product_id', 
            right_by = 'product_id_pp', 
            
            left_on = 'invoice_date', 
            right_on = 'order_date', 

            direction = 'backward')
    )

    return costo_ventas_b

def costo_ventas_func(costo_ventas_a:pd.DataFrame, costo_ventas_b:pd.DataFrame) -> pd.DataFrame:

    costo_ventas = pd.concat([costo_ventas_a, costo_ventas_b])

    costo_ventas['fact_line_id'] = costo_ventas['fact_line_id'].astype('Int64')
    costo_ventas['line_id'] = costo_ventas['line_id'].astype('Int64')

    return costo_ventas

def costo_ventas_corregido_func(correcciones:pd.DataFrame|None, compras_line_id_corrected:pd.DataFrame | None, costo_ventas:pd.DataFrame) -> pd.DataFrame:

    costo_ventas_corregido = costo_ventas.copy()
    costo_ventas_corregido[['is_verified', 'is_corrected','comentario_correct']] = pd.NA
    
    if type(correcciones) != pd.DataFrame:
        print('No se aplicadó ninguna corrección al costo de venta')
    
    else:
        # fact_line_ids verificados
        ids_verified = correcciones[~correcciones['is_verified'].isna()]
        
        for i in range(len(ids_verified)):
            costo_ventas_corregido.loc[
                        costo_ventas_corregido['fact_line_id'] == ids_verified.iloc[i]['fact_line_id']
                        , 
                        ['is_verified', 'comentario_correct']
                    ] = True, ids_verified.iloc[i]['comentario_correct']

        
        # fact_line_ids que van a ser corregidos
        ids_corrected = correcciones[correcciones['is_verified'].isna()]
        
        costo_ventas_corregido.loc[
                costo_ventas_corregido['fact_line_id'].isin(ids_corrected['fact_line_id'])
                , 
                ['is_corrected']
            ] = True
        
        
        # line_id desde el SAE
        ids_sae = correcciones[correcciones['line_id_correct'] == 'SAE']
        
        cols_to_correct_sae = ['line_id', 'order_id', 'partner_id_y', 'partner_name_y', 'product_qty',
        'partner_fact_ref', 'partner_fact_date', 'capturista', 'vendedora']

        for i in range(len(ids_sae)):
            costo_ventas_corregido.loc[
                        costo_ventas_corregido['fact_line_id'] == ids_sae.iloc[i]['fact_line_id']
                        , 
                        cols_to_correct_sae
                    ] = pd.NA
            
            costo_ventas_corregido.loc[
                        costo_ventas_corregido['fact_line_id'] == ids_sae.iloc[i]['fact_line_id']
                        , 
                        ['order_name', 'order_date', 'product_cost', 'comentario_correct']
                    ] = 'SAE', pd.Timestamp(2023, 12, 31), ids_sae.iloc[i]['costo_correct'], ids_sae.iloc[i]['comentario_correct']
        

        # line_id incorrectas, cambiar a nueva line_id correcta
        ids_new_line = correcciones[
                                (correcciones['line_id_correct'] != 'SAE')
                                & (~correcciones['line_id_correct'].isna())
                            ]
        
        # Check para ver que todas la line_id_corrected escritas en el archivo de correcciones se encontraron en Odoo
        check_line_id_correct = ids_new_line['line_id_correct'].astype(int)
        check = check_line_id_correct[~check_line_id_correct.isin(compras_line_id_corrected['line_id'])]

        if not check.empty:
            print(f'\nLos siguientes "line_id_correct" en el archivo de correcciones no se encontraron en Odoo:')
            print(f'    {check.values}')
            return

        cols_to_correct_new_line_id = ['line_id', 'order_id', 'order_name', 'order_date',
        'partner_id_y', 'partner_name_y', 'product_id_pp', 'product_name_y', 'product_qty',
        'partner_fact_ref', 'partner_fact_date', 'capturista', 'vendedora']

        for i in range(len(ids_new_line)):
            costo_ventas_corregido.loc[
                        costo_ventas_corregido['fact_line_id'] == ids_new_line.iloc[i]['fact_line_id']
                        , 
                        cols_to_correct_new_line_id
                    ] = compras_line_id_corrected.loc[
                                compras_line_id_corrected['line_id'] == int(ids_new_line.iloc[i]['line_id_correct'])
                                , cols_to_correct_new_line_id
                            ].values
            
            costo_ventas_corregido.loc[
                    costo_ventas_corregido['fact_line_id'] == ids_new_line.iloc[i]['fact_line_id']
                    , 
                    ['product_cost', 'comentario_correct']
                ] = ids_new_line.iloc[i]['costo_correct'], ids_new_line.iloc[i]['comentario_correct']
            
            
    # Se procede a generar las columnas de utilidades y margenes con las correcciones generadas
    costo_ventas_corregido['cost_subtotal'] = costo_ventas_corregido['product_cost'] * costo_ventas_corregido['quantity']
    costo_ventas_corregido['utilidad_partida_$'] = costo_ventas_corregido['price_subtotal'] - costo_ventas_corregido['cost_subtotal']
    costo_ventas_corregido['utilidad_%'] = ((costo_ventas_corregido['price_subtotal'] / (costo_ventas_corregido['cost_subtotal']) - 1) * 100).round(2)
    costo_ventas_corregido['margen_contribución_%'] = (costo_ventas_corregido['utilidad_partida_$'] / costo_ventas_corregido['price_subtotal'] * 100).round(2)

    # Corregir los margenes de contribución infinitos, ya que se dividieron entre precio cero
    costo_ventas_corregido.replace([np.inf, -np.inf], pd.NA, inplace=True)

    return costo_ventas_corregido

def costo_ventas_after_comisiones_clientes_func(costo_ventas_corregido:pd.DataFrame, clientes_comisiones:pd.DataFrame) -> pd.DataFrame:

    if type(costo_ventas_corregido) != pd.DataFrame:
        return


    costo_ventas_after_comisiones_clientes = costo_ventas_corregido.merge(
                                                        clientes_comisiones[['partner_id_x', '%_comision_cliente']],
                                                        how = 'left',
                                                        on = 'partner_id_x'
                                                    )

    costo_ventas_after_comisiones_clientes['cost_cliente_comision'] = costo_ventas_after_comisiones_clientes['price_subtotal'] * costo_ventas_after_comisiones_clientes['%_comision_cliente'] / 100
    
    costo_ventas_after_comisiones_clientes['cost_subtotal_after_cc'] = costo_ventas_after_comisiones_clientes['cost_subtotal'] + costo_ventas_after_comisiones_clientes['cost_cliente_comision']
    costo_ventas_after_comisiones_clientes['utilidad_partida_$_after_cc'] = costo_ventas_after_comisiones_clientes['price_subtotal'] - costo_ventas_after_comisiones_clientes['cost_subtotal_after_cc']
    costo_ventas_after_comisiones_clientes['utilidad_%_after_cc'] = ((costo_ventas_after_comisiones_clientes['price_subtotal'] / (costo_ventas_after_comisiones_clientes['cost_subtotal_after_cc']) - 1) * 100).round(2)
    costo_ventas_after_comisiones_clientes['margen_contribución_%_after_cc'] = (costo_ventas_after_comisiones_clientes['utilidad_partida_$_after_cc'] / costo_ventas_after_comisiones_clientes['price_subtotal'] * 100).round(2)


    # Corregir utilidad y márgen pd.NA resultante de los clientes que no llevan comisión
    index_clientes_no_comision = costo_ventas_after_comisiones_clientes.loc[
                costo_ventas_after_comisiones_clientes['%_comision_cliente'].isna(),
            ].index

    costo_ventas_after_comisiones_clientes.loc[
            index_clientes_no_comision,
            [
                'cost_subtotal_after_cc',
                'utilidad_partida_$_after_cc',
                'utilidad_%_after_cc',
                'margen_contribución_%_after_cc'
            ]
        ] = costo_ventas_corregido[[
                'cost_subtotal',
                'utilidad_partida_$',
                'utilidad_%',
                'margen_contribución_%'
            ]].iloc[index_clientes_no_comision].values

    return costo_ventas_after_comisiones_clientes

def nuevas_compras_especiales_sin_usar_func(compras_especiales_after_match_1_sin_linea_venta:pd.DataFrame, compras_especiales_after_match_2_sin_linea_venta:pd.DataFrame, compras_especiales_after_match_3_sin_linea_venta:pd.DataFrame, compras_especiales_after_match_4_sin_linea_venta:pd.DataFrame, compras_odoo:pd.DataFrame,) -> pd.DataFrame:

    ids_compras_especiales_sin_usar = pd.concat(
            [
                compras_especiales_after_match_1_sin_linea_venta['line_id'],
                compras_especiales_after_match_2_sin_linea_venta['line_id'],
                compras_especiales_after_match_3_sin_linea_venta['line_id'],
                compras_especiales_after_match_4_sin_linea_venta['line_id'],
            ]
        )

    nuevas_compras_especiales_sin_usar = compras_odoo[
                                    compras_odoo['line_id'].isin(ids_compras_especiales_sin_usar)
                                ]

    return nuevas_compras_especiales_sin_usar

def nuevo_ultimo_costo_func(ultimo_costo:pd.DataFrame, compras_especiales:pd.DataFrame, compras_no_especiales:pd.DataFrame) -> pd.DataFrame:

    nuevo_ultimo_costo = (
            pd.concat(
                    [
                    ultimo_costo, 
                    compras_no_especiales,
                    compras_especiales[
                                ~compras_especiales['product_id_pp'].isin(compras_no_especiales['product_id_pp'])
                            ]
                    ]
                )
            .sort_values('order_date', ascending=False)
            .groupby('product_id_pp')
            .first()
            .reset_index()
        )

    return nuevo_ultimo_costo

def checks_costo_ventas_func(
            mes:int,
            compras_doc:pd.DataFrame,
            prov_locales:pd.DataFrame,
            prov_oficiales:pd.DataFrame,
            compras_especiales_after_match_4:pd.DataFrame,
            match_compras_especiales:pd.DataFrame,
            match_3_to_errase:pd.DataFrame,
            match_3:pd.DataFrame,
            compras_especiales_after_match_1_sin_linea_venta:pd.DataFrame,
            compras_especiales_after_match_2_sin_linea_venta:pd.DataFrame,
            compras_especiales_after_match_3_sin_linea_venta:pd.DataFrame,
            compras_especiales_after_match_4_sin_linea_venta:pd.DataFrame,
            compras_especiales:pd.DataFrame,
            costo_ventas_corregido:pd.DataFrame,
            ventas:pd.DataFrame
    ) -> bool:
    
    # Línea para comprobrar que el 100% de los proveedores de Odoo están calificados en la lista de proveedores oficiales
    check1 = (compras_doc[~compras_doc['partner_id'].isin(pd.concat([prov_locales, prov_oficiales])['partner_id'])]).drop_duplicates('partner_id')


    # Check donde se verifíca que ya no hay más compras especiales por merchar, es decir, ya no se ocupa un match_5.
    check2 = compras_especiales_after_match_4.empty


    # Check donde todas las líneas compras especiales están unidas a líneas de ventas
    all_line_ids_processed = (
            pd.concat(
                [
                    match_compras_especiales['line_id'],
                    match_3_to_errase.loc[~match_3_to_errase['line_id'].isin(match_3), 'line_id'],
                    compras_especiales_after_match_1_sin_linea_venta['line_id'],
                    compras_especiales_after_match_2_sin_linea_venta['line_id'],
                    compras_especiales_after_match_3_sin_linea_venta['line_id'],
                    compras_especiales_after_match_4_sin_linea_venta['line_id'],
                ]
            )
            .drop_duplicates()
            .sort_values()
            .astype('Int64')
            .reset_index()
            ['line_id']
        )

    all_line_ids_to_process = (
            compras_especiales['line_id']
            .sort_values()
            .reset_index()
            ['line_id']
        )

    check3 = all_line_ids_processed.equals(all_line_ids_to_process)


    # Check para ver el tamaño del costo de ventas generado vs las ventas del mes
    check4 = len(costo_ventas_corregido) == len(ventas)


    # Check para ver que no existen lineas de venta fuera del costo de ventas.
    check5 = ventas[~ventas['fact_line_id'].isin(costo_ventas_corregido['fact_line_id'])].empty


    # Check para que todas las líneas de venta del costo de ventas tienen un costo.
    productos_aceptables_sin_costo_ventas = ['Anticipo', 
                                            'Anticipo (PdV)', 
                                            'Servicios de Facturación'
                                            ]

    check6 = costo_ventas_corregido.loc[
            (costo_ventas_corregido['order_name'].isna())
            & (~costo_ventas_corregido['product_name_x'].isin(productos_aceptables_sin_costo_ventas))
        ]


    # Check para ver todas las líneas que tienen una utilidad negativa o en ceros.
    check7 = costo_ventas_corregido.loc[
            (costo_ventas_corregido['move_type'] != 'out_refund')
            & (costo_ventas_corregido['quantity'] != 0)
            & (costo_ventas_corregido['utilidad_partida_$'] <= 0)
            & (costo_ventas_corregido['is_verified'].isna())
            & (costo_ventas_corregido['is_corrected'].isna())
        ].sort_values('utilidad_partida_$')


    # Check para ver todas las líneas que tienen una utilidad mayor a 200%.
    check8 = costo_ventas_corregido.loc[
            (costo_ventas_corregido['move_type'] != 'out_refund')
            & (costo_ventas_corregido['utilidad_%'] >= 200)
            & (costo_ventas_corregido['is_verified'].isna())
            & (costo_ventas_corregido['is_corrected'].isna())
        ].sort_values('utilidad_%', ascending=False)


    # Checar todos los cheks
    all_checks = (
                check1.empty
                and check2
                and check3
                and check4
                and check5
                and check6.empty
                and check7.empty
                and check8.empty
    )

    if all_checks:
        print('¡Todos los checks salieron correctos!')
        return all_checks

    else:
        print(f'Fallaron los cheks:')

        not check1.empty and print('   -Check1: Hay proveedores no calificados en la lista de proveedores oficiales.')
        not check2 and print('   -Check2: Hay compras especiales remanentes después del último match.')
        not check3 and print('   -Check3: Hay líneas de compra especiales que aun no se han procesado.')
        not check4 and print('   -Check4: El tamaño del costo de ventas es diferente al tamaño de las ventas iniciales.')
        not check5 and print('   -Check5: El costo de ventas no contempla unas líneas de venta del dataframe de ventas iniciales.')
        not check6.empty and print(f'   -Check6: Hay {len(check6)} productos que no tienen costo de ventas.')
        not check7.empty and print(f'   -Check7: Hay {len(check7)} líneas de venta con utilidades negativas.')
        not check8.empty and print(f'   -Check8: Hay {len(check8)} líneas de venta con utilidades mayores al 200%')


        # Escribe el archivo .xlsx en el escritorio para la corrección de los checks 7 y 8.
        if not check6.empty or not check7.empty or not check8.empty:
            archivo = f'lineas_por_corregir_costo_ventas_{mes}_2024'
            path = Path.home().joinpath(f'Desktop/{archivo}.xlsx')
            writer = pd.ExcelWriter(path, engine="openpyxl")

            cols_to_write_corrections = ['line_id_correct', 'costo_correct']
            cols_to_excel = list(costo_ventas_corregido.columns) + cols_to_write_corrections

            if not check6.empty:
                check6.loc[:, cols_to_write_corrections] = pd.NA
                check6[cols_to_excel].to_excel(writer, sheet_name='prod_sin_costo')
            
            if not check7.empty:
                check7.loc[:, cols_to_write_corrections] = pd.NA
                check7[cols_to_excel].to_excel(writer, sheet_name='negativos')

            if not check8.empty:
                check8.loc[:, cols_to_write_corrections] = pd.NA
                check8[cols_to_excel].to_excel(writer, sheet_name='exagerados')


            writer.close()
            writer.handles = None

            print(f'\nSe generó el archivo {archivo}.xlsx y se guardó en el escritorio.')


def costo_ventas_mes_func(mes: int, db_mode:str) -> pd.DataFrame:

    api_params = api_params_func()
    search_costo_ventas = search_costo_ventas_func(mes)

    prov_oficiales, prov_locales = prov_locales_df_from_excel()
    clientes_comisiones = clientes_comisiones_df_from_excel(mes)
    ventas, ultimo_costo, compras_especiales_sin_usar = get_dfs_from_database(db_mode, mes)

    correcciones, line_ids_correcciones = correcciones_df_from_excel(mes)

    purchase_doc_ids, purchase_doc_json, ids_doc_correcciones_dif_mes = api_call_purchase_doc_func(api_params, search_costo_ventas, line_ids_correcciones)
    purchase_line_json = api_call_purchase_line_func(api_params, purchase_doc_ids)

    compras_doc = purchase_doc_func(purchase_doc_json, prov_oficiales)
    compras_line = purchase_line_func(purchase_line_json)
    compras_odoo, compras_line_id_corrected = compras_odoo_func(compras_doc, compras_line, line_ids_correcciones, ids_doc_correcciones_dif_mes)

    lista_capturistas = lista_capturistas_func(mes)
    total_compras_odoo, compras_especiales, compras_no_especiales = division_compras_especiales_func(compras_odoo, compras_especiales_sin_usar, lista_capturistas, prov_oficiales)

    match_1_merge, match_1_repetidos = match_1_merge_func(ventas, compras_especiales)
    match_1, ventas_after_match_1, compras_especiales_after_match_1, compras_especiales_after_match_1_sin_linea_venta = match_1_results_func(match_1_merge, match_1_repetidos, ventas, compras_especiales)

    match_2 = match_2_merge_func(ventas_after_match_1, compras_especiales_after_match_1)
    ventas_after_match_2, compras_especiales_after_match_2, compras_especiales_after_match_2_sin_linea_venta = match_2_results_func(match_2, ventas_after_match_1, compras_especiales_after_match_1)

    match_3, match_3_to_errase = match_3_merge_func(ventas_after_match_2, compras_especiales_after_match_2)
    ventas_after_match_3, compras_especiales_after_match_3, compras_especiales_after_match_3_sin_linea_venta = match_3_results_func(match_3_to_errase, ventas_after_match_2, compras_especiales_after_match_2)

    match_4_merge = match_4_merge_func(ventas_after_match_3, compras_especiales_after_match_3)
    match_4, ventas_after_match_4, compras_especiales_after_match_4, compras_especiales_after_match_4_sin_linea_venta = match_4_results_func(match_4_merge, ventas_after_match_3, compras_especiales_after_match_3)

    match_compras_especiales = match_compras_especiales_func(match_1, match_1_repetidos, match_2, match_3, match_4)
    costo_ventas_a = costo_ventas_a_func(match_compras_especiales, ventas, total_compras_odoo)
    costo_ventas_b = costo_ventas_b_func(ventas_after_match_4, compras_no_especiales, ultimo_costo)
    costo_ventas = costo_ventas_func(costo_ventas_a, costo_ventas_b)

    # Costo de venta con las correcciones
    costo_ventas_corregido = costo_ventas_corregido_func(correcciones, compras_line_id_corrected, costo_ventas)

    # Función de checks para detectar erroes antes de generar dataframes finales
    all_checks_costo_ventas = checks_costo_ventas_func(
                                    mes,
                                    compras_doc,
                                    prov_locales,
                                    prov_oficiales,
                                    compras_especiales_after_match_4,
                                    match_compras_especiales,
                                    match_3_to_errase,
                                    match_3,
                                    compras_especiales_after_match_1_sin_linea_venta,
                                    compras_especiales_after_match_2_sin_linea_venta,
                                    compras_especiales_after_match_3_sin_linea_venta,
                                    compras_especiales_after_match_4_sin_linea_venta,
                                    compras_especiales,
                                    costo_ventas_corregido,
                                    ventas
                                )

    if not all_checks_costo_ventas:
        return None, None, None
    
    else:
        # Costo de venta corregido y con las comisiones de los clientes
        costo_ventas_after_comisiones_clientes = costo_ventas_after_comisiones_clientes_func(costo_ventas_corregido, clientes_comisiones)

        # Preparar compras especiales sin usar y último costo para el siguiente mes
        nuevas_compras_especiales_sin_usar = nuevas_compras_especiales_sin_usar_func(compras_especiales_after_match_1_sin_linea_venta, compras_especiales_after_match_2_sin_linea_venta, compras_especiales_after_match_3_sin_linea_venta, compras_especiales_after_match_4_sin_linea_venta, compras_odoo)
        nuevo_ultimo_costo = nuevo_ultimo_costo_func(ultimo_costo, compras_especiales, compras_no_especiales)

    return costo_ventas_after_comisiones_clientes, nuevas_compras_especiales_sin_usar, nuevo_ultimo_costo
