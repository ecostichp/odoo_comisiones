import pandas as pd

pd.set_option('mode.chained_assignment', None)

import warnings
warnings.simplefilter("ignore", category=FutureWarning)

def _merge_warehouse(incoming_df: pd.DataFrame, salesperson_column_name: str) -> pd.DataFrame:
    """
    Función para añadir la información sobre a qué almacén pertenece cada vendedora
    """

    # Se crea el DataFrame que contiene la información sobre a qué almacén pertenece cada vendedora de CE
    warehouses = pd.DataFrame(
        {
            'salesperson': ['Mayra Angelica Parada Manjarrez', 'Irma Carvajal Flores', 'Brenda Luz Acosta Lopez', 'Yamilet Blanco Salas'],
            'warehouse': ['A1', 'A1', 'A2', 'A2']
        }
    )

    # Retorno de la función
    return (
        incoming_df
        .pipe(
            lambda df: (
                # Se hace un Merge con el DataFrame entrante y los almacenes a los que pertenece la vendedora
                pd.merge(
                    left= df,
                    right= warehouses,
                    left_on= salesperson_column_name,
                    right_on= 'salesperson',
                    how= 'left'
                )
                # Se toman sólo las columnas del DataFrame + la columna de almacén
                [list(df.columns) + ['warehouse']]
            )
        )
    )

def _get_id_to_search(sales_df: pd.DataFrame, _sales_df_invoice_date_column: str) -> pd.Series:
    """
    Función que asigna un registro a buscar
    """
    # Se ordena el DataFrame por fecha de más reciente a más antiguo
    sales_df = sales_df.sort_values(_sales_df_invoice_date_column, ascending=False)

    # Filtros guardados en series

    # Se buscan registros no empatados por coincidencia exacta
    exact_match_na = sales_df['exact_match'].isna()
    # Se buscan registros no empatados por coincidencia de almacén
    warehouse_match_na = sales_df['warehouse_match'].isna()
    # Se buscan registros no empatados por coincidencia de grupo
    group_match_na = sales_df['group_match'].isna()
    # Se buscan registros que aún no han sido buscados
    to_search = sales_df['not_found'] == False

    # Resultados de la búsqueda
    results = sales_df[(exact_match_na) & (warehouse_match_na) & (group_match_na) & (to_search)]

    # Retorno de la fila a buscar
    if results.size:
        return results.iloc[0]
    # En caso de no devolver ningún registro, el error se encapsula y se ejecuta la terminación exitosa de la función mayor

def _match_exact_coincidence(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # Estado de ejecución
    finish_process,
    # Columna de ID de línea de DataFrame de ventas
    _sales_df_line_id_column: str,
    # Columna de ID de línea de DataFrame de compras
    _purchase_df_line_id_column: str,
    # Columna de vendedora de DataFrame de ventas
    _sales_df_salesperson_column: str,
    # Columna de vendedora de DataFrame de compras
    _purchase_df_salesperson_column: str,
    # Columna de cantidad de DataFrame de ventas
    _sales_df_product_qty_column: str,
    # Columna de cantidad de DataFrame de compras
    _purchase_df_product_qty_column: str,
    # Columna de fecha de facturación de DataFrame de ventas
    _sales_df_invoice_date_column: str,
    # Columna de fecha de orden de DataFrame de compras
    _purchase_df_order_date_column: str,
    # Rango de días permitido
    _tolerance_days_range: int
) -> dict[bool, list[pd.DataFrame, dict]]:
    
    # Se obtiene el registro de venta a buscar en compras
    ven_line = _get_id_to_search(sales_df, _sales_df_invoice_date_column)

    # Se define la ID de este registro de venta
    try:
        ven_id = ven_line[_sales_df_line_id_column]
    except TypeError:
        finish_process['state'] = True
        # print("Búsqueda de emparejamiento con producto terminada")
        return {
            'matched': False,
            'dfs': [sales_df, purchase_df, finish_process]
        }

    # Definición de las series de numpy

    # Coincidencia por vendedora
    match_salesperson = purchase_df[_purchase_df_salesperson_column] == ven_line[_sales_df_salesperson_column]
    # Coincidencia por cantidad de producto
    match_product_qty = purchase_df[_purchase_df_product_qty_column] == ven_line[_sales_df_product_qty_column]
    # Coincidencia por rango de fecha
    match_date_inferior_range = purchase_df[_purchase_df_order_date_column] + pd.Timedelta(days=_tolerance_days_range) > ven_line[_sales_df_invoice_date_column]
    match_date_superior_range = purchase_df[_purchase_df_order_date_column] < ven_line[_sales_df_invoice_date_column]
    # Se descartan las compras con todas las cantidades vendidas
    not_matched_completely = purchase_df[_purchase_df_product_qty_column] != purchase_df["sold_qty"]

    # Se obtienen las filas con las coincidencias de los criterios
    results = purchase_df[(match_salesperson) & (match_product_qty) & (match_date_inferior_range) & (match_date_superior_range) & (not_matched_completely)]

    # Si sí hay información se procede a realizar el [if]
    if results.size:
        # Registro en el DataFrame de ventas
        sales_df.loc[(sales_df[_sales_df_line_id_column] == ven_id), 'exact_match'] = results.iloc[0][_purchase_df_line_id_column]
        # Registro en el DataFrame de compras
        purchase_df.loc[(purchase_df[_purchase_df_line_id_column] == results.iloc[0][_purchase_df_line_id_column]), 'sold_qty'] = results.iloc[0][_purchase_df_product_qty_column]
        return {
            'matched': True,
            'dfs': [sales_df, purchase_df, finish_process]
        }
    else:
        return {
            'matched': False
        }
    
def _match_warehouse_coincidence(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # Columna de ID de línea de DataFrame de ventas
    _sales_df_line_id_column: str,
    # Columna de ID de línea de DataFrame de compras
    _purchase_df_line_id_column: str,
    # Columna de cantidad de DataFrame de ventas
    _sales_df_product_qty_column: str,
    # Columna de cantidad de DataFrame de compras
    _purchase_df_product_qty_column: str,
    # Columna de fecha de facturación de DataFrame de ventas
    _sales_df_invoice_date_column: str,
    # Columna de fecha de orden de DataFrame de compras
    _purchase_df_order_date_column: str,
    # Rango de días permitido
    _tolerance_days_range: int
) -> dict[bool, list[pd.DataFrame, dict]]:
    
    # Se obtiene el registro de venta a buscar en compras
    ven_line = _get_id_to_search(sales_df, _sales_df_invoice_date_column)

    # Se define la ID de este registro de venta
    ven_id = ven_line[_sales_df_line_id_column]

    # Definición de las series de numpy

    # Coincidencia por vendedora
    match_salesperson = purchase_df["warehouse"] == ven_line["warehouse"]
    # Coincidencia por cantidad de producto
    match_product_qty = purchase_df[_purchase_df_product_qty_column] == ven_line[_sales_df_product_qty_column]
    # Coincidencia por rango de fecha
    match_date_inferior_range = purchase_df[_purchase_df_order_date_column] + pd.Timedelta(days=_tolerance_days_range) > ven_line[_sales_df_invoice_date_column]
    match_date_superior_range = purchase_df[_purchase_df_order_date_column] < ven_line[_sales_df_invoice_date_column]
    # Se descartan las compras con todas las cantidades vendidas
    not_matched_completely = purchase_df[_purchase_df_product_qty_column] != purchase_df["sold_qty"]

    # Búsqueda por coincidencia de los tres criterios
    results = purchase_df[(match_salesperson) & (match_product_qty) & (match_date_inferior_range) & (match_date_superior_range) & (not_matched_completely)]

    # Si sí hay información se procede a realizar el [if]
    if results.size:
        # Registro en el DataFrame de ventas
        sales_df.loc[(sales_df[_sales_df_line_id_column] == ven_id), 'warehouse_match'] = results.iloc[0][_purchase_df_line_id_column]
        # Registro en el DataFrame de compras
        purchase_df.loc[(purchase_df[_purchase_df_line_id_column] == results.iloc[0][_purchase_df_line_id_column]), 'sold_qty'] = results.iloc[0][_purchase_df_product_qty_column]
        return {
            'matched': True,
            'dfs': [sales_df, purchase_df]
        }
    else:
        return {
            'matched': False
        }
    
def _get_same_salesperson(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # Línea de venta a comparar
    ven_line: pd.Series,
    # Número de registros a retornar
    rows_qty: int,
    # Columna de fecha de facturación de DataFrame de ventas
    _sales_df_invoice_date_column: str,
    # Columna de vendedora de DataFrame de ventas
    _sales_df_salesperson_column: str,
) -> pd.DataFrame:
    
    # Se ordena el DataFrame por fecha de manera descendente
    sales_df = sales_df.sort_values(_sales_df_invoice_date_column, ascending=False)

    # Se filtra por los registros ya encontrados
    exact_match_na = sales_df['exact_match'].isna()
    warehouse_match_na = sales_df['warehouse_match'].isna()
    group_match_na = sales_df['group_match'].isna()
    # Se filtra por la misma vendedora
    same_salesperson = sales_df[_sales_df_salesperson_column] == ven_line[_sales_df_salesperson_column]
    # Se buscan registros que aún no han sido buscados
    to_search = sales_df['not_found'] == False

    results = sales_df[(exact_match_na) & (warehouse_match_na) & (group_match_na) & (same_salesperson) & (to_search)]
    if len(results) >= rows_qty:
        return results.iloc[0:rows_qty]
    else:
        return []
    
def _match_group_coincidence(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # Columna de ID de línea de DataFrame de ventas
    _sales_df_line_id_column: str,
    # Columna de ID de línea de DataFrame de compras
    _purchase_df_line_id_column: str,
    # Columna de vendedora de DataFrame de ventas
    _sales_df_salesperson_column: str,
    # Columna de vendedora de DataFrame de compras
    _purchase_df_salesperson_column: str,
    # Columna de cantidad de DataFrame de ventas
    _sales_df_product_qty_column: str,
    # Columna de cantidad de DataFrame de compras
    _purchase_df_product_qty_column: str,
    # Columna de fecha de facturación de DataFrame de ventas
    _sales_df_invoice_date_column: str,
    # Columna de fecha de orden de DataFrame de compras
    _purchase_df_order_date_column: str
) -> dict[bool, list[pd.DataFrame, dict]]:
    
    # Se obtiene el registro de venta a buscar en compras
    ven_line = _get_id_to_search(sales_df, _sales_df_invoice_date_column= _sales_df_invoice_date_column)

    # Definición de las series de numpy

    # Coincidencia por vendedora
    match_salesperson = purchase_df[_purchase_df_salesperson_column] == ven_line[_sales_df_salesperson_column]
    # Coincidencia por cantidad de producto
    upper_product_qty = purchase_df[_purchase_df_product_qty_column] > ven_line[_sales_df_product_qty_column]
    # Coincidencia por rango de fecha
    match_date_inferior_range = purchase_df[_purchase_df_order_date_column] + pd.Timedelta(days=10) > ven_line[_sales_df_invoice_date_column]
    match_date_superior_range = purchase_df[_purchase_df_order_date_column] < ven_line[_sales_df_invoice_date_column]
    # Se descartan las compras con todas las cantidades vendidas
    not_matched_completely = purchase_df[_purchase_df_product_qty_column] != purchase_df["sold_qty"]

    # Búsqueda por coincidencia de los tres criterios
    pur_results = purchase_df[(match_salesperson) & (upper_product_qty) & (match_date_inferior_range) & (match_date_superior_range) & (not_matched_completely)]

    if pur_results.size:
        # Se establece la cantidad comprada
        purchased_qty = pur_results.reset_index().at[0, _purchase_df_product_qty_column]

        # Inicialización para búsqueda por ciclo
        ids = 1
        while True:
            ids += 1
            # Se obtiene el slice
            ven_results = _get_same_salesperson(
                sales_df,
                ven_line,
                ids,
                _sales_df_invoice_date_column,
                _sales_df_salesperson_column,
            )

            if len(ven_results):
                # Se establece la suma de cantidades vendidas
                sold_qty = ven_results.iloc[0:ids][_sales_df_product_qty_column].sum()

                if sold_qty < purchased_qty:
                    continue

                if sold_qty == purchased_qty:
                    # Se registran las IDs a emparejar
                    ven_results_ids = ven_results[_sales_df_line_id_column].to_list()
                    # Se registra la ID de compras en ventas
                    sales_df.loc[(sales_df[_sales_df_line_id_column].isin(ven_results_ids)), 'group_match'] = pur_results.iloc[0][_purchase_df_line_id_column]
                    # Se registra la cantidad vendida en compras
                    purchase_df.loc[(purchase_df[_purchase_df_line_id_column] == pur_results.iloc[0][_purchase_df_line_id_column]), 'sold_qty'] = sold_qty
                    return {
                        'matched': True,
                        'dfs': [sales_df, purchase_df]
                    }

                elif sold_qty > purchased_qty:
                    return {
                        'matched': False
                    }

            else:
                return {
                    'matched': False
                }

    else:
        return {
            'matched': False
        }
    
def _mark_as_not_found(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # Columna de ID de línea de DataFrame de ventas
    _sales_df_line_id_column: str,
    # Columna de fecha de facturación de DataFrame de ventas
    _sales_df_invoice_date_column: str
) -> pd.DataFrame:
    # Se obtiene el registro de venta a buscar en compras
    ven_line = _get_id_to_search(sales_df, _sales_df_invoice_date_column= _sales_df_invoice_date_column)

    # Se define la ID de este registro de venta
    ven_id = ven_line[_sales_df_line_id_column]

    # Se marca el registro como no encontrado
    print("Registro no encontrado")
    sales_df.loc[(sales_df[_sales_df_line_id_column] == ven_id), 'not_found'] = True

    return sales_df

def _match_sales_purchases(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # Estado de ejecución
    finish_process,
    # Columna de ID de línea de DataFrame de ventas
    _sales_df_line_id_column: str,
    # Columna de ID de línea de DataFrame de compras
    _purchase_df_line_id_column: str,
    # Columna de vendedora de DataFrame de ventas
    _sales_df_salesperson_column: str,
    # Columna de vendedora de DataFrame de compras
    _purchase_df_salesperson_column: str,
    # Columna de cantidad de DataFrame de ventas
    _sales_df_product_qty_column: str,
    # Columna de cantidad de DataFrame de compras
    _purchase_df_product_qty_column: str,
    # Columna de fecha de facturación de DataFrame de ventas
    _sales_df_invoice_date_column: str,
    # Columna de fecha de orden de DataFrame de compras
    _purchase_df_order_date_column: str,
    # Rango de días permitido
    _tolerance_days_range: int
) -> list[pd.DataFrame, dict]:

    # Se asignan los almacenes a los DataFrames en caso de no haberlos
    if 'warehouse' not in sales_df.columns:
        sales_df = _merge_warehouse(sales_df, salesperson_column_name= _sales_df_salesperson_column)
    if 'warehouse' not in purchase_df.columns:
        purchase_df = _merge_warehouse(purchase_df, salesperson_column_name= _purchase_df_salesperson_column)

    # Se inicializan la columnas en caso de no existir
    try:
        sales_df["exact_match"]
    except KeyError:
        sales_df["exact_match"] = pd.NA
    try:
        sales_df["warehouse_match"]
    except KeyError:
        sales_df["warehouse_match"] = pd.NA
    try:
        sales_df["group_match"]
    except KeyError:
        sales_df["group_match"] = pd.NA
    try:
        sales_df["not_found"]
    except KeyError:
        sales_df["not_found"] = False
    try:
        purchase_df["sold_qty"]
    except KeyError:
        purchase_df["sold_qty"] = 0

    # Búsqueda por coincidencia exacta
    res_exact = _match_exact_coincidence(
        # DataFrame de ventas
        sales_df,
        # DataFrame de compras
        purchase_df,
        # Estado de ejecución
        finish_process,
        # Columna de ID de línea de DataFrame de ventas
        _sales_df_line_id_column,
        # Columna de ID de línea de DataFrame de compras
        _purchase_df_line_id_column,
        # Columna de vendedora de DataFrame de ventas
        _sales_df_salesperson_column,
        # Columna de vendedora de DataFrame de compras
        _purchase_df_salesperson_column,
        # Columna de cantidad de DataFrame de ventas
        _sales_df_product_qty_column,
        # Columna de cantidad de DataFrame de compras
        _purchase_df_product_qty_column,
        # Columna de fecha de facturación de DataFrame de ventas
        _sales_df_invoice_date_column,
        # Columna de fecha de orden de DataFrame de compras
        _purchase_df_order_date_column,
        # Rango de días permitido
        _tolerance_days_range
    )

    # Coincidencia exacta
    if res_exact['matched']:
        print("Coincidencia exacta")
        # print(len(res_exact['dfs']))
        return res_exact['dfs']
    
    elif finish_process['state'] == True:
        return res_exact['dfs']
    
    else:
        # Búsqueda por coincidencia por almacén
        res_warehouse = _match_warehouse_coincidence(
            # DataFrame de ventas
            sales_df,
            # DataFrame de compras
            purchase_df,
            # Columna de ID de línea de DataFrame de ventas
            _sales_df_line_id_column,
            # Columna de ID de línea de DataFrame de compras
            _purchase_df_line_id_column,
            # Columna de cantidad de DataFrame de ventas
            _sales_df_product_qty_column,
            # Columna de cantidad de DataFrame de compras
            _purchase_df_product_qty_column,
            # Columna de fecha de facturación de DataFrame de ventas
            _sales_df_invoice_date_column,
            # Columna de fecha de orden de DataFrame de compras
            _purchase_df_order_date_column,
            # Rango de días permitido
            _tolerance_days_range
        )

        # Coincidencia por almacén
        if res_warehouse['matched']:
            print("Coincidencia por almacén")
            res_warehouse['dfs'].append(finish_process)
            return res_warehouse['dfs']
        
        else:
            # Búsqueda por grupo
            res_group = _match_group_coincidence(
                # DataFrame de ventas
                sales_df,
                # DataFrame de compras
                purchase_df,
                # Columna de ID de línea de DataFrame de ventas
                _sales_df_line_id_column,
                # Columna de ID de línea de DataFrame de compras
                _purchase_df_line_id_column,
                # Columna de vendedora de DataFrame de ventas
                _sales_df_salesperson_column,
                # Columna de vendedora de DataFrame de compras
                _purchase_df_salesperson_column,
                # Columna de cantidad de DataFrame de ventas
                _sales_df_product_qty_column,
                # Columna de cantidad de DataFrame de compras
                _purchase_df_product_qty_column,
                # Columna de fecha de facturación de DataFrame de ventas
                _sales_df_invoice_date_column,
                # Columna de fecha de orden de DataFrame de compras
                _purchase_df_order_date_column
            )

            # Coincidencia por grupo
            if res_group['matched']:
                print("coincidencia por grupo")
                res_group['dfs'].append(finish_process)
                return res_group['dfs']
            
            # Se marca registro como [no encontrado]
            else:
                sales_df = _mark_as_not_found(
                    # DataFrame de ventas
                    sales_df,
                    # Columna de ID de línea de DataFrame de ventas
                    _sales_df_line_id_column,
                    # Columna de fecha de facturación de DataFrame de ventas
                    _sales_df_invoice_date_column
                )

                # Retorno de los DataFrames
                return [sales_df, purchase_df, finish_process]
            
def _find_cost_per_product(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # ID del producto
    product_id: int,
    # Columna de ID del producto en el DataFrame de ventas
    _sales_df_product_id_column: str,
    # Columna de ID del producto en el DataFrame de compras
    _purchase_df_product_id_column: str,
    # Columna de ID de línea de DataFrame de ventas
    _sales_df_line_id_column: str,
    # Columna de ID de línea de DataFrame de compras
    _purchase_df_line_id_column: str,
    # Columna de vendedora de DataFrame de ventas
    _sales_df_salesperson_column: str,
    # Columna de vendedora de DataFrame de compras
    _purchase_df_salesperson_column: str,
    # Columna de cantidad de DataFrame de ventas
    _sales_df_product_qty_column: str,
    # Columna de cantidad de DataFrame de compras
    _purchase_df_product_qty_column: str,
    # Columna de fecha de facturación de DataFrame de ventas
    _sales_df_invoice_date_column: str,
    # Columna de fecha de orden de DataFrame de compras
    _purchase_df_order_date_column: str,
    # Rango de días permitido
    _tolerance_days_range: int
) -> pd.DataFrame:
    # Se realiza el slice del DataFrame para seccionar únicamente por el producto en cuestión
    sales_df = sales_df[sales_df[_sales_df_product_id_column] == product_id]
    purchase_df = purchase_df[purchase_df[_purchase_df_product_id_column] == product_id]

    # Se guardan los nombres de las columnas originales para uso posterior en el retorno
    sales_df_columns = sales_df.columns

    # Se inicializan la columnas para emparejamiento
    sales_df["exact_match"] = pd.NA
    sales_df["warehouse_match"] = pd.NA
    sales_df["group_match"] = pd.NA
    sales_df["not_found"] = False
    purchase_df["sold_qty"] = 0

    # Se inicia el estado de la ejecución
    finish_process = {'state': False}

    # Búsqueda exhaustiva por cada una de las líneas
    while not finish_process['state']:
        sales_df, purchase_df, finish_process = _match_sales_purchases(
            # DataFrame de ventas
            sales_df,
            # DataFrame de compras
            purchase_df,
            # Estado de ejecución
            finish_process,
            # Columna de ID de línea de DataFrame de ventas
            _sales_df_line_id_column,
            # Columna de ID de línea de DataFrame de compras
            _purchase_df_line_id_column,
            # Columna de vendedora de DataFrame de ventas
            _sales_df_salesperson_column,
            # Columna de vendedora de DataFrame de compras
            _purchase_df_salesperson_column,
            # Columna de cantidad de DataFrame de ventas
            _sales_df_product_qty_column,
            # Columna de cantidad de DataFrame de compras
            _purchase_df_product_qty_column,
            # Columna de fecha de facturación de DataFrame de ventas
            _sales_df_invoice_date_column,
            # Columna de fecha de orden de DataFrame de compras
            _purchase_df_order_date_column,
            # Rango de días permitido
            _tolerance_days_range
        )

    # Se crea un resumen de las IDs encontradas
    summary = (
        sales_df
        [['exact_match', 'warehouse_match', 'group_match', 'not_found']]
        .replace(
            {
                pd.NA: ''
            }
        )
    )

    # Se realiza la transformación del DataFrame para su mejor legibilidad
    summary['matched_id'] = summary['exact_match'].astype(str) + summary['warehouse_match'].astype(str) + summary['group_match'].astype(str)
    summary.loc[summary['exact_match'] != '', 'match_type'] = 'exact match'
    summary.loc[summary['warehouse_match'] != '', 'match_type'] = 'warehouse match'
    summary.loc[summary['group_match'] != '', 'match_type'] = 'group match'
    summary.loc[summary['match_type'].isna(), 'match_type'] = 'not found'
    summary = summary[['matched_id', 'match_type']]

    # Se fusiona el resumen junto con el DataFrame de los productos, usando los nombres de las columnas originales
    sales_df = pd.merge(
    left= sales_df[sales_df_columns],
    right= summary,
    left_index= True,
    right_index= True,
    how= 'left'
)

    # Retorno de ambos DataFrames
    return (sales_df, purchase_df)

def assign_cost(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # Columna de ID del producto en el DataFrame de ventas
    sales_df_product_id_column: str,
    # Columna de ID del producto en el DataFrame de compras
    purchase_df_product_id_column: str,
    # Columna de ID de línea de DataFrame de ventas
    sales_df_line_id_column: str,
    # Columna de ID de línea de DataFrame de compras
    purchase_df_line_id_column: str,
    # Columna de vendedora de DataFrame de ventas
    sales_df_salesperson_column: str,
    # Columna de vendedora de DataFrame de compras
    purchase_df_salesperson_column: str,
    # Columna de cantidad de DataFrame de ventas
    sales_df_product_qty_column: str,
    # Columna de cantidad de DataFrame de compras
    purchase_df_product_qty_column: str,
    # Columna de fecha de facturación de DataFrame de ventas
    sales_df_invoice_date_column: str,
    # Columna de fecha de orden de DataFrame de compras
    purchase_df_order_date_column: str,
    # Rango de días permitido
    tolerance_days_range: int = 10
):
    
    # Se obtiene la lista de códigos de producto
    product_ids = list(sales_df[sales_df_product_id_column].unique())


    # Inicialización de los nuevos DataFrames a retornar
    processed_sales_df = pd.DataFrame(columns=sales_df.columns)
    processed_purchase_df= pd.DataFrame(columns=purchase_df.columns)

    for product_id in product_ids:

        slice_sales_df, slice_purchase_df = _find_cost_per_product(
            # DataFrame de ventas
            sales_df,
            # DataFrame de compras
            purchase_df,
            # ID del producto
            product_id,
            # Columna de ID del producto en el DataFrame de ventas
            sales_df_product_id_column,
            # Columna de ID del producto en el DataFrame de compras
            purchase_df_product_id_column,
            # Columna de ID de línea de DataFrame de ventas
            sales_df_line_id_column,
            # Columna de ID de línea de DataFrame de compras
            purchase_df_line_id_column,
            # Columna de vendedora de DataFrame de ventas
            sales_df_salesperson_column,
            # Columna de vendedora de DataFrame de compras
            purchase_df_salesperson_column,
            # Columna de cantidad de DataFrame de ventas
            sales_df_product_qty_column,
            # Columna de cantidad de DataFrame de compras
            purchase_df_product_qty_column,
            # Columna de fecha de facturación de DataFrame de ventas
            sales_df_invoice_date_column,
            # Columna de fecha de orden de DataFrame de compras
            purchase_df_order_date_column,
            # Rango de días permitido
            tolerance_days_range
        )

        processed_sales_df = pd.concat([processed_sales_df, slice_sales_df])
        processed_purchase_df = pd.concat([processed_purchase_df, slice_purchase_df])

    return processed_sales_df, processed_purchase_df
