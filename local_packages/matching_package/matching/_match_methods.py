import pandas as pd
from ._evaluations import(
    _create_multiple_conditional,
    _get_id_to_search,
    _get_same_salesperson
)
from ._list_storer import ListStorer

def _match_exact_coincidence(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # Estado de ejecución
    finish_process,
    # Diccionario de argumentos
    _arguments: dict
) -> dict[bool, list[pd.DataFrame, dict]]:
    """
    ## Búsqueda por coincidencia exacta
    Función interna de emparejamiento que busca registros de venta en registros
    de compra que cumplan los siguientes criterios:
    - Misma vendedora
    - Misma cantidad de producto
    - Rango de fecha dentro de tolerancia (provisto en la función maestra)
    """
    
    # Se obtiene el registro de venta a buscar en compras
    ven_line = _get_id_to_search(sales_df, _arguments["sales_df_invoice_date_column"])

    # Se define la ID de este registro de venta
    try:
        ven_id = ven_line[_arguments["sales_df_line_id_column"]]
    except TypeError:
        finish_process['state'] = True
        # print("Búsqueda de emparejamiento con producto terminada")
        return {
            'matched': False,
            'dfs': [sales_df, purchase_df, finish_process]
        }

    # Evaluaciones de condiciones definidas para la búsqueda
    evaluations = _create_multiple_conditional(
        df= purchase_df,
        sale_line= ven_line,
        _arguments= _arguments,
        conditions= [
            "match_salesperson",
            "match_product_qty",
            "match_date_range",
            "not_matched_completely"
        ]
    )

    # Se obtienen las filas con las coincidencias de los criterios
    results = purchase_df[evaluations]

    # Si sí hay información se procede a realizar el [if]
    if results.size:
        # Registro en el DataFrame de ventas
        sales_df.loc[(sales_df[_arguments["sales_df_line_id_column"]] == ven_id), 'exact_match'] = results.iloc[0][_arguments["purchase_df_line_id_column"]]
        # Registro en el DataFrame de compras
        purchase_df.loc[(purchase_df[_arguments["purchase_df_line_id_column"]] == results.iloc[0][_arguments["purchase_df_line_id_column"]]), 'sold_qty'] = results.iloc[0][_arguments["purchase_df_product_qty_column"]]
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
    # Diccionario de argumentos
    _arguments: dict
) -> dict[bool, list[pd.DataFrame, dict]]:
    """
    ## Búsqueda por coincidencia en almacén
    Función interna de emparejamiento que busca registros de venta en registros
    de compra que cumplan los siguientes criterios:
    - Mismo almacén designado a la vendedora en venta y en compra
    - Misma cantidad de producto
    - Rango de fecha dentro de tolerancia (provisto en la función maestra)
    """
    
    # Se obtiene el registro de venta a buscar en compras
    ven_line = _get_id_to_search(sales_df, _arguments["sales_df_invoice_date_column"])

    # Se define la ID de este registro de venta
    ven_id = ven_line[_arguments["sales_df_line_id_column"]]

    # Evaluaciones de condiciones definidas para la búsqueda
    evaluations = _create_multiple_conditional(
        df= purchase_df,
        sale_line= ven_line,
        _arguments= _arguments,
        conditions= [
            "match_warehouse",
            "match_product_qty",
            "match_date_range",
            "not_matched_completely"
        ]
    )

    # Se obtienen las filas con las coincidencias de los criterios
    results = purchase_df[evaluations]

    # Si sí hay información se procede a realizar el [if]
    if results.size:
        # Registro en el DataFrame de ventas
        sales_df.loc[(sales_df[_arguments["sales_df_line_id_column"]] == ven_id), 'warehouse_match'] = results.iloc[0][_arguments["purchase_df_line_id_column"]]
        # Registro en el DataFrame de compras
        purchase_df.loc[(purchase_df[_arguments["purchase_df_line_id_column"]] == results.iloc[0][_arguments["purchase_df_line_id_column"]]), 'sold_qty'] = results.iloc[0][_arguments["purchase_df_product_qty_column"]]
        return {
            'matched': True,
            'dfs': [sales_df, purchase_df]
        }
    else:
        return {
            'matched': False
        }

def _match_sale_group_coincidence(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # Diccionario de argumentos
    _arguments: dict
) -> dict[bool, list[pd.DataFrame, dict]]:
    """
    ## Búsqueda de varias ventas con una misma compra
    Función interna de emparejamiento que intenta emparejar varios registros de
    ventas con una misma compra que cumpla con los siguientes criterios:
    - Misma vendedora
    - Misma cantidad de producto
    - Rango de fecha dentro de tolerancia (provisto en la función maestra)
    """
    
    # Se obtiene el registro de venta a buscar en compras
    ven_line = _get_id_to_search(sales_df, _sales_df_invoice_date_column= _arguments["sales_df_invoice_date_column"])

    evaluations = _create_multiple_conditional(
        df= purchase_df,
        sale_line= ven_line,
        _arguments= _arguments,
        conditions= [
            "match_salesperson",
            "upper_product_qty",
            "match_date_range",
            "not_matched_completely"
        ]
    )

    # Se obtienen las filas con las coincidencias de los criterios
    pur_results = purchase_df[evaluations]

    if pur_results.size:
        # Se establece la cantidad comprada
        purchased_qty = pur_results.reset_index().at[0, _arguments["purchase_df_product_qty_column"]]

        # Inicialización para búsqueda por ciclo
        ids = 1
        while True:
            ids += 1
            # Se obtiene el slice
            ven_results = _get_same_salesperson(
                sales_df,
                ven_line,
                ids,
                _arguments,
            )

            # Se evalúa que existan coincidencias
            if len(ven_results):
                # Se establece la suma de cantidades vendidas
                sold_qty = ven_results.iloc[0:ids][_arguments["sales_df_product_qty_column"]].sum()

                # Si la cantidad comprada aún no es igual o mayor a la cantidad vendida, se continúa a la siguiente iteración
                if sold_qty < purchased_qty:
                    continue

                # Si la cantidad comprada es igual a la cantidad vendida, se ejecutan las líneas dentro del bloque
                if sold_qty == purchased_qty:
                    # Se toma la lista de las IDs de ventas a emparejar con compras
                    ven_results_ids = ven_results[_arguments["sales_df_line_id_column"]].to_list()
                    # Se registra la ID de compras en las líneas de sus ventas emparejadas
                    sales_df.loc[(sales_df[_arguments["sales_df_line_id_column"]].isin(ven_results_ids)), 'group_match'] = pur_results.iloc[0][_arguments["purchase_df_line_id_column"]]
                    # Se registra la cantidad vendida en la línea de compras
                    purchase_df.loc[(purchase_df[_arguments["purchase_df_line_id_column"]] == pur_results.iloc[0][_arguments["purchase_df_line_id_column"]]), 'sold_qty'] = sold_qty
                    # Se retornan los DataFrames
                    return {
                        'matched': True,
                        'dfs': [sales_df, purchase_df]
                    }

                # Si la cantidad comprada es superior a la vendida en la iteración actual se procede a descartar el emparejamiento
                elif sold_qty > purchased_qty:
                    # Se retorna un Falso en el indicador de emparejamiento
                    return {
                        'matched': False
                    }
            # Si el tamaño del retorno de la búsqueda es 0 se retorna un Falso en el indicador de emparejamiento
            else:
                return {
                    'matched': False
                }
    # Si no se encontró ningún registro se retorna un Falso en el indicador de emparejamiento
    else:
        return {
            'matched': False
        }

def _match_purchase_group_coincidence(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # Diccionario de argumentos
    _arguments: dict
) -> dict[bool, list[pd.DataFrame, dict]]:
    # Se obtiene el registro de venta a buscar en compras
    ven_line = _get_id_to_search(sales_df, _sales_df_invoice_date_column= _arguments["sales_df_invoice_date_column"])

    evaluations = _create_multiple_conditional(
        df= purchase_df,
        sale_line= ven_line,
        _arguments= _arguments,
        conditions= [
            "match_salesperson",
            "lower_product_qty",
            "match_date_range",
            "not_matched_completely"
        ]
    )

    # Se obtienen las filas con las coincidencias de los criterios
    pur_results = purchase_df[evaluations].sort_values(_arguments["purchase_df_order_date_column"])

    if pur_results.size:
        # Se establece la cantidad comprada
        sold_qty = ven_line[_arguments["sales_df_product_qty_column"]]

        # Inicialización para búsqueda por ciclo
        records_qty = 1
        while True:
            records_qty += 1

            # Se evalúa que existan coincidencias suficientes
            if records_qty <= len(pur_results):
                # Si la cantidad de productos vendida aún es mayor a la suma de cantidad en compras, se inicia una nueva iteración
                if sold_qty > purchase_df[_arguments["purchase_df_product_qty_column"]].iloc[:records_qty].sum():
                    continue

                # Si la cantidad de productos vendidos es igual a la suma de cantidad en compras, se ejecuta el bloque de código
                elif sold_qty > purchase_df[_arguments["purchase_df_product_qty_column"]].iloc[:records_qty].sum():
                    # Se obtiene una lista de IDs en el objeto ListSorter para poder almacenar la lista dentro del valor del DataFrame
                    purchase_ids = ListStorer(purchase_df[_arguments["purchase_df_line_id_column"]].iloc[:records_qty].to_list())
                    # Se asigna la lista de IDs a la ID de venta
                    sales_df.loc[(sales_df[_arguments["sales_df_line_id_column"]] == ven_line['id']), 'group_match'] = purchase_ids
                    # Se copian las cantidades compradas de las líneas a su columna 'sold_qty'
                    purchase_df.loc[(purchase_df[_arguments["purchase_df_line_id_column"]].isin(purchase_ids.values)), 'sold_qty'] = purchase_df[purchase_df[_arguments["purchase_df_product_qty_column"]].isin(purchase_ids.values)]
                    # Retorno del diccionario
                    return {
                        'matched': True,
                        'dfs': [sales_df, purchase_df]
                    }
                else:
                    # Se retorna un Falso en el indicador de emparejamiento
                    return {
                        'matched': False
                    }
            # Si el tamaño del retorno de la búsqueda es 0 se retorna un Falso en el indicador de emparejamiento
            else:
                return {
                    'matched': False
                }
        # Si no se encontró ningún registro se retorna un Falso en el indicador de emparejamiento
    else:
        return {
            'matched': False
        }

def _mark_as_not_found(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # Diccionario de argumentos
    _arguments: dict
) -> pd.DataFrame:
    # Se obtiene el registro de venta a buscar en compras
    ven_line = _get_id_to_search(sales_df, _sales_df_invoice_date_column= _arguments["sales_df_invoice_date_column"])

    # Se define la ID de este registro de venta
    ven_id = ven_line[_arguments["sales_df_line_id_column"]]

    sales_df.loc[(sales_df[_arguments["sales_df_line_id_column"]] == ven_id), 'not_found'] = True

    return sales_df