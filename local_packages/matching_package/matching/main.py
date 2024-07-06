# Importaciones
import pandas as pd
import warnings
from ._temp import _merge_warehouse
from ._match_methods import (
    _match_exact_coincidence,
    _match_warehouse_coincidence,
    _match_sale_group_coincidence,
    _match_purchase_group_coincidence,
    _mark_as_not_found
)
from ._visual_progress import ProgressStats

# Configuración
pd.set_option('mode.chained_assignment', None)
warnings.simplefilter("ignore", category=FutureWarning)

def _match_sales_purchases(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # Estado de ejecución
    finish_process,
    # Diccionario de argumentos
    _arguments: dict,
    # Estadísticas
    stats: ProgressStats
) -> list[pd.DataFrame, dict]:


    # Se asignan los almacenes a los DataFrames en caso de no haberlos
    if 'warehouse' not in sales_df.columns:
        sales_df = _merge_warehouse(sales_df, salesperson_column_name= _arguments["sales_df_salesperson_column"])
    if 'warehouse' not in purchase_df.columns:
        purchase_df = _merge_warehouse(purchase_df, salesperson_column_name= _arguments["purchase_df_salesperson_column"])

    # Búsqueda por coincidencia exacta
    res_exact = _match_exact_coincidence(
        # DataFrame de ventas
        sales_df,
        # DataFrame de compras
        purchase_df,
        # Estado de ejecución
        finish_process,
        # Diccionario de argumentos
        _arguments
    )

    # Coincidencia exacta
    if res_exact['matched']:
        stats.increase("Coincidencia exacta")
        # print(len(res_exact['dfs']))
        return res_exact['dfs']
    
    elif finish_process['state'] == True:
        return res_exact['dfs']
    
    # Búsqueda por coincidencia por almacén
    res_warehouse = _match_warehouse_coincidence(
        # DataFrame de ventas
        sales_df,
        # DataFrame de compras
        purchase_df,
        # Diccionario de argumentos
        _arguments
    )

    # Coincidencia por almacén
    if res_warehouse['matched']:
        stats.increase("Coincidencia por almacén")
        res_warehouse['dfs'].append(finish_process)
        return res_warehouse['dfs']
        
    # Búsqueda por grupo
    sale_group = _match_sale_group_coincidence(
        # DataFrame de ventas
        sales_df,
        # DataFrame de compras
        purchase_df,
        # Diccionario de argumentos
        _arguments
    )

    if sale_group['matched']:
        stats.increase("coincidencia por grupo (ventas)")
        sale_group['dfs'].append(finish_process)
        return sale_group['dfs']

    pur_group = _match_purchase_group_coincidence(
        # DataFrame de ventas
        sales_df,
        # DataFrame de compras
        purchase_df,
        # Diccionario de argumentos
        _arguments
    )

    if pur_group['matched']:
        stats.increase("coincidencia por grupo (compras)")
        pur_group['dfs'].append(finish_process)
        return pur_group['dfs']

    # Se marca registro como [no encontrado]
    sales_df = _mark_as_not_found(
        # DataFrame de ventas
        sales_df,
        # Diccionario de argumentos
        _arguments
    )

    stats.increase("Sin encontrar")

    # Retorno de los DataFrames
    return [sales_df, purchase_df, finish_process]

def _find_cost_per_product(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # DataFrame de compras
    purchase_df: pd.DataFrame,
    # ID del producto
    product_id: int,
    # Diccionario de argumentos
    _arguments: dict,
    # Estadísticas
    stats: ProgressStats
) -> tuple[pd.DataFrame]:
    # Se realiza el slice del DataFrame para seccionar únicamente por el producto en cuestión
    sales_df = sales_df[sales_df[_arguments["sales_df_product_id_column"]] ==product_id]
    purchase_df = purchase_df[purchase_df[_arguments["purchase_df_product_id_column"]] == product_id]

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
            # Diccionario de argumentos
            _arguments,
            # Estadísticas
            stats
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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Definición de un diccionario de argumentos para un uso más fácil:
    _arguments = {
        "sales_df_product_id_column": sales_df_product_id_column,
        "purchase_df_product_id_column": purchase_df_product_id_column,
        "sales_df_line_id_column": sales_df_line_id_column,
        "purchase_df_line_id_column": purchase_df_line_id_column,
        "sales_df_salesperson_column": sales_df_salesperson_column,
        "purchase_df_salesperson_column": purchase_df_salesperson_column,
        "sales_df_product_qty_column": sales_df_product_qty_column,
        "purchase_df_product_qty_column": purchase_df_product_qty_column,
        "sales_df_invoice_date_column": sales_df_invoice_date_column,
        "purchase_df_order_date_column": purchase_df_order_date_column,
        "tolerance_days_range": tolerance_days_range
    }

    # Se obtiene la lista de códigos de producto
    product_ids = list(sales_df[sales_df_product_id_column].unique())

    # Inicialización de los nuevos DataFrames a retornar
    processed_sales_df = pd.DataFrame(columns=sales_df.columns)
    processed_purchase_df= pd.DataFrame(columns=purchase_df.columns)

    stats = ProgressStats(
        [
            "Coincidencia exacta",
            "Coincidencia por almacén",
            "coincidencia por grupo (ventas)",
            "coincidencia por grupo (compras)",
            "coincidencia por grupo (compras)",
            "Sin encontrar"
        ]
    )

    for product_id in product_ids:
        slice_sales_df, slice_purchase_df = _find_cost_per_product(
            # DataFrame de ventas
            sales_df,
            # DataFrame de compras
            purchase_df,
            # ID del producto
            product_id,
            # Diccionario de argumentos
            _arguments,
            # Estadísticas
            stats
        )

        processed_sales_df = pd.concat([processed_sales_df, slice_sales_df])
        processed_purchase_df = pd.concat([processed_purchase_df, slice_purchase_df])

    return processed_sales_df, processed_purchase_df