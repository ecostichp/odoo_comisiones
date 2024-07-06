import pandas as pd

# No contiene un valor en la columna 'exact_match'
_exact_match_na = lambda sales_df: sales_df['exact_match'].isna()
# No contiene un valor en la columna 'warehouse_match'
_warehouse_match_na = lambda sales_df: sales_df['warehouse_match'].isna()
# No contiene un valor en la columna 'group_match'
_group_match_na = lambda sales_df: sales_df['group_match'].isna()
# No ha sido buscado
_to_search = lambda sales_df: sales_df['not_found'] == False

# Coincidencia por vendedora
_match_salesperson = lambda purchase_df, ven_line, _arguments: purchase_df[_arguments['purchase_df_salesperson_column']] == ven_line[_arguments['sales_df_salesperson_column']]
# Coincidencia por almacén
_match_warehouse = lambda purchase_df, ven_line, _arguments: purchase_df["warehouse"] == ven_line["warehouse"]
# Coincidencia por cantidad de producto
_match_product_qty = lambda purchase_df, ven_line, _arguments: purchase_df[_arguments['purchase_df_product_qty_column']] == ven_line[_arguments['sales_df_product_qty_column']]
# Coincidencia por cantidad de producto superior en compras
_upper_product_qty = lambda purchase_df, ven_line, _arguments: purchase_df[_arguments["purchase_df_product_qty_column"]] > ven_line[_arguments["sales_df_product_qty_column"]]
# Coincidencia por cantidad de producto inferior en compras
_lower_product_qty = lambda purchase_df, ven_line, _arguments: purchase_df[_arguments["purchase_df_product_qty_column"]] < ven_line[_arguments["sales_df_product_qty_column"]]
# Se descartan las compras con todas las cantidades vendidas
_not_matched_completely = lambda purchase_df, ven_line, _arguments: purchase_df[_arguments["purchase_df_product_qty_column"]] != purchase_df["sold_qty"]

# Coincidencia por rango de fecha
_match_date_range = lambda purchase_df, ven_line, _arguments: (
    (purchase_df[_arguments['purchase_df_order_date_column']] + pd.Timedelta(days=_arguments['tolerance_days_range']) > ven_line[_arguments['sales_df_invoice_date_column']])
    & (purchase_df[_arguments['purchase_df_order_date_column']] < ven_line[_arguments['sales_df_invoice_date_column']])
)

# Diccionarios contenedores de funciones de validación
sale_conditions = {
    "exact_match_na": _exact_match_na,
    "warehouse_match_na": _warehouse_match_na,
    "group_match_na": _group_match_na,
    "to_search": _to_search
}
purchase_conditions = {
    "match_salesperson": _match_salesperson,
    "match_warehouse": _match_warehouse,
    "match_product_qty": _match_product_qty,
    "not_matched_completely": _not_matched_completely,
    "upper_product_qty": _upper_product_qty,
    "lower_product_qty": _lower_product_qty,
    "match_date_range": _match_date_range
}