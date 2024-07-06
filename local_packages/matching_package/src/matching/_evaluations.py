import pandas as pd
from ._conditions import sale_conditions, purchase_conditions

def _evaluate_records_to_search(
    sales_df: pd.DataFrame
) -> pd.Series:
    """
    ## Evaluación de registros a buscar
    Esta función interna realiza varias evaluaciones en un slice del DataFrame
    de ventas para definir cuáles registros son candidatos a búsqueda.

    La evaluación se realiza por los registros que aún no tengan una ID de
    línea de compra registrada en ninguna de sus columnas de emparejamiento
    o, en su defecto, que tampoco estén marcadas como `No encontradas`.
    """
    # Se buscan registros no empatados por coincidencia exacta
    exact_match_na = sale_conditions["exact_match_na"](sales_df)
    # Se buscan registros no empatados por coincidencia de almacén
    warehouse_match_na = sale_conditions["warehouse_match_na"](sales_df)
    # Se buscan registros no empatados por coincidencia de grupo
    group_match_na = sale_conditions["group_match_na"](sales_df)
    # Se buscan registros que aún no han sido buscados
    to_search = sale_conditions["to_search"](sales_df)

    # Resultados de la búsqueda
    return (exact_match_na) & (warehouse_match_na) & (group_match_na) & (to_search)

def _create_multiple_conditional(
    df: pd.DataFrame,
    sale_line: pd.Series,
    _arguments,
    conditions: list
):
    """
    ## Creación de unión de evaluaciones
    Esta función interna recibe un DataFrame de compra y un registro de venta a
    evaluar junto con una lista de nombres de funciones que validan los
    registros con diferentes criterios y valores. Posterior a esto se retorna
    una serie de Pandas que reune la unión de todas las condiciones (and) para
    poder usarla en un DataFrame como filtro masivo.
    
    Los nombres de funciones disponibles son los siguientes:
    - `match_salesperson`: Evalúa si la vendedora en la compra es la misma en
    la venta.
    - `match_warehouse`: Evalúa si almacén designado de la vendedora en la
    compra es el mismo en la venta.
    - `match_product_qty`: Evalúa si la cantidad de un producto en la compra es
    la misma que en la venta.
    - `not_matched_completely`: Evalúa si aún hay existencias compradas de un
    producto que aún no han sido emparejadas con una venta.
    - `upper_product_qty`: Evalúa si la cantidad de un producto en la compra es
    superior a la cantidad del producto en la venta.
    - `match_date_range`: Evalúa si la compra es cercana a la venta en términos
    de fecha, dependiendo del rango de días provisto en la entrada de la
    función maestra.
    """

    # Se crea una lista vacía para contener las coincidencias por serie de pandas
    evaluations = []
    # Iteración de la lista entrante de cadenas de texto
    for cond in conditions:
        # Se ejecuta la función de validación correspondiente
        evaluations.append(purchase_conditions[cond](df, sale_line, _arguments))

    # Se crea una condición base para almacenar la suma de condiciones
    evaluations.insert(0, (evaluations[0] == True) | (evaluations[0] == False))

    # Se toma la condición base
    final_condition = evaluations[0]

    # Se realiza una iteración por cada una de las condiciones dentro de la lista inicial
    for cond in evaluations[1:]:
        # Unión de condiciones
        final_condition = final_condition & cond

    # Retorno de la unión de todas las coincidencias
    return final_condition

def _get_id_to_search(
    sales_df: pd.DataFrame,
    _sales_df_invoice_date_column: str
) -> pd.Series:
    """
    ## Obtener ID de ventas a buscar en compras
    Función interna que devuelve un registro a buscar
    """
    # Se ordena el DataFrame por fecha de más reciente a más antiguo
    sales_df = sales_df.sort_values(_sales_df_invoice_date_column, ascending=False)

    # Obtención de registros candidatos a búsqueda
    evaluations = _evaluate_records_to_search(sales_df)

    # Se obtienen las filas con las coincidencias de los criterios
    results = sales_df[evaluations]

    # Retorno de la fila a buscar
    if results.size:
        return results.iloc[0]
    # En caso de no devolver ningún registro, el error se encapsula y se ejecuta la terminación exitosa de la función mayor

def _get_same_salesperson(
    # DataFrame de ventas
    sales_df: pd.DataFrame,
    # Línea de venta a comparar
    ven_line: pd.Series,
    # Número de registros a retornar
    rows_qty: int,
    # Diccionario de argumentos
    _arguments: dict
) -> pd.DataFrame:
    """
    ## Búsqueda de líneas con la misma vendedora
    Esta función interna de búsqueda devuelve un slice de DataFrame para
    la búsqueda de varias ventas que corresponden a una misma compra. La
    función padre de esta función provee una cantidad siempre mayor hasta que
    se registra un emparejamiento o se descarta la búsqueda por grupo
    de venta a compra.
    """
    
    # Se ordena el DataFrame por fecha de manera descendente
    sales_df = sales_df.sort_values(_arguments["sales_df_invoice_date_column"], ascending=False)

    # Obtención de registros candidatos a búsqueda
    evaluations = _evaluate_records_to_search(sales_df)

    # Filtro adicional por la misma vendedora
    same_salesperson = sales_df[_arguments["sales_df_salesperson_column"]] == ven_line[_arguments["sales_df_salesperson_column"]]

    # Obtención de coincidencias
    results = sales_df[evaluations & same_salesperson]
    # Si se encuentran coincidencias se realiza la ejecución condicional
    if len(results) >= rows_qty:
        # Se retorna la cantidad n de registros solicitados por la función padre
        return results.iloc[0:rows_qty]
    else:
        # Se retorna una lista vacía para indicar un 0 con evaluación por tamaño
        return []