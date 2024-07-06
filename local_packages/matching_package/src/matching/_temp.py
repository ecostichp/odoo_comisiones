"""
## Herramientas provisionales
Módulo contenedor de funciones y variables provisionales
"""


import pandas as pd
from . import _constants

def _merge_warehouse(
    incoming_df: pd.DataFrame,
    salesperson_column_name: str
) -> pd.DataFrame:
    """
    ## Relacionar almacén con vendedora
    Función interna para añadir la información sobre a qué almacén pertenece cada
    vendedora en cada línea de un DataFrame.
    
    Parámetros de entrada:
    `incoming_df`: DataFrame entrante
    `salesperson_column_name`: Nombre de la columna en donde se encuentran los
    valores de nombres de las vendedoras

    Retorno:
    DataFrame provisto con una columna agregada llamada `warehouse` en donde
    se encuentra el almacén al que la vendedora pertenece si fue encontrada.
    """

    # Se crea el DataFrame que contiene la información sobre a qué almacén pertenece cada vendedora de CE
    warehouses = pd.DataFrame(_constants.salesperson_warehouse_relation)

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