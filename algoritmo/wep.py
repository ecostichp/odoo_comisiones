# Se importan las librerías necesarias
# Librerías para manejo del sistema
import os
from pathlib import Path
from datetime import datetime, timedelta

# Librerías para manejar accesos a datos (SQL y API)
from sqlalchemy import create_engine
import xmlrpc.client

# Librerías para ciencia de datos
import pandas as pd



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


mes = int(input('Escribe el número de mes: ')) 

search_fact = search_fact_func(mes)





# Se importan variables de entorno


api_url = os.environ.get('ODOO_URL_API')
api_db = os.environ.get('ODOO_DB_API')
# api_db = os.environ.get('ODOO_DB_PRUEBA_API')
api_username = os.environ.get('ODOO_USERNAME_API')
api_clave = os.environ.get('ODOO_CLAVE_API')

# Se instancia el objeto engine para manejar el acceso a la base de datos local


db_file = 'local_db.db'
db_file_path_str = str(Path().cwd().parent.joinpath(Path(f'data/{db_file}')))

engine = create_engine(f'sqlite:///{db_file_path_str}')

# Se instancia el objeto models para ejecutar métodos a los modelos del API de Odoo


common = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/common')
uid = common.authenticate(api_db, api_username, api_clave, {})
models = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/object')

# # <span style="color:steelblue">Funciones<span>


def if_list_gt0_idex (item: dict, key: str, index:int ) -> None | int:
        val = item[key]

        if val:
            if len(val) == 0:
                return None
            else:
                return val[index]
        else:
            return None

# # <span style="color:steelblue">Descripciones DataFrames<span>


desc_uripsers_fields = ['name', 'state', 'sale_team_id']
descrip_users_json = models.execute_kw(api_db, uid, api_clave, 'res.users', 'search_read', [], {'fields': desc_uripsers_fields})

descrip_users_data = []

for user in descrip_users_json:
    new = {}
    new['id'] = user['id']
    new['name'] = user['name']
    new['state'] = user['state']
    new['sale_team_id'] = if_list_gt0_idex(user, 'sale_team_id', 0)
    new['sale_team_description'] = if_list_gt0_idex(user, 'sale_team_id', 1)

    descrip_users_data.append(new)


descrip_users_df = pd.DataFrame(descrip_users_data)
descrip_users_df['sale_team_id'] = descrip_users_df['sale_team_id'].astype('Int64')

descrip_users_df.loc[descrip_users_df['sale_team_id'].isin([5,6]), 'business_model'] = 'Piso'
descrip_users_df.loc[descrip_users_df['sale_team_id'].isin([7,8]), 'business_model'] = 'CE'
descrip_users_df.loc[descrip_users_df['sale_team_id'].isin([5,7]), 'warehouse'] = 'A1'
descrip_users_df.loc[descrip_users_df['sale_team_id'].isin([6,8]), 'warehouse'] = 'A2'

descrip_sales_users_df  = descrip_users_df.loc[~descrip_users_df['sale_team_id'].isna()]




desc_product_fields = ['name', 'default_code']
desc_product_json = models.execute_kw(api_db, uid, api_clave, 'product.template', 'search_read', [], {'fields': desc_product_fields})
desc_product_df = pd.DataFrame(desc_product_json)
desc_product_df.columns = ['id_producto', 'descripción_producto', 'código_producto']



# # <span style="color:steelblue">Algoritmo<span>

# Se buscan los ids de las facturas en el modelo <font color="#F414FA">account.move</font> con los criterios de búsqueda `search_fact`. Esto devuelve una lista de enteros (ids de estas facturas) que se almacena en `fact_doc_ids`.
# 
# Después, se especifican los campos necesarios de este modelo en `fact_doc_fields` y se genera la lectura de los ids anteriores. Esto devuelve una lista de diccionarios con la información de cada factura que se almacena en `fact_doc_json`




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
          'invoice_user_id',
          'team_id',
          ]

fact_doc_ids = models.execute_kw(api_db, uid, api_clave, 'account.move', 'search', [search_fact])
fact_doc_json = models.execute_kw(api_db, uid, api_clave, 'account.move', 'read', [fact_doc_ids], {'fields': fact_doc_fields})


# El esquema que se obtiene en el modelo de `fact_doc_json` es un esquema donde cada renglón es una factura. Sin embargo, lo que ocupamos es un esquema donde cada renglón es una línea de producto de cada una de las facturas que se encuentran en `fact_doc_json`.
# 
# Para hacer esto se preparan los datos con un ciclo <font color="#14E4FA">for</font> donde se manipula `fact_doc_json`. Para cada factura dentro de este modelo, se utiliza el campo llamado <font color="#14FA4C">"line_ids"</font> (que son la cantidad de líneas que tiene cada factura) para que con un segundo ciclo "for" se genere un diccionario de "línea de factura" por cada "línea de factura" que tiene cada documento de factura.
# 
# Este diccionario ya manipulado se ingresa a una lista vacía `data_fact`. La lista ya cargada con todos sus elementos es la que se convierte en el DataFrame `fact_doc_df`. Este data frame sólo tiene información de la factura, no hay información de sus líneas, sólo el id de ellas.
# 
# Para obtener la información de las líneas de factura se genera `fact_doc_ids`, el cual es el listado de los ids de las líneas de cada factura del trabajo anterior. Esto es importante porque se asegura que las ids de línea vienen únicamente de las ids de las facturas que se especificaron en los criterios de búsqueda generales `search_fact`.


data_fact = []
fact_line_ids = []

for fact in fact_doc_json:
    for line in fact['line_ids']:
        new = {}
        new['fact_doc_id'] = fact['id']
        new['name'] = fact['name']
        new['invoice_date'] = fact['invoice_date']
        new['state'] = fact['state']
        new['invoice_origin'] = fact['invoice_origin']
        new['module_origin'] = None
        new['pos_doc_id'] = if_list_gt0_idex(fact, 'pos_order_ids', 0)
        new['move_type'] = fact['move_type']
        new['reversal_move_id'] = if_list_gt0_idex(fact, 'reversal_move_id', 0)
        new['reversed_entry_id'] = if_list_gt0_idex(fact, 'reversed_entry_id', 0)
        new['journal_id'] = fact['journal_id'][0]
        new['company_id'] = fact['company_id'][0]
        new['partner_id'] = fact['partner_id'][0]
        new['fact_line_id'] = line

        new['invoice_user_id'] = fact['invoice_user_id'][0]
        new['team_id'] = if_list_gt0_idex(fact, 'team_id', 0)
        

        if not fact['invoice_origin']:
            new['module_origin'] = 'Contabilidad'
        elif fact['invoice_origin'][:2] in ['Pd', 'Sh']:
                new['module_origin'] = 'PdV'
        elif fact['invoice_origin'][0] == 'S':
                new['module_origin'] = 'Ventas'


        fact_line_ids.append(line)
        data_fact.append(new)



fact_doc_df = pd.DataFrame(data_fact)


fact_doc_df['invoice_date'] = pd.to_datetime(fact_doc_df['invoice_date'], format='%Y-%m-%d')
fact_doc_df['pos_doc_id'] = fact_doc_df['pos_doc_id'].astype('Int64')
fact_doc_df['reversal_move_id'] = fact_doc_df['reversal_move_id'].astype('Int64')
fact_doc_df['reversed_entry_id'] = fact_doc_df['reversed_entry_id'].astype('Int64')
fact_doc_df.loc[fact_doc_df['invoice_origin'] == False , ['invoice_origin',]] = pd.NA


check_1 =  len(fact_doc_df[fact_doc_df['module_origin'].isna()]) == 0

# Se especifican los campos necesarios `fact_line_fields` para que con `fact_line_ids` (que se generó arriba) se pueda obtener la información del modelo <font color="#F414FA">account.move.line</font>. Esto devuelve una lista de diccionarios con la información de cada línea de factura que se almacena en `fact_line_json`.


fact_line_fields = [
    'product_id',
    'quantity',
    'price_unit',
    'discount',
    'account_id',
    'price_subtotal',
    'sale_line_ids',
    'create_date'
]

fact_line_json = models.execute_kw(api_db, uid, api_clave, 'account.move.line', 'read', [fact_line_ids], {'fields': fact_line_fields})

# Se preparan los datos con un ciclo <font color="#14E4FA">for</font> donde se manipula `fact_line_json` y se genera un diccionario "línea de factura". Este diccionario ya manipulado se ingresa a una lista vacía `data_line_fact`. La lista ya cargada con todos sus elementos es la que se convierte en el DataFrame `fact_line_df`.


data_line_fact = []

for fact_line in fact_line_json:
    if fact_line['account_id'] and fact_line['account_id'][0] in [85, 197]:
        new = {}
        new['fact_line_id'] = fact_line['id']
        new['create_date'] = fact_line['create_date']
        new['product_id'] = fact_line['product_id'][0]
        new['quantity'] = fact_line['quantity']
        new['price_unit'] = fact_line['price_unit']
        new['discount'] = fact_line['discount'] / 100
        new['price_subtotal'] = fact_line['price_subtotal']
        new['sale_line_id_fact'] = if_list_gt0_idex(fact_line, 'sale_line_ids', 0)

        data_line_fact.append(new)


fact_line_df = pd.DataFrame(data_line_fact)


fact_line_df['fact_line_id'] = fact_line_df['fact_line_id'].astype('Int64')
fact_line_df.loc[fact_line_df['product_id'] == False, ['product_id',]] = pd.NA
fact_line_df['product_id'] = fact_line_df['product_id'].astype('Int64')
fact_line_df['sale_line_id_fact'] = fact_line_df['sale_line_id_fact'].astype('Int64')
fact_line_df['create_date'] = pd.to_datetime(fact_line_df['create_date'])

# Con los dos dataframe anteriores, se procede a generar un <font color="#14E4FA">merge</font> en unión derecha, esto debido a que queremos despreciar todo los id de línea de `fact_doc_df` que tienen un id del campo <font color="#14FA4C">"account_id"</font> diferente a 85 y 197 y que solo se pueden filtrar con la información de `fact_line_df`. Ahora, el dataframe resultante `fact_df` tiene un seguimiento desde la factura hasta cada una de sus líneas con toda la información de ellas. 


fact_df = fact_doc_df.merge(fact_line_df, how='right', on='fact_line_id')

# Para llegar al modelo <font color="#F414FA">sale.order</font> desde el modelo <font color="#F414FA">account.move</font> es necesario en ocasiones pasar por el modelo <font color="#F414FA">pos.order</font>. 
# 
# Para obtener los ids de este modelo, es importante asegurar que vienen únicamente de las ids de las facturas que se especificaron en los criterios de búsqueda generales `search_fact`. Para ello, hay que acceder al modelo de factura, a un campo llamado <font color="#14FA4C">"pos_order_ids"</font> que es donde se encuentra el id del documento pos. Es por esto que no se debe obtener los ids del pos usando un filtro diferente.
# 
# Se porcede a hacer un <font color="#14E4FA">for</font> en `fact_doc_ids`, donde para cada factura se accesa al campo <font color="#14FA4C">"pos_order_ids"</font> y se obtiene el id del documento del pos (PdV). Este id es único para cada factura, y se ingresa a la lista `pos_doc_ids`.


pos_doc_ids = []

for fact in fact_doc_json:
    if fact['pos_order_ids']:
        pos_doc_ids.append(fact['pos_order_ids'][0])

# Hay facturas que se hacen duplicando facturas que se cancelan. Esto significa que las nuevas facturas no tienen un pos_doc_id y por lo tanto no se encuentra la información que se desea. Sin embargo, estas facturas duplicadas sí tienen en el campo <font color="#14FA4C">"invoice_origin"</font> el rastro de qué PdV tienen.
# 
# Se procede a filtrar estas facturas y hacer una lista de los nombres de ellas. Se guarda esta lista en `pos_doc_name_extra` y con estos nombres se buscan los ids en el modelo <font color="#F414FA">pos.order</font>. Devuelve esta búsqueda un json `pos_doc_json`.
# 
# Utilizando un ciclo <font color="#14E4FA">for</font>, se utiliza `pos_doc_json` para complementar `pos_doc_ids` con las ids de tipo documento faltantes.


pos_doc_name_extra = list(fact_df.loc[(fact_df['module_origin'] == 'PdV') & (fact_df['pos_doc_id'].isna()), 'invoice_origin'].unique())

search_pos = [
    ("name", "in", pos_doc_name_extra),
]

pos_doc_fields = [
    'name'
]

pos_doc_ids_extra = models.execute_kw(api_db, uid, api_clave, 'pos.order', 'search', [search_pos])
pos_doc_json_extra = models.execute_kw(api_db, uid, api_clave, 'pos.order', 'read', [pos_doc_ids_extra], {'fields': pos_doc_fields})

for pos in pos_doc_json_extra:
    pos_doc_ids.append(pos['id'])

# Además del trabajo anterior, se ocupa complementar en `fact_df` los ids de documento `pos_doc_ids` de las facturas encontradas en `pos_doc_json_extra`. Se logra esto al generar un for de cada item del json y por ".loc" se agrega a `fact_df`.


for item in pos_doc_json_extra:
    fact_df.loc[fact_df['invoice_origin'] == item['name'], 'pos_doc_id'] = item['id']

# `pos_doc_ids` son ids tipo documento y no son las mismas que las ids que ocupa el modelo <font color="#F414FA">pos.order.line</font>. Es por esto que aquí primero se hace una búsqueda de los ids tipo línea que tienen un id tipo documento porporcionado por `pos_doc_ids`. Cabe mencionar que no debe haber criterios de búsqueda que cambien los criterios originales.
# 
# Se especifican los campos necesarios `pos_doc_fields` para utilizarlos en una búsqueda en el modelo <font color="#F414FA">pos.order.line</font>. Esta búsqueda devuelve una lista con todos los ids tipo línea del pos y se almacena en `pos_line_ids`
# 
# No es necesario accesar al modelo del documento del pos <font color="#F414FA">pos.order</font>, pues la información que se necesita está dentro de cada línea del pos.
# 
# Con los ids tipo línea `pos_line_ids` se consulta la información de cada línea del pos en el modelo. Esto devuelve una lista de diccionarios que se almacenana en `pos_line_json`


pos_line_fields = [
    'order_id',
    'sale_order_line_id',
    'refund_orderline_ids',
    'refunded_orderline_id',
]


pos_line_ids = models.execute_kw(api_db, uid, api_clave, 'pos.order.line', 'search', [[("order_id.id", "=", pos_doc_ids)]])
pos_line_json = models.execute_kw(api_db, uid, api_clave, 'pos.order.line', 'read', [pos_line_ids], {'fields': pos_line_fields})

# Se procede a generar un DataFrame de Pandas. Para esto se preparan los datos con un ciclo <font color="#14E4FA">for</font> donde se manipula `pos_line_json` y se ingresa cada línea del pos ya manipulada a una lista vacía `data_pos_line`. La lista ya cargada con todos sus elementos es la que se convierte en el DataFrame `pos_line_df`. Este dataframe tiene únicamente la información de las líneas del pos y el id del documento del pos. No hay un vínculo con `fact_df`


data_pos_line = []

for pos in pos_line_json:
    new = {}
    new['pos_line_id'] = pos['id']
    new['pos_doc_id'] = pos['order_id'][0]
    new['sale_line_id_pos'] = if_list_gt0_idex(pos, 'sale_order_line_id', 0)
    new['refund_orderline_ids'] = if_list_gt0_idex(pos, 'refund_orderline_ids', 0)
    new['refunded_orderline_id'] = if_list_gt0_idex(pos, 'refunded_orderline_id', 0)
    

    data_pos_line.append(new)

dfpos_line = pd.DataFrame(data_pos_line)
dfpos_line['sale_line_id_pos'] = dfpos_line['sale_line_id_pos'].astype('Int64')
dfpos_line['pos_line_id'] = dfpos_line['pos_line_id'].astype('Int64')
dfpos_line['refund_orderline_ids'] = dfpos_line['refund_orderline_ids'].astype('Int64')
dfpos_line['refunded_orderline_id'] = dfpos_line['refunded_orderline_id'].astype('Int64')

# Debido a que `fact_df` contiene el total de las facturas de Odoo, se ocupa segmentar sólo las facturas que provienen del módulo de PdV. Esto con el fin de hacer un <font color="#14E4FA">merge</font> entre `fact_df` y `pos_line_df` de manera más sencilla. El resultado se almacena en `fact_pos_doc_df`.


fact_pos_doc_df = fact_df[~fact_df['pos_doc_id'].isna()][['fact_doc_id','name', 'fact_line_id', 'pos_doc_id', 'product_id']]

# Para aumentar la seguridad del código, es necesario verificar que el total de líneas del modelo <font color="#F414FA">pos.order.line</font> es igual al total de líneas del modelo <font color="#F414FA">account.move.line</font> `check_total_size` y que cada factura tiene el mismo número de líneas que su orden `check_each_document_size`. Esto es necesario para evitar equivocaciones en el <font color="#14E4FA">merge</font> (enmaquetado) de ambos dataframes [`fact_df`, `pos_line_df`], derivado a que no hay un vínculo claro en común.
# 
# Para lograr `check_total_size`:
# - Se agrupan ambos dataframes por el campo propio de id de línea, dando como resultado `fact_pos_doc_df` y `group_dfpos_line`.
# - Se compara el tamaño de ambos grupos y el valor booleano se almacena en `check_total_size`.
# 
# Para lograr `check_each_document_size`:
# - se concatenan `fact_pos_doc_df` y `group_dfpos_line` y se almacena en el dataframe `groups_concat`.
# - se compara el campo id de fac con id del pos y el booleano resultante se almacen una columna nueva de `groups_concat` <font color="#14FA4C">"lines_per_doc"</font>.
# - se filtran los valores <font color="#14FA4C">False</font> de <font color="#14FA4C">"lines_per_doc"</font> y al tamaño del resultado se le compara con cero. El resultado se almacena en `check_each_document_size`


group_fact_pos_df = fact_pos_doc_df.groupby('pos_doc_id').count()['fact_doc_id']
group_pos_line_df = dfpos_line.groupby('pos_doc_id').count()['pos_line_id']

check_total_size = len(group_fact_pos_df) == len(group_pos_line_df)

groups_concat = pd.concat([group_fact_pos_df, group_pos_line_df], axis=1)
groups_concat['lines_per_doc'] = groups_concat['fact_doc_id'] == groups_concat['pos_line_id']
check_each_document_size = len(groups_concat[~groups_concat['lines_per_doc']]) == 0

# Para poder hacer un <font color="#14E4FA">merge</font> entre `fact_df`, `pos_line_df` se busca hacer un id temporal `id_vinculo` que sirva de vínculo entre ambos dataframes.
# 
# Para generar `id_vinculo`, se utiliza el único campo en común para ambos dataframes <font color="#14FA4C">"pos_order_ids"</font> en `fact_df` y <font color="#14FA4C">"pos_doc_id"</font> en `pos_line_df`.
# 
# Se generó la función `id_vinculo_generator`, la cual recibe tres parámetros: el dataframe, el nombre de la columna donde se almacenan los ids de los documentos del pos y el nombre de la columna donde se almacenan los ids de las líneas de su modelo.


def id_vinculo_generator(df_base: pd.DataFrame, pos_doc_column_name: str, df_id_line_name:str ) -> pd.DataFrame:
    
    df = df_base.sort_values(by=[pos_doc_column_name, df_id_line_name])
    df['id_relative_pos'] = None

    pos_orders = df[pos_doc_column_name].unique()

    for pos in pos_orders:
        mini_df = df.loc[df[pos_doc_column_name] == pos]
        df.loc[df[pos_doc_column_name] == pos, 'id_relative_pos'] = [i for i in range(len(mini_df))]


    df['id_vinculo'] = df[pos_doc_column_name].astype(str) + '-' + df['id_relative_pos'].astype(str)

    return df

# Se procede a ejecutar la función anterior para cada dataframe. Se guardan sus resultados respectivos en `fact_pos_link_df` y `pos_line_link_df`


fact_pos_doc_link_df = id_vinculo_generator( fact_pos_doc_df, 'pos_doc_id', 'fact_line_id')
pos_line_link_df = id_vinculo_generator(dfpos_line, 'pos_doc_id', 'pos_line_id')

# Se procede a generar el merge de ambos dataframes de linea, dando como resultado `fac_pos_linked_df`.


fac_pos_linked_df =  fact_pos_doc_link_df.merge(pos_line_link_df, how='outer', on='id_vinculo')

# Se procede a integrar `fac_pos_linked_df` al dataframe general `fact_df` por medio de un <font color="#14E4FA">merge</font> tipo "left join" en su campo de línea de factura <font color="#14FA4C">"id_x"</font>, obteniendo `fact_pos_df`. Al haber vinculado <font color="#F414FA">pos.order.line</font> al dataframe general, cada línea de venta (ya sea que venga del módulo de "Ventas" o del módulo de "PdV") tiene ya un id de línea del modelo <font color="#F414FA">sale.order.line</font>, que es donde se encuentra el nombre de la vendedora.


cols_to_keep = ['fact_line_id', 'pos_line_id', 'sale_line_id_pos', 'refund_orderline_ids', 'refunded_orderline_id']
fact_pos_df = fact_df.merge(fac_pos_linked_df[cols_to_keep], how='left', on='fact_line_id')

# Al haber hecho merge en dos dataframes diferentes en `fact_pos_df`, se tienen los ids del modelo <font color="#F414FA">sale.order.line</font> en diferentes columnas. Se procede a juntar dichos ids en una sóla columna <font color="#14FA4C">"sale_line_join_id"</font>.
# 
# Se genera `ids_sale_line` que es una lista de los ids únicos para poder utilizarlos a posterior.


fact_pos_df['sale_line_id'] = None
fact_pos_df.loc[(~fact_pos_df['sale_line_id_fact'].isna() & fact_pos_df['sale_line_id_pos'].isna()), 'sale_line_id'] = fact_pos_df['sale_line_id_fact']
fact_pos_df.loc[(fact_pos_df['sale_line_id_fact'].isna() & ~fact_pos_df['sale_line_id_pos'].isna()), 'sale_line_id'] = fact_pos_df['sale_line_id_pos']
fact_pos_df['sale_line_id'] = fact_pos_df['sale_line_id'].astype('Int64')

fact_pos_df.drop(columns=['sale_line_id_fact', 'sale_line_id_pos'], inplace=True)

sale_line_ids = []
for sid in fact_pos_df['sale_line_id'].sort_values(ascending=True).unique()[:-1]:
    sale_line_ids.append(int(sid))

# Se especifican los campos necesarios `sale_line_fields` para que con `sale_line_ids` (que se generó arriba) se pueda obtener la información del modelo <font color="#F414FA">sale.order.line</font>. Esto devuelve una lista de diccionarios con la información de cada línea de venta, que se guarda en `sale_line_json`.


sale_line_fields = [
    'salesman_id',
]

sale_line_json = models.execute_kw(api_db, uid, api_clave, 'sale.order.line', 'read', [sale_line_ids], {'fields': sale_line_fields})

# Se procede a generar el DataFrame `sale_line_df`. Para esto se preparan los datos con un ciclo <font color="#14E4FA">for</font> donde se manipula `sale_line_json`.


data_sale_line = []

for sale in sale_line_json:
    new = {}
    new['sale_line_id'] = sale['id']
    new['salesman_id'] = sale['salesman_id'][0]

    data_sale_line.append(new)


sale_line_df = pd.DataFrame(data_sale_line)
sale_line_df['salesman_id'] = sale_line_df['salesman_id'].astype('Int64')

# Se procede a integrar `sale_line_df` al dataframe general `fact_pos_df` por medio de un <font color="#14E4FA">merge</font> tipo "left join" en su campo de línea de sale <font color="#14FA4C">"sale_line_id"</font>, obteniendo `complete_df`.


complete_df = fact_pos_df.merge(sale_line_df, how='left', on='sale_line_id').set_index('name')

# Se filtra las líneas de factura que son de reversiones o notas de crédito/devolución. Estas líneas tienen que cambiar su signo a negativo para que la analítica de sumas y acumulados refleje la realidad en `complete_df`.


complete_df.loc[complete_df['move_type'] == 'out_refund', ['quantity', 'price_subtotal']] = complete_df.loc[complete_df['move_type'] == 'out_refund', ['quantity', 'price_subtotal']] * -1

complete_df.loc[complete_df['module_origin'] == 'Contabilidad', 'salesman_id'] = complete_df.loc[complete_df['module_origin'] == 'Contabilidad', 'invoice_user_id']