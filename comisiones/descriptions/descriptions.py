import os
from pathlib import Path
import xmlrpc.client
import pandas as pd
from sqlalchemy import create_engine

db_file = 'comisiones.db'
db_file_path_str = str(Path().cwd().parent.parent.joinpath(f'data/{db_file}'))


engine = create_engine(f'sqlite:///{db_file_path_str}')



api_url = os.environ.get('ODOO_URL_API')
api_db = os.environ.get('ODOO_DB_API')
api_username = os.environ.get('ODOO_USERNAME_API')
api_clave = os.environ.get('ODOO_CLAVE_API')


common = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/common')
uid = common.authenticate(api_db, api_username, api_clave, {})
models = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/object')



# Tabla descripciones usuarios:

descrip_users_fields = ['name', 'active', 'x_salesteam_ref']
descrip_users_json = models.execute_kw(api_db, uid, api_clave, 'res.users', 'search_read', [["|", ("active", "=", True), ("active", "=", False)]], {'fields': descrip_users_fields})

descrip_users_data = []

for user in descrip_users_json:
    new = {}
    new['salesperson_id'] = user['id']
    new['salesperson_name'] = user['name']
    new['active'] = user['active']
    new['sale_team_id'] = user['x_salesteam_ref'][0] if user['x_salesteam_ref'] else pd.NA
    new['sale_team_description'] = user['x_salesteam_ref'][1] if user['x_salesteam_ref'] else pd.NA

    descrip_users_data.append(new)


descrip_users_df = pd.DataFrame(descrip_users_data)
descrip_users_df['sale_team_id'] = descrip_users_df['sale_team_id'].astype('Int64')

descrip_users_df.loc[descrip_users_df['sale_team_id'] == 5, ['business_model', 'warehouse']] = ['Piso', 'A1']
descrip_users_df.loc[descrip_users_df['sale_team_id'] == 6, ['business_model', 'warehouse']] = ['Piso', 'A2']
descrip_users_df.loc[descrip_users_df['sale_team_id'] == 7, ['business_model', 'warehouse']] = ['CE', 'A1']
descrip_users_df.loc[descrip_users_df['sale_team_id'] == 8, ['business_model', 'warehouse']] = ['CE', 'A2']

descrip_sales_users_df  = descrip_users_df.loc[~descrip_users_df['sale_team_id'].isna()]




# Tabla descripciones productos:

descrip_product_fields = ['name', 'default_code']
descrip_product_json = models.execute_kw(api_db, uid, api_clave, 'product.template', 'search_read', [], {'fields': descrip_product_fields})
descrip_product_df1 = pd.DataFrame(descrip_product_json)
descrip_product_df1.columns = ['product_id', 'prod_descripción', 'prod_código']


with engine.connect() as conn, conn.begin():  
    lineas_productos_df1 = pd.read_sql_table('lineas_proveedores', conn)
engine.dispose()


lineas_productos_df1.columns = ['prod_código', 'prod_descripción', 'prod_línea']
lineas_productos_df = lineas_productos_df1[['prod_código', 'prod_línea']]


descrip_product_df = descrip_product_df1.merge(lineas_productos_df, how='left', on='prod_código')



# Tabla descripciones clientes:

descrip_partner_fields = ['name']
descrip_partner_json = models.execute_kw(api_db, uid, api_clave, 'res.partner', 'search_read', [], {'fields': descrip_partner_fields})
descrip_partner_df = pd.DataFrame(descrip_partner_json)
descrip_partner_df.columns = ['partner_id', 'partner_name']
