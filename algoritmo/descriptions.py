import os
import xmlrpc.client
import pandas as pd


api_url = os.environ.get('ODOO_URL_API')
api_db = os.environ.get('ODOO_DB_API')
api_username = os.environ.get('ODOO_USERNAME_API')
api_clave = os.environ.get('ODOO_CLAVE_API')


common = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/common')
uid = common.authenticate(api_db, api_username, api_clave, {})
models = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/object')


def if_list_gt0_idex (item: dict, key: str, index:int ) -> None | int:
        val = item[key]

        if val:
            if len(val) == 0:
                return None
            else:
                return val[index]
        else:
            return None



descrip_uripsers_fields = ['name', 'state', 'sale_team_id']
descrip_users_json = models.execute_kw(api_db, uid, api_clave, 'res.users', 'search_read', [], {'fields': descrip_uripsers_fields})

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




descrip_product_fields = ['name', 'default_code']
descrip_product_json = models.execute_kw(api_db, uid, api_clave, 'product.template', 'search_read', [], {'fields': descrip_product_fields})
descrip_product_df = pd.DataFrame(descrip_product_json)
descrip_product_df.columns = ['id_producto', 'descripción_producto', 'código_producto']
