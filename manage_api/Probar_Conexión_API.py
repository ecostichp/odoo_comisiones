
import xmlrpc.client
import os


api_url = os.environ.get('ODOO_URL_API')
api_db = os.environ.get('ODOO_DB_API')
api_username = os.environ.get('ODOO_USERNAME_API')
api_clave = os.environ.get('ODOO_CLAVE_API')
# Verificar que la conexión es correcta

common = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/common')
common.version()
# Función para autenticar al usuario_API

uid = common.authenticate(api_db, api_username, api_clave, {})
uid
# Función para invocar el objeto models

models = xmlrpc.client.ServerProxy(f'{api_url}/xmlrpc/2/object')
# Función para checar si el usuario tiene permisos de acceso al modelo en cuestión