import sqlite3
from pathlib import Path

# Se define la ruta para la base de datos local SQLite
project_name = Path.cwd().name
data_projetc_path = Path.home().joinpath(f'Dropbox/La Casa Del Carpintero/Departamento de Programación/data_projects_git/data_{project_name}')
db_file = 'comisiones.db'
db_file_path_str = str(data_projetc_path.joinpath(db_file))


def connect_to_test_db():
    
    print('\nTratando de conectarse a SQLite, de manera local...')
    conn = None
    
    try:
        # connect to the SQLite local
        conn = sqlite3.connect(db_file_path_str)
    
        # create a cursor
        cur = conn.cursor()
        
	    # execute a statement
        print('>>> ¡Conexión exitosa! La versión de la base de datos de SQLite es:')
        
        cur.execute('SELECT sqlite_version()')

        # display the SQLite database server version
        db_version = cur.fetchall()

        print(db_version)
       
	    # close the communication with the PostgreSQL
        cur.close()

    except (sqlite3.Error) as error:
        print('\nHubo un error...')
        print(error)
    
    finally:
        if conn is not None:
            conn.close()
            print('Se cerró la conexión a la base de datos.\n')


if __name__ == '__main__':
    connect_to_test_db()