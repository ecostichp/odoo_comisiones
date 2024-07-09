# algoritmo_comisiones_Odoo

### Para crear el entorno python de trabajo en el proyecto:  

##### 1. Actualiza PIP global.
```
python -m pip install -U Pip  
```

##### 2. Crea el entorno para el proyecto (lo llamaremos 'env').
```
python -m venv env_comisiones-Odoo
env_comisiones-Odoo\Scripts\activate
```

##### 3. Actualiza PIP en tu entorno e instala las dependencias del proyecto.
```
python -m pip install -U Pip
python -m pip install -U Pandas OpenPyXL iPyKernel SQLAlchemy PyArrow MatPlotLib Seaborn IACele
```