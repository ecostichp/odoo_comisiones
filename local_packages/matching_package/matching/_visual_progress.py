from IPython.display import update_display, display

class ProgressStats():
    """
    ## Visualización de estadísticas en tiempo real
    Esta clase provee un objeto el cual muestra las estadísticas definidas
    en la creación de la instancia misma (Provistas en una lista de cadenas
    de texto) mientras se realiza una ejecución de duración prolongada.

    ### Definición
    ```py
    stats = ProgressStats(
        [
            "Exitoso",
            "No encontrado",
            "Fallido"
        ]
    )
    ```

    ### Actualización de las estadísticas:
    ```py
    stats.increase("Exitoso")

    # -- Impresión en el notebook --
    # Exitoso: 1 (100.00%)
    # No encontrado: 0 (0.00%)
    # Fallido: 0 (0.00%)
    # total de registros: 1
    ```
    """

    # Inicialización de la suma total de registros
    _total_entries = 0

    def __init__(self, keys: list[str]):
        # Creación del diccionario usando los valores de la lista entrante como llaves
        self._stats = {key: 0 for key in keys}
        # Se muestra el primer estado de las estadísticas y se guarda la ID del display
        self._dsp_id = display(self, display_id= True).display_id
    
    def increase(self, key) -> None:
        """
        ## Incrementar el conteo en alguno de los individuos de las estadísticas
        Este método permite incrementar el conteo en alguno de los individuos
        provistos en la creación de la instancia misma.

        ```py
        stats.increase("Exitoso")

        # -- Impresión en el notebook --
        # Exitoso: 1 (100.00%)
        # No encontrado: 0 (0.00%)
        # Fallido: 0 (0.00%)
        # total de registros: 1
        ```
        """

        # Se realiza una encapsulación de posible error de llave
        try:
            # Se incrementa en 1 el conteo de la llave especificada en el argumento
            self._stats[key] += 1
            # Se incrementa en 1 el conteo total
            self._total_entries += 1
            # Se actualiza el display con la nueva información
            update_display(self, display_id= self._dsp_id)

        # Encapsulación de error de llave
        except KeyError:
            print("La llave provista no existe")
            print("Accede al atributo `stats.keys` para consultar las llaves disponibles")

    # Configuración del comportamiento por defecto para mostrar la instancia
    def __repr__(self) -> str:

        # Inicialización de la cadena de texto
        init_repr = ""

        # Iteración por cada uno de los inidividuos
        for key in self._stats.keys():
            # Se evita la división entre cero
            if not self._total_entries == 0:
                # Cálculo de la representación porcentual del individuo con respecto al conteo total
                percentage = round(self._stats[key]/self._total_entries*100, 3)
            # Se establece el porcentaje en 0 si aún no hay conteo inicial
            else:
                percentage = 0

            # Se convierte a cadena de texto la representación del individuo actual en la iteración
            init_repr += f"{key}: {self._stats[key]} ({percentage:.3f}%)\n"

        # Se añade al final el total de conteos
        init_repr += f"Total de registros: {self._total_entries}"

        # Retorno de la cadena de texto para su impresión
        return init_repr
    
    @property
    def keys(self) -> list:
        """
        ## Individuos existentes en la instancia
        Este atributo contiene los nombres de los individuos a llamar en el
        método `increase` de la instancia.
        """

        # Se retona una lista con las llaves disponibles
        return [key for key in self._stats.keys()]