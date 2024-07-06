class ListStorer():
    """
    ## Listas en valores de DataFrames
    Clase utilizada para almacenar listas de valores capaces de ser almacenadas
    en DataFrames dentro de un mismo valor.

    Creación de objeto:
    ```py
    values = ListStorer([1, 2, 3, 4, 5])
    print(values)
    # [1, 2, 3, 4, 5]
    print(values[0])
    # 1
    ```
    """
    def __init__(self, values: list[int]):
        self.values = values

    # Comportamiento para mostrar la lista
    def __repr__(self) -> str:
        return str(self.values)
    
    # Comportamiento para acceder a los índices de la lista
    def __getitem__(self, index) -> int:
        return self.values[index]
    
    # Comportamiento para mostrar el tamaño de la lista.
    def __len__(self) -> int:
        return len(self.values)