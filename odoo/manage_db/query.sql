-- SQLite
-- SELECT * FROM dfOriginal WHERE ID = '14894.0'

-- CREATE TABLE IF NOT EXISTS ven (
-- 	id_ven INTEGER NOT NULL PRIMARY KEY,
-- 	ID INTEGER,
-- 	Creado_el TEXT,
--     Estado_de_la_orden TEXT,
--     Almacen TEXT,
--     Cliente TEXT,
--     Vendedor TEXT,
--     Referencia_de_la_orden TEXT,
--     Cantidad REAL,
--     Cantidad_de_entrega REAL,
--     Cantidad_facturada REAL,
--     Producto_Referencia_interna TEXT,
--     Producto TEXT,
--     Cost REAL,
--     Descuento_percentage REAL,
--     Subtotal REAL,
--     Margen REAL,
--     Margen_percentage REAL,
--     Total_de_impuestos REAL,
--     Total REAL,
--     "Líneas de orden que se trasladaron al punto de venta" TEXT,
--     "Líneas de orden que se trasladaron al punto de venta/Número de línea" TEXT,
--     "Líneas de orden que se trasladaron al punto de venta/Orden de ventas vinculada" TEXT
-- );

INSERT INTO TABLE ven (
	ID,
	Creado_el,
    Estado_de_la_orden,
    Almacen,
    Cliente,
    Vendedor,
    Referencia_de_la_orden,
    Cantidad,
    Cantidad_de_entrega,
    Cantidad_facturada,
    Producto_Referencia_interna,
    Producto,
    Cost,
    Descuento_percentage,
    Subtotal,
    Margen,
    Margen_percentage,
    Total_de_impuestos,
    Total,
    "Líneas de orden que se trasladaron al punto de venta",
    "Líneas de orden que se trasladaron al punto de venta/Número de línea",
    "Líneas de orden que se trasladaron al punto de venta/Orden de ventas vinculada"
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);

DROP TABLE ven;