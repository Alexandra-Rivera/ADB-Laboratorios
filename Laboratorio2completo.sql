--Ejercicio 3
CREATE OR REPLACE PROCEDURE info_compania(cod_compania INT)
IS
    CURSOR info_compania IS SELECT * FROM COMPANIA1
                            WHERE idCompania = cod_compania;
    
    row_compania info_compania%rowtype;
    my_json_array json_array_t;
    compania_object json_object_t;
    videojuego_object json_object_t;
    
    CURSOR info_videojuegos IS SELECT V.codvideojuego, V.titulo, V.fechalanzamiento, V.precio, V.idcondicion
                                FROM COMPANIA1 C
                                INNER JOIN VIDEOJUEGOXCOMPANIA1 VC
                                    ON C.idcompania = VC.idcompania
                                INNER JOIN VIDEOJUEGO1 V
                                    ON V.codvideojuego = VC.codvideojuego
                                WHERE C.idcompania = cod_compania;
                                
    row_videojuego info_videojuegos%rowtype;
BEGIN
    my_json_array := new json_array_t;
    
    OPEN info_compania;
        FETCH info_compania INTO row_compania;
        
        compania_object := new json_object_t();
        compania_object.put ('idcompania', row_compania.idcompania);
        compania_object.put ('nombre', row_compania.nombre);
        compania_object.put ('fechadefundacion', row_compania.fechadefundacion);
        CLOSE info_compania;
        
        OPEN info_videojuegos;
        LOOP
            FETCH info_videojuegos INTO row_videojuego;
            EXIT WHEN info_videojuegos%NOTFOUND;
            
            videojuego_object := new json_object_t();
            videojuego_object.put ('codvideojuego', row_videojuego.codvideojuego);
            videojuego_object.put ('titulo', row_videojuego.titulo);
            videojuego_object.put ('fechalanzamiento', row_videojuego.fechalanzamiento);
            videojuego_object.put ('precio', row_videojuego.precio);
            videojuego_object.put ('idcondicion', row_videojuego.idCondicion);
            
            my_json_array.append(videojuego_object);
            END LOOP;
            
            CLOSE info_videojuegos;
            compania_object.put ('Coleccion', my_json_array);
            
            DBMS_OUTPUT.PUT_LINE(compania_object.to_string);
END;

SET SERVEROUTPUT ON;
EXEC info_compania(3);

-- Ejercicio 1

SELECT F.idFactura, C.idCliente, C.nombre, F.fecha, SUM(FD.cantidad) AS VIDEOJUEGOS_COMPRADOS,
                                SUM(FD.cantidad*V.precio) AS TOTAL
                                FROM CLIENTE1 C
                                INNER JOIN FACTURA1 F
                                    ON C.idCliente = F.idcliente
                                INNER JOIN EMPLEADO1 E
                                    ON E.idEmpleado = F.idEmpleado
                                INNER JOIN FACTURADETALLE1 FD
                                    ON F.idFactura = FD.idFactura
                                INNER JOIN VIDEOJUEGO1 V
                                    ON V.codvideojuego = FD.codvideojuego
                                GROUP BY F.idFactura, C.idCliente, C.nombre, F.fecha
                                ORDER BY F.idFactura;

CREATE OR REPLACE TYPE type_row_compras AS OBJECT(
    IDFACTURA INT,
    IDCLIENTE INT,
    NOMBRE VARCHAR(64),
    FECHA DATE,
    VIDEOJUEGOS_COMPRADOR INT,
    TOTAL FLOAT,
    TOTAL_DESCUENTO FLOAT
);

DROP TYPE type_sale_collection;
DROP TYPE type_row_compras;

CREATE OR REPLACE TYPE type_sale_collection AS TABLE OF type_row_compras;

CREATE OR REPLACE FUNCTION info_compras(venta_objetivo INT, descuento FLOAT)
RETURN type_sale_collection PIPELINED
AS
    CURSOR cursor_videojuego IS SELECT F.idFactura, C.idCliente, C.nombre, F.fecha, SUM(FD.cantidad) AS VIDEOJUEGOS_COMPRADOS,
                                SUM(FD.cantidad*V.precio) AS TOTAL, SUM(FD.cantidad*V.precio) AS TOTAL_DESCUENTO
                                FROM CLIENTE1 C
                                INNER JOIN FACTURA1 F
                                    ON C.idCliente = F.idcliente
                                INNER JOIN EMPLEADO1 E
                                    ON E.idEmpleado = F.idEmpleado
                                INNER JOIN FACTURADETALLE1 FD
                                    ON F.idFactura = FD.idFactura
                                INNER JOIN VIDEOJUEGO1 V
                                    ON V.codvideojuego = FD.codvideojuego
                                GROUP BY F.idFactura, C.idCliente, C.nombre, F.fecha
                                ORDER BY F.idFactura;
    cantidad INT;
    descuento_aplicado FLOAT;
    row_cursor_videojuego cursor_videojuego%ROWTYPE;
    
BEGIN
    OPEN cursor_videojuego;
    LOOP
        FETCH cursor_videojuego INTO row_cursor_videojuego;
        EXIT WHEN cursor_videojuego%NOTFOUND;
        
        IF (row_cursor_videojuego.VIDEOJUEGOS_COMPRADOS > venta_objetivo) THEN
            descuento_aplicado := descuento;
        ELSE
            descuento_aplicado := 0.0;
        END IF;
        
        PIPE ROW(type_row_compras(row_cursor_videojuego.idFactura, row_cursor_videojuego.idCliente, row_cursor_videojuego.nombre,
                                row_cursor_videojuego.Fecha, row_cursor_videojuego.Videojuegos_comprados, row_cursor_videojuego.TOTAL
                                , (row_cursor_videojuego.TOTAL_DESCUENTO)*descuento_aplicado));
    END LOOP;
    CLOSE cursor_videojuego;
END;

SELECT * FROM TABLE (info_compras(5, 0.1)) ORDER BY idFactura ASC

SELECT * FROM CLIENTE1