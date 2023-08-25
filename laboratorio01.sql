--Definici?n de tablas
--******************************************************************************

CREATE TABLE COMPANIA(
	idCompania INT PRIMARY KEY,
	nombre VARCHAR2(50), --BUSCAR TIPOS DE DATOS EN PL SQL 
	fechaDeFundacion DATE NULL
);


CREATE TABLE CONDICION(
	idCondicion INT PRIMARY KEY,
	condicion VARCHAR2(50)
);

CREATE TABLE VIDEOJUEGO(
	codVideojuego INT PRIMARY KEY,
	titulo VARCHAR2(50),
	fechaLanzamiento DATE,
	precio FLOAT,
	idCondicion INT
);

CREATE TABLE VIDEOJUEGOXCOMPANIA(
	idCompania INT NOT NULL, --Esta columna no admite nulos
	codVideojuego INT NOT NULL -- Esta columna tampoco admite nulos 
);

CREATE TABLE CLIENTE(
	idCliente INT PRIMARY KEY,
	nombre VARCHAR2(50),
	telefono CHAR(9),
	direccion VARCHAR2(50)
);

CREATE TABLE DEPARTAMENTO(
	idDepartamento INT PRIMARY KEY,
	departamento VARCHAR2(25)
);

CREATE TABLE EMPLEADO(
	idEmpleado INT PRIMARY KEY,
	nombre VARCHAR2(50),
	direccion VARCHAR2(50),
	idDepartamento INT
);

CREATE TABLE FACTURA(
	idFactura INT PRIMARY KEY,
	idEmpleado INT,
	idCliente INT,
	fecha DATE
);

CREATE TABLE FACTURADETALLE(
	idFactura INT NOT NULL,
	codVideojuego INT NOT NULL,
	cantidad FLOAT
);

--Definici?n de llaves for?neas y primarias
--******************************************************************************
--FK Videojuego
ALTER TABLE VIDEOJUEGO ADD CONSTRAINT FK_videojuego_condicion FOREIGN KEY (IdCondicion) REFERENCES CONDICION(idCondicion);
--PK,FK videojuegoxcompania
ALTER TABLE VIDEOJUEGOXCOMPANIA ADD PRIMARY KEY (idCompania ,codVideojuego);
ALTER TABLE VIDEOJUEGOXCOMPANIA ADD CONSTRAINT FK_compania_x FOREIGN KEY (idCompania) REFERENCES COMPANIA(idCompania);
ALTER TABLE VIDEOJUEGOXCOMPANIA ADD CONSTRAINT FK_Videojuego_x FOREIGN KEY (codVideojuego) REFERENCES VIDEOJUEGO(codVideojuego);
--FK empleado
ALTER TABLE EMPLEADO ADD CONSTRAINT FK_empleado_depto FOREIGN KEY (idDepartamento) REFERENCES DEPARTAMENTO(idDepartamento);
--FK factura
ALTER TABLE FACTURA ADD CONSTRAINT FK_factura_empleado FOREIGN KEY (idEmpleado) REFERENCES EMPLEADO(idEmpleado);
ALTER TABLE FACTURA ADD CONSTRAINT FK_factura_cliente FOREIGN KEY (idCliente) REFERENCES CLIENTE(idCliente);
--FK Factura Detalle
ALTER TABLE FACTURADETALLE ADD CONSTRAINT FK_detallefactura_f FOREIGN KEY (idFactura) REFERENCES FACTURA(idFactura);
ALTER TABLE FACTURADETALLE ADD CONSTRAINT FK_detallefactura_l FOREIGN KEY (codVideojuego) REFERENCES VIDEOJUEGO(codVideojuego);

--Banco de datos
--******************************************************************************
--Compania


INSERT INTO COMPANIA VALUES(1,'Nintendo','23-09-1889');
INSERT INTO COMPANIA VALUES(2,'Electronic Arts','27-05-1982');
INSERT INTO COMPANIA VALUES(3,'Ubisoft','28-03-1986');
INSERT INTO COMPANIA VALUES(3, 'Ubisoft', TO_DATE('28-MAR-1983', 'DD-MON-YYYY'));

INSERT INTO COMPANIA VALUES(4,'Activision Blizzard','02-12-1979');
INSERT INTO COMPANIA VALUES(4, 'Activision Blizzard', TO_DATE('02-DEC-1979', 'DD-MON-YYYY'));


INSERT INTO COMPANIA VALUES(5,'Sony Interactive Entertainment','16-11-1993');
INSERT INTO COMPANIA VALUES(5, 'Sony Interactive Entertainment', TO_DATE('16-NOV-1993', 'DD-MON-YYYY'));


INSERT INTO COMPANIA VALUES(6,'Rockstar Games','28-04-1998');
INSERT INTO COMPANIA VALUES(6, 'Rockstar Games', TO_DATE('28-APR-1998', 'DD-MON-YYYY'));


INSERT INTO COMPANIA VALUES(7,'Square Enix','01-04-1986');
INSERT INTO COMPANIA VALUES(7, 'Square Enix', TO_DATE('01-APR-1986', 'DD-MON-YYYY'));

INSERT INTO COMPANIA VALUES(8,'Valve Corporation','01-04-1996');
INSERT INTO COMPANIA VALUES(8, 'Valve Corporation', TO_DATE('01-APR-1996', 'DD-MON-YYYY'));

INSERT INTO COMPANIA VALUES(9,'Epic Games','21-11-1991');
INSERT INTO COMPANIA VALUES(9, 'Epic Games', TO_DATE('21-NOV-1991', 'DD-MON-YYYY'));

INSERT INTO COMPANIA VALUES(10,'Capcom','30-05-1979');
INSERT INTO COMPANIA VALUES(10, 'Capcom', TO_DATE('30-MAY-1979', 'DD-MON-YYYY'));

--CONDICION
INSERT INTO condicion VALUES(1,'Nuevo');
INSERT INTO condicion VALUES(2,'Usado');

-- VIDEOJUEGO
INSERT INTO VIDEOJUEGO VALUES(1,'The Witcher 3: Wild Hunt',TO_DATE('19-MAY-2015', 'DD-MON-YYYY'),39.98,2);
INSERT INTO VIDEOJUEGO VALUES(2,'Red Dead Redemption 2',TO_DATE('26-OCT-2018', 'DD-MON-YYYY'),29.10,2);
INSERT INTO VIDEOJUEGO VALUES(3,'Grand Theft Auto V',TO_DATE('17-AUG-2013', 'DD-MON-YYYY'),90.30,1);
INSERT INTO VIDEOJUEGO VALUES(4,'The Legend of Zelda: Breath of the Wild',TO_DATE('03-MAR-2017', 'DD-MON-YYYY'),63.77,2);
INSERT INTO VIDEOJUEGO VALUES(5,'Dark Souls III',TO_DATE('24-MAR-2016', 'DD-MON-YYYY'),39.13,1);
INSERT INTO VIDEOJUEGO VALUES(6,'Bloodborne',TO_DATE('24-MAR-2015', 'DD-MON-YYYY'),72.81,1);
INSERT INTO VIDEOJUEGO VALUES(7,'The Elder Scrolls V: Skyrim',TO_DATE('11-NOV-2011', 'DD-MON-YYYY'),53.92,1);
INSERT INTO VIDEOJUEGO VALUES(8,'Doom',TO_DATE('13-MAY-2016', 'DD-MON-YYYY'),54.39,1);
INSERT INTO VIDEOJUEGO VALUES(9,'The Last of Us Part II',TO_DATE('19-JUN-2020', 'DD-MON-YYYY'),60.34,2);
INSERT INTO VIDEOJUEGO VALUES(10,'Sekiro: Shadows Die Twice',TO_DATE('22-MAR-2019', 'DD-MON-YYYY'),61.29,1);
INSERT INTO VIDEOJUEGO VALUES(11,'Horizon Zero Dawn',TO_DATE('28-FEB-2017', 'DD-MON-YYYY'),97.82,1);
INSERT INTO VIDEOJUEGO VALUES(12,'God of War (2018)',TO_DATE('20-APR-2018', 'DD-MON-YYYY'),82.05,1);
INSERT INTO VIDEOJUEGO VALUES(13,'Dark Souls Remastered',TO_DATE('24-MAY-2018', 'DD-MON-YYYY'),50.96,2);
INSERT INTO VIDEOJUEGO VALUES(14,'The Outer Worlds',TO_DATE('25-OCT-2019', 'DD-MON-YYYY'),35.86,2);
INSERT INTO VIDEOJUEGO VALUES(15,'BioShock Infinite',TO_DATE('26-MAR-2013', 'DD-MON-YYYY'),63.44,2);
INSERT INTO VIDEOJUEGO VALUES(16,'Hollow Knight',TO_DATE('24-FEB-2017', 'DD-MON-YYYY'),56.09,2);
INSERT INTO VIDEOJUEGO VALUES(17,'Borderlands 2',TO_DATE('18-AUG-2012', 'DD-MON-YYYY'),37.04,2);
INSERT INTO VIDEOJUEGO VALUES(18,'Dark Souls II',TO_DATE('11-MAR-2014', 'DD-MON-YYYY'),39.05,2);
INSERT INTO VIDEOJUEGO VALUES(19,'NieR: Automata',TO_DATE('23-FEB-2017', 'DD-MON-YYYY'),79.80,2);
INSERT INTO VIDEOJUEGO VALUES(20,'Uncharted 4',TO_DATE('10-MAY-2016', 'DD-MON-YYYY'),61.75,1);

--VIDEOJUEGOXCOMPANIA
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(4,5);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(6,8);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(3,5);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(5,3);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(9,7);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(4,12);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(8,6);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(1,9);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(7,20);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(5,20);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(3,14);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(1,16);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(3,3);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(4,14);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(1,10);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(6,14);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(6,12);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(7,9);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(4,13);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(9,20);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(7,7);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(9,9);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(2,3);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(8,10);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(10,20);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(5,9);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(3,8);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(10,19);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(5,17);
INSERT INTO VIDEOJUEGOXCOMPANIA VALUES(6,15);

--CLIENTE
INSERT INTO CLIENTE VALUES(1,'Samuel de Luque Batuecas','7079-7355','Ap #177-8082 Tempus, Road');
INSERT INTO CLIENTE VALUES(2,'Guillermo D?az Ib??ez','7311-4691','Ap #966-954 At, St.');
INSERT INTO CLIENTE VALUES(3,'Alejandro Bravo Ya?ez','7247-8502','705-6765 Ante Ave');
INSERT INTO CLIENTE VALUES(4,'Ibai Llanos Garatea','7725-8954','664-9905 Lacus, Rd.');
INSERT INTO CLIENTE VALUES(5,'Luis Fernando Flores Alvarado','7495-3540','Ap #867-8619 Nulla Rd.');
INSERT INTO CLIENTE VALUES(6,'Germ?n Alejandro Garmendia Aranis','7849-1137','P.O. Box 412, 3982 Tempus Av.');
INSERT INTO CLIENTE VALUES(7,'Luis Arturo Villar Sudek','7075-1329','902-151 Sit Road');
INSERT INTO CLIENTE VALUES(8,'Iv?n Bustillo','7180-1825','Ap #146-7468 Phasellus Street');
INSERT INTO CLIENTE VALUES(9,'Carlos Mauricio G?mez','7898-0668','Ap #581-6013 Mollis Ave');
INSERT INTO CLIENTE VALUES(10,'Rub?n Doblas Gundersen','7175-4045','P.O. Box 913, 757 Lectus Rd.');

--DEPARTAMENTO
INSERT INTO DEPARTAMENTO VALUES(1,'Ventas');
INSERT INTO DEPARTAMENTO VALUES(2,'Administracion');
INSERT INTO DEPARTAMENTO VALUES(3,'Control de inventario');

--EMPLEADO
INSERT INTO EMPLEADO VALUES(1,'Liam Anderson','222-5718 Pellentesque Avenue',1);
INSERT INTO EMPLEADO VALUES(2,'Sophia Carter','3988 Cursus Avenue',1);
INSERT INTO EMPLEADO VALUES(3,'Noah Gray','183-445 Mi. Road',1);
INSERT INTO EMPLEADO VALUES(4,'Olivia Evans','P.O. Box 135, 8791 Et Avenue',1);
INSERT INTO EMPLEADO VALUES(5,'Emma Foster','P.O. Box 573, 6063 Lorem Road',1);
INSERT INTO EMPLEADO VALUES(6,'Liam Garcia','Ap #906-3122 Eu Av.',1);
INSERT INTO EMPLEADO VALUES(7,'Ava Hernandez','9139 Mollis. St.',1);
INSERT INTO EMPLEADO VALUES(8,'Noah Jackson','P.O. Box 952, 274 Felis. St.',1);
INSERT INTO EMPLEADO VALUES(9,'Isabella King','120-4539 Magna Street',1);
INSERT INTO EMPLEADO VALUES(10,'Sophia Lewis','Ap #886-5973 Cras Street',1);
INSERT INTO EMPLEADO VALUES(11,'Oliver Martinez','527-7983 Turpis Road',1);
INSERT INTO EMPLEADO VALUES(12,'Emma Nelson','861-8983 Molestie St.',1);
INSERT INTO EMPLEADO VALUES(13,'Aiden Parker','Ap #240-8663 Tincidunt St.',2);
INSERT INTO EMPLEADO VALUES(14,'Mia Perez','P.O. Box 717, 5892 Natoque Rd.',3);
INSERT INTO EMPLEADO VALUES(15,'Liam Robinson','P.O. Box 846, 9367 Dui. Rd.',1);

--FACTURA
INSERT INTO FACTURA VALUES(1,3,9,TO_DATE('24/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(2,15,9,TO_DATE('25/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(3,4,8,TO_DATE('04/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(4,3,1,TO_DATE('12/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(5,8,6,TO_DATE('29/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(6,6,3,TO_DATE('20/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(7,1,2,TO_DATE('26/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(8,1,6,TO_DATE('16/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(9,12,6,TO_DATE('31/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(10,6,9,TO_DATE('20/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(11,1,4,TO_DATE('04/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(12,7,8,TO_DATE('24/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(13,1,3,TO_DATE('06/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(14,10,1,TO_DATE('20/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(15,15,5,TO_DATE('17/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(16,2,4,TO_DATE('04/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(17,13,2,TO_DATE('24/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(18,12,9,TO_DATE('03/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(19,1,2,TO_DATE('02/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(20,10,8,TO_DATE('01/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(21,6,9,TO_DATE('20/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(22,15,6,TO_DATE('16/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(23,12,5,TO_DATE('02/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(24,5,5,TO_DATE('15/JAN/2019', 'DD/MON/YYYY'));
INSERT INTO FACTURA VALUES(25,3,3,TO_DATE('11/JAN/2019', 'DD/MON/YYYY'));

--DETALLE FACTURA
INSERT INTO FACTURADETALLE VALUES(20,20,1);
INSERT INTO FACTURADETALLE VALUES(3,8,2);
INSERT INTO FACTURADETALLE VALUES(17,6,4);
INSERT INTO FACTURADETALLE VALUES(11,16,2);
INSERT INTO FACTURADETALLE VALUES(10,20,4);
INSERT INTO FACTURADETALLE VALUES(3,16,1);
INSERT INTO FACTURADETALLE VALUES(23,19,5);
INSERT INTO FACTURADETALLE VALUES(11,18,5);
INSERT INTO FACTURADETALLE VALUES(7,15,1);
INSERT INTO FACTURADETALLE VALUES(13,3,1);
INSERT INTO FACTURADETALLE VALUES(20,16,3);
INSERT INTO FACTURADETALLE VALUES(21,6,5);
INSERT INTO FACTURADETALLE VALUES(2,5,5);
INSERT INTO FACTURADETALLE VALUES(24,12,2);
INSERT INTO FACTURADETALLE VALUES(1,4,2);
INSERT INTO FACTURADETALLE VALUES(21,12,3);
INSERT INTO FACTURADETALLE VALUES(18,11,2);
INSERT INTO FACTURADETALLE VALUES(18,6,5);
INSERT INTO FACTURADETALLE VALUES(14,7,5);
INSERT INTO FACTURADETALLE VALUES(22,19,5);
INSERT INTO FACTURADETALLE VALUES(9,5,3);
INSERT INTO FACTURADETALLE VALUES(22,19,4);
INSERT INTO FACTURADETALLE VALUES(8,6,2);
INSERT INTO FACTURADETALLE VALUES(21,19,1);
INSERT INTO FACTURADETALLE VALUES(22,14,4);
INSERT INTO FACTURADETALLE VALUES(11,18,5);
INSERT INTO FACTURADETALLE VALUES(22,15,3);
INSERT INTO FACTURADETALLE VALUES(4,10,3);
INSERT INTO FACTURADETALLE VALUES(1,3,3);
INSERT INTO FACTURADETALLE VALUES(10,3,1);
INSERT INTO FACTURADETALLE VALUES(24,5,5);
INSERT INTO FACTURADETALLE VALUES(16,15,1);
INSERT INTO FACTURADETALLE VALUES(2,10,4);
INSERT INTO FACTURADETALLE VALUES(12,8,3);
INSERT INTO FACTURADETALLE VALUES(22,19,2);
INSERT INTO FACTURADETALLE VALUES(19,16,4);
INSERT INTO FACTURADETALLE VALUES(13,10,5);
INSERT INTO FACTURADETALLE VALUES(5,6,2);
INSERT INTO FACTURADETALLE VALUES(2,2,5);
INSERT INTO FACTURADETALLE VALUES(3,12,3);
INSERT INTO FACTURADETALLE VALUES(13,5,2);
INSERT INTO FACTURADETALLE VALUES(9,18,3);
INSERT INTO FACTURADETALLE VALUES(2,14,1);
INSERT INTO FACTURADETALLE VALUES(3,17,4);
INSERT INTO FACTURADETALLE VALUES(13,7,3);
INSERT INTO FACTURADETALLE VALUES(14,3,2);
INSERT INTO FACTURADETALLE VALUES(4,12,2);
INSERT INTO FACTURADETALLE VALUES(15,9,2);
INSERT INTO FACTURADETALLE VALUES(22,20,3);
INSERT INTO FACTURADETALLE VALUES(22,10,2);
INSERT INTO FACTURADETALLE VALUES(22,16,3);
INSERT INTO FACTURADETALLE VALUES(1,19,5);
INSERT INTO FACTURADETALLE VALUES(5,11,3);
INSERT INTO FACTURADETALLE VALUES(23,8,5);
INSERT INTO FACTURADETALLE VALUES(4,8,4);
INSERT INTO FACTURADETALLE VALUES(7,20,4);
INSERT INTO FACTURADETALLE VALUES(10,15,1);
INSERT INTO FACTURADETALLE VALUES(9,7,3);
INSERT INTO FACTURADETALLE VALUES(20,17,3);
INSERT INTO FACTURADETALLE VALUES(4,8,1);
INSERT INTO FACTURADETALLE VALUES(9,1,4);
INSERT INTO FACTURADETALLE VALUES(20,13,3);
INSERT INTO FACTURADETALLE VALUES(24,16,2);
INSERT INTO FACTURADETALLE VALUES(21,19,4);
INSERT INTO FACTURADETALLE VALUES(23,7,3);
INSERT INTO FACTURADETALLE VALUES(1,5,3);
INSERT INTO FACTURADETALLE VALUES(16,6,1);
INSERT INTO FACTURADETALLE VALUES(3,19,3);
INSERT INTO FACTURADETALLE VALUES(14,11,2);
INSERT INTO FACTURADETALLE VALUES(19,12,2);

commit;


--Ejercicio 01
SELECT COMPANIA.IDCOMPANIA, COMPANIA.NOMBRE, COUNT(FACTURADETALLE.CANTIDAD)AS VENTAS FROM SYS.FACTURA 
JOIN SYS.FACTURADETALLE ON FACTURA.IDFACTURA = FACTURADETALLE.IDFACTURA
JOIN SYS.VIDEOJUEGO ON FACTURADETALLE.CODVIDEOJUEGO = VIDEOJUEGO.CODVIDEOJUEGO
JOIN SYS.VIDEOJUEGOXCOMPANIA ON VIDEOJUEGO.CODVIDEOJUEGO = VIDEOJUEGOXCOMPANIA.CODVIDEOJUEGO
JOIN SYS.COMPANIA ON VIDEOJUEGOXCOMPANIA.IDCOMPANIA = COMPANIA.IDCOMPANIA
WHERE FACTURA.FECHA BETWEEN '01/JAN/2019' AND '15/JAN/2019'
GROUP BY COMPANIA.IDCOMPANIA, COMPANIA.NOMBRE ORDER BY VENTAS DESC, COMPANIA.NOMBRE ASC, COMPANIA.NOMBRE DESC FETCH FIRST 6 ROWS ONLY 
; 


--Ejercicio 02
SELECT FACTURA.IDFACTURA, FACTURA.FECHA, CLIENTE.NOMBRE AS NOMBRE_CLIENTE, VIDEOJUEGO.TITULO AS TITULO_VIDEOJUEGO, VIDEOJUEGO.PRECIO, FACTURADETALLE.CANTIDAD FROM 
SYS.FACTURA JOIN SYS.FACTURADETALLE ON FACTURA.IDFACTURA = FACTURADETALLE.IDFACTURA 
JOIN SYS.VIDEOJUEGO ON VIDEOJUEGO.CODVIDEOJUEGO = FACTURADETALLE.CODVIDEOJUEGO
JOIN SYS.CLIENTE ON CLIENTE.IDCLIENTE = FACTURA.IDCLIENTE
ORDER BY FACTURA.IDFACTURA ASC, VIDEOJUEGO.PRECIO DESC
; 

--Ejercicio 03
SELECT FACTURA.IDFACTURA, FACTURA.FECHA, CLIENTE.NOMBRE AS nombre_cliente, SUM(VIDEOJUEGO.PRECIO * FACTURADETALLE.CANTIDAD) AS TOTAL FROM SYS.FACTURA
JOIN SYS.FACTURADETALLE ON FACTURA.IDFACTURA = FACTURADETALLE.IDFACTURA
JOIN SYS.CLIENTE ON CLIENTE.IDCLIENTE = FACTURA.IDCLIENTE
JOIN SYS.VIDEOJUEGO ON VIDEOJUEGO.CODVIDEOJUEGO = FACTURADETALLE.CODVIDEOJUEGO
GROUP BY FACTURA.IDFACTURA, FACTURA.FECHA, CLIENTE.NOMBRE ORDER BY FACTURA.IDFACTURA ASC
;


--Ejercicio 04
SELECT CLIENTE.IDCLIENTE, CLIENTE.NOMBRE, SUM(FACTURADETALLE.CANTIDAD) AS CANTIDAD FROM SYS.FACTURA
JOIN SYS.CLIENTE ON CLIENTE.IDCLIENTE = FACTURA.IDCLIENTE 
JOIN SYS.FACTURADETALLE ON FACTURA.IDFACTURA = FACTURADETALLE.IDFACTURA 
WHERE FACTURA.FECHA BETWEEN '01/JAN/2019' AND '31/JAN/2019'
GROUP BY CLIENTE.IDCLIENTE, CLIENTE.NOMBRE 
HAVING SUM(FACTURADETALLE.CANTIDAD) >= 10 ORDER BY CANTIDAD DESC FETCH FIRST 3 ROWS ONLY  
;
