# Proyecto 2: Implementación de un Data Warehouse

Este repositorio contiene toda la documentación y los archivos necesarios para la implementacion de un Data Warehouse

## Índice
- [Requisitos Previos](#requisitos)
- [Instalación de Paquetes de Python](#instalación-de-paquetes-de-python)
- [Ejecución de los Scripts](#ejecución-de-los-scripts)

---

## Requisitos
- Python 3.8 o superior.

---

## Instalación de Paquetes de Python
1. Instalar el paquete de pandas para python utilizando el comando en la terminal: "pip install pandas"
2. Instalar el paquete de sqlalchemy para python utilizando el comando en la terminal: "pip install sqlalchemy"
3. Instalar el paquete de matplotlib para python utilizando el comando en la terminal: "pip install matplotlib"

---

## Ejecución de los Scripts
1. Primeramente sería necesario extraer el archivo csv que contiene los datos utilizados, el cual se encuentra en el archivo comprimido "categorical_data_and_dimensions.zip"
2. Para el proceso ETL del Data Warehouse, se tienen 2 scripts diferentes con los nombres de "pruebaproyecto2.py" y "prueba2proyecto2.py", el primero mencionado fue una versión previa con una pequeña limpieza de los datos, donde se rellenaban los campos nulos para así poder cargar los datos a la base de datos.
El segundo script hace una limpieza más profunda de los datos, donde además de rellenar los valores nulos, se eliminan registros 'outliers', además que se promedian los registros que se consideran duplicados, esto para evitar diferentes valores en las tablas de hechos de registros que contuvieran los mismos id de sus dimensiones.
3. Una vez ejecutado el script anterior, se obtiene un archivo ".db" el cual será consultado en el archivo "consultasAnalisis.py", en el cual se generan diferentes graficos que permiten observar KPIs relevantes o insights interesantes que se pueden obtener a traves de los datos en la Data Warehouse.