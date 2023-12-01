import pandas as pd
import cx_Oracle
import time
import re
import tkinter as tk
from tkinter import filedialog

 
print("Ejecutando operaciones relacionadas con Oracle...")
time.sleep(2)
# Configura la ubicación de la biblioteca cliente de Oracle
cx_Oracle.init_oracle_client(lib_dir=r'D:\\proyectos_python\\conexion_oracle\\oracle_py\\ORACLE_LIGTH\\instantclient-basiclite-windows.x64-11.2.0.4.0\\instantclient_11_2')

# Configura la cadena de conexión
dsn_tns = cx_Oracle.makedsn(
    'CONEXION',
    1521,
    service_name='NOMBRE_BD'
)

try:
    # Configura el timeout aquí si es posible (depende de la versión)
    connection = cx_Oracle.connect(
        user='USER',
        password='CONTRA',
        dsn=dsn_tns,
        nencoding='UTF-8',
        events=False,
        threaded=True
        
    )
    time.sleep(2)
    print("Conexión correcta a Oracle")
    

###############3
#cursor = connection.cursor()
#cursor.execute('select * from NOMBRE_TABLA WHERE ROWNUM <= 10')
#resultados_consulta_1 = cursor.fetchall()
#print("Consulta 2 ejecutada con éxito")
#for row in resultados_consulta_1:
#    print(row)
# Obtiene y muestra los resultados
#for row in cursor.fetchall():
#   print(row)

################

# Ventana para seleccionar el archivo Excel
    ventana = tk.Tk()
    ventana.withdraw()  # Oculta la ventana principal

# Diálogo para seleccionar el archivo Excel
    excel_file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx")])

    if not excel_file_path:
        print("No se seleccionó un archivo Excel. Saliendo del programa.")
        exit()

except cx_Oracle.DatabaseError as e:
    # Maneja la excepción de timeout aquí
    if "timeout" in str(e):
        print("Error de timeout al conectar a Oracle volver a ejecutar el programa")
    else:
        print(f"Error de base de datos, puede ser credenciales o caidas, favor de reporat al DBA: {str(e)}")
# Realiza la consulta 1: TRUNCATE TABLE NOMBRE_TABLA
time.sleep(5)

cursor = connection.cursor()
cursor.execute('TRUNCATE TABLE NOMBRE_TABLA')
connection.commit()
print("Consulta 1 ejecutada con éxito, TRUNCATE_TABLE")

time.sleep(5)

# Lee el archivo Excel en un DataFrame de pandas
df = pd.read_excel(excel_file_path)

print("Columnas disponibles en el archivo Excel:")
print(df.columns)

columnas_seleccionadas = input("Ingrese las columnas que desea insertar (separadas por comas): ").split(',')

try:
    cursor2 = connection.cursor()

    for index, row in df[columnas_seleccionadas].iterrows():
        # Asumiendo que 'nombre_tabla' es el nombre de tu tabla en Oracle
        insert_query = f"INSERT INTO NOMBRE_TABLA ({', '.join(columnas_seleccionadas)}) VALUES ({', '.join([':{}'.format(col) for col in columnas_seleccionadas])})"
        cursor2.execute(insert_query, row.to_dict())
    
    # Commit para aplicar los cambios
    connection.commit()
    print("Datos insertados con éxito!!!")

except cx_Oracle.DatabaseError as e:
    print(f"Error al insertar datos, validar su red: {str(e)}")


time.sleep(5)

# Realiza la consulta 2: select t.*,rowid from NOMBRE_TABLA t
cursor.execute('select t.*, rowid from NOMBRE_TABLA t')
resultados_consulta_2 = cursor.fetchall()
print("Consulta 2 ejecutada con éxito, SELECT * FROM NOMBRE_TABLA ")
for row in resultados_consulta_2:
    print(row)
# Obtiene y muestra los resultados
for row in cursor.fetchall():
    print(row)

time.sleep(5)

# Realiza la consulta 3
cursor.execute("""
    SELECT
        "QUERY QUE ME DA LOS ARCHIVOS TIPO LOB
        )
""")

# Obtiene y muestra los resultados   
resultados_consulta_3 = cursor.fetchall()
print(f"Cantidad de registros obtenidos: {len(resultados_consulta_3)}")
# Procesa el archivo de texto
time.sleep(10)


def procesar_archivo(input_file, output_file):
    with open(input_file, 'r') as archivo_entrada, open(output_file, 'w') as archivo_salida:
        contenido = archivo_entrada.read()
        bloques = re.split(r'(?=CAMPO_LOB:)', contenido)

        for bloque in bloques:
            if 'CAMPO_LOB:' in bloque:
                lineas = bloque.split('\n')

                # Filtra las líneas que comienzan con "Fila: (" o "DEDR|"
                lineas = [linea for linea in lineas if not (linea.startswith("Fila: (") or linea.startswith("DEDR|"))]

                for i, linea in enumerate(lineas):
                    if re.match(r'^DE\|\d+\|', linea) and linea.endswith('||||'):
                        datos = linea.split('|')
                        if len(datos) > 9:
                            try:
                                segundo_numero = float(datos[2])
                                nuevo_valor = round(segundo_numero / 1.18, 2)
                                datos[-2] = f'{nuevo_valor:.2f}'
                                datos[-3] = f'{nuevo_valor:.2f}'
                                lineas[i] = '|'.join(datos)
                            except ValueError:
                                print(f"Error al procesar la línea: {linea}")
                            else:
                                # Conserva todos los datos hasta el sexto '|' desde el final
                                datos_hasta_sexo = datos[:-6]
                                # Agrega los resultados al final
                                datos_finales = datos_hasta_sexo + [f'{nuevo_valor:.2f}'] * 2 + ['|||']
                                lineas[i] = '|'.join(datos_finales)

                bloque_modificado = '\n'.join(lineas)
                archivo_salida.write(bloque_modificado + '\n\n')

time.sleep(5)
# Escribe los resultados de la consulta en un archivo de texto
with open("resultado_consulta3.txt", "w") as archivo_resultados:
    archivo_resultados.write("Número de filas en resultados_consulta_3: {}\n".format(len(resultados_consulta_3)))

    for row in resultados_consulta_3:
        archivo_resultados.write("Fila: {}\n".format(row))

        # Accede a los objetos LOB
        envio_xml_enviado = row[5]
        envio_xml_recibido = row[6]

        # Imprime el contenido de los objetos LOB si no son None
        if envio_xml_enviado is not None:
            archivo_resultados.write("CAMPO_LOB:\n")
            archivo_resultados.write(envio_xml_enviado.read())  # No es necesario decode en Python 3

        if envio_xml_recibido is not None:
            archivo_resultados.write("CAMPO_LOB2:\n")
            archivo_resultados.write(envio_xml_recibido.read())    # No es necesario decode en Python 3



        archivo_resultados.write("\n" + "-"*50 + "\n")

print(f"Cantidad de registros procesados: {len(resultados_consulta_3)}")           
time.sleep(10)

# Cierra la conexión
if connection:
    connection.close()
    print("Conexión a la base de datos cerrada")
    
# Procesa el archivo de texto
procesar_archivo("resultado_consulta3.txt", "resultado_final.txt")

